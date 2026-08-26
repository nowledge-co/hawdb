#[cfg(test)]
use crate::api::{
    KnowledgeEntityDeleteBatchOutput, KnowledgeEntityDeleteBatchRequest,
    KnowledgeMemoryEvolvesCreateBatchOutput, KnowledgeMemoryEvolvesCreateBatchRequest,
    KnowledgeMemoryLifecycleBatchOutput, KnowledgeMemoryLifecycleBatchRequest,
};
use crate::search::{
    AdaptiveVectorSearchOptions, CompressedVectorSearchMode, SearchCandidateSetReport,
    SearchFallbackReasonCode, SearchFusionWeights, SearchLexicalProductionQualificationReport,
    SearchMode, SearchOutOfCoreConfig, SearchOutOfCoreHydrationOutput, SearchOutOfCoreMetrics,
    SearchOutOfCoreReader, SearchQueryOptions, SearchRangeReadConfig,
    VectorRecallProductionQualificationReport, VectorRecallValidationOptions,
    VectorRecallValidationReport, NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
    VECTOR_RECALL_VALIDATION_PROTOCOL,
};
use crate::search_projection_evidence::{
    nowledge_search_projection_evidence_json, nowledge_search_projection_shadow_evidence_json,
    NowledgeSearchProjectionEvidenceReport,
};
use crate::{
    cypher, BackgroundMaintenanceKind, BackgroundMaintenanceOptions, BackgroundMaintenanceSummary,
    BackgroundWorkHint, BackgroundWorkPlan, BoundedReadQueryOutput, Database, DatabaseConfig,
    DatabaseReadTransaction, GraphRagGeneratedQuery, GraphRagSchemaContext,
    GraphRagSchemaContextOptions, KnowledgeRetrievalOutput, KnowledgeRetrievalRequest,
    LocalQosPolicy, LocalQosScheduler, LocalQosState, NowledgeGraphStatement, PlanCacheLookup,
    QueryOutput, QueryStreamOptions, QueryStreamReport, ReadExecutionProfile, Result,
    ScheduledSearchProjectionCatchUpReport, SearchDocument, SearchIndex,
    SearchProjectionCatchUpReport, SearchProjectionChangeBatch,
    SearchProjectionChangefeedReadiness, SearchProjectionChangefeedStatus, SearchProjectionDelta,
    SearchProjectionDeltaReport, SearchProjectionFreshness, SearchProjectionGraphDeltaRequest,
    SearchProjectionMutationId, SearchProjectionProbeOptions, SearchProjectionRelationalDelta,
    SearchResultSet, SkeinError, SkeinLightningBootstrapManifest,
    SkeinLightningInitialImportApplyReport, SkeinLightningInitialImportCheckpoint,
    SkeinLightningInitialImportCutoverCatchUpReport, SkeinLightningInitialImportDocumentIdentity,
    SkeinLightningInitialImportRecoveryReadinessReport, SlowQueryLogRecordSummary,
    StorageResourceProfileLimits, StorageResourceProfileReport, TelemetrySink, Value,
    STORAGE_RESOURCE_PROFILE_PROTOCOL,
};
use crate::{
    graph_route_readiness::NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
    nowledge_inventory::{
        background_maintenance_evidence_health, background_maintenance_summary_to_json,
        replacement_readiness_family_evidence_health, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
    },
    route_ownership::NowledgeMemRouteOwnershipReadinessReport,
    search_route_ownership::{
        NowledgeMemActiveSearchRouteOwnershipReadinessReport,
        NowledgeMemActiveSearchRouteReadinessReport,
        NowledgeMemSearchRouteOwnershipReadinessReport,
        NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL,
        NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL, REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES,
        REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES,
    },
    store::{
        RecoveryMode, ScanPruningReport, ScanPruningStrategy, StorageOpenTimings,
        StorageRecoveryReport,
    },
    workload_fixtures::{
        NowledgeGraphRouteWorkloadFixtureReport, NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
    },
};
use skein_core::RuntimeTaskContext;
use skein_optimizer::AdaptiveVectorBackendPolicy;
use skein_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeGovernorSnapshot,
    RuntimePermit, RuntimeWorkKind, RuntimeWorkPriority, RuntimeWorkRequest, StorageDeviceProfile,
    WorkPriority,
};
pub use skein_readiness::{NowledgeMemReadinessAreaMap, NowledgeMemReadinessAreaSummary};
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;

#[cfg(test)]
#[path = "nowledge_mem_app_read_snapshot_tests.rs"]
mod app_read_snapshot_tests;
mod serving_path;
pub use serving_path::{
    NowledgeMemServingEntrypoint, NowledgeMemServingPathReadiness,
    NOWLEDGE_MEM_SERVING_PATH_READINESS_PROTOCOL,
};

const TYPED_CONTROL_STATEMENT_MEMORY_BYTES: u64 = 1024 * 1024;
const SEARCH_PROJECTION_CHANGEFEED_OPERATION_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemGraphMode {
    ShadowReadOnly,
    WritableCutover,
}

pub fn nowledge_mem_graph_config(mode: NowledgeMemGraphMode) -> DatabaseConfig {
    nowledge_mem_graph_config_with_search_mode(mode, CompressedVectorSearchMode::Disabled)
}

pub fn nowledge_mem_graph_config_with_search_mode(
    mode: NowledgeMemGraphMode,
    compressed_vector_search_mode: CompressedVectorSearchMode,
) -> DatabaseConfig {
    DatabaseConfig {
        read_only: matches!(mode, NowledgeMemGraphMode::ShadowReadOnly),
        compressed_vector_search_mode,
        ..DatabaseConfig::default()
    }
}

fn default_nowledge_mem_runtime_governor(
    path: &Path,
    database_config: &DatabaseConfig,
) -> RuntimeGovernor {
    let storage_device = StorageDeviceProfile::detect(path);
    let mut governor_config = RuntimeGovernorConfig::desktop_bound();
    if let Some(max_payload_bytes) = database_config.max_read_result_payload_bytes {
        governor_config.result_budget_bytes = u64::try_from(max_payload_bytes).unwrap_or(u64::MAX);
    }
    RuntimeGovernor::detect(
        governor_config,
        IoConcurrencyBudget::desktop_bound_for_device(storage_device),
    )
}

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

fn advised_compressed_vector_search_mode(
    requested: CompressedVectorSearchMode,
    advisor: &NowledgeMemRetrievalProjectionAdvisor,
) -> CompressedVectorSearchMode {
    if requested == CompressedVectorSearchMode::Disabled || advisor.ready() {
        requested
    } else {
        CompressedVectorSearchMode::Disabled
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemOpenOptions {
    pub graph_path: PathBuf,
    pub search_projection_path: Option<PathBuf>,
    pub search_projection_open_mode: NowledgeMemSearchProjectionOpenMode,
    pub mode: NowledgeMemGraphMode,
    pub database_config: Option<DatabaseConfig>,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub adaptive_vector_backend_policy: AdaptiveVectorBackendPolicy,
    pub retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor,
    pub search_range_read_config: Option<SearchRangeReadConfig>,
    pub system_schema_registries: Vec<crate::SystemSchemaRegistry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQualifiedOutOfCoreSearchOptions {
    pub config: SearchOutOfCoreConfig,
    pub qualification: SearchLexicalProductionQualificationReport,
    pub expected_identity: crate::ProductionQualificationIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NowledgeMemSearchProjectionOpenMode {
    FullResidencyMaintenance,
    QualifiedOutOfCore(Box<NowledgeMemQualifiedOutOfCoreSearchOptions>),
}

impl NowledgeMemSearchProjectionOpenMode {
    fn role(&self) -> NowledgeMemSearchProjectionRole {
        match self {
            Self::FullResidencyMaintenance => {
                NowledgeMemSearchProjectionRole::FullResidencyMaintenance
            }
            Self::QualifiedOutOfCore(_) => NowledgeMemSearchProjectionRole::QualifiedOutOfCore,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemSearchProjectionRole {
    FullResidencyMaintenance,
    QualifiedOutOfCore,
}

impl NowledgeMemSearchProjectionRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullResidencyMaintenance => "full_residency_maintenance",
            Self::QualifiedOutOfCore => "qualified_out_of_core",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NowledgeMemOpenDiagnosticOptions {
    pub include_local_paths: bool,
}

impl NowledgeMemOpenOptions {
    pub fn graph_only(graph_path: impl Into<PathBuf>, mode: NowledgeMemGraphMode) -> Self {
        Self {
            graph_path: graph_path.into(),
            search_projection_path: None,
            search_projection_open_mode:
                NowledgeMemSearchProjectionOpenMode::FullResidencyMaintenance,
            mode,
            database_config: None,
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            adaptive_vector_backend_policy: AdaptiveVectorBackendPolicy::default(),
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
            search_range_read_config: None,
            system_schema_registries: Vec::new(),
        }
    }

    pub fn with_search_projection(
        graph_path: impl Into<PathBuf>,
        search_projection_path: impl Into<PathBuf>,
        mode: NowledgeMemGraphMode,
    ) -> Self {
        Self {
            graph_path: graph_path.into(),
            search_projection_path: Some(search_projection_path.into()),
            search_projection_open_mode:
                NowledgeMemSearchProjectionOpenMode::FullResidencyMaintenance,
            mode,
            database_config: None,
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            adaptive_vector_backend_policy: AdaptiveVectorBackendPolicy::default(),
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
            search_range_read_config: None,
            system_schema_registries: Vec::new(),
        }
    }

    pub fn with_qualified_out_of_core_search_projection(
        graph_path: impl Into<PathBuf>,
        search_projection_path: impl Into<PathBuf>,
        mode: NowledgeMemGraphMode,
        qualified: NowledgeMemQualifiedOutOfCoreSearchOptions,
    ) -> Self {
        Self {
            graph_path: graph_path.into(),
            search_projection_path: Some(search_projection_path.into()),
            search_projection_open_mode: NowledgeMemSearchProjectionOpenMode::QualifiedOutOfCore(
                Box::new(qualified),
            ),
            mode,
            database_config: None,
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            adaptive_vector_backend_policy: AdaptiveVectorBackendPolicy::default(),
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
            search_range_read_config: None,
            system_schema_registries: Vec::new(),
        }
    }

    pub fn with_compressed_vector_search_mode(mut self, mode: CompressedVectorSearchMode) -> Self {
        self.compressed_vector_search_mode = mode;
        self
    }

    /// Overrides resource and storage policy while retaining facade-owned mode
    /// and vector-backend settings.
    pub fn with_database_config(mut self, config: DatabaseConfig) -> Self {
        self.database_config = Some(config);
        self
    }

    /// Registers application-owned system schemas that must be current before
    /// the embedded store is returned to its host.
    pub fn with_system_schema_registry(mut self, registry: crate::SystemSchemaRegistry) -> Self {
        self.system_schema_registries.push(registry);
        self
    }

    pub fn with_adaptive_vector_backend_policy(
        mut self,
        policy: AdaptiveVectorBackendPolicy,
    ) -> Self {
        self.adaptive_vector_backend_policy = policy;
        self
    }

    pub fn with_retrieval_projection_advisor(
        mut self,
        advisor: NowledgeMemRetrievalProjectionAdvisor,
    ) -> Self {
        self.retrieval_projection_advisor = advisor;
        self
    }

    pub fn with_search_range_read_config(mut self, config: SearchRangeReadConfig) -> Self {
        self.search_range_read_config = Some(config);
        self
    }

    fn effective_compressed_vector_search_mode(&self) -> CompressedVectorSearchMode {
        advised_compressed_vector_search_mode(
            self.compressed_vector_search_mode,
            &self.retrieval_projection_advisor,
        )
    }

    fn effective_database_config(&self) -> DatabaseConfig {
        let mut config = self.database_config.clone().unwrap_or_default();
        config.read_only = matches!(self.mode, NowledgeMemGraphMode::ShadowReadOnly);
        config.compressed_vector_search_mode = self.effective_compressed_vector_search_mode();
        config.adaptive_vector_backend_policy = self.adaptive_vector_backend_policy;
        config
    }

    fn validate(&self) -> Result<()> {
        match (
            self.search_projection_path.as_ref(),
            &self.search_projection_open_mode,
        ) {
            (None, NowledgeMemSearchProjectionOpenMode::QualifiedOutOfCore(_)) => {
                return Err(SkeinError::Semantic(
                    "qualified out-of-core search requires search_projection_path".to_string(),
                ));
            }
            (Some(_), NowledgeMemSearchProjectionOpenMode::QualifiedOutOfCore(qualified)) => {
                qualified.expected_identity.validate()?;
                if self.search_range_read_config.is_some() {
                    return Err(SkeinError::Semantic(
                        "search_range_read_config applies only to the full-residency maintenance projection"
                            .to_string(),
                    ));
                }
            }
            _ => {}
        }
        if self.search_projection_path.is_none() && self.search_range_read_config.is_some() {
            return Err(SkeinError::Semantic(
                "search_range_read_config requires search_projection_path".to_string(),
            ));
        }
        let mut schema_owners = BTreeSet::new();
        for registry in &self.system_schema_registries {
            if !schema_owners.insert(registry.owner()) {
                return Err(SkeinError::Semantic(format!(
                    "system schema owner {} is registered more than once",
                    registry.owner()
                )));
            }
        }
        Ok(())
    }

    pub fn sanitized_report(&self) -> NowledgeMemOpenReport {
        let effective_compressed_vector_search_mode =
            self.effective_compressed_vector_search_mode();
        NowledgeMemOpenReport {
            protocol: NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL.to_string(),
            mode: self.mode,
            graph_configured: true,
            search_projection_configured: self.search_projection_path.is_some(),
            search_projection_role: self
                .search_projection_path
                .as_ref()
                .map(|_| self.search_projection_open_mode.role()),
            compressed_vector_search_mode: effective_compressed_vector_search_mode,
            requested_compressed_vector_search_mode: self.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.adaptive_vector_backend_policy,
            retrieval_projection_advisor: self.retrieval_projection_advisor.clone(),
            retrieval_projection_advisor_blocker_codes: if self.compressed_vector_search_mode
                == effective_compressed_vector_search_mode
            {
                Vec::new()
            } else {
                self.retrieval_projection_advisor.blocker_codes()
            },
            graph_opened: false,
            search_projection_opened: false,
            search_production_qualification_bound: false,
            system_schema_upgrades: Vec::new(),
        }
    }

    pub fn diagnostic_report_json(
        &self,
        options: NowledgeMemOpenDiagnosticOptions,
    ) -> serde_json::Value {
        let mut report = self.sanitized_report().json();
        if let Some(object) = report.as_object_mut() {
            object.insert(
                "debug_local_paths_included".to_string(),
                serde_json::Value::Bool(options.include_local_paths),
            );
            object.insert(
                "local_paths_redacted".to_string(),
                serde_json::Value::Bool(!options.include_local_paths),
            );
            if options.include_local_paths {
                object.insert(
                    "graph_path".to_string(),
                    serde_json::Value::String(self.graph_path.to_string_lossy().into_owned()),
                );
                object.insert(
                    "search_projection_path".to_string(),
                    self.search_projection_path
                        .as_ref()
                        .map(|path| serde_json::Value::String(path.to_string_lossy().into_owned()))
                        .unwrap_or(serde_json::Value::Null),
                );
            }
        }
        report
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemOpenReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub graph_configured: bool,
    pub search_projection_configured: bool,
    pub search_projection_role: Option<NowledgeMemSearchProjectionRole>,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub requested_compressed_vector_search_mode: CompressedVectorSearchMode,
    pub adaptive_vector_backend_policy: AdaptiveVectorBackendPolicy,
    pub retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor,
    pub retrieval_projection_advisor_blocker_codes: Vec<String>,
    pub graph_opened: bool,
    pub search_projection_opened: bool,
    pub search_production_qualification_bound: bool,
    pub system_schema_upgrades: Vec<crate::SystemSchemaUpgradeReport>,
}

impl NowledgeMemOpenReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "graph_configured": self.graph_configured,
            "search_projection_configured": self.search_projection_configured,
            "search_projection_role": self.search_projection_role.map(NowledgeMemSearchProjectionRole::as_str),
            "compressed_vector_search_mode": self.compressed_vector_search_mode.as_str(),
            "requested_compressed_vector_search_mode": self.requested_compressed_vector_search_mode.as_str(),
            "adaptive_vector_backend_policy": {
                "flat_scan_max_documents": self.adaptive_vector_backend_policy.flat_scan_max_documents,
                "high_filter_selectivity_per_million": self.adaptive_vector_backend_policy.high_filter_selectivity_per_million,
                "flat_scan_memory_budget_bytes": self.adaptive_vector_backend_policy.flat_scan_memory_budget_bytes,
            },
            "retrieval_projection_advisor": self.retrieval_projection_advisor.json(),
            "retrieval_projection_advisor_blocker_codes": self.retrieval_projection_advisor_blocker_codes,
            "graph_opened": self.graph_opened,
            "search_projection_opened": self.search_projection_opened,
            "search_production_qualification_bound": self.search_production_qualification_bound,
            "system_schema_upgrades": self.system_schema_upgrades.iter().map(|upgrade| serde_json::json!({
                "owner": upgrade.owner,
                "previous_version": upgrade.previous_version,
                "current_version": upgrade.current_version,
                "applied_versions": upgrade.applied_versions,
                "commit_epoch_before": upgrade.commit_epoch_before,
                "commit_epoch_after": upgrade.commit_epoch_after,
            })).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemRuntimeStatus {
    pub protocol: String,
    pub graph_commit_epoch: u64,
    pub changefeed: SearchProjectionChangefeedStatus,
    pub projection_freshness: Option<SearchProjectionFreshness>,
}

impl NowledgeMemRuntimeStatus {
    pub fn projection_commit_lag(&self) -> u64 {
        self.changefeed.projection_commit_lag_after(
            self.projection_freshness
                .as_ref()
                .and_then(|freshness| freshness.durable_source_graph_commit_epoch)
                .unwrap_or(0),
        )
    }

    pub fn projection_stale(&self) -> bool {
        self.projection_commit_lag() > 0
    }

    pub fn json(&self) -> serde_json::Value {
        let freshness = self.projection_freshness.as_ref();
        serde_json::json!({
            "protocol": self.protocol,
            "graph_commit_epoch": self.graph_commit_epoch,
            "changefeed": {
                "graph_commit_epoch": self.changefeed.graph_commit_epoch,
                "resume_floor_commit_epoch": self.changefeed.resume_floor_commit_epoch,
                "oldest_retained_mutation_id": self.changefeed.oldest_retained_mutation_id.map(SearchProjectionMutationId::commit_epoch),
                "newest_retained_mutation_id": self.changefeed.newest_retained_mutation_id.map(SearchProjectionMutationId::commit_epoch),
                "retained_mutation_count": self.changefeed.retained_mutation_count,
                "restart_recoverable": self.changefeed.restart_recoverable,
            },
            "projection": {
                "opened": freshness.is_some(),
                "document_count": freshness.map(|freshness| freshness.document_count),
                "source_graph_commit_epoch": freshness.and_then(|freshness| freshness.source_graph_commit_epoch),
                "durable_source_graph_commit_epoch": freshness.and_then(|freshness| freshness.durable_source_graph_commit_epoch),
                "has_uncheckpointed_changes": freshness.is_some_and(|freshness| freshness.has_uncheckpointed_changes),
                "full_reindex_needed": freshness.is_some_and(|freshness| freshness.full_reindex_needed),
                "full_reindex_reasons": freshness.map(|freshness| freshness.full_reindex_reasons.as_slice()).unwrap_or_default(),
                "metadata_repair_needed": freshness.is_some_and(|freshness| freshness.metadata_repair_needed),
                "metadata_repair_reasons": freshness.map(|freshness| freshness.metadata_repair_reasons.as_slice()).unwrap_or_default(),
                "embedding_model": freshness.and_then(|freshness| freshness.embedding_model.as_deref()),
                "embedding_version": freshness.and_then(|freshness| freshness.embedding_version.as_deref()),
                "embedding_dimension": freshness.and_then(|freshness| freshness.embedding_dimension),
                "commit_lag": self.projection_commit_lag(),
                "stale": self.projection_stale(),
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemProductionStatus {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub graph_open: bool,
    pub graph_read_only: bool,
    pub graph_skein_cutover_effective: bool,
    pub graph_route_ownership_present: bool,
    pub graph_route_ownership_ready: bool,
    pub graph_skein_route_count: usize,
    pub graph_legacy_route_count: usize,
    pub search_projection_open: bool,
    pub search_skein_cutover_effective: bool,
    pub graph_commit_epoch: u64,
    pub search_projection_source_graph_commit_epoch: Option<u64>,
    pub search_projection_durable_source_graph_commit_epoch: Option<u64>,
    pub search_projection_commit_lag: u64,
    pub search_projection_stale: bool,
    pub search_projection_full_reindex_needed: bool,
    pub search_projection_metadata_repair_needed: bool,
    pub search_projection_changefeed_restart_recoverable: bool,
    pub blocker_codes: Vec<String>,
    pub runtime_status: NowledgeMemRuntimeStatus,
    pub route_ownership: Option<NowledgeMemRouteOwnershipReadinessReport>,
}

impl NowledgeMemProductionStatus {
    fn from_runtime(
        mode: NowledgeMemGraphMode,
        graph_read_only: bool,
        runtime_status: NowledgeMemRuntimeStatus,
        route_ownership: Option<NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Self {
        let freshness = runtime_status.projection_freshness.as_ref();
        let search_projection_open = freshness.is_some();
        let search_projection_commit_lag = runtime_status.projection_commit_lag();
        let search_projection_stale = search_projection_open && runtime_status.projection_stale();
        let search_projection_full_reindex_needed =
            freshness.is_some_and(|freshness| freshness.full_reindex_needed);
        let search_projection_metadata_repair_needed =
            freshness.is_some_and(|freshness| freshness.metadata_repair_needed);
        let graph_route_ownership_present = route_ownership.is_some();
        let graph_route_ownership_ready =
            route_ownership.as_ref().is_some_and(|report| report.ready);
        let graph_skein_route_count = route_ownership
            .as_ref()
            .map(|report| report.skein_route_count)
            .unwrap_or(0);
        let graph_legacy_route_count = route_ownership
            .as_ref()
            .map(|report| report.legacy_route_count)
            .unwrap_or(0);
        let mut blocker_codes = Vec::new();
        if graph_read_only {
            blocker_codes.push("graph_opened_read_only".to_string());
        }
        if !graph_route_ownership_present {
            blocker_codes.push("graph_route_ownership_missing".to_string());
        } else if !graph_route_ownership_ready {
            blocker_codes.push("graph_route_ownership_not_ready".to_string());
        }
        if graph_legacy_route_count > 0 {
            blocker_codes.push("graph_legacy_routes_remaining".to_string());
        }
        if !search_projection_open {
            blocker_codes.push("search_projection_not_open".to_string());
        }
        if search_projection_stale {
            blocker_codes.push("search_projection_stale".to_string());
        }
        if search_projection_full_reindex_needed {
            blocker_codes.push("search_projection_full_reindex_needed".to_string());
        }
        if search_projection_metadata_repair_needed {
            blocker_codes.push("search_projection_metadata_repair_needed".to_string());
        }
        if !runtime_status.changefeed.restart_recoverable {
            blocker_codes.push("search_projection_changefeed_not_restart_recoverable".to_string());
        }

        let graph_skein_cutover_effective = !graph_read_only
            && route_ownership
                .as_ref()
                .is_some_and(|report| report.production_cutover_ready);
        let search_skein_cutover_effective = search_projection_open
            && !search_projection_stale
            && !search_projection_full_reindex_needed
            && !search_projection_metadata_repair_needed
            && runtime_status.changefeed.restart_recoverable;

        Self {
            protocol: NOWLEDGE_MEM_PRODUCTION_STATUS_PROTOCOL.to_string(),
            mode,
            graph_open: true,
            graph_read_only,
            graph_skein_cutover_effective,
            graph_route_ownership_present,
            graph_route_ownership_ready,
            graph_skein_route_count,
            graph_legacy_route_count,
            search_projection_open,
            search_skein_cutover_effective,
            graph_commit_epoch: runtime_status.graph_commit_epoch,
            search_projection_source_graph_commit_epoch: freshness
                .and_then(|freshness| freshness.source_graph_commit_epoch),
            search_projection_durable_source_graph_commit_epoch: freshness
                .and_then(|freshness| freshness.durable_source_graph_commit_epoch),
            search_projection_commit_lag,
            search_projection_stale,
            search_projection_full_reindex_needed,
            search_projection_metadata_repair_needed,
            search_projection_changefeed_restart_recoverable: runtime_status
                .changefeed
                .restart_recoverable,
            blocker_codes,
            runtime_status,
            route_ownership,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "graph": {
                "open": self.graph_open,
                "read_only": self.graph_read_only,
                "skein_cutover_effective": self.graph_skein_cutover_effective,
                "route_ownership_present": self.graph_route_ownership_present,
                "route_ownership_ready": self.graph_route_ownership_ready,
                "skein_route_count": self.graph_skein_route_count,
                "legacy_route_count": self.graph_legacy_route_count,
                "commit_epoch": self.graph_commit_epoch,
            },
            "search": {
                "projection_open": self.search_projection_open,
                "skein_cutover_effective": self.search_skein_cutover_effective,
                "source_graph_commit_epoch": self.search_projection_source_graph_commit_epoch,
                "durable_source_graph_commit_epoch": self.search_projection_durable_source_graph_commit_epoch,
                "commit_lag": self.search_projection_commit_lag,
                "stale": self.search_projection_stale,
                "full_reindex_needed": self.search_projection_full_reindex_needed,
                "metadata_repair_needed": self.search_projection_metadata_repair_needed,
                "changefeed_restart_recoverable": self.search_projection_changefeed_restart_recoverable,
            },
            "blocker_codes": self.blocker_codes,
            "runtime_status": self.runtime_status.json(),
            "route_ownership": self.route_ownership.as_ref().map(NowledgeMemRouteOwnershipReadinessReport::json),
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemReadControl {
    Legacy,
    Skein,
}

impl NowledgeMemReadControl {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Skein => "skein",
        }
    }

    pub const fn selects_skein(self) -> bool {
        matches!(self, Self::Skein)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemWorkControl {
    Disabled,
    Enabled,
}

impl NowledgeMemWorkControl {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Enabled => "enabled",
        }
    }

    pub const fn enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemCutoverControls {
    pub graph_reads: NowledgeMemReadControl,
    pub search_reads: NowledgeMemReadControl,
    pub dual_writes: NowledgeMemWorkControl,
    pub initial_import: NowledgeMemWorkControl,
    pub projection_catch_up: NowledgeMemWorkControl,
}

impl NowledgeMemCutoverControls {
    pub const fn legacy() -> Self {
        Self {
            graph_reads: NowledgeMemReadControl::Legacy,
            search_reads: NowledgeMemReadControl::Legacy,
            dual_writes: NowledgeMemWorkControl::Disabled,
            initial_import: NowledgeMemWorkControl::Disabled,
            projection_catch_up: NowledgeMemWorkControl::Disabled,
        }
    }

    pub const fn skein_shadow() -> Self {
        Self {
            graph_reads: NowledgeMemReadControl::Legacy,
            search_reads: NowledgeMemReadControl::Legacy,
            dual_writes: NowledgeMemWorkControl::Enabled,
            initial_import: NowledgeMemWorkControl::Enabled,
            projection_catch_up: NowledgeMemWorkControl::Enabled,
        }
    }

    pub const fn skein_reads() -> Self {
        Self {
            graph_reads: NowledgeMemReadControl::Skein,
            search_reads: NowledgeMemReadControl::Skein,
            dual_writes: NowledgeMemWorkControl::Enabled,
            initial_import: NowledgeMemWorkControl::Disabled,
            projection_catch_up: NowledgeMemWorkControl::Enabled,
        }
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "graph_reads": self.graph_reads.as_str(),
            "search_reads": self.search_reads.as_str(),
            "dual_writes": self.dual_writes.as_str(),
            "initial_import": self.initial_import.as_str(),
            "projection_catch_up": self.projection_catch_up.as_str(),
        })
    }
}

pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_PATCH_DELETE: &str = "source_patch_delete";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_LIFECYCLE: &str = "source_lifecycle";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_GRAPH_DELETE: &str = "source_graph_delete";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE: &str = "source_ingest_create";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_CONTENT_REFRESH_REPARSE: &str =
    "source_content_refresh_reparse";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INDEXED_TRANSITION: &str =
    "source_indexed_transition";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_REVISION_EDGES: &str = "source_revision_edges";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_SEARCH_PROJECTION_EFFECTS: &str =
    "source_search_projection_effects";

pub const REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES: &[&str] = &[
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_PATCH_DELETE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_LIFECYCLE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_GRAPH_DELETE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_CONTENT_REFRESH_REPARSE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INDEXED_TRANSITION,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_REVISION_EDGES,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_SEARCH_PROJECTION_EFFECTS,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSourceMutationFamilyRequirement {
    pub family: String,
    pub requires_search_projection_payload: bool,
}

impl NowledgeMemSourceMutationFamilyRequirement {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "family": self.family,
            "requires_search_projection_payload": self.requires_search_projection_payload,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSourceMutationDualWriteEvidence {
    pub family: String,
    pub payload_frozen: bool,
    pub legacy_ack_recorded: bool,
    pub skein_ack_recorded: bool,
    pub independent_watermarks_recorded: bool,
    pub replay_idempotent: bool,
    pub search_projection_payload_frozen: bool,
}

impl NowledgeMemSourceMutationDualWriteEvidence {
    pub fn ready(family: impl Into<String>) -> Self {
        let family = family.into();
        Self {
            search_projection_payload_frozen: source_mutation_family_requires_projection(&family),
            family,
            payload_frozen: true,
            legacy_ack_recorded: true,
            skein_ack_recorded: true,
            independent_watermarks_recorded: true,
            replay_idempotent: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSourceMutationDualWriteReadinessReport {
    pub protocol: String,
    pub ready: bool,
    pub required_family_count: usize,
    pub evidence_family_count: usize,
    pub ready_family_count: usize,
    pub requirements: Vec<NowledgeMemSourceMutationFamilyRequirement>,
    pub evidence: Vec<NowledgeMemSourceMutationDualWriteEvidence>,
    pub ready_families: Vec<String>,
    pub missing_required_families: Vec<String>,
    pub unknown_families: Vec<String>,
    pub duplicate_families: Vec<String>,
    pub payload_not_frozen_families: Vec<String>,
    pub legacy_ack_missing_families: Vec<String>,
    pub skein_ack_missing_families: Vec<String>,
    pub independent_watermarks_missing_families: Vec<String>,
    pub replay_not_idempotent_families: Vec<String>,
    pub search_projection_payload_not_frozen_families: Vec<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemSourceMutationDualWriteReadinessReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "required_family_count": self.required_family_count,
            "evidence_family_count": self.evidence_family_count,
            "ready_family_count": self.ready_family_count,
            "requirements": self.requirements.iter().map(NowledgeMemSourceMutationFamilyRequirement::json).collect::<Vec<_>>(),
            "evidence": self.evidence.iter().map(source_mutation_dual_write_evidence_json).collect::<Vec<_>>(),
            "ready_families": self.ready_families,
            "missing_required_families": self.missing_required_families,
            "unknown_families": self.unknown_families,
            "duplicate_families": self.duplicate_families,
            "payload_not_frozen_families": self.payload_not_frozen_families,
            "legacy_ack_missing_families": self.legacy_ack_missing_families,
            "skein_ack_missing_families": self.skein_ack_missing_families,
            "independent_watermarks_missing_families": self.independent_watermarks_missing_families,
            "replay_not_idempotent_families": self.replay_not_idempotent_families,
            "search_projection_payload_not_frozen_families": self.search_projection_payload_not_frozen_families,
            "blocker_codes": self.blocker_codes,
        })
    }
}

pub fn nowledge_mem_source_mutation_family_requirements(
) -> Vec<NowledgeMemSourceMutationFamilyRequirement> {
    REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES
        .iter()
        .map(|family| NowledgeMemSourceMutationFamilyRequirement {
            family: (*family).to_string(),
            requires_search_projection_payload: source_mutation_family_requires_projection(family),
        })
        .collect()
}

pub fn nowledge_mem_source_mutation_dual_write_evidence_all_ready(
) -> Vec<NowledgeMemSourceMutationDualWriteEvidence> {
    REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES
        .iter()
        .map(|family| NowledgeMemSourceMutationDualWriteEvidence::ready(*family))
        .collect()
}

pub fn nowledge_mem_source_mutation_dual_write_readiness(
    evidence: &[NowledgeMemSourceMutationDualWriteEvidence],
) -> NowledgeMemSourceMutationDualWriteReadinessReport {
    let required_families = REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut family_counts = BTreeMap::<&str, usize>::new();
    for item in evidence {
        *family_counts.entry(item.family.as_str()).or_default() += 1;
    }
    let observed_required_families = family_counts
        .keys()
        .copied()
        .filter(|family| required_families.contains(family))
        .collect::<BTreeSet<_>>();
    let missing_required_families = REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES
        .iter()
        .copied()
        .filter(|family| !observed_required_families.contains(family))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let unknown_families = family_counts
        .keys()
        .copied()
        .filter(|family| !required_families.contains(family))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let duplicate_families = family_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(family, _)| (*family).to_string())
        .collect::<Vec<_>>();
    let payload_not_frozen_families =
        source_mutation_families_where(evidence, |item| !item.payload_frozen);
    let legacy_ack_missing_families =
        source_mutation_families_where(evidence, |item| !item.legacy_ack_recorded);
    let skein_ack_missing_families =
        source_mutation_families_where(evidence, |item| !item.skein_ack_recorded);
    let independent_watermarks_missing_families =
        source_mutation_families_where(evidence, |item| !item.independent_watermarks_recorded);
    let replay_not_idempotent_families =
        source_mutation_families_where(evidence, |item| !item.replay_idempotent);
    let search_projection_payload_not_frozen_families =
        source_mutation_families_where(evidence, |item| {
            source_mutation_family_requires_projection(&item.family)
                && !item.search_projection_payload_frozen
        });
    let ready_families = source_mutation_families_where(evidence, |item| {
        required_families.contains(item.family.as_str())
            && item.payload_frozen
            && item.legacy_ack_recorded
            && item.skein_ack_recorded
            && item.independent_watermarks_recorded
            && item.replay_idempotent
            && (!source_mutation_family_requires_projection(&item.family)
                || item.search_projection_payload_frozen)
    });

    let mut blocker_codes = Vec::new();
    if !missing_required_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_missing_required_families".to_string());
    }
    if !unknown_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_unknown_families".to_string());
    }
    if !duplicate_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_duplicate_families".to_string());
    }
    if !payload_not_frozen_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_payload_not_frozen".to_string());
    }
    if !legacy_ack_missing_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_legacy_ack_missing".to_string());
    }
    if !skein_ack_missing_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_skein_ack_missing".to_string());
    }
    if !independent_watermarks_missing_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_independent_watermarks_missing".to_string());
    }
    if !replay_not_idempotent_families.is_empty() {
        blocker_codes.push("source_mutation_dual_write_replay_not_idempotent".to_string());
    }
    if !search_projection_payload_not_frozen_families.is_empty() {
        blocker_codes
            .push("source_mutation_dual_write_search_projection_payload_not_frozen".to_string());
    }

    let ready = blocker_codes.is_empty();
    NowledgeMemSourceMutationDualWriteReadinessReport {
        protocol: NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL.to_string(),
        ready,
        required_family_count: REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len(),
        evidence_family_count: observed_required_families.len(),
        ready_family_count: ready_families.len(),
        requirements: nowledge_mem_source_mutation_family_requirements(),
        evidence: normalized_source_mutation_evidence(evidence),
        ready_families,
        missing_required_families,
        unknown_families,
        duplicate_families,
        payload_not_frozen_families,
        legacy_ack_missing_families,
        skein_ack_missing_families,
        independent_watermarks_missing_families,
        replay_not_idempotent_families,
        search_projection_payload_not_frozen_families,
        blocker_codes,
    }
}

fn source_mutation_family_requires_projection(family: &str) -> bool {
    matches!(
        family,
        NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE
            | NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_CONTENT_REFRESH_REPARSE
            | NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INDEXED_TRANSITION
            | NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_SEARCH_PROJECTION_EFFECTS
    )
}

fn source_mutation_families_where(
    evidence: &[NowledgeMemSourceMutationDualWriteEvidence],
    predicate: impl Fn(&NowledgeMemSourceMutationDualWriteEvidence) -> bool,
) -> Vec<String> {
    let mut families = evidence
        .iter()
        .filter(|item| predicate(item))
        .map(|item| item.family.clone())
        .collect::<Vec<_>>();
    families.sort();
    families.dedup();
    families
}

fn normalized_source_mutation_evidence(
    evidence: &[NowledgeMemSourceMutationDualWriteEvidence],
) -> Vec<NowledgeMemSourceMutationDualWriteEvidence> {
    let mut normalized = evidence.to_vec();
    normalized.sort_by(|left, right| left.family.cmp(&right.family));
    normalized
}

fn source_mutation_dual_write_evidence_json(
    evidence: &NowledgeMemSourceMutationDualWriteEvidence,
) -> serde_json::Value {
    serde_json::json!({
        "family": evidence.family,
        "payload_frozen": evidence.payload_frozen,
        "legacy_ack_recorded": evidence.legacy_ack_recorded,
        "skein_ack_recorded": evidence.skein_ack_recorded,
        "independent_watermarks_recorded": evidence.independent_watermarks_recorded,
        "replay_idempotent": evidence.replay_idempotent,
        "search_projection_payload_frozen": evidence.search_projection_payload_frozen,
    })
}

impl Default for NowledgeMemCutoverControls {
    fn default() -> Self {
        Self::legacy()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemCutoverControlsReport {
    pub protocol: String,
    pub ready: bool,
    pub controls: NowledgeMemCutoverControls,
    pub graph_read_selected_skein: bool,
    pub graph_read_effective: bool,
    pub search_read_selected_skein: bool,
    pub search_read_effective: bool,
    pub dual_writes_enabled: bool,
    pub initial_import_enabled: bool,
    pub initial_import_inactive_for_cutover: bool,
    pub initial_import_cutover_catch_up_ready: bool,
    pub initial_import_safe_for_read_cutover: bool,
    pub projection_catch_up_enabled: bool,
    pub blocker_codes: Vec<String>,
    pub production_status: NowledgeMemProductionStatus,
}

impl NowledgeMemCutoverControlsReport {
    fn from_status_with_initial_import_cutover_catch_up(
        controls: NowledgeMemCutoverControls,
        production_status: NowledgeMemProductionStatus,
        initial_import_cutover_catch_up: Option<&SkeinLightningInitialImportCutoverCatchUpReport>,
    ) -> Self {
        let graph_read_selected_skein = controls.graph_reads.selects_skein();
        let search_read_selected_skein = controls.search_reads.selects_skein();
        let dual_writes_enabled = controls.dual_writes.enabled();
        let initial_import_enabled = controls.initial_import.enabled();
        let initial_import_inactive_for_cutover = !initial_import_enabled;
        let initial_import_cutover_catch_up_ready =
            initial_import_cutover_catch_up.is_some_and(|report| report.ready);
        let initial_import_safe_for_read_cutover =
            initial_import_inactive_for_cutover || initial_import_cutover_catch_up_ready;
        let projection_catch_up_enabled = controls.projection_catch_up.enabled();
        let graph_read_effective =
            !graph_read_selected_skein || production_status.graph_skein_cutover_effective;
        let search_read_effective =
            !search_read_selected_skein || production_status.search_skein_cutover_effective;
        let mut blocker_codes = Vec::new();
        if !graph_read_effective {
            blocker_codes.push("graph_read_selected_skein_but_not_effective".to_string());
        }
        if !search_read_effective {
            blocker_codes.push("search_read_selected_skein_but_not_effective".to_string());
        }
        if search_read_selected_skein && !projection_catch_up_enabled {
            blocker_codes
                .push("search_read_selected_skein_without_projection_catch_up".to_string());
        }
        if initial_import_enabled && !dual_writes_enabled {
            blocker_codes.push("initial_import_enabled_without_dual_writes".to_string());
        }
        if !initial_import_safe_for_read_cutover {
            blocker_codes.push("initial_import_active_blocks_read_cutover".to_string());
        }

        Self {
            protocol: NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL.to_string(),
            ready: blocker_codes.is_empty(),
            controls,
            graph_read_selected_skein,
            graph_read_effective,
            search_read_selected_skein,
            search_read_effective,
            dual_writes_enabled,
            initial_import_enabled,
            initial_import_inactive_for_cutover,
            initial_import_cutover_catch_up_ready,
            initial_import_safe_for_read_cutover,
            projection_catch_up_enabled,
            blocker_codes,
            production_status,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "controls": self.controls.json(),
            "graph": {
                "read_selected_skein": self.graph_read_selected_skein,
                "read_effective": self.graph_read_effective,
            },
            "search": {
                "read_selected_skein": self.search_read_selected_skein,
                "read_effective": self.search_read_effective,
            },
            "work": {
                "dual_writes_enabled": self.dual_writes_enabled,
                "initial_import_enabled": self.initial_import_enabled,
                "initial_import_inactive_for_cutover": self.initial_import_inactive_for_cutover,
                "initial_import_cutover_catch_up_ready": self.initial_import_cutover_catch_up_ready,
                "initial_import_safe_for_read_cutover": self.initial_import_safe_for_read_cutover,
                "projection_catch_up_enabled": self.projection_catch_up_enabled,
            },
            "blocker_codes": self.blocker_codes,
            "production_status": self.production_status.json(),
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
            },
        })
    }
}

pub const NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL: &str = "skein-nowledge-mem-open-report";
pub const NOWLEDGE_MEM_RUNTIME_STATUS_PROTOCOL: &str = "skein-nowledge-mem-runtime-status-v1";
pub const NOWLEDGE_MEM_PRODUCTION_STATUS_PROTOCOL: &str = "skein-nowledge-mem-production-status-v1";
pub const NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL: &str = "skein-nowledge-mem-cutover-controls-v1";
pub const NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL: &str =
    "skein-nowledge-mem-source-mutation-dual-write-readiness-v1";
pub const NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL: &str =
    "skein-nowledge-mem-operations-readiness-v1";
pub const NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL: &str = "skein-nowledge-mem-query-report-v1";
pub const NOWLEDGE_MEM_READ_REPORT_PROTOCOL: &str = "skein-nowledge-mem-read-report";
pub const NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL: &str = "skein-nowledge-mem-retrieval-report";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-search-candidate-report-v1";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL: &str =
    "skein-nowledge-mem-search-candidate-readiness-v1";
pub const NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL: &str = "skein-nowledge-mem-slow-query-report-v1";
pub const NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL: &str =
    "skein-nowledge-mem-storage-lifecycle-decision-v1";
pub const NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL: &str =
    "skein-nowledge-mem-readiness-dashboard-v1";
pub const NOWLEDGE_MEM_READ_SNAPSHOT_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-read-snapshot-report-v1";
pub const NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-mem-bounded-read-evidence-v2";
pub const NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL: &str = "skein-nowledge-mem-library-readiness-v1";
pub const NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL: &str =
    "skein-nowledge-query-runtime-preflight-v1";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-candidate-shadow-evidence";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE: &str = "nmem-rust-bridge";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE: &str =
    "search_candidate_shadow_trace";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE: &str =
    "/search-index/skein-shadow/candidate-evidence";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE: &str = "skein";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE: &str = "skein-shadow";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE: &str = "lancedb";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE: &str = "skein";
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
const NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-evidence";
const NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-shadow-evidence";
const NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE: &str = "skein-rust-cli";
const SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY: &str =
    "search_projection_shadow_pushdown_evidence_not_ready";
const SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING: &str =
    "skein_search_projection_segment_descriptor_missing";
const SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING: &str =
    "skein_search_projection_segment_descriptor_fields_missing";
pub const NOWLEDGE_MEM_SEARCH_ROUTE: &str = "/graph/search";
pub const REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES: &[&str] = &[
    "/communities",
    "/communities/{community_id}",
    "/graph/overview",
    "/graph/sample",
    NOWLEDGE_MEM_SEARCH_ROUTE,
    "/graph/explore",
    "/graph/expand/{node_id}",
    "/graph/live-preview",
    "/graph/live-preview/{node_id}",
    "/graph/community-members/{community_id}",
    "/library/community/{community_id}/subgraph",
    "/library/community/{community_id}/recent-memories",
    "/library/community/{community_id}/related",
    "/graph/analysis",
    "/graph/augmentation/state",
    "/graph/augmentation/pagerank/plan",
    "/graph/node-details/{node_id}",
    "/graph/orphans",
    "/graph/shortest-path",
    "/sources/{source_id}",
    "/stats/entity-relations",
    "/stats/sources",
    "/stats/top-communities",
    "/entities",
    "/entities/{entity_id}/relationships",
    "/agent/evolves",
];
pub const NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION: &str =
    "nowledge-mem-graph-read-route-catalog-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemGraphReadRouteOwner {
    GraphRuntime,
    SearchRuntime,
    ReadBatchRuntime,
}

impl NowledgeMemGraphReadRouteOwner {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GraphRuntime => "graph_runtime",
            Self::SearchRuntime => "search_runtime",
            Self::ReadBatchRuntime => "read_batch_runtime",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemGraphReadRouteEvidenceKind {
    GraphRouteExecution,
    SearchCandidateShadow,
    ReadBatchRuntime,
}

impl NowledgeMemGraphReadRouteEvidenceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GraphRouteExecution => "graph_route_execution",
            Self::SearchCandidateShadow => "search_candidate_shadow",
            Self::ReadBatchRuntime => "read_batch_runtime",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemGraphReadRouteSpec {
    pub route: &'static str,
    pub owner: NowledgeMemGraphReadRouteOwner,
    pub required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind,
    pub stale_on_catalog_change: bool,
}

const fn graph_route(route: &'static str) -> NowledgeMemGraphReadRouteSpec {
    NowledgeMemGraphReadRouteSpec {
        route,
        owner: NowledgeMemGraphReadRouteOwner::GraphRuntime,
        required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind::GraphRouteExecution,
        stale_on_catalog_change: true,
    }
}

const fn search_route(route: &'static str) -> NowledgeMemGraphReadRouteSpec {
    NowledgeMemGraphReadRouteSpec {
        route,
        owner: NowledgeMemGraphReadRouteOwner::SearchRuntime,
        required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind::SearchCandidateShadow,
        stale_on_catalog_change: true,
    }
}

const fn read_batch_route(route: &'static str) -> NowledgeMemGraphReadRouteSpec {
    NowledgeMemGraphReadRouteSpec {
        route,
        owner: NowledgeMemGraphReadRouteOwner::ReadBatchRuntime,
        required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind::ReadBatchRuntime,
        stale_on_catalog_change: true,
    }
}

pub const NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS: &[NowledgeMemGraphReadRouteSpec] = &[
    read_batch_route("/communities"),
    read_batch_route("/communities/{community_id}"),
    graph_route("/graph/overview"),
    graph_route("/graph/sample"),
    search_route(NOWLEDGE_MEM_SEARCH_ROUTE),
    graph_route("/graph/explore"),
    graph_route("/graph/expand/{node_id}"),
    graph_route("/graph/live-preview"),
    graph_route("/graph/live-preview/{node_id}"),
    graph_route("/graph/community-members/{community_id}"),
    graph_route("/library/community/{community_id}/subgraph"),
    graph_route("/library/community/{community_id}/recent-memories"),
    graph_route("/library/community/{community_id}/related"),
    graph_route("/graph/analysis"),
    graph_route("/graph/augmentation/state"),
    graph_route("/graph/augmentation/pagerank/plan"),
    graph_route("/graph/node-details/{node_id}"),
    graph_route("/graph/orphans"),
    graph_route("/graph/shortest-path"),
    read_batch_route("/sources/{source_id}"),
    read_batch_route("/stats/entity-relations"),
    read_batch_route("/stats/sources"),
    read_batch_route("/stats/top-communities"),
    read_batch_route("/entities"),
    read_batch_route("/entities/{entity_id}/relationships"),
    read_batch_route("/agent/evolves"),
];

pub fn nowledge_mem_graph_read_route_spec(
    route: &str,
) -> Option<&'static NowledgeMemGraphReadRouteSpec> {
    NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS
        .iter()
        .find(|spec| spec.route == route)
}

pub fn nowledge_mem_graph_read_route_spec_json(
    spec: &NowledgeMemGraphReadRouteSpec,
) -> serde_json::Value {
    serde_json::json!({
        "route": spec.route,
        "owner": spec.owner.as_str(),
        "required_evidence_kind": spec.required_evidence_kind.as_str(),
        "stale_on_catalog_change": spec.stale_on_catalog_change,
    })
}

pub fn nowledge_mem_graph_read_route_specs_json() -> serde_json::Value {
    serde_json::json!(NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS
        .iter()
        .map(nowledge_mem_graph_read_route_spec_json)
        .collect::<Vec<_>>())
}

pub fn nowledge_mem_graph_read_route_catalog_digest() -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for spec in NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS {
        fnv1a_update(&mut hash, spec.route.as_bytes());
        fnv1a_update(&mut hash, spec.owner.as_str().as_bytes());
        fnv1a_update(&mut hash, spec.required_evidence_kind.as_str().as_bytes());
        fnv1a_update(
            &mut hash,
            if spec.stale_on_catalog_change {
                b"true"
            } else {
                b"false"
            },
        );
    }
    format!("fnv1a64:{hash:016x}")
}

fn fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    *hash ^= 0xff;
    *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
}

pub fn nowledge_mem_required_query_families_for_route(route: &str) -> &'static [&'static str] {
    match route {
        "/communities"
        | "/communities/{community_id}"
        | "/sources/{source_id}"
        | "/stats/entity-relations"
        | "/stats/sources"
        | "/stats/top-communities"
        | "/entities"
        | "/entities/{entity_id}/relationships"
        | "/agent/evolves" => &["label_stats_read"],
        NOWLEDGE_MEM_SEARCH_ROUTE => &["search_projection"],
        "/graph/overview"
        | "/graph/sample"
        | "/graph/live-preview"
        | "/graph/live-preview/{node_id}"
        | "/graph/community-members/{community_id}"
        | "/library/community/{community_id}/recent-memories"
        | "/graph/node-details/{node_id}" => &["memory_lookup"],
        "/graph/explore"
        | "/graph/expand/{node_id}"
        | "/library/community/{community_id}/subgraph"
        | "/library/community/{community_id}/related"
        | "/graph/orphans"
        | "/graph/shortest-path" => &["graph_traversal"],
        "/graph/analysis" | "/graph/augmentation/state" | "/graph/augmentation/pagerank/plan" => {
            &["projected_graph"]
        }
        _ => &[],
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightProbe {
    pub name: String,
    pub route: Option<String>,
    pub query_family: Option<String>,
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
    pub require_scan_pruning: bool,
    pub require_pruned: bool,
    pub min_scan_pruning_reports: usize,
    pub max_output_rows: Option<usize>,
}

impl NowledgeQueryRuntimePreflightProbe {
    pub fn new(name: impl Into<String>, cypher: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            route: None,
            query_family: None,
            cypher: cypher.into(),
            parameters: BTreeMap::new(),
            require_scan_pruning: false,
            require_pruned: false,
            min_scan_pruning_reports: 1,
            max_output_rows: None,
        }
    }

    pub fn with_route(mut self, route: impl Into<String>) -> Self {
        self.route = Some(route.into());
        self
    }

    pub fn with_query_family(mut self, query_family: impl Into<String>) -> Self {
        self.query_family = Some(query_family.into());
        self
    }

    pub fn with_parameters(mut self, parameters: BTreeMap<String, Value>) -> Self {
        self.parameters = parameters;
        self
    }

    pub fn require_scan_pruning(mut self, min_scan_pruning_reports: usize) -> Self {
        self.require_scan_pruning = true;
        self.min_scan_pruning_reports = min_scan_pruning_reports;
        self
    }

    pub fn require_pruned(mut self) -> Self {
        self.require_pruned = true;
        self
    }

    pub fn with_max_output_rows(mut self, max_output_rows: usize) -> Self {
        self.max_output_rows = Some(max_output_rows);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightReport {
    pub protocol: String,
    pub ready: bool,
    pub database_opened: bool,
    pub redaction: NowledgeQueryRuntimePreflightRedactionSummary,
    pub probe_count: usize,
    pub passed_probe_count: usize,
    pub failed_probe_count: usize,
    pub required_route_count: usize,
    pub covered_route_count: usize,
    pub covered_routes: Vec<String>,
    pub missing_required_routes: Vec<String>,
    pub required_routes_covered: bool,
    pub unknown_routes: Vec<String>,
    pub duplicate_routes: Vec<String>,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub route_coverage_ready: bool,
    pub route_coverage_blocker_codes: Vec<String>,
    pub blocker_codes: Vec<String>,
    pub probes: Vec<NowledgeQueryRuntimePreflightProbeReport>,
}

impl NowledgeQueryRuntimePreflightReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "database_opened": self.database_opened,
            "redaction": self.redaction.json(),
            "probe_count": self.probe_count,
            "passed_probe_count": self.passed_probe_count,
            "failed_probe_count": self.failed_probe_count,
            "required_route_count": self.required_route_count,
            "covered_route_count": self.covered_route_count,
            "covered_routes": self.covered_routes,
            "missing_required_routes": self.missing_required_routes,
            "required_routes_covered": self.required_routes_covered,
            "unknown_routes": self.unknown_routes,
            "duplicate_routes": self.duplicate_routes,
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "route_coverage_ready": self.route_coverage_ready,
            "route_coverage_blocker_codes": self.route_coverage_blocker_codes,
            "blocker_codes": self.blocker_codes,
            "probes": self.probes.iter().map(NowledgeQueryRuntimePreflightProbeReport::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NowledgeQueryRuntimePreflightRedactionSummary {
    pub rows_copied: bool,
    pub parameters_copied: bool,
    pub local_paths_copied: bool,
    pub raw_errors_copied: bool,
}

impl NowledgeQueryRuntimePreflightRedactionSummary {
    pub fn ready(&self) -> bool {
        !self.rows_copied
            && !self.parameters_copied
            && !self.local_paths_copied
            && !self.raw_errors_copied
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "rows_copied": self.rows_copied,
            "parameters_copied": self.parameters_copied,
            "local_paths_copied": self.local_paths_copied,
            "raw_errors_copied": self.raw_errors_copied,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightProbeReport {
    pub name: String,
    pub route: Option<String>,
    pub query_family: Option<String>,
    pub ready: bool,
    pub success: bool,
    pub output_row_count: usize,
    pub selected_plan_fingerprint: Option<String>,
    pub search_mode: Option<String>,
    pub selected_plan_operator_counts: BTreeMap<String, usize>,
    pub selected_plan_class_counts: BTreeMap<String, usize>,
    pub optimizer_decision_count: usize,
    pub optimizer_rule_event_count: usize,
    pub plan_cache_lookup: Option<String>,
    pub plan_cache_bypass_reason: Option<String>,
    pub plan_cache_cacheable: bool,
    pub plan_cache_hit: bool,
    pub plan_cache_miss: bool,
    pub plan_cache_bypassed: bool,
    pub work_priority: Option<String>,
    pub work_class: Option<String>,
    pub estimated_operations: Option<usize>,
    pub max_rows: Option<usize>,
    pub detection_row_cap: Option<usize>,
    pub row_limit_enforced_before_output: bool,
    pub operator_row_cap_enabled: bool,
    pub blocking_operator_kinds: Vec<String>,
    pub scan_pruning_reports: Vec<ScanPruningReport>,
    pub pruned_scan_count: usize,
    pub error_class: Option<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeQueryRuntimePreflightProbeReport {
    pub fn json(&self) -> serde_json::Value {
        let mut value = serde_json::json!({
            "name": self.name,
            "route": self.route,
            "query_family": self.query_family,
            "ready": self.ready,
            "success": self.success,
            "blocker_codes": self.blocker_codes,
        });
        let object = value
            .as_object_mut()
            .expect("query runtime preflight probe report is an object");
        if self.success {
            object.insert(
                "output_row_count".to_string(),
                serde_json::json!(self.output_row_count),
            );
            object.insert(
                "selected_plan_fingerprint".to_string(),
                serde_json::json!(self.selected_plan_fingerprint),
            );
            object.insert(
                "search_mode".to_string(),
                serde_json::json!(self.search_mode),
            );
            object.insert(
                "selected_plan_operator_counts".to_string(),
                serde_json::json!(self.selected_plan_operator_counts),
            );
            object.insert(
                "selected_plan_class_counts".to_string(),
                serde_json::json!(self.selected_plan_class_counts),
            );
            object.insert(
                "optimizer_decision_count".to_string(),
                serde_json::json!(self.optimizer_decision_count),
            );
            object.insert(
                "optimizer_rule_event_count".to_string(),
                serde_json::json!(self.optimizer_rule_event_count),
            );
            object.insert(
                "plan_cache_lookup".to_string(),
                serde_json::json!(self.plan_cache_lookup),
            );
            object.insert(
                "plan_cache".to_string(),
                serde_json::json!({
                    "lookup": self.plan_cache_lookup,
                    "bypass_reason": self.plan_cache_bypass_reason,
                    "cacheable": self.plan_cache_cacheable,
                    "hit": self.plan_cache_hit,
                    "miss": self.plan_cache_miss,
                    "bypassed": self.plan_cache_bypassed,
                }),
            );
            object.insert(
                "work_request".to_string(),
                serde_json::json!({
                    "priority": self.work_priority,
                    "class": self.work_class,
                    "estimated_operations": self.estimated_operations,
                }),
            );
            object.insert(
                "execution_profile".to_string(),
                serde_json::json!({
                    "max_rows": self.max_rows,
                    "detection_row_cap": self.detection_row_cap,
                    "row_limit_enforced_before_output": self.row_limit_enforced_before_output,
                    "operator_row_cap_enabled": self.operator_row_cap_enabled,
                    "blocking_operator_kinds": self.blocking_operator_kinds,
                    "scan_pruning_report_count": self.scan_pruning_reports.len(),
                    "pruned_scan_count": self.pruned_scan_count,
                    "scan_pruning_reports": self.scan_pruning_reports.iter().map(scan_pruning_report_json).collect::<Vec<_>>(),
                }),
            );
        } else {
            object.insert(
                "error_class".to_string(),
                serde_json::json!(self.error_class),
            );
        }
        value
    }
}

pub const DEFAULT_NOWLEDGE_MEM_READ_MAX_ROWS: usize = 512;
pub const DEFAULT_NOWLEDGE_MEM_READ_MAX_ESTIMATED_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

impl NowledgeMemGraphMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ShadowReadOnly => "shadow_read_only",
            Self::WritableCutover => "writable_cutover",
        }
    }
}

#[derive(Debug)]
pub struct NowledgeMemGraph {
    db: Database,
    mode: NowledgeMemGraphMode,
    runtime_governor: RuntimeGovernor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadOptions {
    pub max_rows: Option<usize>,
    pub max_estimated_payload_bytes: Option<usize>,
}

impl Default for NowledgeMemReadOptions {
    fn default() -> Self {
        Self {
            max_rows: Some(DEFAULT_NOWLEDGE_MEM_READ_MAX_ROWS),
            max_estimated_payload_bytes: Some(
                DEFAULT_NOWLEDGE_MEM_READ_MAX_ESTIMATED_PAYLOAD_BYTES,
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub row_count: usize,
    pub max_rows: Option<usize>,
    pub execution_row_cap: Option<usize>,
    pub estimated_payload_bytes: usize,
    pub max_estimated_payload_bytes: Option<usize>,
    pub row_budget_exceeded: bool,
    pub payload_budget_exceeded: bool,
    pub row_limit_enforced_before_output: bool,
    pub operator_row_cap_enabled: bool,
    pub blocking_operator_count: usize,
    pub blocking_operator_kinds: Vec<String>,
    pub blocking_operator_memory_reports: Vec<skein_executor::BlockingOperatorMemoryReport>,
    pub intermediate_rows: usize,
    pub intermediate_payload_bytes: usize,
    pub output_payload_bytes: usize,
    pub steady_resident_bytes: Option<u64>,
    pub peak_resident_bytes: Option<u64>,
    pub total_page_faults: Option<u64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
    pub streaming: bool,
}

impl NowledgeMemReadReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "row_count": self.row_count,
            "max_rows": self.max_rows,
            "execution_row_cap": self.execution_row_cap,
            "estimated_payload_bytes": self.estimated_payload_bytes,
            "max_estimated_payload_bytes": self.max_estimated_payload_bytes,
            "row_budget_exceeded": self.row_budget_exceeded,
            "payload_budget_exceeded": self.payload_budget_exceeded,
            "row_limit_enforced_before_output": self.row_limit_enforced_before_output,
            "operator_row_cap_enabled": self.operator_row_cap_enabled,
            "blocking_operator_count": self.blocking_operator_count,
            "blocking_operator_kinds": self.blocking_operator_kinds,
            "blocking_operator_memory_reports": self.blocking_operator_memory_reports.iter().map(blocking_operator_memory_report_json).collect::<Vec<_>>(),
            "intermediate_rows": self.intermediate_rows,
            "intermediate_payload_bytes": self.intermediate_payload_bytes,
            "output_payload_bytes": self.output_payload_bytes,
            "steady_resident_bytes": self.steady_resident_bytes,
            "peak_resident_bytes": self.peak_resident_bytes,
            "total_page_faults": self.total_page_faults,
            "minor_page_faults": self.minor_page_faults,
            "major_page_faults": self.major_page_faults,
            "streaming": self.streaming,
        })
    }

    pub fn bounded_read_evidence_json(&self) -> serde_json::Value {
        nowledge_mem_bounded_read_evidence_json(self)
    }
}

pub fn nowledge_mem_bounded_read_evidence_json(
    report: &NowledgeMemReadReport,
) -> serde_json::Value {
    nowledge_mem_bounded_read_evidence_json_with_routes(report, &[])
}

pub fn nowledge_mem_bounded_read_evidence_json_with_routes(
    report: &NowledgeMemReadReport,
    covered_routes: &[String],
) -> serde_json::Value {
    nowledge_mem_bounded_read_evidence_json_with_route_readiness(report, covered_routes, None)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemRouteReadinessSummary {
    pub route_primary_ready: bool,
    pub primary_ready_routes: Vec<String>,
    pub route_query_plan_evidence_ready: bool,
    pub route_query_profile_evidence_ready: bool,
    pub route_query_api_behavior_evidence_ready: bool,
    pub relationship_property_pruning_required_count: u64,
    pub relationship_property_pruning_report_count: u64,
    pub route_relationship_property_pruning_evidence_ready: bool,
}

pub fn nowledge_mem_bounded_read_evidence_json_with_route_readiness(
    report: &NowledgeMemReadReport,
    covered_routes: &[String],
    route_readiness: Option<&NowledgeMemRouteReadinessSummary>,
) -> serde_json::Value {
    let blocker_codes = nowledge_mem_bounded_read_blocker_codes(report);
    let missing_covered_routes = missing_nowledge_mem_bounded_read_routes(covered_routes);
    let route_readiness_blocker = route_readiness.and_then(|summary| {
        (!summary.route_primary_ready
            || !summary.route_query_plan_evidence_ready
            || !summary.route_query_profile_evidence_ready
            || !summary.route_query_api_behavior_evidence_ready
            || !summary.route_relationship_property_pruning_evidence_ready
            || summary.relationship_property_pruning_required_count
                != summary.relationship_property_pruning_report_count)
            .then_some("graph_route_readiness_not_ready")
    });
    let blocker_codes = blocker_codes
        .into_iter()
        .chain((!missing_covered_routes.is_empty()).then_some("missing_covered_routes"))
        .chain((route_readiness.is_none()).then_some("graph_route_readiness_missing"))
        .chain(route_readiness_blocker)
        .collect::<Vec<_>>();
    let ready = blocker_codes.is_empty();
    let route_primary_ready = route_readiness.map(|summary| summary.route_primary_ready);
    let primary_ready_routes = route_readiness
        .map(|summary| summary.primary_ready_routes.clone())
        .unwrap_or_default();
    let route_query_plan_evidence_ready =
        route_readiness.map(|summary| summary.route_query_plan_evidence_ready);
    let route_query_profile_evidence_ready =
        route_readiness.map(|summary| summary.route_query_profile_evidence_ready);
    let route_query_api_behavior_evidence_ready =
        route_readiness.map(|summary| summary.route_query_api_behavior_evidence_ready);
    let relationship_property_pruning_required_count =
        route_readiness.map(|summary| summary.relationship_property_pruning_required_count);
    let relationship_property_pruning_report_count =
        route_readiness.map(|summary| summary.relationship_property_pruning_report_count);
    let route_relationship_property_pruning_evidence_ready =
        route_readiness.map(|summary| summary.route_relationship_property_pruning_evidence_ready);
    let blocking_operator_memory_reports_complete =
        blocking_operator_memory_reports_complete(report);
    let blocking_operator_memory_within_budget = blocking_operator_memory_within_budget(report);
    let spill_within_budget = blocking_operator_spill_within_budget(report);

    serde_json::json!({
        "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
        "present": true,
        "ready": ready,
        "mode": report.mode.as_str(),
        "max_rows": report.max_rows,
        "execution_row_cap": report.execution_row_cap,
        "estimated_payload_bytes": report.estimated_payload_bytes,
        "max_estimated_payload_bytes": report.max_estimated_payload_bytes,
        "row_limit_enforced_before_output": report.row_limit_enforced_before_output,
        "operator_row_cap_enabled": report.operator_row_cap_enabled,
        "streaming": report.streaming,
        "blocking_operator_count": report.blocking_operator_count,
        "blocking_operator_kinds": report.blocking_operator_kinds,
        "blocking_operator_memory_reports": report.blocking_operator_memory_reports.iter().map(blocking_operator_memory_report_json).collect::<Vec<_>>(),
        "blocking_operator_memory_reports_complete": blocking_operator_memory_reports_complete,
        "blocking_operator_memory_within_budget": blocking_operator_memory_within_budget,
        "spill_within_budget": spill_within_budget,
        "row_budget_exceeded": report.row_budget_exceeded,
        "payload_budget_exceeded": report.payload_budget_exceeded,
        "covered_routes": covered_routes,
        "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
        "missing_covered_routes": missing_covered_routes,
        "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
        "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
        "route_primary_ready": route_primary_ready,
        "primary_ready_routes": primary_ready_routes,
        "route_query_plan_evidence_ready": route_query_plan_evidence_ready,
        "route_query_profile_evidence_ready": route_query_profile_evidence_ready,
        "route_query_api_behavior_evidence_ready": route_query_api_behavior_evidence_ready,
        "relationship_property_pruning_required_count": relationship_property_pruning_required_count,
        "relationship_property_pruning_report_count": relationship_property_pruning_report_count,
        "route_relationship_property_pruning_evidence_ready": route_relationship_property_pruning_evidence_ready,
        "blocker_codes": blocker_codes,
    })
}

fn nowledge_mem_bounded_read_blocker_codes(report: &NowledgeMemReadReport) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    let expected_row_cap = match report.max_rows {
        Some(0) => {
            blockers.push("invalid_max_rows");
            None
        }
        Some(max_rows) => max_rows.checked_add(1),
        None => {
            blockers.push("missing_max_rows");
            None
        }
    };
    if report.mode != NowledgeMemGraphMode::ShadowReadOnly {
        blockers.push("not_shadow_read_only");
    }

    match (report.execution_row_cap, expected_row_cap) {
        (Some(execution_row_cap), Some(expected_row_cap))
            if execution_row_cap == expected_row_cap => {}
        (Some(_), _) => blockers.push("execution_row_cap_mismatch"),
        (None, _) => blockers.push("missing_execution_row_cap"),
    }
    if !report.row_limit_enforced_before_output {
        blockers.push("row_limit_not_enforced_before_output");
    }
    if !report.operator_row_cap_enabled {
        blockers.push("operator_row_cap_disabled");
    }
    if report.row_budget_exceeded {
        blockers.push("row_budget_exceeded");
    }
    if report.payload_budget_exceeded {
        blockers.push("payload_budget_exceeded");
    }
    if !blocking_operator_memory_reports_complete(report) {
        blockers.push("blocking_operator_memory_report_incomplete");
    }
    if !blocking_operator_memory_within_budget(report) {
        blockers.push("blocking_operator_memory_budget_exceeded");
    }
    if !blocking_operator_spill_within_budget(report) {
        blockers.push("blocking_operator_spill_budget_exceeded");
    }
    blockers
}

fn blocking_operator_memory_reports_complete(report: &NowledgeMemReadReport) -> bool {
    let expected = report
        .blocking_operator_kinds
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let actual = report
        .blocking_operator_memory_reports
        .iter()
        .map(|report| report.operator.as_str())
        .collect::<BTreeSet<_>>();
    report.blocking_operator_count == expected.len() && actual == expected
}

fn blocking_operator_memory_within_budget(report: &NowledgeMemReadReport) -> bool {
    report
        .blocking_operator_memory_reports
        .iter()
        .all(|report| report.budget_bytes > 0 && report.peak_tracked_bytes <= report.budget_bytes)
}

fn blocking_operator_spill_within_budget(report: &NowledgeMemReadReport) -> bool {
    report
        .blocking_operator_memory_reports
        .iter()
        .all(|report| {
            report.max_spill_bytes > 0
                && report.max_spill_runs > 0
                && report.spilled_bytes <= report.max_spill_bytes
                && report.spill_run_count <= report.max_spill_runs
                && ((report.spill_run_count == 0 && report.spilled_bytes == 0)
                    || (report.spill_run_count > 0 && report.spilled_bytes > 0))
        })
}

fn blocking_operator_memory_report_json(
    report: &skein_executor::BlockingOperatorMemoryReport,
) -> serde_json::Value {
    serde_json::json!({
        "operator": report.operator,
        "budget_bytes": report.budget_bytes,
        "peak_tracked_bytes": report.peak_tracked_bytes,
        "input_rows": report.input_rows,
        "max_spill_bytes": report.max_spill_bytes,
        "max_spill_runs": report.max_spill_runs,
        "spilled_bytes": report.spilled_bytes,
        "spill_run_count": report.spill_run_count,
        "spilled_rows": report.spilled_rows,
    })
}

fn missing_nowledge_mem_bounded_read_routes(covered_routes: &[String]) -> Vec<&'static str> {
    let covered_routes = covered_routes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !covered_routes.contains(route))
        .collect()
}

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
    report: &crate::search::SearchPredicateFieldPruningReport,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadOutput {
    pub output: QueryOutput,
    pub report: NowledgeMemReadReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemQueryExecutionPath {
    FastPath,
    OptimizedPath,
}

impl NowledgeMemQueryExecutionPath {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FastPath => "fast_path",
            Self::OptimizedPath => "optimized_path",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub statement_kind: String,
    pub execution_path: NowledgeMemQueryExecutionPath,
    pub fast_path_reason: Option<String>,
    pub elapsed_micros: u128,
    pub slow_log_threshold_micros: Option<u128>,
    pub slow_log_candidate: bool,
    pub physical_plan_captured: bool,
    pub plan_cache_lookup: Option<String>,
    pub plan_cache_bypass_reason: Option<String>,
    pub plan_cache_cacheable: bool,
    pub plan_cache_hit: bool,
    pub plan_cache_miss: bool,
    pub plan_cache_bypassed: bool,
    pub physical_operator_counts: BTreeMap<String, usize>,
    pub optimizer_decision_count: usize,
    pub optimizer_rule_event_count: usize,
    pub scan_pruning_reports: Vec<ScanPruningReport>,
    pub vector_execution_reports: Vec<skein_executor::VectorExecutionReport>,
    pub graph_expansion_reports: Vec<skein_executor::GraphExpansionExecutionReport>,
    pub pipeline_memory_report: Option<skein_executor::PipelineMemoryReport>,
    pub output_row_shape: NowledgeMemQueryOutputRowShape,
    pub api_behavior: NowledgeMemQueryApiBehavior,
}

impl NowledgeMemQueryReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "statement_kind": self.statement_kind,
            "execution_path": self.execution_path.as_str(),
            "fast_path_reason": self.fast_path_reason,
            "fast_path_selected": self.execution_path == NowledgeMemQueryExecutionPath::FastPath,
            "elapsed_micros": self.elapsed_micros,
            "slow_log_threshold_micros": self.slow_log_threshold_micros,
            "slow_log_candidate": self.slow_log_candidate,
            "physical_plan_captured": self.physical_plan_captured,
            "plan_cache_lookup": self.plan_cache_lookup,
            "plan_cache_bypass_reason": self.plan_cache_bypass_reason,
            "plan_cache_cacheable": self.plan_cache_cacheable,
            "plan_cache_hit": self.plan_cache_hit,
            "plan_cache_miss": self.plan_cache_miss,
            "plan_cache_bypassed": self.plan_cache_bypassed,
            "plan_cache": {
                "lookup": self.plan_cache_lookup,
                "bypass_reason": self.plan_cache_bypass_reason,
                "cacheable": self.plan_cache_cacheable,
                "hit": self.plan_cache_hit,
                "miss": self.plan_cache_miss,
                "bypassed": self.plan_cache_bypassed,
            },
            "physical_operator_counts": self.physical_operator_counts,
            "optimizer_decision_count": self.optimizer_decision_count,
            "optimizer_rule_event_count": self.optimizer_rule_event_count,
            "scan_pruning_report_count": self.scan_pruning_reports.len(),
            "scan_pruning_reports": self.scan_pruning_reports.iter().map(scan_pruning_report_json).collect::<Vec<_>>(),
            "vector_execution_report_count": self.vector_execution_reports.len(),
            "vector_execution_reports": self.vector_execution_reports.iter().map(vector_execution_report_json).collect::<Vec<_>>(),
            "graph_expansion_report_count": self.graph_expansion_reports.len(),
            "graph_expansion_reports": self.graph_expansion_reports.iter().map(graph_expansion_report_json).collect::<Vec<_>>(),
            "pipeline_memory_report": self.pipeline_memory_report.as_ref().map(pipeline_memory_report_json),
            "output_row_shape": self.output_row_shape.json(),
            "api_behavior": self.api_behavior.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryOutputRowShape {
    pub row_count: usize,
    pub column_count: usize,
    pub columns: Vec<String>,
}

impl NowledgeMemQueryOutputRowShape {
    fn from_output(output: &QueryOutput) -> Self {
        let columns = output
            .schema()
            .columns()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        Self {
            row_count: output.rows.len(),
            column_count: columns.len(),
            columns,
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": self.columns,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryApiBehavior {
    pub include_metadata_false_strips_metadata: bool,
    pub ordering_contract_recorded: bool,
    pub pagination_contract_recorded: bool,
    pub error_class_stable: bool,
    pub statement_has_ordering: bool,
    pub statement_has_pagination: bool,
}

impl NowledgeMemQueryApiBehavior {
    fn from_statement(statement: &cypher::Statement) -> Self {
        let body = nowledge_statement_body(statement);
        Self {
            include_metadata_false_strips_metadata: true,
            ordering_contract_recorded: true,
            pagination_contract_recorded: true,
            error_class_stable: true,
            statement_has_ordering: statement_has_ordering(body),
            statement_has_pagination: statement_has_pagination(body),
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "include_metadata_false_strips_metadata": self.include_metadata_false_strips_metadata,
            "ordering_contract_recorded": self.ordering_contract_recorded,
            "pagination_contract_recorded": self.pagination_contract_recorded,
            "error_class_stable": self.error_class_stable,
            "statement_has_ordering": self.statement_has_ordering,
            "statement_has_pagination": self.statement_has_pagination,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemQueryOutput {
    pub output: QueryOutput,
    pub report: NowledgeMemQueryReport,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NowledgeMemQueryReportOptions {
    pub capture_physical_plan: bool,
    pub slow_log_threshold_micros: Option<u128>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSlowQueryRecord {
    pub sequence: u64,
    pub query_language: String,
    pub statement_kind: String,
    pub query_digest: String,
    pub started_unix_micros: i64,
    pub elapsed_micros: i64,
    pub row_count: i64,
    pub success: bool,
    pub slow_log_candidate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemSlowQueryReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub present: bool,
    pub ready: bool,
    pub capacity: usize,
    pub threshold_micros: u128,
    pub record_count: usize,
    pub latest_sequence: Option<u64>,
    pub max_elapsed_micros: Option<i64>,
    pub total_row_count: i64,
    pub records: Vec<NowledgeMemSlowQueryRecord>,
}

impl NowledgeMemSlowQueryReport {
    fn from_summaries(
        mode: NowledgeMemGraphMode,
        capacity: usize,
        threshold_micros: u128,
        records: Vec<SlowQueryLogRecordSummary>,
    ) -> Self {
        let records = records
            .into_iter()
            .map(|record| NowledgeMemSlowQueryRecord {
                sequence: record.sequence,
                query_language: record.query_language,
                statement_kind: record.statement_kind,
                query_digest: record.query_digest,
                started_unix_micros: record.started_unix_micros,
                elapsed_micros: record.elapsed_micros,
                row_count: record.row_count,
                success: record.success,
                slow_log_candidate: record.slow_log_candidate,
            })
            .collect::<Vec<_>>();
        let latest_sequence = records.iter().map(|record| record.sequence).max();
        let max_elapsed_micros = records.iter().map(|record| record.elapsed_micros).max();
        let total_row_count = records
            .iter()
            .map(|record| record.row_count)
            .fold(0i64, i64::saturating_add);

        Self {
            protocol: NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL.to_string(),
            mode,
            present: true,
            ready: true,
            capacity,
            threshold_micros,
            record_count: records.len(),
            latest_sequence,
            max_elapsed_micros,
            total_row_count,
            records,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "present": self.present,
            "ready": self.ready,
            "capacity": self.capacity,
            "threshold_micros": self.threshold_micros,
            "record_count": self.record_count,
            "latest_sequence": self.latest_sequence,
            "max_elapsed_micros": self.max_elapsed_micros,
            "total_row_count": self.total_row_count,
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            },
            "records": self.records.iter().map(|record| {
                serde_json::json!({
                    "sequence": record.sequence,
                    "query_language": record.query_language,
                    "statement_kind": record.statement_kind,
                    "query_digest": record.query_digest,
                    "started_unix_micros": record.started_unix_micros,
                    "elapsed_micros": record.elapsed_micros,
                    "row_count": record.row_count,
                    "success": record.success,
                    "slow_log_candidate": record.slow_log_candidate,
                })
            }).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NowledgeMemReadinessOptions {
    pub bounded_read_probe: Option<NowledgeGraphStatement>,
    pub bounded_read_evidence: Option<serde_json::Value>,
    pub covered_routes: Vec<String>,
    pub graph_route_readiness: Option<NowledgeMemRouteReadinessSummary>,
    pub search_route_ownership: Option<NowledgeMemSearchRouteOwnershipReadinessReport>,
    pub active_search_route_ownership: Option<NowledgeMemActiveSearchRouteOwnershipReadinessReport>,
    pub active_search_route_readiness: Option<NowledgeMemActiveSearchRouteReadinessReport>,
    pub replacement_readiness_by_query_family: Option<serde_json::Value>,
    pub read_options: NowledgeMemReadOptions,
    pub search_projection_evidence: Option<serde_json::Value>,
    pub search_projection_probe_options: SearchProjectionProbeOptions,
    pub primary_search_projection_probe: Option<serde_json::Value>,
    pub search_projection_shadow_evidence: Option<serde_json::Value>,
    pub search_candidate_shadow_evidence: Option<serde_json::Value>,
    pub workload_fixture_evidence: Option<NowledgeGraphRouteWorkloadFixtureReport>,
    pub production_resource_profile: Option<StorageResourceProfileReport>,
    pub qos_policy: LocalQosPolicy,
    pub qos_state: LocalQosState,
    pub background_maintenance_options: BackgroundMaintenanceOptions,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemLibraryReadinessReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub mode: NowledgeMemGraphMode,
    pub redaction: NowledgeMemReadinessRedactionSummary,
    pub production_path: NowledgeMemLibraryProductionPathSummary,
    pub blocker_codes: Vec<String>,
    pub readiness_by_area: NowledgeMemReadinessAreaMap,
    pub ready_area_count: usize,
    pub blocked_area_count: usize,
    pub graph_open: bool,
    pub graph_read_only: bool,
    pub graph_route_readiness: serde_json::Value,
    pub search_route_ownership: serde_json::Value,
    pub active_search_route_ownership: serde_json::Value,
    pub active_search_route_readiness: serde_json::Value,
    pub bounded_read_evidence: serde_json::Value,
    pub storage_recovery: serde_json::Value,
    pub background_maintenance: serde_json::Value,
    pub query_family_evidence: serde_json::Value,
    pub search_projection_evidence: serde_json::Value,
    pub search_projection_shadow_evidence: serde_json::Value,
    pub search_candidate_shadow_evidence: serde_json::Value,
    pub workload_fixture_evidence: serde_json::Value,
    pub production_resource_profile: serde_json::Value,
}

impl NowledgeMemLibraryReadinessReport {
    pub fn areas(&self) -> Vec<NowledgeMemReadinessAreaSummary> {
        self.readiness_by_area.areas()
    }

    pub fn json(&self) -> serde_json::Value {
        let areas = self.areas();
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "mode": self.mode.as_str(),
            "redaction": self.redaction.json(),
            "production_path": self.production_path.json(),
            "blocker_codes": self.blocker_codes,
            "readiness_by_area": self.readiness_by_area.json(),
            "areas": areas.iter().map(NowledgeMemReadinessAreaSummary::json).collect::<Vec<_>>(),
            "ready_area_count": self.ready_area_count,
            "blocked_area_count": self.blocked_area_count,
            "graph": {
                "open": self.graph_open,
                "mode": self.mode.as_str(),
                "read_only": self.graph_read_only,
            },
            "graph_route_readiness": self.graph_route_readiness,
            "search_route_ownership": self.search_route_ownership,
            "active_search_route_ownership": self.active_search_route_ownership,
            "active_search_route_readiness": self.active_search_route_readiness,
            "bounded_read_evidence": self.bounded_read_evidence,
            "storage_recovery": self.storage_recovery,
            "background_maintenance": self.background_maintenance,
            "query_family_evidence": self.query_family_evidence,
            "search_projection_evidence": self.search_projection_evidence,
            "search_projection_shadow_evidence": self.search_projection_shadow_evidence,
            "search_candidate_shadow_evidence": self.search_candidate_shadow_evidence,
            "workload_fixture_evidence": self.workload_fixture_evidence,
            "production_resource_profile": self.production_resource_profile,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemLibraryProductionPathSummary {
    pub in_process: bool,
    pub cli_required: bool,
    pub env_control_plane_required: bool,
    pub spawned_helper_required: bool,
}

impl Default for NowledgeMemLibraryProductionPathSummary {
    fn default() -> Self {
        Self {
            in_process: true,
            cli_required: false,
            env_control_plane_required: false,
            spawned_helper_required: false,
        }
    }
}

impl NowledgeMemLibraryProductionPathSummary {
    pub fn ready(&self) -> bool {
        self.in_process
            && !self.cli_required
            && !self.env_control_plane_required
            && !self.spawned_helper_required
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "in_process": self.in_process,
            "cli_required": self.cli_required,
            "env_control_plane_required": self.env_control_plane_required,
            "spawned_helper_required": self.spawned_helper_required,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NowledgeMemReadinessRedactionSummary {
    pub query_text_copied: bool,
    pub parameters_copied: bool,
    pub local_paths_copied: bool,
}

impl NowledgeMemReadinessRedactionSummary {
    pub fn ready(&self) -> bool {
        !self.query_text_copied && !self.parameters_copied && !self.local_paths_copied
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready(),
            "query_text_copied": self.query_text_copied,
            "parameters_copied": self.parameters_copied,
            "local_paths_copied": self.local_paths_copied,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadinessDashboard {
    pub protocol: String,
    pub ready: bool,
    pub mode: NowledgeMemGraphMode,
    pub area_count: usize,
    pub ready_area_count: usize,
    pub blocked_area_count: usize,
    pub blocker_codes: Vec<String>,
    pub areas: Vec<NowledgeMemReadinessAreaSummary>,
    pub storage_lifecycle_action: NowledgeMemStorageLifecycleActionKind,
    pub storage_lifecycle_ready: bool,
    pub slow_query_ready: bool,
    pub slow_query_record_count: usize,
}

impl NowledgeMemReadinessDashboard {
    fn from_reports(
        library: &NowledgeMemLibraryReadinessReport,
        storage_lifecycle: &NowledgeMemStorageLifecycleDecision,
        slow_query: &NowledgeMemSlowQueryReport,
    ) -> Self {
        let areas = nowledge_mem_readiness_dashboard_areas(library, slow_query);
        let blocked_area_count = areas.iter().filter(|area| !area.ready).count();
        let ready_area_count = areas.len().saturating_sub(blocked_area_count);

        Self {
            protocol: NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL.to_string(),
            ready: library.ready && slow_query.ready,
            mode: library.mode,
            area_count: areas.len(),
            ready_area_count,
            blocked_area_count,
            blocker_codes: library.blocker_codes.clone(),
            areas,
            storage_lifecycle_action: storage_lifecycle.action,
            storage_lifecycle_ready: storage_lifecycle.ready_for_mem_lifecycle,
            slow_query_ready: slow_query.ready,
            slow_query_record_count: slow_query.record_count,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "mode": self.mode.as_str(),
            "area_count": self.area_count,
            "ready_area_count": self.ready_area_count,
            "blocked_area_count": self.blocked_area_count,
            "blocker_codes": self.blocker_codes,
            "areas": self.areas.iter().map(NowledgeMemReadinessAreaSummary::json).collect::<Vec<_>>(),
            "storage_lifecycle": {
                "ready": self.storage_lifecycle_ready,
                "action": self.storage_lifecycle_action.as_str(),
            },
            "slow_query": {
                "ready": self.slow_query_ready,
                "record_count": self.slow_query_record_count,
            },
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemOperationsReadinessReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub mode: NowledgeMemGraphMode,
    pub graph_open: bool,
    pub graph_read_only: bool,
    pub search_projection_open: bool,
    pub graph_commit_epoch: u64,
    pub projection_commit_lag: u64,
    pub projection_stale: bool,
    pub storage_lifecycle_action: NowledgeMemStorageLifecycleActionKind,
    pub storage_lifecycle_ready: bool,
    pub storage_recovery_ready: bool,
    pub slow_query_ready: bool,
    pub background_maintenance_ready: bool,
    pub blocker_codes: Vec<String>,
    pub runtime_status: NowledgeMemRuntimeStatus,
    pub storage_lifecycle_decision: NowledgeMemStorageLifecycleDecision,
    pub storage_recovery: NowledgeMemStorageRecoveryReport,
    pub slow_query: NowledgeMemSlowQueryReport,
    pub background_maintenance: NowledgeMemBackgroundMaintenanceReport,
}

impl NowledgeMemOperationsReadinessReport {
    fn from_reports(
        mode: NowledgeMemGraphMode,
        graph_read_only: bool,
        runtime_status: NowledgeMemRuntimeStatus,
        storage_recovery: NowledgeMemStorageRecoveryReport,
        slow_query: NowledgeMemSlowQueryReport,
        background_maintenance: NowledgeMemBackgroundMaintenanceReport,
    ) -> Self {
        let search_projection_open = runtime_status.projection_freshness.is_some();
        let projection_commit_lag = runtime_status.projection_commit_lag();
        let projection_stale = search_projection_open && runtime_status.projection_stale();
        let storage_lifecycle_decision =
            NowledgeMemStorageLifecycleDecision::from_storage_recovery(storage_recovery.clone());
        let mut blocker_codes = Vec::new();
        if !storage_lifecycle_decision.ready_for_mem_lifecycle {
            blocker_codes.push("storage_recovery_not_ready".to_string());
            blocker_codes.extend(
                storage_lifecycle_decision
                    .blocker_codes
                    .iter()
                    .map(|code| format!("storage_lifecycle.{code}")),
            );
        }
        if !slow_query.ready {
            blocker_codes.push("slow_query_report_not_ready".to_string());
        }
        if !background_maintenance.ready {
            blocker_codes.push("background_maintenance_not_ready".to_string());
        }
        if search_projection_open && projection_stale {
            blocker_codes.push("search_projection_stale".to_string());
        }

        Self {
            protocol: NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL.to_string(),
            present: true,
            ready: blocker_codes.is_empty(),
            mode,
            graph_open: true,
            graph_read_only,
            search_projection_open,
            graph_commit_epoch: runtime_status.graph_commit_epoch,
            projection_commit_lag,
            projection_stale,
            storage_lifecycle_action: storage_lifecycle_decision.action,
            storage_lifecycle_ready: storage_lifecycle_decision.ready_for_mem_lifecycle,
            storage_recovery_ready: storage_recovery.ready,
            slow_query_ready: slow_query.ready,
            background_maintenance_ready: background_maintenance.ready,
            blocker_codes,
            runtime_status,
            storage_lifecycle_decision,
            storage_recovery,
            slow_query,
            background_maintenance,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "mode": self.mode.as_str(),
            "graph": {
                "open": self.graph_open,
                "read_only": self.graph_read_only,
                "commit_epoch": self.graph_commit_epoch,
            },
            "search_projection": {
                "open": self.search_projection_open,
                "commit_lag": self.projection_commit_lag,
                "stale": self.projection_stale,
            },
            "storage_lifecycle": {
                "ready": self.storage_lifecycle_ready,
                "action": self.storage_lifecycle_action.as_str(),
            },
            "readiness": {
                "storage_lifecycle_ready": self.storage_lifecycle_ready,
                "storage_recovery_ready": self.storage_recovery_ready,
                "slow_query_ready": self.slow_query_ready,
                "background_maintenance_ready": self.background_maintenance_ready,
            },
            "blocker_codes": self.blocker_codes,
            "runtime_status": self.runtime_status.json(),
            "storage_lifecycle_decision": self.storage_lifecycle_decision.json(),
            "storage_recovery": self.storage_recovery.json(),
            "slow_query": self.slow_query.json(),
            "background_maintenance": self.background_maintenance.json(),
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemStorageRecoveryReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
    pub open_timings: StorageOpenTimings,
    pub durable: bool,
    pub recovery_mode: RecoveryMode,
    pub checkpoint_epoch: Option<u64>,
    pub checkpoint_commit_epoch: Option<u64>,
    pub wal_present: bool,
    pub wal_generation: Option<u64>,
    pub wal_replay_start_lsn: Option<u64>,
    pub next_lsn_after_replay: Option<u64>,
    pub replayed_wal_entries: usize,
    pub replayed_wal_bytes: u64,
    pub max_wal_replay_entries: Option<usize>,
    pub max_wal_replay_bytes: Option<u64>,
    pub max_wal_record_bytes: Option<usize>,
    pub torn_tail_ignored: bool,
    pub torn_tail_repaired: bool,
    pub discarded_wal_tail_bytes: u64,
    pub torn_tail_reason: Option<String>,
    pub recovered_commit_epoch: u64,
    pub durable_recovery_observed: bool,
    pub checkpoint_boundary_present: bool,
    pub wal_replay_bounded: bool,
    pub replay_boundary_consistent: bool,
    pub torn_tail_clean: bool,
    pub open_timing_consistent: bool,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemStorageRecoveryReport {
    pub fn from_storage_report(report: &StorageRecoveryReport) -> Self {
        let durable_recovery_observed = report.durable;
        let checkpoint_boundary_present =
            report.checkpoint_epoch.is_some() && report.checkpoint_commit_epoch.is_some();
        let wal_replay_bounded = report
            .max_wal_replay_entries
            .is_some_and(|limit| report.replayed_wal_entries <= limit)
            && report
                .max_wal_replay_bytes
                .is_some_and(|limit| report.replayed_wal_bytes <= limit)
            && report.max_wal_record_bytes.is_some();
        let replay_boundary_consistent = storage_recovery_replay_boundary_consistent(report);
        let torn_tail_clean = (!report.torn_tail_ignored && report.torn_tail_reason.is_none())
            || report.torn_tail_repaired;
        let open_timing_consistent = report.open_timings.is_consistent();
        let mut blocker_codes = Vec::new();
        if !durable_recovery_observed {
            blocker_codes.push("durable_recovery_not_observed".to_string());
        }
        if !checkpoint_boundary_present {
            blocker_codes.push("checkpoint_boundary_missing".to_string());
        }
        if !wal_replay_bounded {
            blocker_codes.push("wal_replay_unbounded".to_string());
        }
        if !replay_boundary_consistent {
            blocker_codes.push("replay_boundary_inconsistent".to_string());
        }
        if !torn_tail_clean {
            blocker_codes.push("torn_tail_observed".to_string());
        }
        if !open_timing_consistent {
            blocker_codes.push("storage_open_timing_inconsistent".to_string());
        }

        Self {
            protocol: "skein-storage-recovery-report".to_string(),
            present: true,
            ready: blocker_codes.is_empty(),
            open_timings: report.open_timings,
            durable: report.durable,
            recovery_mode: report.recovery_mode,
            checkpoint_epoch: report.checkpoint_epoch,
            checkpoint_commit_epoch: report.checkpoint_commit_epoch,
            wal_present: report.wal_present,
            wal_generation: report.wal_generation,
            wal_replay_start_lsn: report.wal_replay_start_lsn,
            next_lsn_after_replay: report.next_lsn_after_replay,
            replayed_wal_entries: report.replayed_wal_entries,
            replayed_wal_bytes: report.replayed_wal_bytes,
            max_wal_replay_entries: report.max_wal_replay_entries,
            max_wal_replay_bytes: report.max_wal_replay_bytes,
            max_wal_record_bytes: report.max_wal_record_bytes,
            torn_tail_ignored: report.torn_tail_ignored,
            torn_tail_repaired: report.torn_tail_repaired,
            discarded_wal_tail_bytes: report.discarded_wal_tail_bytes,
            torn_tail_reason: report.torn_tail_reason.clone(),
            recovered_commit_epoch: report.recovered_commit_epoch,
            durable_recovery_observed,
            checkpoint_boundary_present,
            wal_replay_bounded,
            replay_boundary_consistent,
            torn_tail_clean,
            open_timing_consistent,
            blocker_codes,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "open_timings": {
                "durable_manifest_open_micros": self.open_timings.durable_manifest_open_micros,
                "checkpoint_root_open_micros": self.open_timings.checkpoint_root_open_micros,
                "wal_replay_micros": self.open_timings.wal_replay_micros,
                "post_replay_open_micros": self.open_timings.post_replay_open_micros,
                "accounted_micros": self.open_timings.accounted_micros(),
                "unaccounted_micros": self.open_timings.unaccounted_micros(),
                "total_open_micros": self.open_timings.total_open_micros,
            },
            "durable": self.durable,
            "recovery_mode": recovery_mode_name(self.recovery_mode),
            "checkpoint_epoch": self.checkpoint_epoch,
            "checkpoint_commit_epoch": self.checkpoint_commit_epoch,
            "wal_present": self.wal_present,
            "wal_generation": self.wal_generation,
            "wal_replay_start_lsn": self.wal_replay_start_lsn,
            "next_lsn_after_replay": self.next_lsn_after_replay,
            "replayed_wal_entries": self.replayed_wal_entries,
            "replayed_wal_bytes": self.replayed_wal_bytes,
            "max_wal_replay_entries": self.max_wal_replay_entries,
            "max_wal_replay_bytes": self.max_wal_replay_bytes,
            "max_wal_record_bytes": self.max_wal_record_bytes,
            "torn_tail_ignored": self.torn_tail_ignored,
            "torn_tail_repaired": self.torn_tail_repaired,
            "discarded_wal_tail_bytes": self.discarded_wal_tail_bytes,
            "torn_tail_reason": self.torn_tail_reason,
            "recovered_commit_epoch": self.recovered_commit_epoch,
            "readiness": {
                "durable_recovery_observed": self.durable_recovery_observed,
                "checkpoint_boundary_present": self.checkpoint_boundary_present,
                "wal_replay_bounded": self.wal_replay_bounded,
                "replay_boundary_consistent": self.replay_boundary_consistent,
                "torn_tail_clean": self.torn_tail_clean,
                "open_timing_consistent": self.open_timing_consistent,
            },
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemStorageLifecycleActionKind {
    Ready,
    RunCheckpoint,
    RepairWalTail,
    Quarantine,
    OpenReadOnlyInspect,
}

impl NowledgeMemStorageLifecycleActionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::RunCheckpoint => "run_checkpoint",
            Self::RepairWalTail => "repair_wal_tail",
            Self::Quarantine => "quarantine",
            Self::OpenReadOnlyInspect => "open_read_only_inspect",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemStorageLifecycleDecision {
    pub protocol: String,
    pub action: NowledgeMemStorageLifecycleActionKind,
    pub ready_for_mem_lifecycle: bool,
    pub storage_recovery_ready: bool,
    pub checkpoint_required: bool,
    pub repair_required: bool,
    pub quarantine_required: bool,
    pub read_only_inspection_required: bool,
    pub blocker_codes: Vec<String>,
    pub recovery: NowledgeMemStorageRecoveryReport,
}

impl NowledgeMemStorageLifecycleDecision {
    pub fn from_storage_recovery(recovery: NowledgeMemStorageRecoveryReport) -> Self {
        let mut blocker_codes = recovery.blocker_codes.clone();
        let action = if recovery.ready {
            NowledgeMemStorageLifecycleActionKind::Ready
        } else if !recovery.durable_recovery_observed {
            push_unique_blocker(&mut blocker_codes, "storage_not_durable");
            NowledgeMemStorageLifecycleActionKind::OpenReadOnlyInspect
        } else if !recovery.torn_tail_clean {
            push_unique_blocker(&mut blocker_codes, "wal_tail_repair_required");
            NowledgeMemStorageLifecycleActionKind::RepairWalTail
        } else if !recovery.checkpoint_boundary_present {
            push_unique_blocker(&mut blocker_codes, "checkpoint_required");
            NowledgeMemStorageLifecycleActionKind::RunCheckpoint
        } else if !recovery.replay_boundary_consistent || !recovery.wal_replay_bounded {
            push_unique_blocker(&mut blocker_codes, "storage_recovery_quarantine_required");
            NowledgeMemStorageLifecycleActionKind::Quarantine
        } else {
            push_unique_blocker(&mut blocker_codes, "storage_recovery_unknown_blocker");
            NowledgeMemStorageLifecycleActionKind::Quarantine
        };

        Self {
            protocol: NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL.to_string(),
            ready_for_mem_lifecycle: action == NowledgeMemStorageLifecycleActionKind::Ready,
            storage_recovery_ready: recovery.ready,
            checkpoint_required: action == NowledgeMemStorageLifecycleActionKind::RunCheckpoint,
            repair_required: action == NowledgeMemStorageLifecycleActionKind::RepairWalTail,
            quarantine_required: action == NowledgeMemStorageLifecycleActionKind::Quarantine,
            read_only_inspection_required: action
                == NowledgeMemStorageLifecycleActionKind::OpenReadOnlyInspect,
            action,
            blocker_codes,
            recovery,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "action": self.action.as_str(),
            "ready_for_mem_lifecycle": self.ready_for_mem_lifecycle,
            "storage_recovery_ready": self.storage_recovery_ready,
            "checkpoint_required": self.checkpoint_required,
            "repair_required": self.repair_required,
            "quarantine_required": self.quarantine_required,
            "read_only_inspection_required": self.read_only_inspection_required,
            "blocker_codes": self.blocker_codes,
            "recovery": self.recovery.json(),
        })
    }
}

fn push_unique_blocker(blocker_codes: &mut Vec<String>, code: &str) {
    if !blocker_codes.iter().any(|existing| existing == code) {
        blocker_codes.push(code.to_string());
    }
}

fn storage_recovery_replay_boundary_consistent(report: &StorageRecoveryReport) -> bool {
    let Some(checkpoint_commit_epoch) = report.checkpoint_commit_epoch else {
        return false;
    };
    let Some(wal_replay_start_lsn) = report.wal_replay_start_lsn else {
        return false;
    };
    let Some(next_lsn_after_replay) = report.next_lsn_after_replay else {
        return false;
    };
    let Ok(replayed_wal_entries) = u64::try_from(report.replayed_wal_entries) else {
        return false;
    };
    checkpoint_commit_epoch <= report.recovered_commit_epoch
        && wal_replay_start_lsn.checked_add(replayed_wal_entries) == Some(next_lsn_after_replay)
        && checkpoint_commit_epoch.checked_add(replayed_wal_entries)
            == Some(report.recovered_commit_epoch)
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemBackgroundMaintenanceReport {
    pub protocol: String,
    pub present: bool,
    pub ready: bool,
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
    pub foreground_admission_probe_ready: Option<bool>,
    pub foreground_admission_probe_admission_name: Option<String>,
    pub memory_pressure_ready: Option<bool>,
    pub memory_budget_bytes: Option<u64>,
    pub estimated_memory_bytes: Option<u64>,
    pub slow_query_ready: Option<bool>,
    pub slow_query_record_count: Option<u64>,
    pub slow_query_capacity: Option<u64>,
    pub slow_query_redaction_ready: Option<bool>,
    pub top_admitted_kind: Option<BackgroundMaintenanceKind>,
    pub top_admitted_name: Option<String>,
    pub ranked_count: u64,
    pub foreground_ranked_count: u64,
    pub unknown_admission_count: u64,
    pub blocker_codes: Vec<String>,
    summary: serde_json::Value,
}

impl NowledgeMemBackgroundMaintenanceReport {
    pub fn from_summary(summary: &BackgroundMaintenanceSummary) -> Self {
        Self::from_summary_with_slow_query(summary, None)
    }

    fn from_summary_with_slow_query(
        summary: &BackgroundMaintenanceSummary,
        slow_query: Option<&NowledgeMemSlowQueryReport>,
    ) -> Self {
        let mut json = background_maintenance_summary_to_json(summary);
        if let Some(object) = json.as_object_mut() {
            object.insert(
                "protocol".to_string(),
                serde_json::Value::String("skein-background-maintenance-report".to_string()),
            );
            if let Some(slow_query) = slow_query {
                object.insert(
                    "slow_query".to_string(),
                    background_maintenance_slow_query_json(slow_query),
                );
                object.insert(
                    "slow_query_ready".to_string(),
                    serde_json::Value::Bool(slow_query.ready),
                );
                object.insert(
                    "slow_query_record_count".to_string(),
                    serde_json::json!(slow_query.record_count),
                );
                object.insert(
                    "slow_query_capacity".to_string(),
                    serde_json::json!(slow_query.capacity),
                );
            }
        }
        let health = background_maintenance_evidence_health(Some(&json), true);

        Self {
            protocol: "skein-background-maintenance-report".to_string(),
            present: true,
            ready: health.ready,
            total_candidates: summary.total_candidates,
            admitted_count: summary.admitted_count,
            deferred_count: summary.deferred_count,
            rejected_count: summary.rejected_count,
            total_estimated_operations: summary.total_estimated_operations,
            admitted_estimated_operations: summary.admitted_estimated_operations,
            deferred_estimated_operations: summary.deferred_estimated_operations,
            rejected_estimated_operations: summary.rejected_estimated_operations,
            executable_search_projection_graph_delta_count: summary
                .executable_search_projection_graph_delta_count,
            admitted_search_projection_graph_delta_count: summary
                .admitted_search_projection_graph_delta_count,
            deferred_search_projection_graph_delta_count: summary
                .deferred_search_projection_graph_delta_count,
            rejected_search_projection_graph_delta_count: summary
                .rejected_search_projection_graph_delta_count,
            executable_search_projection_graph_delta_operations: summary
                .executable_search_projection_graph_delta_operations,
            admitted_search_projection_graph_delta_operations: summary
                .admitted_search_projection_graph_delta_operations,
            max_search_projection_graph_delta_complete_through_graph_commit_epoch: summary
                .max_search_projection_graph_delta_complete_through_graph_commit_epoch,
            foreground_admission_probe_ready: health.foreground_admission_probe_ready,
            foreground_admission_probe_admission_name: health
                .foreground_admission_probe_admission_name,
            memory_pressure_ready: health.memory_pressure_ready,
            memory_budget_bytes: health.memory_budget_bytes,
            estimated_memory_bytes: health.estimated_memory_bytes,
            slow_query_ready: health.slow_query_ready,
            slow_query_record_count: health.slow_query_record_count,
            slow_query_capacity: health.slow_query_capacity,
            slow_query_redaction_ready: health.slow_query_redaction_ready,
            top_admitted_kind: summary.top_admitted_kind,
            top_admitted_name: summary.top_admitted_name.clone(),
            ranked_count: health.ranked_count.unwrap_or_default(),
            foreground_ranked_count: health.foreground_ranked_count,
            unknown_admission_count: health.unknown_admission_count,
            blocker_codes: health.blocker_codes,
            summary: json,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        self.summary.clone()
    }
}

fn background_maintenance_slow_query_json(
    slow_query: &NowledgeMemSlowQueryReport,
) -> serde_json::Value {
    serde_json::json!({
        "protocol": slow_query.protocol,
        "ready": slow_query.ready,
        "capacity": slow_query.capacity,
        "record_count": slow_query.record_count,
        "latest_sequence": slow_query.latest_sequence,
        "max_elapsed_micros": slow_query.max_elapsed_micros,
        "redaction": {
            "query_text_copied": false,
            "parameters_copied": false,
            "local_paths_copied": false,
        },
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemRetrievalReport {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub graph_commit_epoch: u64,
    pub projection_source_graph_commit_epoch: Option<u64>,
    pub projection_commit_lag: u64,
    pub projection_stale: bool,
    pub search_document_count: usize,
    pub search_filtered_document_count: usize,
    pub search_total_hits: usize,
    pub candidate_count: usize,
    pub candidate_total_count: usize,
    pub evidence_count: usize,
    pub graph_seed_count: usize,
    pub graph_context_path_count: usize,
    pub search_backend: Option<String>,
    pub vector_backend: Option<String>,
    pub text_backend: Option<String>,
    pub search_fallback_reason_codes: Vec<String>,
    pub retriever_fallback_reason_codes: Vec<String>,
    pub knowledge_fallback_reason_codes: Vec<String>,
    pub truncation_reason_codes: Vec<String>,
    pub warning_count: usize,
    pub warnings: Vec<String>,
}

impl NowledgeMemRetrievalReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "compressed_vector_search_mode": self.compressed_vector_search_mode.as_str(),
            "graph_commit_epoch": self.graph_commit_epoch,
            "projection_source_graph_commit_epoch": self.projection_source_graph_commit_epoch,
            "projection_commit_lag": self.projection_commit_lag,
            "projection_stale": self.projection_stale,
            "search_document_count": self.search_document_count,
            "search_filtered_document_count": self.search_filtered_document_count,
            "search_total_hits": self.search_total_hits,
            "candidate_count": self.candidate_count,
            "candidate_total_count": self.candidate_total_count,
            "evidence_count": self.evidence_count,
            "graph_seed_count": self.graph_seed_count,
            "graph_context_path_count": self.graph_context_path_count,
            "search_backend": self.search_backend,
            "vector_backend": self.vector_backend,
            "text_backend": self.text_backend,
            "search_fallback_reason_codes": self.search_fallback_reason_codes,
            "retriever_fallback_reason_codes": self.retriever_fallback_reason_codes,
            "knowledge_fallback_reason_codes": self.knowledge_fallback_reason_codes,
            "truncation_reason_codes": self.truncation_reason_codes,
            "warning_count": self.warning_count,
            "warnings": self.warnings,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemRetrievalOutput {
    pub output: KnowledgeRetrievalOutput,
    pub report: NowledgeMemRetrievalReport,
    pub out_of_core_search_metrics: Option<SearchOutOfCoreMetrics>,
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

impl NowledgeMemGraph {
    pub fn open(path: impl AsRef<Path>, mode: NowledgeMemGraphMode) -> Result<Self> {
        let path = path.as_ref();
        Self::open_with_config(path, nowledge_mem_graph_config(mode))
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        let path = path.as_ref();
        let runtime_governor = default_nowledge_mem_runtime_governor(path, &config);
        Self::open_with_config_and_runtime_governor(path, config, runtime_governor)
    }

    pub fn open_with_config_and_runtime_governor(
        path: impl AsRef<Path>,
        config: DatabaseConfig,
        runtime_governor: RuntimeGovernor,
    ) -> Result<Self> {
        let mode = if config.read_only {
            NowledgeMemGraphMode::ShadowReadOnly
        } else {
            NowledgeMemGraphMode::WritableCutover
        };
        let mut db = Database::open_with_config(path, config)?;
        db.set_runtime_governor(runtime_governor.clone());
        Ok(Self {
            db,
            mode,
            runtime_governor,
        })
    }

    pub fn from_database(db: Database, mode: NowledgeMemGraphMode) -> Self {
        let runtime_governor = default_nowledge_mem_runtime_governor(Path::new("."), db.config());
        Self::from_database_with_runtime_governor(db, mode, runtime_governor)
    }

    pub fn from_database_with_runtime_governor(
        mut db: Database,
        mode: NowledgeMemGraphMode,
        runtime_governor: RuntimeGovernor,
    ) -> Self {
        db.set_runtime_governor(runtime_governor.clone());
        Self {
            db,
            mode,
            runtime_governor,
        }
    }

    pub fn mode(&self) -> NowledgeMemGraphMode {
        self.mode
    }

    pub fn database(&self) -> &Database {
        &self.db
    }

    pub fn database_mut(&mut self) -> &mut Database {
        &mut self.db
    }

    pub fn runtime_governor(&self) -> &RuntimeGovernor {
        &self.runtime_governor
    }

    pub fn runtime_governor_snapshot(&self) -> RuntimeGovernorSnapshot {
        self.runtime_governor.snapshot()
    }

    pub fn refresh_runtime_resources(&self) -> bool {
        self.runtime_governor.refresh_from_host()
    }

    pub fn into_database(self) -> Database {
        self.db
    }

    pub fn query(&mut self, cypher: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher, &BTreeMap::new())
    }

    pub fn query_with_report(&mut self, cypher: &str) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report(cypher, &BTreeMap::new())
    }

    pub fn query_with_report_options(
        &mut self,
        cypher: &str,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report_options(cypher, &BTreeMap::new(), options)
    }

    pub fn query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        Ok(self
            .query_with_params_with_report(cypher, parameters)?
            .output)
    }

    pub fn query_with_params_with_report(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report_options(
            cypher,
            parameters,
            NowledgeMemQueryReportOptions::default(),
        )
    }

    pub fn query_with_params_with_report_options(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        let mut external = crate::executor::NoExternalReadOperator;
        self.query_with_params_with_report_options_and_external(
            cypher,
            parameters,
            options,
            &mut external,
        )
    }

    pub fn query_with_params_with_report_options_context(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
        task_context: &RuntimeTaskContext,
    ) -> Result<NowledgeMemQueryOutput> {
        let mut external = crate::executor::NoExternalReadOperator;
        self.query_with_params_with_report_options_and_external_context(
            cypher,
            parameters,
            options,
            &mut external,
            task_context,
        )
    }

    fn query_with_params_with_report_options_and_external(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
        external: &mut dyn crate::executor::ExternalReadOperator,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report_options_and_external_context(
            cypher,
            parameters,
            options,
            external,
            &RuntimeTaskContext::default(),
        )
    }

    fn query_with_params_with_report_options_and_external_context(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
        external: &mut dyn crate::executor::ExternalReadOperator,
        task_context: &RuntimeTaskContext,
    ) -> Result<NowledgeMemQueryOutput> {
        self.check_runtime_context(task_context)?;
        let (permit, is_mutation) = self.admit_materialized_query(cypher, parameters)?;
        let execution_task_context = task_context.clone().with_admitted_parallelism(
            NonZeroUsize::new(permit.request().cpu_slots).unwrap_or(NonZeroUsize::MIN),
        );
        let started = Instant::now();
        let result = self.db.query_with_params_trace_and_external_with_context(
            cypher,
            parameters,
            options.capture_physical_plan,
            external,
            None,
            Some(&execution_task_context),
        );
        self.record_runtime_cancellation(result.as_ref().err(), task_context);
        let (output, execution_trace) = result?;
        if !is_mutation {
            self.check_runtime_context(task_context)?;
        }
        let elapsed_micros = started.elapsed().as_micros();
        let report = nowledge_mem_query_report(NowledgeMemQueryReportInput {
            mode: self.mode,
            statement: &execution_trace.statement,
            trace: execution_trace.optimizer_trace.as_ref(),
            plan_cache_lookup: execution_trace.plan_cache_lookup,
            execution_profile: execution_trace.execution_profile.as_ref(),
            output: &output,
            options,
            elapsed_micros,
        });
        Ok(NowledgeMemQueryOutput { output, report })
    }

    fn admit_materialized_query(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<(RuntimePermit, bool)> {
        let admission = self.db.runtime_admission_plan(cypher, parameters)?;
        let is_mutation = admission.is_mutation;
        let governor_budget = self.runtime_governor.snapshot().limits.result_budget_bytes;
        let result_budget = if admission.is_mutation {
            governor_budget
        } else {
            let configured = self
                .db
                .config()
                .max_read_result_payload_bytes
                .ok_or_else(|| {
                    SkeinError::Execution(
                        "admitted materialized query requires max_read_result_payload_bytes"
                            .to_string(),
                    )
                })?;
            let configured = u64::try_from(configured).unwrap_or(u64::MAX);
            if configured > governor_budget {
                return Err(SkeinError::Execution(format!(
                    "admitted materialized query payload budget {configured} exceeds runtime result budget {governor_budget}"
                )));
            }
            configured
        };
        self.try_admit_runtime(
            admission
                .runtime_work_request_for_snapshot(result_budget, self.runtime_governor.snapshot()),
        )
        .map(|permit| (permit, is_mutation))
    }

    fn admit_streaming_query(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        result_budget_bytes: usize,
    ) -> Result<RuntimePermit> {
        let admission = self.db.runtime_admission_plan(cypher, parameters)?;
        if admission.is_mutation {
            return Err(SkeinError::Execution(
                "admitted streaming query must be read-only".to_string(),
            ));
        }
        self.try_admit_runtime(admission.runtime_work_request_for_snapshot(
            u64::try_from(result_budget_bytes).unwrap_or(u64::MAX),
            self.runtime_governor.snapshot(),
        ))
    }

    fn try_admit_runtime(&self, request: skein_qos::RuntimeWorkRequest) -> Result<RuntimePermit> {
        self.runtime_governor.try_admit(request).map_err(|error| {
            if error.is_retryable() {
                self.runtime_governor
                    .record_admission_wait(request, error.code);
            }
            SkeinError::Execution(error.to_string())
        })
    }

    fn admitted_streaming_result_bytes(&self, options: &NowledgeMemReadOptions) -> Result<usize> {
        let governor_budget =
            usize::try_from(self.runtime_governor.snapshot().limits.result_budget_bytes)
                .unwrap_or(usize::MAX);
        let configured = self
            .db
            .config()
            .max_read_result_payload_bytes
            .unwrap_or(governor_budget);
        let requested = options
            .max_estimated_payload_bytes
            .unwrap_or(governor_budget);
        let admitted = governor_budget.min(configured).min(requested);
        if admitted == 0 {
            return Err(SkeinError::Execution(
                "admitted streaming query requires a non-zero result byte budget".to_string(),
            ));
        }
        Ok(admitted)
    }

    fn check_runtime_context(&self, task_context: &RuntimeTaskContext) -> Result<()> {
        task_context.checkpoint().map_err(|reason| {
            self.runtime_governor.record_cancellation(reason);
            SkeinError::Execution(format!("runtime task {reason}"))
        })
    }

    fn record_runtime_cancellation(
        &self,
        error: Option<&SkeinError>,
        task_context: &RuntimeTaskContext,
    ) {
        if error.is_some()
            && let Err(reason) = task_context.checkpoint()
        {
            self.runtime_governor.record_cancellation(reason);
        }
    }

    pub fn slow_query_report(&self) -> NowledgeMemSlowQueryReport {
        let config = self.db.config();
        NowledgeMemSlowQueryReport::from_summaries(
            self.mode,
            config.slow_query_log_capacity,
            config.slow_query_log_threshold_micros,
            self.db.slow_query_log_snapshot(),
        )
    }

    pub fn slow_query_report_json(&self) -> serde_json::Value {
        self.slow_query_report().json()
    }

    pub fn read_query(&self, cypher: &str) -> Result<NowledgeMemReadOutput> {
        self.read_query_with_params(cypher, &BTreeMap::new(), &NowledgeMemReadOptions::default())
    }

    pub fn read_query_with_options(
        &self,
        cypher: &str,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.read_query_with_params(cypher, &BTreeMap::new(), options)
    }

    pub fn read_query_with_params(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        let mut output = self
            .read_query_with_params_streaming_collect(cypher, parameters, options)
            .map_err(|error| legacy_nowledge_mem_read_error(error, options))?;
        output.report.streaming = false;
        Ok(output)
    }

    pub fn read_query_with_params_streaming(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
        consumer: impl FnMut(BTreeMap<String, Value>) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.read_query_with_params_streaming_context(
            cypher,
            parameters,
            options,
            &RuntimeTaskContext::default(),
            consumer,
        )
    }

    pub fn read_query_with_params_streaming_context(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
        task_context: &RuntimeTaskContext,
        consumer: impl FnMut(BTreeMap<String, Value>) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.check_runtime_context(task_context)?;
        let max_payload_bytes = self.admitted_streaming_result_bytes(options)?;
        let permit = self.admit_streaming_query(cypher, parameters, max_payload_bytes)?;
        let execution_task_context = task_context.clone().with_admitted_parallelism(
            NonZeroUsize::new(permit.request().cpu_slots).unwrap_or(NonZeroUsize::MIN),
        );
        let result = self
            .db
            .begin_read_transaction()
            .query_with_params_streaming_context(
                cypher,
                parameters,
                QueryStreamOptions {
                    max_rows: options.max_rows,
                    max_payload_bytes: Some(max_payload_bytes),
                },
                &execution_task_context,
                consumer,
            );
        self.record_runtime_cancellation(result.as_ref().err(), task_context);
        result
    }

    /// Collects host-owned rows through the streaming consumer boundary.
    /// This retains the legacy `NowledgeMemReadOutput` shape while avoiding a
    /// second executor-owned result vector and enforcing payload bytes before
    /// each row crosses into the host.
    pub fn read_query_with_params_streaming_collect(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        let mut rows = Vec::new();
        let streamed =
            self.read_query_with_params_streaming(cypher, parameters, options, |row| {
                rows.push(row);
                Ok(())
            })?;
        streamed_nowledge_mem_read_output(self.mode, rows, streamed, options)
    }

    pub fn graph_rag_schema_context(
        &self,
        options: GraphRagSchemaContextOptions,
    ) -> GraphRagSchemaContext {
        self.db.graph_rag_schema_context(options)
    }

    pub fn read_generated_graph_rag(
        &self,
        query: &GraphRagGeneratedQuery,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        let bounded = self
            .db
            .begin_read_transaction()
            .query_generated_graph_rag_bounded_profile(query, parameters, options.max_rows)?;
        bounded_nowledge_mem_read_output(self.mode, bounded, options)
    }
}

#[derive(Debug)]
pub struct NowledgeMemSearchProjection {
    index: SearchIndex,
}

#[derive(Debug)]
pub struct NowledgeMemOutOfCoreSearchProjection {
    reader: SearchOutOfCoreReader,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemOutOfCoreSearchCandidateOutput {
    pub result: SearchResultSet,
    pub report: NowledgeMemSearchCandidateReport,
    pub metrics: SearchOutOfCoreMetrics,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemSearchHydrationOutput {
    pub documents: Vec<SearchDocument>,
    pub out_of_core_metrics: Option<SearchOutOfCoreMetrics>,
}

impl From<NowledgeMemOutOfCoreSearchCandidateOutput> for NowledgeMemSearchCandidateOutput {
    fn from(output: NowledgeMemOutOfCoreSearchCandidateOutput) -> Self {
        Self {
            result: output.result,
            report: output.report,
            out_of_core_metrics: Some(output.metrics),
        }
    }
}

impl NowledgeMemOutOfCoreSearchProjection {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            reader: SearchOutOfCoreReader::open(path)?,
        })
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: SearchOutOfCoreConfig) -> Result<Self> {
        Ok(Self {
            reader: SearchOutOfCoreReader::open_with_config(path, config)?,
        })
    }

    pub fn open_production(
        path: impl AsRef<Path>,
        qualification: &SearchLexicalProductionQualificationReport,
        expected_identity: &crate::ProductionQualificationIdentity,
    ) -> Result<Self> {
        let projection = Self::open(path)?;
        projection.validate_lexical_production_qualification(qualification, expected_identity)?;
        Ok(projection)
    }

    pub fn open_production_with_config(
        path: impl AsRef<Path>,
        config: SearchOutOfCoreConfig,
        qualification: &SearchLexicalProductionQualificationReport,
        expected_identity: &crate::ProductionQualificationIdentity,
    ) -> Result<Self> {
        let projection = Self::open_with_config(path, config)?;
        projection.validate_lexical_production_qualification(qualification, expected_identity)?;
        Ok(projection)
    }

    pub fn validate_lexical_production_qualification(
        &self,
        qualification: &SearchLexicalProductionQualificationReport,
        expected_identity: &crate::ProductionQualificationIdentity,
    ) -> Result<()> {
        qualification.validate_for_projection_and_release(
            &self.reader.production_qualification_identity(),
            expected_identity,
        )
    }

    pub fn reader(&self) -> &SearchOutOfCoreReader {
        &self.reader
    }

    pub fn freshness(&self) -> SearchProjectionFreshness {
        self.reader.projection_freshness()
    }

    fn runtime_admission_memory_bytes(
        &self,
        working_memory_bytes: u64,
        result_budget_bytes: u64,
    ) -> u64 {
        let config = self.reader.config();
        working_memory_bytes
            .saturating_add(config.max_vector_search_working_bytes.get() as u64)
            .saturating_add(config.max_uncompressed_segment_bytes.get())
            .saturating_add(config.max_candidate_block_bytes.get())
            .saturating_add(config.max_hydrated_bytes.get().min(result_budget_bytes))
            .saturating_add(config.max_matched_span_bytes.get())
    }

    pub fn hydrate_documents(
        &self,
        document_ids: &[String],
    ) -> Result<SearchOutOfCoreHydrationOutput> {
        self.reader.hydrate_documents(document_ids)
    }

    pub fn search_candidates(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<SearchResultSet> {
        Ok(self.search_candidates_with_report(request)?.result)
    }

    pub fn search_candidates_with_report(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<NowledgeMemOutOfCoreSearchCandidateOutput> {
        let effective_compressed_vector_search_mode =
            request.effective_compressed_vector_search_mode();
        let output = self
            .reader
            .search_with_options_compressed_vector_projection_mode(
                &request.query_text,
                request.query_embedding.as_deref(),
                request.mode,
                SearchQueryOptions {
                    limit: request.limit,
                    offset: request.offset,
                    rank_window: request.rank_window,
                    fusion_weights: request.fusion_weights,
                    metadata_filters: request.metadata_filters.clone(),
                    policy_epoch: None,
                },
                effective_compressed_vector_search_mode,
            )?;
        let report = nowledge_mem_search_candidate_report(
            request,
            effective_compressed_vector_search_mode,
            &output.result,
        );
        Ok(NowledgeMemOutOfCoreSearchCandidateOutput {
            result: output.result,
            report,
            metrics: output.metrics,
        })
    }
}

impl NowledgeMemSearchProjection {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            index: SearchIndex::open(path)?,
        })
    }

    pub fn from_index(index: SearchIndex) -> Self {
        Self { index }
    }

    pub fn set_range_read_config(&mut self, config: SearchRangeReadConfig) {
        self.index.set_range_read_config(config);
    }

    pub fn index(&self) -> &SearchIndex {
        &self.index
    }

    pub fn index_mut(&mut self) -> &mut SearchIndex {
        &mut self.index
    }

    pub fn set_telemetry_sink(&mut self, telemetry: Option<Arc<dyn TelemetrySink>>) {
        self.index.set_telemetry_sink(telemetry);
    }

    pub fn into_index(self) -> SearchIndex {
        self.index
    }

    pub fn probe_json(&self, options: SearchProjectionProbeOptions) -> serde_json::Value {
        self.index.nowledge_search_projection_probe_json(options)
    }

    pub fn evidence_json(&self, options: SearchProjectionProbeOptions) -> serde_json::Value {
        nowledge_search_projection_evidence_json(&self.probe_json(options))
    }

    pub fn evidence_report(
        &self,
        options: SearchProjectionProbeOptions,
    ) -> NowledgeSearchProjectionEvidenceReport {
        NowledgeSearchProjectionEvidenceReport::from_probe(&self.probe_json(options))
    }

    pub fn validate_sampled_vector_recall(
        &self,
        options: VectorRecallValidationOptions,
    ) -> VectorRecallValidationReport {
        self.index.validate_sampled_vector_recall(options)
    }

    pub fn qualify_sampled_vector_recall_for_production(
        &self,
        options: VectorRecallValidationOptions,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> VectorRecallProductionQualificationReport {
        self.index.qualify_sampled_vector_recall_for_production(
            options,
            evidence_binding,
            expected_identity,
        )
    }

    pub fn shadow_evidence_json(
        &self,
        primary_probe: &serde_json::Value,
        options: SearchProjectionProbeOptions,
    ) -> serde_json::Value {
        nowledge_search_projection_shadow_evidence_json(primary_probe, &self.probe_json(options))
    }

    pub fn freshness(&self) -> SearchProjectionFreshness {
        self.index.projection_freshness()
    }

    pub fn search_candidates(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> SearchResultSet {
        self.search_candidates_with_report(request).result
    }

    pub fn search_candidates_with_report(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> NowledgeMemSearchCandidateOutput {
        let effective_compressed_vector_search_mode =
            request.effective_compressed_vector_search_mode();
        let result = self.index.search_with_options_adaptive_vector_projection(
            &request.query_text,
            request.query_embedding.as_deref(),
            request.mode,
            SearchQueryOptions {
                limit: request.limit,
                offset: request.offset,
                rank_window: request.rank_window,
                fusion_weights: request.fusion_weights,
                metadata_filters: request.metadata_filters.clone(),
                policy_epoch: None,
            },
            AdaptiveVectorSearchOptions {
                compression_mode: effective_compressed_vector_search_mode,
                backend_policy: request.adaptive_vector_backend_policy,
                recall_validation_probe: request.recall_validation_probe,
            },
        );
        let report = nowledge_mem_search_candidate_report(
            request,
            effective_compressed_vector_search_mode,
            &result,
        );
        NowledgeMemSearchCandidateOutput {
            result,
            report,
            out_of_core_metrics: None,
        }
    }

    pub fn try_search_candidates_with_report(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<NowledgeMemSearchCandidateOutput> {
        let effective_compressed_vector_search_mode =
            request.effective_compressed_vector_search_mode();
        let result = self
            .index
            .try_search_with_options_adaptive_vector_projection(
                &request.query_text,
                request.query_embedding.as_deref(),
                request.mode,
                SearchQueryOptions {
                    limit: request.limit,
                    offset: request.offset,
                    rank_window: request.rank_window,
                    fusion_weights: request.fusion_weights,
                    metadata_filters: request.metadata_filters.clone(),
                    policy_epoch: None,
                },
                AdaptiveVectorSearchOptions {
                    compression_mode: effective_compressed_vector_search_mode,
                    backend_policy: request.adaptive_vector_backend_policy,
                    recall_validation_probe: request.recall_validation_probe,
                },
            )?;
        let report = nowledge_mem_search_candidate_report(
            request,
            effective_compressed_vector_search_mode,
            &result,
        );
        Ok(NowledgeMemSearchCandidateOutput {
            result,
            report,
            out_of_core_metrics: None,
        })
    }

    pub fn search_candidate_readiness(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        options: &NowledgeMemSearchCandidateReadinessOptions,
    ) -> NowledgeMemSearchCandidateReadinessReport {
        self.search_candidates_with_report(request)
            .readiness_report(options)
    }

    pub fn search_candidate_shadow_evidence<I, S>(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        primary_candidate_ids: I,
    ) -> NowledgeMemSearchCandidateShadowEvidence
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let output = self.search_candidates_with_report(request);
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_search_candidate_output(primary_candidate_ids, &output);
        accumulator.evidence()
    }

    pub fn search_candidate_shadow_evidence_json<I, S>(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        primary_candidate_ids: I,
    ) -> serde_json::Value
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.search_candidate_shadow_evidence(request, primary_candidate_ids)
            .json()
    }
}

struct SearchProjectionExternalReadOperator<'a> {
    projection: Option<&'a NowledgeMemSearchProjection>,
    out_of_core_projection: Option<&'a NowledgeMemOutOfCoreSearchProjection>,
    vector_seed_execution_count: usize,
}

impl crate::executor::ExternalReadOperator for SearchProjectionExternalReadOperator<'_> {
    fn execute_vector_seed(
        &mut self,
        request: crate::executor::VectorSeedExecutionRequest<'_>,
    ) -> Result<crate::executor::VectorSeedExecutionOutput> {
        let top_k = vector_plan_top_k(request.vector_plan)?;
        let search_request =
            NowledgeMemSearchCandidateRequest::vector(request.embedding.to_vec(), top_k)
                .with_metadata_filters(request.metadata_filters.clone());
        let output = match (self.projection, self.out_of_core_projection) {
            (Some(projection), None) => {
                projection.try_search_candidates_with_report(&search_request)?
            }
            (None, Some(projection)) => projection
                .search_candidates_with_report(&search_request)?
                .into(),
            (None, None) => return Err(missing_search_projection_error()),
            (Some(_), Some(_)) => {
                return Err(SkeinError::Storage(
                    "nowledge mem search projection ownership is ambiguous".to_string(),
                ));
            }
        };
        let retriever = output
            .result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "vector")
            .ok_or_else(|| {
                SkeinError::Execution(
                    "vector search did not produce a vector retriever report".to_string(),
                )
            })?;
        let candidate_score_source = vector_score_source(&retriever.candidate_score_source)?;
        let execution_output = crate::executor::VectorSeedExecutionOutput {
            rows: output
                .result
                .hits
                .into_iter()
                .map(|hit| crate::executor::VectorSeedExecutionRow {
                    id: hit.id,
                    external_id: hit.external_id,
                    score: hit.vector_score,
                })
                .collect(),
            report: skein_executor::VectorExecutionReport {
                backend: vector_execution_backend(candidate_score_source),
                compression_mode: vector_compression_mode(
                    output.report.compressed_vector_search_mode,
                ),
                candidate_source: vector_plan_candidate_source(request.vector_plan)?,
                backend_selection_reason: retriever.backend_selection_reason,
                estimated_raw_vector_bytes: retriever.estimated_raw_vector_bytes,
                filter_selectivity_per_million: retriever.filter_selectivity_per_million,
                candidate_score_source,
                final_score_source: vector_score_source(&retriever.final_score_source)?,
                generated_candidate_count: retriever.generated_candidate_count,
                descriptor_pruned_count: retriever.descriptor_pruned_count,
                scalar_filtered_count: retriever.scalar_filtered_count,
                residual_filtered_count: retriever.residual_filtered_count,
                candidate_scan_rounds: retriever.candidate_scan_rounds,
                reranked_candidate_count: retriever.reranked_candidate_count,
                returned_count: retriever.candidate_count,
                raw_vector_bytes_read: retriever.raw_vector_bytes_read,
                candidate_scan_metrics: retriever.candidate_scan_kernel.as_ref().map(|kernel| {
                    skein_executor::VectorCandidateScanMetrics {
                        kernel: kernel.clone(),
                        worker_count: retriever.candidate_scan_worker_count,
                        segment_count: retriever.candidate_scan_segment_count,
                        scanned_segment_count: retriever.candidate_scan_scanned_segment_count,
                        scored_document_count: retriever.candidate_scan_scored_document_count,
                        filtered_document_count: retriever.candidate_scan_filtered_document_count,
                        scanned_block_count: retriever.candidate_scan_scanned_block_count,
                        skipped_block_count: retriever.candidate_scan_skipped_block_count,
                        payload_bytes_read: retriever.candidate_scan_payload_bytes_read,
                        admitted_working_bytes: retriever.candidate_scan_admitted_working_bytes,
                    }
                }),
                index_covered_document_count: Some(retriever.index_covered_document_count),
                index_candidate_document_count: Some(retriever.index_candidate_document_count),
                index_coverage_complete: Some(retriever.index_coverage_complete),
                fallback_reason_codes: retriever
                    .fallback_reason_codes
                    .iter()
                    .filter_map(|code| vector_fallback_reason_code(*code))
                    .collect(),
            },
        };
        self.vector_seed_execution_count = self
            .vector_seed_execution_count
            .checked_add(1)
            .ok_or_else(|| {
                SkeinError::Execution(
                    "bounded external vector seed execution count overflowed".to_string(),
                )
            })?;
        Ok(execution_output)
    }
}

fn vector_plan_top_k(plan: &skein_plan::VectorPhysicalPlan) -> Result<usize> {
    match plan {
        skein_plan::VectorPhysicalPlan::TopK { limit, .. } => Ok(*limit),
        _ => Err(SkeinError::Execution(
            "vector seed physical plan is missing TopK".to_string(),
        )),
    }
}

fn vector_plan_candidate_source(
    plan: &skein_plan::VectorPhysicalPlan,
) -> Result<skein_plan::VectorCandidateSource> {
    match plan {
        skein_plan::VectorPhysicalPlan::VectorCandidateScan { source, .. } => Ok(*source),
        skein_plan::VectorPhysicalPlan::ResidualFilter { input, .. }
        | skein_plan::VectorPhysicalPlan::RawVectorRerank { input, .. }
        | skein_plan::VectorPhysicalPlan::TopK { input, .. } => vector_plan_candidate_source(input),
        skein_plan::VectorPhysicalPlan::Filter { .. } => Err(SkeinError::Execution(
            "vector seed physical plan is missing VectorCandidateScan".to_string(),
        )),
    }
}

fn vector_score_source(value: &str) -> Result<skein_executor::VectorScoreSource> {
    match value {
        "unavailable" | "none" => Ok(skein_executor::VectorScoreSource::Unavailable),
        "raw_vector" => Ok(skein_executor::VectorScoreSource::RawVector),
        "ann_approximate" => Ok(skein_executor::VectorScoreSource::AnnApproximate),
        "quantized_approximate" => Ok(skein_executor::VectorScoreSource::QuantizedApproximate),
        _ => Err(SkeinError::Execution(
            "vector search returned an unsupported score source".to_string(),
        )),
    }
}

fn vector_execution_backend(
    score_source: skein_executor::VectorScoreSource,
) -> skein_executor::VectorExecutionBackend {
    match score_source {
        skein_executor::VectorScoreSource::Unavailable => {
            skein_executor::VectorExecutionBackend::Unavailable
        }
        skein_executor::VectorScoreSource::RawVector => {
            skein_executor::VectorExecutionBackend::ScalarFlat
        }
        skein_executor::VectorScoreSource::AnnApproximate => {
            skein_executor::VectorExecutionBackend::AnnProjection
        }
        skein_executor::VectorScoreSource::QuantizedApproximate => {
            skein_executor::VectorExecutionBackend::QuantizedProjection
        }
    }
}

fn vector_compression_mode(
    mode: CompressedVectorSearchMode,
) -> skein_executor::VectorCompressionMode {
    match mode {
        CompressedVectorSearchMode::Disabled => skein_executor::VectorCompressionMode::Disabled,
        CompressedVectorSearchMode::Preferred => skein_executor::VectorCompressionMode::Preferred,
        CompressedVectorSearchMode::Required => skein_executor::VectorCompressionMode::Required,
    }
}

fn vector_fallback_reason_code(
    code: SearchFallbackReasonCode,
) -> Option<skein_executor::VectorFallbackReasonCode> {
    match code {
        SearchFallbackReasonCode::VectorDimensionMismatch => {
            Some(skein_executor::VectorFallbackReasonCode::VectorDimensionMismatch)
        }
        SearchFallbackReasonCode::VectorIndexEmpty => {
            Some(skein_executor::VectorFallbackReasonCode::VectorIndexEmpty)
        }
        SearchFallbackReasonCode::CompressedVectorProjectionUnavailable => {
            Some(skein_executor::VectorFallbackReasonCode::CompressedVectorProjectionUnavailable)
        }
        SearchFallbackReasonCode::QueryEmbeddingMissing => {
            Some(skein_executor::VectorFallbackReasonCode::QueryEmbeddingMissing)
        }
        SearchFallbackReasonCode::TextQueryEmpty => None,
    }
}

#[derive(Debug)]
pub struct NowledgeMemEmbeddedStore {
    graph: NowledgeMemGraph,
    search_projection: Option<NowledgeMemSearchProjection>,
    out_of_core_search_projection: Option<NowledgeMemOutOfCoreSearchProjection>,
    retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor,
}

#[derive(Debug, Clone)]
pub struct NowledgeMemEmbeddedStoreHandle {
    inner: Arc<RwLock<NowledgeMemEmbeddedStore>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemReadSnapshotBudget {
    pub max_rows: usize,
    pub max_payload_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemReadSnapshotReport {
    pub commit_epoch: u64,
    pub search_projection_present: bool,
    pub search_projection_source_graph_commit_epoch: Option<u64>,
    pub search_projection_durable_source_graph_commit_epoch: Option<u64>,
    pub max_rows: usize,
    pub max_payload_bytes: usize,
    pub cypher_statement_count: usize,
    pub sql_statement_count: usize,
    pub vector_seed_execution_count: usize,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub remaining_rows: usize,
    pub remaining_payload_bytes: usize,
}

impl NowledgeMemReadSnapshotReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_READ_SNAPSHOT_REPORT_PROTOCOL,
            "commit_epoch": self.commit_epoch,
            "search_projection_present": self.search_projection_present,
            "search_projection_source_graph_commit_epoch": self.search_projection_source_graph_commit_epoch,
            "search_projection_durable_source_graph_commit_epoch": self.search_projection_durable_source_graph_commit_epoch,
            "max_rows": self.max_rows,
            "max_payload_bytes": self.max_payload_bytes,
            "cypher_statement_count": self.cypher_statement_count,
            "sql_statement_count": self.sql_statement_count,
            "vector_seed_execution_count": self.vector_seed_execution_count,
            "output_rows": self.output_rows,
            "output_payload_bytes": self.output_payload_bytes,
            "remaining_rows": self.remaining_rows,
            "remaining_payload_bytes": self.remaining_payload_bytes,
        })
    }
}

pub struct NowledgeMemReadSnapshot<'a> {
    transaction: crate::DatabaseReadTransaction,
    external: SearchProjectionExternalReadOperator<'a>,
    search_projection_source_graph_commit_epoch: Option<u64>,
    search_projection_durable_source_graph_commit_epoch: Option<u64>,
    budget: NowledgeMemReadSnapshotBudget,
    cypher_statement_count: usize,
    sql_statement_count: usize,
    output_rows: usize,
    output_payload_bytes: usize,
}

impl NowledgeMemReadSnapshot<'_> {
    pub fn commit_epoch(&self) -> u64 {
        self.transaction.commit_epoch()
    }

    pub fn query_cypher(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: usize,
    ) -> Result<QueryOutput> {
        let completed_statement_count =
            self.cypher_statement_count.checked_add(1).ok_or_else(|| {
                SkeinError::Execution(
                    "bounded read snapshot Cypher statement count overflowed".to_string(),
                )
            })?;
        let max_rows = self.statement_row_budget(max_rows)?;
        let max_payload_bytes = self.remaining_payload_bytes()?;
        let mut rows = Vec::new();
        let report = self.transaction.query_with_params_streaming_external(
            cypher,
            parameters,
            QueryStreamOptions {
                max_rows: Some(max_rows),
                max_payload_bytes: Some(max_payload_bytes),
            },
            &mut self.external,
            |row| {
                rows.push(row);
                Ok(())
            },
        )?;
        self.consume(report.output_rows, report.output_payload_bytes)?;
        self.cypher_statement_count = completed_statement_count;
        Ok(QueryOutput { rows: rows.into() })
    }

    pub fn query_sql(
        &mut self,
        sql: &str,
        parameters: &[Value],
        max_rows: usize,
    ) -> Result<QueryOutput> {
        let completed_statement_count =
            self.sql_statement_count.checked_add(1).ok_or_else(|| {
                SkeinError::Execution(
                    "bounded read snapshot SQL statement count overflowed".to_string(),
                )
            })?;
        let max_rows = self.statement_row_budget(max_rows)?;
        let max_payload_bytes = self.remaining_payload_bytes()?;
        let output = self.transaction.query_sql_with_params_options(
            sql,
            parameters,
            QueryStreamOptions {
                max_rows: Some(max_rows),
                max_payload_bytes: Some(max_payload_bytes),
            },
        )?;
        let output_rows = output.rows.len();
        let output_payload_bytes = estimate_query_output_payload_bytes(&output);
        self.consume(output_rows, output_payload_bytes)?;
        self.sql_statement_count = completed_statement_count;
        Ok(output)
    }

    pub fn report(&self) -> NowledgeMemReadSnapshotReport {
        NowledgeMemReadSnapshotReport {
            commit_epoch: self.commit_epoch(),
            search_projection_present: self.external.projection.is_some()
                || self.external.out_of_core_projection.is_some(),
            search_projection_source_graph_commit_epoch: self
                .search_projection_source_graph_commit_epoch,
            search_projection_durable_source_graph_commit_epoch: self
                .search_projection_durable_source_graph_commit_epoch,
            max_rows: self.budget.max_rows,
            max_payload_bytes: self.budget.max_payload_bytes,
            cypher_statement_count: self.cypher_statement_count,
            sql_statement_count: self.sql_statement_count,
            vector_seed_execution_count: self.external.vector_seed_execution_count,
            output_rows: self.output_rows,
            output_payload_bytes: self.output_payload_bytes,
            remaining_rows: self.budget.max_rows.saturating_sub(self.output_rows),
            remaining_payload_bytes: self
                .budget
                .max_payload_bytes
                .saturating_sub(self.output_payload_bytes),
        }
    }

    fn statement_row_budget(&self, requested: usize) -> Result<usize> {
        if requested == 0 {
            return Err(SkeinError::Execution(
                "bounded read statement requires max_rows greater than zero".to_string(),
            ));
        }
        let remaining = self.budget.max_rows.saturating_sub(self.output_rows);
        if remaining == 0 {
            return Err(SkeinError::Execution(
                "bounded read snapshot exhausted max_rows".to_string(),
            ));
        }
        Ok(requested.min(remaining))
    }

    fn remaining_payload_bytes(&self) -> Result<usize> {
        let remaining = self
            .budget
            .max_payload_bytes
            .saturating_sub(self.output_payload_bytes);
        if remaining == 0 {
            return Err(SkeinError::Execution(
                "bounded read snapshot exhausted max_payload_bytes".to_string(),
            ));
        }
        Ok(remaining)
    }

    fn consume(&mut self, rows: usize, payload_bytes: usize) -> Result<()> {
        let output_rows = self.output_rows.saturating_add(rows);
        let output_payload_bytes = self.output_payload_bytes.saturating_add(payload_bytes);
        if output_rows > self.budget.max_rows {
            return Err(SkeinError::Execution(format!(
                "bounded read snapshot produced {output_rows} rows, exceeding max_rows {}",
                self.budget.max_rows
            )));
        }
        if output_payload_bytes > self.budget.max_payload_bytes {
            return Err(SkeinError::Execution(format!(
                "bounded read snapshot produced {output_payload_bytes} payload bytes, exceeding max_payload_bytes {}",
                self.budget.max_payload_bytes
            )));
        }
        self.output_rows = output_rows;
        self.output_payload_bytes = output_payload_bytes;
        Ok(())
    }
}

impl NowledgeMemEmbeddedStoreHandle {
    pub fn new(store: NowledgeMemEmbeddedStore) -> Self {
        Self {
            inner: Arc::new(RwLock::new(store)),
        }
    }

    pub fn open_with_options(
        options: NowledgeMemOpenOptions,
    ) -> Result<(Self, NowledgeMemOpenReport)> {
        let (store, report) = NowledgeMemEmbeddedStore::open_with_options(options)?;
        Ok((Self::new(store), report))
    }

    pub fn open_with_options_and_runtime_governor(
        options: NowledgeMemOpenOptions,
        runtime_governor: RuntimeGovernor,
    ) -> Result<(Self, NowledgeMemOpenReport)> {
        let (store, report) = NowledgeMemEmbeddedStore::open_with_options_and_runtime_governor(
            options,
            runtime_governor,
        )?;
        Ok((Self::new(store), report))
    }

    pub fn runtime_governor_snapshot(&self) -> Result<RuntimeGovernorSnapshot> {
        Ok(self.read_store()?.runtime_governor_snapshot())
    }

    /// Returns a point-in-time storage residency snapshot through the admitted
    /// embedded facade. Qualification uses this to prove cancellation does not
    /// leak cache pins without reaching through to the storage engine.
    pub fn storage_residency_report(&self) -> Result<crate::StorageResidencyReport> {
        Ok(self
            .read_store()?
            .graph
            .database()
            .storage_residency_report())
    }

    /// Reports whether a fail-closed storage error poisoned this handle.
    pub fn storage_handle_poisoned(&self) -> Result<bool> {
        Ok(self
            .read_store()?
            .graph
            .database()
            .storage_handle_poisoned())
    }

    /// Reports the admitted facade contract. Production hosts must bind this
    /// report to their long-lived runtime identity before using it as evidence.
    pub fn serving_path_readiness(&self) -> NowledgeMemServingPathReadiness {
        NowledgeMemServingPathReadiness::embedded_store_handle()
    }

    pub fn refresh_runtime_resources(&self) -> Result<bool> {
        Ok(self.read_store()?.refresh_runtime_resources())
    }

    /// Persist the current graph generation through the admitted embedded
    /// maintenance path.
    ///
    /// Import and qualification hosts use this typed boundary instead of
    /// issuing a textual `CHECKPOINT` statement or reaching through the
    /// facade to the storage engine. The single maintenance admission's
    /// memory request is extended by the columnar shadow's builder-lifetime
    /// reservation, and that pre-admitted context travels into the
    /// checkpoint so the shadow build never issues a nested admission
    /// against the permit this method already holds.
    pub fn checkpoint(&self) -> Result<()> {
        let shadow_admission_bytes = self
            .read_store()?
            .graph
            .database()
            .columnar_shadow_admission_bytes();
        let _permit = self.admit_typed_maintenance(
            usize::try_from(shadow_admission_bytes).unwrap_or(usize::MAX),
            1,
        )?;
        self.write_store()?
            .graph_mut()
            .database_mut()
            .checkpoint_with_shadow_admission(crate::store::ColumnarShadowAdmission::pre_admitted(
                shadow_admission_bytes,
            ))
    }

    pub fn query_with_report(&self, cypher: &str) -> Result<NowledgeMemQueryOutput> {
        self.write_store()?.query_with_report(cypher)
    }

    pub fn set_telemetry_sink(&self, telemetry: Option<Arc<dyn TelemetrySink>>) -> Result<()> {
        self.write_store()?.set_telemetry_sink(telemetry);
        Ok(())
    }

    pub fn query_with_report_options(
        &self,
        cypher: &str,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.write_store()?
            .query_with_report_options(cypher, options)
    }

    pub fn query_with_params_with_report(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<NowledgeMemQueryOutput> {
        self.write_store()?
            .query_with_params_with_report(cypher, parameters)
    }

    /// Hydrates an explicitly bounded set of search-projection documents.
    ///
    /// Candidate search intentionally returns compact identities. Embedded
    /// hosts use this method for projection-owned payloads, such as source
    /// chunks, that have no authoritative graph node to hydrate from.
    pub fn search_projection_documents(
        &self,
        document_ids: &[String],
        max_documents: usize,
    ) -> Result<Vec<SearchDocument>> {
        Ok(self
            .search_projection_documents_with_report(document_ids, max_documents)?
            .documents)
    }

    pub fn search_projection_documents_with_report(
        &self,
        document_ids: &[String],
        max_documents: usize,
    ) -> Result<NowledgeMemSearchHydrationOutput> {
        if document_ids.len() > max_documents {
            return Err(SkeinError::Execution(format!(
                "search projection document hydration requested {} rows, limit is {max_documents}",
                document_ids.len()
            )));
        }
        let _permit = self.admit_typed_search()?;
        let store = self.read_store()?;
        let max_payload_bytes = store
            .graph
            .database()
            .config()
            .max_read_result_payload_bytes
            .ok_or_else(|| {
                SkeinError::Execution(
                    "admitted search hydration requires max_read_result_payload_bytes".to_string(),
                )
            })?;
        match (
            store.search_projection.as_ref(),
            store.out_of_core_search_projection.as_ref(),
        ) {
            (Some(projection), None) => {
                let mut documents = Vec::with_capacity(document_ids.len());
                let mut payload_bytes = 0usize;
                for id in document_ids {
                    let Some(document) = projection.index().document(id) else {
                        continue;
                    };
                    payload_bytes =
                        payload_bytes.saturating_add(search_document_payload_bytes(document));
                    if payload_bytes > max_payload_bytes {
                        return Err(SkeinError::Execution(format!(
                            "search projection document hydration produced {payload_bytes} payload bytes, limit is {max_payload_bytes}"
                        )));
                    }
                    documents.push(document.clone());
                }
                Ok(NowledgeMemSearchHydrationOutput {
                    documents,
                    out_of_core_metrics: None,
                })
            }
            (None, Some(projection)) => {
                let output = projection.hydrate_documents(document_ids)?;
                if output.metrics.hydrated_bytes
                    > u64::try_from(max_payload_bytes).unwrap_or(u64::MAX)
                {
                    return Err(SkeinError::Execution(format!(
                        "search projection document hydration produced {} payload bytes, limit is {max_payload_bytes}",
                        output.metrics.hydrated_bytes
                    )));
                }
                Ok(NowledgeMemSearchHydrationOutput {
                    documents: output.documents,
                    out_of_core_metrics: Some(output.metrics),
                })
            }
            (None, None) => Err(missing_search_projection_error()),
            (Some(_), Some(_)) => Err(ambiguous_search_projection_error()),
        }
    }
}

#[cfg(test)]
impl NowledgeMemEmbeddedStoreHandle {
    pub fn create_knowledge_memory_evolves_batch(
        &self,
        request: &KnowledgeMemoryEvolvesCreateBatchRequest,
    ) -> Result<KnowledgeMemoryEvolvesCreateBatchOutput> {
        let _permit = self.admit_typed_mutation()?;
        self.write_store()?
            .graph_mut()
            .database_mut()
            .create_knowledge_memory_evolves_batch(request)
    }

    pub fn update_knowledge_memory_lifecycle_batch(
        &self,
        request: &KnowledgeMemoryLifecycleBatchRequest,
    ) -> Result<KnowledgeMemoryLifecycleBatchOutput> {
        let _permit = self.admit_typed_mutation()?;
        self.write_store()?
            .graph_mut()
            .database_mut()
            .update_knowledge_memory_lifecycle_batch(request)
    }

    pub fn delete_knowledge_entity_batch(
        &self,
        request: &KnowledgeEntityDeleteBatchRequest,
    ) -> Result<KnowledgeEntityDeleteBatchOutput> {
        let _permit = self.admit_typed_mutation()?;
        self.write_store()?
            .graph_mut()
            .database_mut()
            .delete_knowledge_entity_batch(request)
    }
}

impl NowledgeMemEmbeddedStoreHandle {
    pub fn transaction(
        &self,
        statements: &[NowledgeGraphStatement],
    ) -> Result<crate::NowledgeGraphTransactionOutput> {
        let _permit = self.admit_transaction(statements)?;
        let mut store = self.write_store()?;
        let db = store.graph_mut().database_mut();
        let mut transaction = db.begin_transaction();
        let mut statement_outputs = Vec::with_capacity(statements.len());
        for statement in statements {
            statement_outputs
                .push(transaction.query_with_params(&statement.cypher, &statement.parameters)?);
        }
        let commit_output = transaction.commit()?;
        Ok(crate::NowledgeGraphTransactionOutput {
            statement_outputs,
            commit_output,
        })
    }

    /// Executes caller-owned Cypher and SQL statements in one canonical
    /// graph/relational transaction.
    ///
    /// The callback receives the ordinary query-first transaction surface;
    /// no route-specific mutation API or second durability boundary is
    /// introduced. Returning an error rolls back every staged statement.
    pub fn with_transaction<T>(
        &self,
        operation: impl FnOnce(&mut crate::DatabaseTransaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let _permit = self.admit_typed_mutation()?;
        let mut store = self.write_store()?;
        let mut transaction = store.graph_mut().database_mut().begin_transaction();
        match operation(&mut transaction) {
            Ok(output) => {
                transaction.commit()?;
                Ok(output)
            }
            Err(error) => {
                transaction.rollback();
                Err(error)
            }
        }
    }

    /// Executes a caller-owned group of bounded Cypher reads against one
    /// immutable graph snapshot.
    pub fn with_read_transaction<T>(
        &self,
        max_estimated_payload_bytes: usize,
        operation: impl FnOnce(&mut crate::DatabaseReadTransaction) -> Result<T>,
    ) -> Result<T> {
        let _permit = self.admit_typed_read(max_estimated_payload_bytes)?;
        let store = self.read_store()?;
        let mut transaction = store.graph().database().begin_read_transaction();
        operation(&mut transaction)
    }

    /// Executes App-owned Cypher and PostgreSQL reads against one immutable
    /// graph/relational snapshot while the search projection is pinned.
    /// Budgets are cumulative across every statement in the callback.
    pub fn with_bounded_read_snapshot<T>(
        &self,
        budget: NowledgeMemReadSnapshotBudget,
        operation: impl FnOnce(&mut NowledgeMemReadSnapshot<'_>) -> Result<T>,
    ) -> Result<T> {
        if budget.max_rows == 0 {
            return Err(SkeinError::Execution(
                "bounded read snapshot requires max_rows greater than zero".to_string(),
            ));
        }
        if budget.max_payload_bytes == 0 {
            return Err(SkeinError::Execution(
                "bounded read snapshot requires max_payload_bytes greater than zero".to_string(),
            ));
        }
        let _permit = self.admit_typed_read(budget.max_payload_bytes)?;
        let store = self.read_store()?;
        let configured_rows = store
            .graph
            .database()
            .config()
            .max_read_result_rows
            .ok_or_else(|| {
                SkeinError::Execution(
                    "bounded read snapshot requires max_read_result_rows".to_string(),
                )
            })?;
        if budget.max_rows > configured_rows {
            return Err(SkeinError::Execution(format!(
                "bounded read snapshot row budget {} exceeds configured limit {configured_rows}",
                budget.max_rows
            )));
        }
        let transaction = store.graph.database().begin_read_transaction();
        let projection_freshness = match (
            store.search_projection.as_ref(),
            store.out_of_core_search_projection.as_ref(),
        ) {
            (Some(projection), None) => Some(projection.freshness()),
            (None, Some(projection)) => Some(projection.freshness()),
            (None, None) | (Some(_), Some(_)) => None,
        };
        let external = SearchProjectionExternalReadOperator {
            projection: store.search_projection.as_ref(),
            out_of_core_projection: store.out_of_core_search_projection.as_ref(),
            vector_seed_execution_count: 0,
        };
        let mut snapshot = NowledgeMemReadSnapshot {
            transaction,
            external,
            search_projection_source_graph_commit_epoch: projection_freshness
                .as_ref()
                .and_then(|freshness| freshness.source_graph_commit_epoch),
            search_projection_durable_source_graph_commit_epoch: projection_freshness
                .as_ref()
                .and_then(|freshness| freshness.durable_source_graph_commit_epoch),
            budget,
            cypher_statement_count: 0,
            sql_statement_count: 0,
            output_rows: 0,
            output_payload_bytes: 0,
        };
        operation(&mut snapshot)
    }

    pub fn skein_lightning_initial_import_apply_with_document_identities(
        &self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &SkeinLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&SkeinLightningInitialImportCheckpoint>,
        document_identities: &[SkeinLightningInitialImportDocumentIdentity],
    ) -> Result<SkeinLightningInitialImportApplyReport> {
        let estimated_input_bytes = encoded_graph_stream
            .len()
            .saturating_add(encoded_relational_stream.len())
            .saturating_add(
                document_identities
                    .iter()
                    .map(|identity| identity.document_id.len())
                    .sum::<usize>(),
            );
        let _permit = self.admit_typed_maintenance(estimated_input_bytes, 1)?;
        self.write_store()?
            .graph_mut()
            .database_mut()
            .skein_lightning_initial_import_apply_with_document_identities(
                encoded_graph_stream,
                encoded_relational_stream,
                manifest,
                projection_freshness,
                checkpoint,
                document_identities,
            )
    }

    /// Applies an externally materialized projection batch and makes it
    /// durable before returning. This is the library boundary used when Mem
    /// imports a legacy LanceDB projection without rebuilding embeddings.
    pub fn apply_search_projection_delta_and_checkpoint(
        &self,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        let _permit = self.admit_typed_maintenance(search_projection_delta_bytes(&delta), 1)?;
        self.write_store()?
            .apply_search_projection_delta_and_checkpoint(delta)
    }

    /// Applies one externally materialized import batch, records its immutable
    /// source provenance, and checkpoints both before acknowledging success.
    pub fn apply_initial_import_projection_delta_and_checkpoint(
        &self,
        delta: SearchProjectionDelta,
        import_source_graph_commit_epoch: u64,
    ) -> Result<SearchProjectionDeltaReport> {
        let _permit = self.admit_typed_maintenance(search_projection_delta_bytes(&delta), 1)?;
        self.write_store()?
            .apply_initial_import_projection_delta_and_checkpoint(
                delta,
                import_source_graph_commit_epoch,
            )
    }

    pub fn query_with_params_with_report_options(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.write_store()?
            .query_with_params_with_report_options(cypher, parameters, options)
    }

    pub fn query_with_params_with_report_options_context(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
        task_context: &RuntimeTaskContext,
    ) -> Result<NowledgeMemQueryOutput> {
        self.write_store()?
            .query_with_params_with_report_options_context(
                cypher,
                parameters,
                options,
                task_context,
            )
    }

    pub fn read_query(
        &self,
        cypher: &str,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.read_store()?.read_query_with_options(cypher, options)
    }

    pub fn read_query_with_params(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.read_store()?
            .read_query_with_params(cypher, parameters, options)
    }

    pub fn read_query_with_params_streaming(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
        consumer: impl FnMut(BTreeMap<String, Value>) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.read_store()?
            .read_query_with_params_streaming(cypher, parameters, options, consumer)
    }

    pub fn read_query_with_params_streaming_context(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
        task_context: &RuntimeTaskContext,
        consumer: impl FnMut(BTreeMap<String, Value>) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.read_store()?.read_query_with_params_streaming_context(
            cypher,
            parameters,
            options,
            task_context,
            consumer,
        )
    }

    pub fn read_query_with_params_streaming_collect(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.read_store()?
            .read_query_with_params_streaming_collect(cypher, parameters, options)
    }

    pub fn graph_rag_schema_context(
        &self,
        options: GraphRagSchemaContextOptions,
    ) -> Result<GraphRagSchemaContext> {
        Ok(self.read_store()?.graph_rag_schema_context(options))
    }

    pub fn read_generated_graph_rag(
        &self,
        query: &GraphRagGeneratedQuery,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.read_store()?
            .read_generated_graph_rag(query, parameters, options)
    }
}
impl NowledgeMemEmbeddedStoreHandle {
    pub fn search_candidates(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<SearchResultSet> {
        let _permit = self.admit_typed_search()?;
        Ok(self.read_store()?.search_candidates(request)?.result)
    }

    pub fn search_candidates_with_report(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<NowledgeMemSearchCandidateOutput> {
        let _permit = self.admit_typed_search()?;
        self.read_store()?.search_candidates(request)
    }

    pub fn validate_sampled_vector_recall(
        &self,
        options: VectorRecallValidationOptions,
    ) -> Result<VectorRecallValidationReport> {
        self.read_store()?.validate_sampled_vector_recall(options)
    }

    pub fn qualify_sampled_vector_recall_for_production(
        &self,
        options: VectorRecallValidationOptions,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> Result<VectorRecallProductionQualificationReport> {
        self.read_store()?
            .qualify_sampled_vector_recall_for_production(
                options,
                evidence_binding,
                expected_identity,
            )
    }

    pub fn retrieve_knowledge(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        let _permit = self.admit_typed_search()?;
        self.read_store()?.retrieve_knowledge(request)
    }

    pub fn retrieve_knowledge_with_report(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<NowledgeMemRetrievalOutput> {
        let _permit = self.admit_typed_search()?;
        self.read_store()?.retrieve_knowledge_with_report(request)
    }

    pub fn search_candidate_readiness(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        options: &NowledgeMemSearchCandidateReadinessOptions,
    ) -> Result<NowledgeMemSearchCandidateReadinessReport> {
        let _permit = self.admit_typed_search()?;
        self.read_store()?
            .search_candidate_readiness(request, options)
    }

    pub fn search_candidate_shadow_evidence_json<I, S>(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        primary_candidate_ids: I,
    ) -> Result<serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let _permit = self.admit_typed_search()?;
        self.read_store()?
            .search_candidate_shadow_evidence_json(request, primary_candidate_ids)
    }

    pub fn slow_query_report(&self) -> Result<NowledgeMemSlowQueryReport> {
        Ok(self.read_store()?.slow_query_report())
    }

    pub fn slow_query_report_json(&self) -> Result<serde_json::Value> {
        Ok(self.read_store()?.slow_query_report_json())
    }

    pub fn runtime_status(&self) -> Result<NowledgeMemRuntimeStatus> {
        Ok(self.read_store()?.runtime_status())
    }

    pub fn production_resource_profile(
        &self,
        statement: &NowledgeGraphStatement,
        limits: StorageResourceProfileLimits,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> Result<StorageResourceProfileReport> {
        let store = self.read_store()?;
        let permit = store.graph.admit_streaming_query(
            &statement.cypher,
            &statement.parameters,
            limits.max_output_payload_bytes,
        )?;
        let task_context = RuntimeTaskContext::default().with_admitted_parallelism(
            NonZeroUsize::new(permit.request().cpu_slots).unwrap_or(NonZeroUsize::MIN),
        );
        store
            .graph
            .database()
            .storage_resource_profile_for_production_with_context(
                &statement.cypher,
                &statement.parameters,
                limits,
                evidence_binding,
                expected_identity,
                &task_context,
            )
    }

    pub fn production_status(
        &self,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<NowledgeMemProductionStatus> {
        Ok(self.read_store()?.production_status(route_ownership))
    }

    pub fn production_status_json(
        &self,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<serde_json::Value> {
        Ok(self.read_store()?.production_status_json(route_ownership))
    }

    pub fn cutover_controls_report(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<NowledgeMemCutoverControlsReport> {
        Ok(self
            .read_store()?
            .cutover_controls_report(controls, route_ownership))
    }

    pub fn cutover_controls_report_with_initial_import_cutover_catch_up(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_cutover_catch_up: Option<&SkeinLightningInitialImportCutoverCatchUpReport>,
    ) -> Result<NowledgeMemCutoverControlsReport> {
        Ok(self
            .read_store()?
            .cutover_controls_report_with_initial_import_cutover_catch_up(
                controls,
                route_ownership,
                initial_import_cutover_catch_up,
            ))
    }

    pub fn cutover_controls_report_with_initial_import_recovery(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_recovery: Option<&SkeinLightningInitialImportRecoveryReadinessReport>,
    ) -> Result<NowledgeMemCutoverControlsReport> {
        Ok(self
            .read_store()?
            .cutover_controls_report_with_initial_import_recovery(
                controls,
                route_ownership,
                initial_import_recovery,
            ))
    }

    pub fn cutover_controls_report_json(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<serde_json::Value> {
        Ok(self
            .read_store()?
            .cutover_controls_report_json(controls, route_ownership))
    }

    pub fn cutover_controls_report_json_with_initial_import_cutover_catch_up(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_cutover_catch_up: Option<&SkeinLightningInitialImportCutoverCatchUpReport>,
    ) -> Result<serde_json::Value> {
        Ok(self
            .read_store()?
            .cutover_controls_report_json_with_initial_import_cutover_catch_up(
                controls,
                route_ownership,
                initial_import_cutover_catch_up,
            ))
    }

    pub fn library_readiness(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<NowledgeMemLibraryReadinessReport> {
        Ok(self.read_store()?.library_readiness(options))
    }

    pub fn library_readiness_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.read_store()?.library_readiness_json(options))
    }

    pub fn readiness_dashboard(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<NowledgeMemReadinessDashboard> {
        Ok(self.read_store()?.readiness_dashboard(options))
    }

    pub fn readiness_dashboard_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.read_store()?.readiness_dashboard_json(options))
    }

    pub fn operations_readiness(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<NowledgeMemOperationsReadinessReport> {
        Ok(self.read_store()?.operations_readiness(options))
    }

    pub fn operations_readiness_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.read_store()?.operations_readiness_json(options))
    }

    pub fn query_runtime_preflight(
        &self,
        probes: &[NowledgeQueryRuntimePreflightProbe],
    ) -> Result<NowledgeQueryRuntimePreflightReport> {
        Ok(self.write_store()?.query_runtime_preflight(probes))
    }

    pub fn query_runtime_preflight_json(
        &self,
        probes: &[NowledgeQueryRuntimePreflightProbe],
    ) -> Result<serde_json::Value> {
        Ok(self.write_store()?.query_runtime_preflight_json(probes))
    }

    pub fn catch_up_search_projection(
        &self,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<SearchProjectionCatchUpReport> {
        let estimated_operations = max_operations_per_batch.saturating_mul(max_batches);
        let estimated_input_bytes =
            estimated_operations.saturating_mul(SEARCH_PROJECTION_CHANGEFEED_OPERATION_BYTES);
        let _permit = self.admit_typed_maintenance(estimated_input_bytes, 1)?;
        self.write_store()?
            .catch_up_search_projection(max_operations_per_batch, max_batches)
    }

    /// Runs bounded unified graph-and-relational projection catch-up through
    /// the embedded library handle.
    pub fn catch_up_search_projection_with_relational<F>(
        &self,
        max_operations_per_batch: usize,
        max_batches: usize,
        relational_hydrator: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        let estimated_operations = max_operations_per_batch.saturating_mul(max_batches);
        let estimated_input_bytes =
            estimated_operations.saturating_mul(SEARCH_PROJECTION_CHANGEFEED_OPERATION_BYTES);
        let _permit = self.admit_typed_maintenance(estimated_input_bytes, 1)?;
        self.write_store()?
            .catch_up_search_projection_with_relational(
                max_operations_per_batch,
                max_batches,
                relational_hydrator,
            )
    }

    /// Runs bounded unified projection catch-up through the embedded library
    /// handle and invokes the host hydrator for every selected batch.
    pub fn catch_up_search_projection_with_batch_hydrator<F>(
        &self,
        max_change_operations_per_batch: usize,
        max_projection_operations_per_batch: usize,
        max_batches: usize,
        batch_hydrator: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        let estimated_operations = max_change_operations_per_batch
            .max(max_projection_operations_per_batch)
            .saturating_mul(max_batches);
        let estimated_input_bytes =
            estimated_operations.saturating_mul(SEARCH_PROJECTION_CHANGEFEED_OPERATION_BYTES);
        let _permit = self.admit_typed_maintenance(estimated_input_bytes, 1)?;
        self.write_store()?
            .catch_up_search_projection_with_batch_hydrator(
                max_change_operations_per_batch,
                max_projection_operations_per_batch,
                max_batches,
                batch_hydrator,
            )
    }

    pub fn search_projection_changefeed_readiness(
        &self,
        require_restart_recoverable: bool,
        max_operations: Option<usize>,
    ) -> Result<SearchProjectionChangefeedReadiness> {
        self.read_store()?
            .search_projection_changefeed_readiness(require_restart_recoverable, max_operations)
    }

    pub fn catch_up_search_projection_with_scheduler(
        &self,
        scheduler: &mut LocalQosScheduler,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<ScheduledSearchProjectionCatchUpReport> {
        let estimated_operations = max_operations_per_batch.saturating_mul(max_batches);
        let estimated_input_bytes =
            estimated_operations.saturating_mul(SEARCH_PROJECTION_CHANGEFEED_OPERATION_BYTES);
        let _permit = self.admit_typed_maintenance(estimated_input_bytes, 1)?;
        self.write_store()?
            .catch_up_search_projection_with_scheduler(
                scheduler,
                max_operations_per_batch,
                max_batches,
            )
    }

    fn admit_typed_mutation(&self) -> Result<RuntimePermit> {
        let store = self.read_store()?;
        let config = store.graph.database().config();
        let estimated_memory_bytes = crate::executor::estimated_mutation_memory_bytes(
            config.mutation_limits,
            config.max_wal_record_bytes,
        );
        store
            .graph
            .try_admit_runtime(RuntimeWorkRequest::foreground_mutation(
                estimated_memory_bytes,
            ))
    }

    fn admit_typed_read(&self, max_estimated_payload_bytes: usize) -> Result<RuntimePermit> {
        let store = self.read_store()?;
        let config = store.graph.database().config();
        let configured_result_bytes = config.max_read_result_payload_bytes.ok_or_else(|| {
            SkeinError::Execution(
                "admitted typed read requires max_read_result_payload_bytes".to_string(),
            )
        })?;
        if max_estimated_payload_bytes > configured_result_bytes {
            return Err(SkeinError::Execution(format!(
                "typed read payload budget {max_estimated_payload_bytes} exceeds configured limit {configured_result_bytes}"
            )));
        }
        let result_bytes = u64::try_from(max_estimated_payload_bytes).unwrap_or(u64::MAX);
        let working_memory_bytes =
            u64::try_from(config.execution_memory.blocking_operator_bytes.get())
                .unwrap_or(u64::MAX);
        store.graph.try_admit_runtime(
            RuntimeWorkRequest::foreground_query(
                working_memory_bytes.saturating_add(result_bytes),
                result_bytes,
            )
            .with_io_slots(1)
            .with_blocking(true),
        )
    }

    fn admit_typed_maintenance(
        &self,
        estimated_input_bytes: usize,
        io_slots: usize,
    ) -> Result<RuntimePermit> {
        let store = self.read_store()?;
        let config = store.graph.database().config();
        let working_memory_bytes =
            u64::try_from(config.execution_memory.blocking_operator_bytes.get())
                .unwrap_or(u64::MAX);
        let estimated_input_bytes = u64::try_from(estimated_input_bytes).unwrap_or(u64::MAX);
        store.graph.try_admit_runtime(
            RuntimeWorkRequest::background_maintenance(
                working_memory_bytes.saturating_add(estimated_input_bytes),
            )
            .with_io_slots(io_slots)
            .with_blocking(true),
        )
    }

    fn admit_typed_search(&self) -> Result<RuntimePermit> {
        let store = self.read_store()?;
        let config = store.graph.database().config();
        let result_bytes = config
            .max_read_result_payload_bytes
            .ok_or_else(|| {
                SkeinError::Execution(
                    "admitted typed search requires max_read_result_payload_bytes".to_string(),
                )
            })
            .and_then(|bytes| {
                u64::try_from(bytes).map_err(|_| {
                    SkeinError::Execution(
                        "admitted typed search result budget exceeds u64".to_string(),
                    )
                })
            })?;
        let working_memory_bytes =
            u64::try_from(config.execution_memory.blocking_operator_bytes.get())
                .unwrap_or(u64::MAX);
        let memory_bytes = store.search_memory_bytes(working_memory_bytes, result_bytes)?;
        let io_slots = store.search_io_slots()?;
        store.graph.try_admit_runtime(
            RuntimeWorkRequest::foreground_query(memory_bytes, result_bytes)
                .with_io_slots(io_slots)
                .with_blocking(true),
        )
    }

    fn admit_transaction(&self, statements: &[NowledgeGraphStatement]) -> Result<RuntimePermit> {
        let store = self.read_store()?;
        let db = store.graph.database();
        let config = db.config();
        let mut priority = RuntimeWorkPriority::Background;
        let mut kind = RuntimeWorkKind::Query;
        let mut estimated_memory_bytes = TYPED_CONTROL_STATEMENT_MEMORY_BYTES;
        let mut result_bytes = 0u64;
        let mut io_slots = 0usize;
        for statement in statements {
            let admission = db.runtime_admission_plan(&statement.cypher, &statement.parameters)?;
            if admission.work_request.priority == WorkPriority::Foreground {
                priority = RuntimeWorkPriority::Foreground;
            }
            if admission.is_mutation {
                kind = RuntimeWorkKind::Mutation;
            }
            estimated_memory_bytes = estimated_memory_bytes.max(admission.estimated_memory_bytes);
            io_slots = io_slots.max(admission.required_io_slots);
            let statement_result_bytes = if admission.is_mutation {
                config.mutation_limits.max_result_payload_bytes.get()
            } else {
                config.max_read_result_payload_bytes.ok_or_else(|| {
                    SkeinError::Execution(
                        "admitted transaction requires max_read_result_payload_bytes".to_string(),
                    )
                })?
            };
            result_bytes = result_bytes
                .saturating_add(u64::try_from(statement_result_bytes).unwrap_or(u64::MAX));
        }
        let request = RuntimeWorkRequest::new(priority, kind)
            .with_cpu_slots(1)
            .with_memory_bytes(estimated_memory_bytes)
            .with_io_slots(io_slots)
            .with_result_bytes(result_bytes)
            .with_blocking(true);
        store.graph.try_admit_runtime(request)
    }

    fn read_store(&self) -> Result<RwLockReadGuard<'_, NowledgeMemEmbeddedStore>> {
        self.inner.read().map_err(|_| {
            SkeinError::Execution("nowledge mem embedded store read lock poisoned".to_string())
        })
    }

    fn write_store(&self) -> Result<RwLockWriteGuard<'_, NowledgeMemEmbeddedStore>> {
        self.inner.write().map_err(|_| {
            SkeinError::Execution("nowledge mem embedded store write lock poisoned".to_string())
        })
    }
}

fn search_projection_delta_bytes(delta: &SearchProjectionDelta) -> usize {
    let upsert_bytes = delta.upserts.iter().fold(0usize, |total, row| {
        let embedding_bytes = row.embedding.as_ref().map_or(0, |embedding| {
            embedding.len().saturating_mul(size_of::<f32>())
        });
        let metadata_bytes = row
            .metadata
            .iter()
            .fold(0usize, |metadata_total, (key, value)| {
                metadata_total
                    .saturating_add(key.len())
                    .saturating_add(value.len())
            });
        total
            .saturating_add(size_of::<crate::SearchProjectionRow>())
            .saturating_add(row.external_id.len())
            .saturating_add(row.title.len())
            .saturating_add(row.body.len())
            .saturating_add(row.source_id.as_ref().map_or(0, String::len))
            .saturating_add(embedding_bytes)
            .saturating_add(metadata_bytes)
    });
    delta.deletes.iter().fold(upsert_bytes, |total, id| {
        total
            .saturating_add(size_of::<String>())
            .saturating_add(id.len())
    })
}

fn search_document_payload_bytes(document: &SearchDocument) -> usize {
    let embedding_bytes = document.embedding.as_ref().map_or(0, |embedding| {
        embedding.len().saturating_mul(size_of::<f32>())
    });
    let metadata_bytes = document
        .metadata
        .iter()
        .fold(0usize, |total, (key, value)| {
            total.saturating_add(key.len()).saturating_add(value.len())
        });
    size_of::<SearchDocument>()
        .saturating_add(document.id.len())
        .saturating_add(document.title.len())
        .saturating_add(document.content.len())
        .saturating_add(embedding_bytes)
        .saturating_add(metadata_bytes)
}

impl NowledgeMemEmbeddedStore {
    pub fn new(
        graph: NowledgeMemGraph,
        mut search_projection: Option<NowledgeMemSearchProjection>,
    ) -> Self {
        if let Some(projection) = search_projection.as_mut() {
            let mut range_read_config = projection.index().range_read_config();
            range_read_config.io_depth = range_read_config
                .io_depth
                .min(graph.runtime_governor_snapshot().limits.foreground_io_depth);
            projection.set_range_read_config(range_read_config);
        }
        Self {
            graph,
            search_projection,
            out_of_core_search_projection: None,
            retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor::default(),
        }
    }

    fn new_with_out_of_core_search(
        graph: NowledgeMemGraph,
        out_of_core_search_projection: NowledgeMemOutOfCoreSearchProjection,
        retrieval_projection_advisor: NowledgeMemRetrievalProjectionAdvisor,
    ) -> Self {
        Self {
            graph,
            search_projection: None,
            out_of_core_search_projection: Some(out_of_core_search_projection),
            retrieval_projection_advisor,
        }
    }

    pub fn open_with_options(
        options: NowledgeMemOpenOptions,
    ) -> Result<(Self, NowledgeMemOpenReport)> {
        let graph_config = options.effective_database_config();
        let runtime_governor =
            default_nowledge_mem_runtime_governor(&options.graph_path, &graph_config);
        Self::open_with_options_and_runtime_governor(options, runtime_governor)
    }

    pub fn open_with_options_and_runtime_governor(
        options: NowledgeMemOpenOptions,
        runtime_governor: RuntimeGovernor,
    ) -> Result<(Self, NowledgeMemOpenReport)> {
        options.validate()?;
        let mut report = options.sanitized_report();
        let graph_config = options.effective_database_config();
        let mut graph = NowledgeMemGraph::open_with_config_and_runtime_governor(
            &options.graph_path,
            graph_config,
            runtime_governor,
        )?;
        for registry in &options.system_schema_registries {
            report.system_schema_upgrades.push(
                graph
                    .database_mut()
                    .apply_system_schema_registry(registry)?,
            );
        }
        report.graph_opened = true;
        let default_search_range_read_config = SearchRangeReadConfig {
            io_depth: graph.runtime_governor_snapshot().limits.foreground_io_depth,
            ..SearchRangeReadConfig::default()
        };
        let Some(path) = options.search_projection_path.as_ref() else {
            return Ok((Self::new(graph, None), report));
        };
        match &options.search_projection_open_mode {
            NowledgeMemSearchProjectionOpenMode::FullResidencyMaintenance => {
                let mut projection = NowledgeMemSearchProjection::open(path)?;
                projection.set_range_read_config(
                    options
                        .search_range_read_config
                        .unwrap_or(default_search_range_read_config),
                );
                report.search_projection_opened = true;
                Ok((Self::new(graph, Some(projection)), report))
            }
            NowledgeMemSearchProjectionOpenMode::QualifiedOutOfCore(qualified) => {
                let graph_commit_epoch = graph.database().commit_epoch();
                if graph_commit_epoch != qualified.expected_identity.canonical_graph_commit_epoch {
                    return Err(SkeinError::Storage(format!(
                        "qualified out-of-core search expected canonical graph commit epoch {}, opened graph is at {graph_commit_epoch}",
                        qualified.expected_identity.canonical_graph_commit_epoch
                    )));
                }
                let projection = NowledgeMemOutOfCoreSearchProjection::open_production_with_config(
                    path,
                    qualified.config.clone(),
                    &qualified.qualification,
                    &qualified.expected_identity,
                )?;
                report.search_projection_opened = true;
                report.search_production_qualification_bound = true;
                Ok((
                    Self::new_with_out_of_core_search(
                        graph,
                        projection,
                        options.retrieval_projection_advisor.clone(),
                    ),
                    report,
                ))
            }
        }
    }

    pub fn graph(&self) -> &NowledgeMemGraph {
        &self.graph
    }

    pub fn graph_mut(&mut self) -> &mut NowledgeMemGraph {
        &mut self.graph
    }

    pub fn runtime_governor_snapshot(&self) -> RuntimeGovernorSnapshot {
        self.graph.runtime_governor_snapshot()
    }

    pub fn refresh_runtime_resources(&self) -> bool {
        self.graph.refresh_runtime_resources()
    }

    pub fn search_projection(&self) -> Option<&NowledgeMemSearchProjection> {
        self.search_projection.as_ref()
    }

    pub fn search_projection_mut(&mut self) -> Option<&mut NowledgeMemSearchProjection> {
        self.search_projection.as_mut()
    }

    pub fn out_of_core_search_projection(&self) -> Option<&NowledgeMemOutOfCoreSearchProjection> {
        self.out_of_core_search_projection.as_ref()
    }

    /// Applies a projection delta and checkpoints it before acknowledging
    /// success to the host. A checkpoint failure is returned to the caller,
    /// leaving the host free to retry its idempotent batch.
    pub fn apply_search_projection_delta_and_checkpoint(
        &mut self,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        let projection = require_search_projection_mut(&mut self.search_projection)?;
        let report = projection.index_mut().apply_projection_delta(delta)?;
        projection.index().checkpoint()?;
        Ok(report)
    }

    pub fn apply_initial_import_projection_delta_and_checkpoint(
        &mut self,
        delta: SearchProjectionDelta,
        import_source_graph_commit_epoch: u64,
    ) -> Result<SearchProjectionDeltaReport> {
        // The frozen legacy source epoch is provenance only. The database import
        // created a new local WAL history, so the projection cursor must be
        // stamped from that local history before it is made durable.
        let local_graph_commit_epoch = self.graph.database().commit_epoch();
        let projection = require_search_projection_mut(&mut self.search_projection)?;
        projection
            .index()
            .validate_import_source_graph_commit_epoch(import_source_graph_commit_epoch)?;
        let report = projection
            .index_mut()
            .apply_projection_delta(SearchProjectionDelta {
                source_graph_commit_epoch: Some(local_graph_commit_epoch),
                ..delta
            })?;
        projection
            .index_mut()
            .record_import_source_graph_commit_epoch(import_source_graph_commit_epoch)?;
        projection.index().checkpoint()?;
        Ok(report)
    }

    pub fn set_telemetry_sink(&mut self, telemetry: Option<Arc<dyn TelemetrySink>>) {
        self.graph
            .database_mut()
            .set_telemetry_sink(telemetry.clone());
        if let Some(search_projection) = &mut self.search_projection {
            search_projection.set_telemetry_sink(telemetry);
        }
    }

    pub fn storage_recovery_report(&self) -> NowledgeMemStorageRecoveryReport {
        NowledgeMemStorageRecoveryReport::from_storage_report(
            &self.graph.database().storage_recovery_report(),
        )
    }

    pub fn storage_recovery_report_json(&self) -> serde_json::Value {
        self.storage_recovery_report().json()
    }

    pub fn production_resource_profile(
        &self,
        statement: &NowledgeGraphStatement,
        limits: StorageResourceProfileLimits,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> Result<StorageResourceProfileReport> {
        self.graph
            .database()
            .storage_resource_profile_for_production(
                &statement.cypher,
                &statement.parameters,
                limits,
                evidence_binding,
                expected_identity,
            )
    }

    pub fn storage_lifecycle_decision(&self) -> NowledgeMemStorageLifecycleDecision {
        NowledgeMemStorageLifecycleDecision::from_storage_recovery(self.storage_recovery_report())
    }

    pub fn storage_lifecycle_decision_json(&self) -> serde_json::Value {
        self.storage_lifecycle_decision().json()
    }

    pub fn runtime_status(&self) -> NowledgeMemRuntimeStatus {
        let graph_commit_epoch = self.graph.database().commit_epoch();
        NowledgeMemRuntimeStatus {
            protocol: NOWLEDGE_MEM_RUNTIME_STATUS_PROTOCOL.to_string(),
            graph_commit_epoch,
            changefeed: self.graph.database().search_projection_changefeed_status(),
            projection_freshness: self.search_projection_freshness(),
        }
    }

    pub fn production_status(
        &self,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> NowledgeMemProductionStatus {
        NowledgeMemProductionStatus::from_runtime(
            self.graph.mode(),
            self.graph.database().config().read_only,
            self.runtime_status(),
            route_ownership.cloned(),
        )
    }

    pub fn production_status_json(
        &self,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> serde_json::Value {
        self.production_status(route_ownership).json()
    }

    pub fn cutover_controls_report(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> NowledgeMemCutoverControlsReport {
        self.cutover_controls_report_with_initial_import_cutover_catch_up(
            controls,
            route_ownership,
            None,
        )
    }

    pub fn cutover_controls_report_with_initial_import_cutover_catch_up(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_cutover_catch_up: Option<&SkeinLightningInitialImportCutoverCatchUpReport>,
    ) -> NowledgeMemCutoverControlsReport {
        NowledgeMemCutoverControlsReport::from_status_with_initial_import_cutover_catch_up(
            controls,
            self.production_status(route_ownership),
            initial_import_cutover_catch_up,
        )
    }

    pub fn cutover_controls_report_with_initial_import_recovery(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_recovery: Option<&SkeinLightningInitialImportRecoveryReadinessReport>,
    ) -> NowledgeMemCutoverControlsReport {
        let recovery_ready = initial_import_recovery.is_some_and(|report| {
            report.ready
                && report.startup.readiness.ready_for_cutover
                && report
                    .startup
                    .cutover_catch_up
                    .as_ref()
                    .is_some_and(|catch_up| catch_up.ready)
        });
        let catch_up = recovery_ready.then(|| {
            initial_import_recovery.and_then(|report| report.startup.cutover_catch_up.as_ref())
        });
        let mut report = self.cutover_controls_report_with_initial_import_cutover_catch_up(
            controls,
            route_ownership,
            catch_up.flatten(),
        );
        if report.initial_import_enabled && !recovery_ready {
            report.ready = false;
            report
                .blocker_codes
                .push("initial_import_recovery_not_ready".to_string());
            report.blocker_codes.sort();
            report.blocker_codes.dedup();
        }
        report
    }

    pub fn cutover_controls_report_json(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> serde_json::Value {
        self.cutover_controls_report(controls, route_ownership)
            .json()
    }

    pub fn cutover_controls_report_json_with_initial_import_cutover_catch_up(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_cutover_catch_up: Option<&SkeinLightningInitialImportCutoverCatchUpReport>,
    ) -> serde_json::Value {
        self.cutover_controls_report_with_initial_import_cutover_catch_up(
            controls,
            route_ownership,
            initial_import_cutover_catch_up,
        )
        .json()
    }

    pub fn build_search_projection_graph_delta_request_from_freshness(
        &self,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionGraphDeltaRequest>> {
        let search_projection = self.require_search_projection()?;
        self.graph
            .database()
            .build_search_projection_graph_delta_request_from_freshness(
                search_projection.index(),
                max_operations,
            )
    }

    pub fn search_projection_changefeed_readiness(
        &self,
        require_restart_recoverable: bool,
        max_operations: Option<usize>,
    ) -> Result<SearchProjectionChangefeedReadiness> {
        let search_projection = self.require_search_projection()?;
        Ok(self
            .graph
            .database()
            .search_projection_changefeed_readiness(
                search_projection.index(),
                require_restart_recoverable,
                max_operations,
            ))
    }

    pub fn search_projection_graph_delta_background_work_plan(
        &self,
        request: &SearchProjectionGraphDeltaRequest,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let search_projection = self.search_projection.as_ref()?;
        self.graph
            .database()
            .search_projection_graph_delta_freshness_background_work_plan(
                search_projection.index(),
                request,
                hint,
            )
    }

    pub fn apply_search_projection_graph_delta(
        &mut self,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        let Self {
            graph,
            search_projection,
            ..
        } = self;
        let search_projection = require_search_projection_mut(search_projection)?;
        graph
            .database()
            .apply_search_projection_graph_delta(search_projection.index_mut(), request)
    }

    pub fn catch_up_search_projection(
        &mut self,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<SearchProjectionCatchUpReport> {
        let Self {
            graph,
            search_projection,
            ..
        } = self;
        let search_projection = require_search_projection_mut(search_projection)?;
        graph.database().catch_up_search_projection(
            search_projection.index_mut(),
            max_operations_per_batch,
            max_batches,
        )
    }

    pub fn catch_up_search_projection_with_relational<F>(
        &mut self,
        max_operations_per_batch: usize,
        max_batches: usize,
        relational_hydrator: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        let Self {
            graph,
            search_projection,
            ..
        } = self;
        let search_projection = require_search_projection_mut(search_projection)?;
        graph.database().catch_up_search_projection_with_relational(
            search_projection.index_mut(),
            max_operations_per_batch,
            max_batches,
            relational_hydrator,
        )
    }

    pub fn catch_up_search_projection_with_batch_hydrator<F>(
        &mut self,
        max_change_operations_per_batch: usize,
        max_projection_operations_per_batch: usize,
        max_batches: usize,
        batch_hydrator: F,
    ) -> Result<SearchProjectionCatchUpReport>
    where
        F: FnMut(
            &mut DatabaseReadTransaction,
            &SearchProjectionChangeBatch,
        ) -> Result<SearchProjectionRelationalDelta>,
    {
        let Self {
            graph,
            search_projection,
            ..
        } = self;
        let search_projection = require_search_projection_mut(search_projection)?;
        graph
            .database()
            .catch_up_search_projection_with_batch_hydrator(
                search_projection.index_mut(),
                max_change_operations_per_batch,
                max_projection_operations_per_batch,
                max_batches,
                batch_hydrator,
            )
    }

    pub fn catch_up_search_projection_with_scheduler(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<ScheduledSearchProjectionCatchUpReport> {
        let Self {
            graph,
            search_projection,
            ..
        } = self;
        let search_projection = require_search_projection_mut(search_projection)?;
        graph.database().catch_up_search_projection_with_scheduler(
            search_projection.index_mut(),
            scheduler,
            max_operations_per_batch,
            max_batches,
        )
    }

    pub fn apply_scheduled_background_search_projection_graph_delta(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        let Self {
            graph,
            search_projection,
            ..
        } = self;
        let search_projection = require_search_projection_mut(search_projection)?;
        graph
            .database()
            .apply_scheduled_background_search_projection_graph_delta(
                search_projection.index_mut(),
                scheduler,
                request,
            )
    }

    pub fn search_projection_probe_json(
        &self,
        options: SearchProjectionProbeOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.require_search_projection()?.probe_json(options))
    }

    pub fn search_projection_evidence_json(
        &self,
        options: SearchProjectionProbeOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.require_search_projection()?.evidence_json(options))
    }

    pub fn search_projection_evidence_report(
        &self,
        options: SearchProjectionProbeOptions,
    ) -> Result<NowledgeSearchProjectionEvidenceReport> {
        Ok(self.require_search_projection()?.evidence_report(options))
    }

    pub fn validate_sampled_vector_recall(
        &self,
        options: VectorRecallValidationOptions,
    ) -> Result<VectorRecallValidationReport> {
        Ok(self
            .require_search_projection()?
            .validate_sampled_vector_recall(options))
    }

    pub fn qualify_sampled_vector_recall_for_production(
        &self,
        options: VectorRecallValidationOptions,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> Result<VectorRecallProductionQualificationReport> {
        Ok(self
            .require_search_projection()?
            .qualify_sampled_vector_recall_for_production(
                options,
                evidence_binding,
                expected_identity,
            ))
    }

    pub fn search_projection_shadow_evidence_json(
        &self,
        primary_probe: &serde_json::Value,
        options: SearchProjectionProbeOptions,
    ) -> Result<serde_json::Value> {
        Ok(self
            .require_search_projection()?
            .shadow_evidence_json(primary_probe, options))
    }

    pub fn search_candidates(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<NowledgeMemSearchCandidateOutput> {
        match (
            self.search_projection.as_ref(),
            self.out_of_core_search_projection.as_ref(),
        ) {
            (Some(projection), None) => projection.try_search_candidates_with_report(request),
            (None, Some(projection)) => {
                Ok(projection.search_candidates_with_report(request)?.into())
            }
            (None, None) => Err(missing_search_projection_error()),
            (Some(_), Some(_)) => Err(ambiguous_search_projection_error()),
        }
    }

    pub fn search_candidate_readiness(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        options: &NowledgeMemSearchCandidateReadinessOptions,
    ) -> Result<NowledgeMemSearchCandidateReadinessReport> {
        Ok(self.search_candidates(request)?.readiness_report(options))
    }

    pub fn search_candidate_shadow_evidence_json<I, S>(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        primary_candidate_ids: I,
    ) -> Result<serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Ok(self
            .require_search_projection()?
            .search_candidate_shadow_evidence_json(request, primary_candidate_ids))
    }

    pub fn retrieve_knowledge(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        Ok(self.retrieve_knowledge_with_report(request)?.output)
    }

    pub fn retrieve_knowledge_with_report(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<NowledgeMemRetrievalOutput> {
        let (output, out_of_core_search_metrics) = match (
            self.search_projection.as_ref(),
            self.out_of_core_search_projection.as_ref(),
        ) {
            (Some(search_projection), None) => (
                self.graph
                    .database()
                    .try_retrieve_knowledge(search_projection.index(), request)?,
                None,
            ),
            (None, Some(search_projection)) => {
                let search = search_projection
                    .search_candidates_with_report(&self.search_request_for_retrieval(request))?;
                let output = self.graph.database().try_retrieve_knowledge_from_search(
                    search.result,
                    search_projection.freshness(),
                    request,
                )?;
                (output, Some(search.metrics))
            }
            (None, None) => return Err(missing_search_projection_error()),
            (Some(_), Some(_)) => return Err(ambiguous_search_projection_error()),
        };
        let report = nowledge_mem_retrieval_report(
            self.graph.mode(),
            self.graph.database().config().compressed_vector_search_mode,
            &output,
        );
        Ok(NowledgeMemRetrievalOutput {
            output,
            report,
            out_of_core_search_metrics,
        })
    }

    pub fn read_query(&self, cypher: &str) -> Result<NowledgeMemReadOutput> {
        self.graph.read_query(cypher)
    }

    pub fn query_with_report(&mut self, cypher: &str) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report_options(
            cypher,
            &BTreeMap::new(),
            NowledgeMemQueryReportOptions::default(),
        )
    }

    pub fn query_with_report_options(
        &mut self,
        cypher: &str,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report_options(cypher, &BTreeMap::new(), options)
    }

    pub fn query_with_params_with_report(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_with_params_with_report_options(
            cypher,
            parameters,
            NowledgeMemQueryReportOptions::default(),
        )
    }

    pub fn query_with_params_with_report_options(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        let Self {
            graph,
            search_projection,
            out_of_core_search_projection,
            ..
        } = self;
        let mut external = SearchProjectionExternalReadOperator {
            projection: search_projection.as_ref(),
            out_of_core_projection: out_of_core_search_projection.as_ref(),
            vector_seed_execution_count: 0,
        };
        graph.query_with_params_with_report_options_and_external(
            cypher,
            parameters,
            options,
            &mut external,
        )
    }

    pub fn query_with_params_with_report_options_context(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: NowledgeMemQueryReportOptions,
        task_context: &RuntimeTaskContext,
    ) -> Result<NowledgeMemQueryOutput> {
        let Self {
            graph,
            search_projection,
            out_of_core_search_projection,
            ..
        } = self;
        let mut external = SearchProjectionExternalReadOperator {
            projection: search_projection.as_ref(),
            out_of_core_projection: out_of_core_search_projection.as_ref(),
            vector_seed_execution_count: 0,
        };
        graph.query_with_params_with_report_options_and_external_context(
            cypher,
            parameters,
            options,
            &mut external,
            task_context,
        )
    }

    pub fn query_runtime_preflight(
        &mut self,
        probes: &[NowledgeQueryRuntimePreflightProbe],
    ) -> NowledgeQueryRuntimePreflightReport {
        nowledge_query_runtime_preflight_report(self.graph.database_mut(), probes)
    }

    pub fn query_runtime_preflight_json(
        &mut self,
        probes: &[NowledgeQueryRuntimePreflightProbe],
    ) -> serde_json::Value {
        self.query_runtime_preflight(probes).json()
    }

    pub fn slow_query_report(&self) -> NowledgeMemSlowQueryReport {
        self.graph.slow_query_report()
    }

    pub fn slow_query_report_json(&self) -> serde_json::Value {
        self.slow_query_report().json()
    }

    pub fn read_query_with_options(
        &self,
        cypher: &str,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.graph.read_query_with_options(cypher, options)
    }

    pub fn read_query_with_params(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.graph
            .read_query_with_params(cypher, parameters, options)
    }

    pub fn read_query_with_params_streaming(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
        consumer: impl FnMut(BTreeMap<String, Value>) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.graph
            .read_query_with_params_streaming(cypher, parameters, options, consumer)
    }

    pub fn read_query_with_params_streaming_context(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
        task_context: &RuntimeTaskContext,
        consumer: impl FnMut(BTreeMap<String, Value>) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.graph.read_query_with_params_streaming_context(
            cypher,
            parameters,
            options,
            task_context,
            consumer,
        )
    }

    pub fn read_query_with_params_streaming_collect(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.graph
            .read_query_with_params_streaming_collect(cypher, parameters, options)
    }

    pub fn graph_rag_schema_context(
        &self,
        options: GraphRagSchemaContextOptions,
    ) -> GraphRagSchemaContext {
        self.graph.graph_rag_schema_context(options)
    }

    pub fn read_generated_graph_rag(
        &self,
        query: &GraphRagGeneratedQuery,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        self.graph
            .read_generated_graph_rag(query, parameters, options)
    }
}
impl NowledgeMemEmbeddedStore {
    pub fn background_maintenance_summary(
        &self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> BackgroundMaintenanceSummary {
        self.graph.database().background_maintenance_summary(
            self.search_projection
                .as_ref()
                .map(NowledgeMemSearchProjection::index),
            policy,
            state,
            options,
        )
    }

    pub fn background_maintenance_report(
        &self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> NowledgeMemBackgroundMaintenanceReport {
        let summary = self.background_maintenance_summary(policy, state, options);
        let slow_query = self.slow_query_report();
        NowledgeMemBackgroundMaintenanceReport::from_summary_with_slow_query(
            &summary,
            Some(&slow_query),
        )
    }

    pub fn background_maintenance_report_json(
        &self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> serde_json::Value {
        self.background_maintenance_report(policy, state, options)
            .json()
    }

    pub fn library_readiness_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> serde_json::Value {
        self.library_readiness(options).json()
    }

    pub fn library_readiness(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> NowledgeMemLibraryReadinessReport {
        let bounded_read_evidence = options
            .bounded_read_evidence
            .clone()
            .unwrap_or_else(|| self.bounded_read_probe_evidence_json(options));
        let storage_recovery = self.storage_recovery_report_json();
        let background_maintenance = self.background_maintenance_report_json(
            &options.qos_policy,
            &options.qos_state,
            options.background_maintenance_options.clone(),
        );
        let query_family_evidence = query_family_replacement_evidence_json(
            options.replacement_readiness_by_query_family.as_ref(),
        );
        let graph_route_readiness =
            nowledge_mem_graph_route_readiness_json(options.graph_route_readiness.as_ref());
        let search_route_ownership = options
            .search_route_ownership
            .as_ref()
            .map(NowledgeMemSearchRouteOwnershipReadinessReport::json)
            .unwrap_or_else(missing_search_route_ownership_json);
        let active_search_route_ownership = options
            .active_search_route_ownership
            .as_ref()
            .map(NowledgeMemActiveSearchRouteOwnershipReadinessReport::json)
            .unwrap_or_else(missing_active_search_route_ownership_json);
        let active_search_route_readiness = options
            .active_search_route_readiness
            .as_ref()
            .map(NowledgeMemActiveSearchRouteReadinessReport::json)
            .unwrap_or_else(missing_active_search_route_readiness_json);
        let search_projection_evidence =
            options
                .search_projection_evidence
                .clone()
                .unwrap_or_else(|| {
                    self.search_projection_evidence_json(
                        options.search_projection_probe_options.clone(),
                    )
                    .unwrap_or_else(|_| missing_search_projection_evidence_json())
                });
        let search_projection_shadow_evidence = options
            .search_projection_shadow_evidence
            .clone()
            .unwrap_or_else(|| {
                options
                    .primary_search_projection_probe
                    .as_ref()
                    .map(|primary_probe| {
                        self.search_projection_shadow_evidence_json(
                            primary_probe,
                            options.search_projection_probe_options.clone(),
                        )
                        .unwrap_or_else(|_| missing_search_projection_shadow_evidence_json())
                    })
                    .unwrap_or_else(missing_primary_search_projection_probe_json)
            });
        let search_candidate_shadow_evidence = options
            .search_candidate_shadow_evidence
            .clone()
            .unwrap_or_else(missing_search_candidate_shadow_evidence_json);
        let workload_fixture_evidence = options
            .workload_fixture_evidence
            .as_ref()
            .map(NowledgeGraphRouteWorkloadFixtureReport::json)
            .unwrap_or_else(missing_workload_fixture_evidence_json);
        let production_resource_profile = options
            .production_resource_profile
            .as_ref()
            .map(StorageResourceProfileReport::json)
            .unwrap_or_else(missing_production_resource_profile_json);
        let evidence = LibraryReadinessEvidence {
            bounded_read_evidence: &bounded_read_evidence,
            storage_recovery: &storage_recovery,
            background_maintenance: &background_maintenance,
            query_family_evidence: &query_family_evidence,
            graph_route_readiness: &graph_route_readiness,
            search_route_ownership: &search_route_ownership,
            active_search_route_ownership: &active_search_route_ownership,
            active_search_route_readiness: &active_search_route_readiness,
            search_projection_evidence: &search_projection_evidence,
            search_projection_shadow_evidence: &search_projection_shadow_evidence,
            search_candidate_shadow_evidence: &search_candidate_shadow_evidence,
            workload_fixture_evidence: &workload_fixture_evidence,
            production_resource_profile: &production_resource_profile,
        };
        let blocker_codes = library_readiness_blocker_codes(&evidence);
        let readiness_by_area = library_readiness_by_area(&evidence);
        let areas = readiness_by_area.areas();
        let ready_area_count = areas.iter().filter(|area| area.ready).count();
        let blocked_area_count = areas.len().saturating_sub(ready_area_count);
        let blocker_codes = blocker_codes
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        let ready = blocker_codes.is_empty();

        NowledgeMemLibraryReadinessReport {
            protocol: NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL.to_string(),
            present: true,
            ready,
            mode: self.graph.mode(),
            redaction: NowledgeMemReadinessRedactionSummary::default(),
            production_path: NowledgeMemLibraryProductionPathSummary::default(),
            blocker_codes,
            readiness_by_area,
            ready_area_count,
            blocked_area_count,
            graph_open: true,
            graph_read_only: self.graph.database().config().read_only,
            graph_route_readiness,
            search_route_ownership,
            active_search_route_ownership,
            active_search_route_readiness,
            bounded_read_evidence,
            storage_recovery,
            background_maintenance,
            query_family_evidence,
            search_projection_evidence,
            search_projection_shadow_evidence,
            search_candidate_shadow_evidence,
            workload_fixture_evidence,
            production_resource_profile,
        }
    }

    pub fn readiness_dashboard(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> NowledgeMemReadinessDashboard {
        let library = self.library_readiness(options);
        let storage_lifecycle = self.storage_lifecycle_decision();
        let slow_query = self.slow_query_report();
        NowledgeMemReadinessDashboard::from_reports(&library, &storage_lifecycle, &slow_query)
    }

    pub fn readiness_dashboard_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> serde_json::Value {
        self.readiness_dashboard(options).json()
    }

    pub fn operations_readiness(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> NowledgeMemOperationsReadinessReport {
        let runtime_status = self.runtime_status();
        let storage_recovery = self.storage_recovery_report();
        let slow_query = self.slow_query_report();
        let background_maintenance = self.background_maintenance_report(
            &options.qos_policy,
            &options.qos_state,
            options.background_maintenance_options.clone(),
        );
        NowledgeMemOperationsReadinessReport::from_reports(
            self.graph.mode(),
            self.graph.database().config().read_only,
            runtime_status,
            storage_recovery,
            slow_query,
            background_maintenance,
        )
    }

    pub fn operations_readiness_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> serde_json::Value {
        self.operations_readiness(options).json()
    }

    fn bounded_read_probe_evidence_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> serde_json::Value {
        let Some(probe) = options.bounded_read_probe.as_ref() else {
            return serde_json::json!({
                "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
                "present": false,
                "ready": false,
                "blocker_codes": ["bounded_read_probe_missing"],
            });
        };
        match self.read_query_with_params(&probe.cypher, &probe.parameters, &options.read_options) {
            Ok(read) => nowledge_mem_bounded_read_evidence_json_with_route_readiness(
                &read.report,
                &options.covered_routes,
                options.graph_route_readiness.as_ref(),
            ),
            Err(_) => serde_json::json!({
                "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
                "present": true,
                "ready": false,
                "blocker_codes": ["bounded_read_probe_failed"],
            }),
        }
    }

    fn search_projection_freshness(&self) -> Option<SearchProjectionFreshness> {
        match (
            self.search_projection.as_ref(),
            self.out_of_core_search_projection.as_ref(),
        ) {
            (Some(projection), None) => Some(projection.freshness()),
            (None, Some(projection)) => Some(projection.freshness()),
            _ => None,
        }
    }

    fn search_io_slots(&self) -> Result<usize> {
        match (
            self.search_projection.as_ref(),
            self.out_of_core_search_projection.as_ref(),
        ) {
            (Some(projection), None) => Ok(projection.index().range_read_config().io_depth.get()),
            (None, Some(_)) => Ok(1),
            (None, None) => Err(missing_search_projection_error()),
            (Some(_), Some(_)) => Err(ambiguous_search_projection_error()),
        }
    }

    fn search_memory_bytes(
        &self,
        working_memory_bytes: u64,
        result_budget_bytes: u64,
    ) -> Result<u64> {
        match (
            self.search_projection.as_ref(),
            self.out_of_core_search_projection.as_ref(),
        ) {
            (Some(_), None) => Ok(working_memory_bytes),
            (None, Some(projection)) => Ok(projection
                .runtime_admission_memory_bytes(working_memory_bytes, result_budget_bytes)),
            (None, None) => Err(missing_search_projection_error()),
            (Some(_), Some(_)) => Err(ambiguous_search_projection_error()),
        }
    }

    fn search_request_for_retrieval(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> NowledgeMemSearchCandidateRequest {
        let config = self.graph.database().config();
        NowledgeMemSearchCandidateRequest {
            query_text: request.query_text.clone(),
            query_embedding: request.query_embedding.clone(),
            mode: request.mode,
            limit: request.limit,
            offset: request.offset,
            rank_window: request.rank_window,
            fusion_weights: request.search_fusion_weights,
            metadata_filters: request.metadata_filters.clone(),
            compressed_vector_search_mode: config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: config.adaptive_vector_backend_policy,
            recall_validation_probe: false,
            retrieval_projection_advisor: self.retrieval_projection_advisor.clone(),
        }
    }

    fn require_search_projection(&self) -> Result<&NowledgeMemSearchProjection> {
        self.search_projection
            .as_ref()
            .ok_or_else(missing_search_projection_error)
    }
}

fn missing_search_projection_error() -> SkeinError {
    SkeinError::Storage("nowledge mem search projection is not configured".to_string())
}

fn ambiguous_search_projection_error() -> SkeinError {
    SkeinError::Storage("nowledge mem search projection ownership is ambiguous".to_string())
}

fn require_search_projection_mut(
    search_projection: &mut Option<NowledgeMemSearchProjection>,
) -> Result<&mut NowledgeMemSearchProjection> {
    search_projection
        .as_mut()
        .ok_or_else(missing_search_projection_error)
}

struct NowledgeMemQueryReportInput<'a> {
    mode: NowledgeMemGraphMode,
    statement: &'a cypher::Statement,
    trace: Option<&'a crate::optimizer::OptimizerTrace>,
    plan_cache_lookup: Option<PlanCacheLookup>,
    execution_profile: Option<&'a ReadExecutionProfile>,
    output: &'a QueryOutput,
    options: NowledgeMemQueryReportOptions,
    elapsed_micros: u128,
}

fn nowledge_mem_query_report(input: NowledgeMemQueryReportInput<'_>) -> NowledgeMemQueryReport {
    let statement_kind = crate::api::statement_kind(nowledge_statement_body(input.statement));
    let decision = nowledge_mem_fast_path_classification(input.statement);
    let slow_log_candidate = input
        .options
        .slow_log_threshold_micros
        .is_some_and(|threshold| input.elapsed_micros >= threshold);
    let plan_cache = NowledgeMemPlanCacheReport::from_lookup(input.plan_cache_lookup);
    NowledgeMemQueryReport {
        protocol: NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL.to_string(),
        mode: input.mode,
        statement_kind: statement_kind.to_string(),
        execution_path: decision.execution_path,
        fast_path_reason: decision.fast_path_reason.map(str::to_string),
        elapsed_micros: input.elapsed_micros,
        slow_log_threshold_micros: input.options.slow_log_threshold_micros,
        slow_log_candidate,
        physical_plan_captured: input.trace.is_some(),
        plan_cache_lookup: input
            .plan_cache_lookup
            .map(|lookup| lookup.as_str().to_string()),
        plan_cache_bypass_reason: input
            .plan_cache_lookup
            .and_then(|lookup| lookup.bypass_reason())
            .map(|reason| reason.as_str().to_string()),
        plan_cache_cacheable: plan_cache.cacheable,
        plan_cache_hit: plan_cache.hit,
        plan_cache_miss: plan_cache.miss,
        plan_cache_bypassed: plan_cache.bypassed,
        physical_operator_counts: input
            .trace
            .map(|trace| trace.selected_plan_operator_counts.clone())
            .unwrap_or_default(),
        optimizer_decision_count: input
            .trace
            .map(|trace| trace.decisions.len())
            .unwrap_or_default(),
        optimizer_rule_event_count: input
            .trace
            .map(|trace| trace.rule_events.len())
            .unwrap_or_default(),
        scan_pruning_reports: input
            .execution_profile
            .map(|profile| profile.scan_pruning_reports.clone())
            .unwrap_or_default(),
        vector_execution_reports: input
            .execution_profile
            .map(|profile| profile.vector_execution_reports.clone())
            .unwrap_or_default(),
        graph_expansion_reports: input
            .execution_profile
            .map(|profile| profile.graph_expansion_reports.clone())
            .unwrap_or_default(),
        pipeline_memory_report: input
            .execution_profile
            .map(|profile| profile.pipeline_memory_report.clone()),
        output_row_shape: NowledgeMemQueryOutputRowShape::from_output(input.output),
        api_behavior: NowledgeMemQueryApiBehavior::from_statement(input.statement),
    }
}

fn vector_execution_report_json(
    report: &skein_executor::VectorExecutionReport,
) -> serde_json::Value {
    serde_json::json!({
        "backend": report.backend.as_str(),
        "compression_mode": report.compression_mode.as_str(),
        "candidate_source": report.candidate_source.as_str(),
        "backend_selection_reason": report.backend_selection_reason.map(|reason| reason.as_str()),
        "estimated_raw_vector_bytes": report.estimated_raw_vector_bytes,
        "filter_selectivity_per_million": report.filter_selectivity_per_million,
        "candidate_score_source": report.candidate_score_source.as_str(),
        "final_score_source": report.final_score_source.as_str(),
        "generated_candidate_count": report.generated_candidate_count,
        "descriptor_pruned_count": report.descriptor_pruned_count,
        "scalar_filtered_count": report.scalar_filtered_count,
        "residual_filtered_count": report.residual_filtered_count,
        "candidate_scan_rounds": report.candidate_scan_rounds,
        "reranked_candidate_count": report.reranked_candidate_count,
        "returned_count": report.returned_count,
        "raw_vector_bytes_read": report.raw_vector_bytes_read,
        "candidate_scan": report.candidate_scan_metrics.as_ref().map(|metrics| serde_json::json!({
            "kernel": metrics.kernel,
            "worker_count": metrics.worker_count,
            "segment_count": metrics.segment_count,
            "scanned_segment_count": metrics.scanned_segment_count,
            "scored_document_count": metrics.scored_document_count,
            "filtered_document_count": metrics.filtered_document_count,
            "scanned_block_count": metrics.scanned_block_count,
            "skipped_block_count": metrics.skipped_block_count,
            "payload_bytes_read": metrics.payload_bytes_read,
            "admitted_working_bytes": metrics.admitted_working_bytes,
        })),
        "index_covered_document_count": report.index_covered_document_count,
        "index_candidate_document_count": report.index_candidate_document_count,
        "index_coverage_complete": report.index_coverage_complete,
        "fallback_reason_codes": report.fallback_reason_codes.iter().map(|code| code.as_str()).collect::<Vec<_>>(),
    })
}

fn pipeline_memory_report_json(report: &skein_executor::PipelineMemoryReport) -> serde_json::Value {
    serde_json::json!({
        "intermediate_rows": report.intermediate_rows,
        "intermediate_payload_bytes": report.intermediate_payload_bytes,
        "peak_batch_rows": report.peak_batch_rows,
        "peak_batch_payload_bytes": report.peak_batch_payload_bytes,
        "output_rows": report.output_rows,
        "output_payload_bytes": report.output_payload_bytes,
        "start_resident_bytes": report.start_resident_bytes,
        "start_peak_resident_bytes": report.start_peak_resident_bytes,
        "steady_resident_bytes": report.steady_resident_bytes,
        "peak_resident_bytes": report.peak_resident_bytes,
        "steady_resident_growth_bytes": report.steady_resident_growth_bytes,
        "lifetime_peak_resident_growth_bytes": report.lifetime_peak_resident_growth_bytes,
        "total_page_faults": report.total_page_faults,
        "minor_page_faults": report.minor_page_faults,
        "major_page_faults": report.major_page_faults,
    })
}

fn graph_expansion_report_json(
    report: &skein_executor::GraphExpansionExecutionReport,
) -> serde_json::Value {
    serde_json::json!({
        "seed_count": report.seed_count,
        "expanded_node_count": report.expanded_node_count,
        "expanded_edge_count": report.expanded_edge_count,
        "relation_types": report.relation_types,
        "min_hops": report.min_hops,
        "max_hops": report.max_hops,
        "reranked_seed_count": report.reranked_seed_count,
        "candidate_limit": report.candidate_limit,
        "payload_byte_limit": report.payload_byte_limit,
        "payload_bytes_used": report.payload_bytes_used,
        "returned_count": report.returned_count,
        "truncated": report.truncated(),
        "truncation_reason": report.truncation_reason.map(|reason| reason.as_str()),
    })
}

fn scan_pruning_report_json(report: &ScanPruningReport) -> serde_json::Value {
    serde_json::json!({
        "target_kind": report.target_kind.as_str(),
        "label_id": report.label_id.map(|label_id| label_id.0),
        "rel_type_id": report.rel_type_id.map(|rel_type_id| rel_type_id.0),
        "strategy": scan_pruning_strategy_json(&report.strategy),
        "pruned": report.pruned,
        "exact_empty": report.exact_empty,
        "candidate_count_before_pruning": report.candidate_count_before_pruning,
        "pruned_candidate_count": report.pruned_candidate_count,
        "candidate_count_before_filter": report.candidate_count_before_filter,
        "output_count": report.output_count,
        "filtered_out_count": report.filtered_out_count,
    })
}

fn scan_pruning_strategy_json(strategy: &ScanPruningStrategy) -> serde_json::Value {
    match strategy {
        ScanPruningStrategy::FullLabelScan => serde_json::json!({"kind": "full_label_scan"}),
        ScanPruningStrategy::ExactCount => serde_json::json!({"kind": "exact_count"}),
        ScanPruningStrategy::Empty => serde_json::json!({"kind": "empty"}),
        ScanPruningStrategy::IdEq => serde_json::json!({"kind": "id_eq"}),
        ScanPruningStrategy::IdIn => serde_json::json!({"kind": "id_in"}),
        ScanPruningStrategy::IdRange => serde_json::json!({"kind": "id_range"}),
        ScanPruningStrategy::PropertyEq { property } => {
            serde_json::json!({"kind": "property_eq", "property": property})
        }
        ScanPruningStrategy::PropertyNotEq { property } => {
            serde_json::json!({"kind": "property_not_eq", "property": property})
        }
        ScanPruningStrategy::PropertyMissingOrNull { property } => {
            serde_json::json!({"kind": "property_missing_or_null", "property": property})
        }
        ScanPruningStrategy::PropertyExists { property } => {
            serde_json::json!({"kind": "property_exists", "property": property})
        }
        ScanPruningStrategy::PropertyDefaultIfNullEq { property } => {
            serde_json::json!({"kind": "property_default_if_null_eq", "property": property})
        }
        ScanPruningStrategy::PropertyDefaultIfNullNotEq { property } => {
            serde_json::json!({"kind": "property_default_if_null_not_eq", "property": property})
        }
        ScanPruningStrategy::PropertyIn { property } => {
            serde_json::json!({"kind": "property_in", "property": property})
        }
        ScanPruningStrategy::CompositePropertyEq { properties } => {
            serde_json::json!({"kind": "composite_property_eq", "properties": properties})
        }
        ScanPruningStrategy::CompositePropertyRange { properties } => serde_json::json!({
            "kind": "composite_property_range",
            "properties": properties,
        }),
        ScanPruningStrategy::PropertyRange { property } => {
            serde_json::json!({"kind": "property_range", "property": property})
        }
        ScanPruningStrategy::FullText { property } => {
            serde_json::json!({"kind": "full_text", "property": property})
        }
        ScanPruningStrategy::OrUnion => serde_json::json!({"kind": "or_union"}),
    }
}

#[derive(Debug)]
struct NowledgeQueryRuntimeRouteCoverage {
    required_route_count: usize,
    covered_route_count: usize,
    covered_routes: Vec<String>,
    missing_required_routes: Vec<String>,
    required_routes_covered: bool,
    unknown_routes: Vec<String>,
    duplicate_routes: Vec<String>,
    ready: bool,
    blocker_codes: Vec<String>,
}

fn nowledge_query_runtime_preflight_report(
    db: &mut Database,
    probes: &[NowledgeQueryRuntimePreflightProbe],
) -> NowledgeQueryRuntimePreflightReport {
    let route_coverage = nowledge_query_runtime_route_coverage(probes);
    let probe_reports = probes
        .iter()
        .map(|probe| nowledge_query_runtime_probe_report(db, probe))
        .collect::<Vec<_>>();
    let passed_probe_count = probe_reports.iter().filter(|probe| probe.ready).count();
    let failed_probe_count = probe_reports.len().saturating_sub(passed_probe_count);
    let blocker_codes =
        query_runtime_preflight_blocker_codes(probes.len(), failed_probe_count, &route_coverage);

    NowledgeQueryRuntimePreflightReport {
        protocol: NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL.to_string(),
        ready: blocker_codes.is_empty(),
        database_opened: true,
        redaction: NowledgeQueryRuntimePreflightRedactionSummary::default(),
        probe_count: probes.len(),
        passed_probe_count,
        failed_probe_count,
        required_route_count: route_coverage.required_route_count,
        covered_route_count: route_coverage.covered_route_count,
        covered_routes: route_coverage.covered_routes,
        missing_required_routes: route_coverage.missing_required_routes,
        required_routes_covered: route_coverage.required_routes_covered,
        unknown_routes: route_coverage.unknown_routes,
        duplicate_routes: route_coverage.duplicate_routes,
        route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
        route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
        route_coverage_ready: route_coverage.ready,
        route_coverage_blocker_codes: route_coverage.blocker_codes,
        blocker_codes,
        probes: probe_reports,
    }
}

fn nowledge_query_runtime_route_coverage(
    probes: &[NowledgeQueryRuntimePreflightProbe],
) -> NowledgeQueryRuntimeRouteCoverage {
    let required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut route_counts = BTreeMap::<&str, usize>::new();
    for route in probes.iter().filter_map(|probe| probe.route.as_deref()) {
        *route_counts.entry(route).or_default() += 1;
    }
    let observed_routes = route_counts.keys().copied().collect::<BTreeSet<_>>();
    let covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| observed_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !observed_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let unknown_routes = observed_routes
        .iter()
        .filter(|route| !required_routes.contains(**route))
        .map(|route| (*route).to_string())
        .collect::<Vec<_>>();
    let duplicate_routes = route_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(route, _)| (*route).to_string())
        .collect::<Vec<_>>();
    let required_routes_covered = missing_required_routes.is_empty();
    let mut blocker_codes = Vec::new();
    if !required_routes_covered {
        blocker_codes.push("query_runtime_route_coverage_missing".to_string());
    }
    if !unknown_routes.is_empty() {
        blocker_codes.push("query_runtime_unknown_routes".to_string());
    }
    let ready = blocker_codes.is_empty();

    NowledgeQueryRuntimeRouteCoverage {
        required_route_count: REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        covered_route_count: covered_routes.len(),
        covered_routes,
        missing_required_routes,
        required_routes_covered,
        unknown_routes,
        duplicate_routes,
        ready,
        blocker_codes,
    }
}

fn nowledge_query_runtime_probe_report(
    db: &mut Database,
    probe: &NowledgeQueryRuntimePreflightProbe,
) -> NowledgeQueryRuntimePreflightProbeReport {
    match db.explain_analyze_query_with_params(&probe.cypher, &probe.parameters) {
        Ok(output) => {
            let scan_pruning_report_count = output.execution_profile.scan_pruning_reports.len();
            let pruned_scan_count = output
                .execution_profile
                .scan_pruning_reports
                .iter()
                .filter(|report| report.pruned)
                .count();
            let output_row_count = output.output.rows.len();
            let blocker_codes = query_runtime_probe_blocker_codes(
                probe,
                scan_pruning_report_count,
                pruned_scan_count,
                output_row_count,
            );
            let plan_cache_lookup = output.plan_cache_lookup;
            let plan_cache = NowledgeMemPlanCacheReport::from_lookup(Some(plan_cache_lookup));
            NowledgeQueryRuntimePreflightProbeReport {
                name: probe.name.clone(),
                route: probe.route.clone(),
                query_family: probe.query_family.clone(),
                ready: blocker_codes.is_empty(),
                success: true,
                output_row_count,
                selected_plan_fingerprint: Some(output.trace.selected_plan_fingerprint),
                search_mode: Some(output.trace.search_mode.as_str().to_string()),
                selected_plan_operator_counts: output.trace.selected_plan_operator_counts,
                selected_plan_class_counts: output.trace.selected_plan_class_counts,
                optimizer_decision_count: output.trace.decisions.len(),
                optimizer_rule_event_count: output.trace.rule_events.len(),
                plan_cache_lookup: Some(plan_cache_lookup.as_str().to_string()),
                plan_cache_bypass_reason: plan_cache_lookup
                    .bypass_reason()
                    .map(|reason| reason.as_str().to_string()),
                plan_cache_cacheable: plan_cache.cacheable,
                plan_cache_hit: plan_cache.hit,
                plan_cache_miss: plan_cache.miss,
                plan_cache_bypassed: plan_cache.bypassed,
                work_priority: Some(output.work_request.priority.as_str().to_string()),
                work_class: Some(output.work_request.class.as_str().to_string()),
                estimated_operations: Some(output.work_request.estimated_operations),
                max_rows: output.execution_profile.max_rows,
                detection_row_cap: output.execution_profile.detection_row_cap,
                row_limit_enforced_before_output: output
                    .execution_profile
                    .row_limit_enforced_before_output,
                operator_row_cap_enabled: output.execution_profile.operator_row_cap_enabled,
                blocking_operator_kinds: output.execution_profile.blocking_operator_kinds,
                scan_pruning_reports: output.execution_profile.scan_pruning_reports,
                pruned_scan_count,
                error_class: None,
                blocker_codes,
            }
        }
        Err(error) => NowledgeQueryRuntimePreflightProbeReport {
            name: probe.name.clone(),
            route: probe.route.clone(),
            query_family: probe.query_family.clone(),
            ready: false,
            success: false,
            output_row_count: 0,
            selected_plan_fingerprint: None,
            search_mode: None,
            selected_plan_operator_counts: BTreeMap::new(),
            selected_plan_class_counts: BTreeMap::new(),
            optimizer_decision_count: 0,
            optimizer_rule_event_count: 0,
            plan_cache_lookup: None,
            plan_cache_bypass_reason: None,
            plan_cache_cacheable: false,
            plan_cache_hit: false,
            plan_cache_miss: false,
            plan_cache_bypassed: false,
            work_priority: None,
            work_class: None,
            estimated_operations: None,
            max_rows: None,
            detection_row_cap: None,
            row_limit_enforced_before_output: false,
            operator_row_cap_enabled: false,
            blocking_operator_kinds: Vec::new(),
            scan_pruning_reports: Vec::new(),
            pruned_scan_count: 0,
            error_class: Some(skein_error_class(&error).to_string()),
            blocker_codes: vec!["query_runtime_failed".to_string()],
        },
    }
}

fn query_runtime_preflight_blocker_codes(
    probe_count: usize,
    failed_probe_count: usize,
    route_coverage: &NowledgeQueryRuntimeRouteCoverage,
) -> Vec<String> {
    let mut blockers = Vec::new();
    if probe_count == 0 {
        blockers.push("query_runtime_probes_missing".to_string());
    }
    if failed_probe_count > 0 {
        blockers.push("query_runtime_probe_failed".to_string());
    }
    blockers.extend(route_coverage.blocker_codes.iter().cloned());
    blockers
}

fn query_runtime_probe_blocker_codes(
    probe: &NowledgeQueryRuntimePreflightProbe,
    scan_pruning_report_count: usize,
    pruned_scan_count: usize,
    output_row_count: usize,
) -> Vec<String> {
    let mut blockers = Vec::new();
    blockers.extend(query_runtime_probe_identity_blocker_codes(probe));
    if probe.require_scan_pruning && scan_pruning_report_count < probe.min_scan_pruning_reports {
        blockers.push("scan_pruning_report_missing".to_string());
    }
    if probe.require_pruned && pruned_scan_count == 0 {
        blockers.push("scan_pruning_not_pruned".to_string());
    }
    if let Some(max_output_rows) = probe.max_output_rows
        && output_row_count > max_output_rows
    {
        blockers.push("output_row_count_exceeded".to_string());
    }
    blockers
}

fn query_runtime_probe_identity_blocker_codes(
    probe: &NowledgeQueryRuntimePreflightProbe,
) -> Vec<String> {
    let mut blockers = Vec::new();
    if probe.name.trim().is_empty() || probe.name == "unnamed" {
        blockers.push("query_runtime_probe_name_missing".to_string());
    }
    match probe.route.as_deref() {
        Some(route) if REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(&route) => {}
        Some(_) => blockers.push("query_runtime_probe_unknown_route".to_string()),
        None => blockers.push("query_runtime_probe_route_missing".to_string()),
    }
    match probe.query_family.as_deref() {
        Some(family) if REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.contains(&family) => {}
        Some(_) => blockers.push("query_runtime_probe_unknown_query_family".to_string()),
        None => blockers.push("query_runtime_probe_query_family_missing".to_string()),
    }
    blockers
}

fn skein_error_class(error: &SkeinError) -> &'static str {
    match error {
        SkeinError::Parse(_) => "parse",
        SkeinError::Semantic(_) => "semantic",
        SkeinError::Storage(_) | SkeinError::StorageIntegrity(_) => "storage",
        SkeinError::Execution(_) => "execution",
        SkeinError::CapabilityUnavailable { .. } => "capability_unavailable",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemFastPathClassification {
    pub execution_path: NowledgeMemQueryExecutionPath,
    pub fast_path_reason: Option<&'static str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NowledgeMemPlanCacheReport {
    cacheable: bool,
    hit: bool,
    miss: bool,
    bypassed: bool,
}

impl NowledgeMemPlanCacheReport {
    fn from_lookup(lookup: Option<PlanCacheLookup>) -> Self {
        match lookup {
            Some(PlanCacheLookup::Hit) => Self {
                cacheable: true,
                hit: true,
                miss: false,
                bypassed: false,
            },
            Some(PlanCacheLookup::Miss) => Self {
                cacheable: true,
                hit: false,
                miss: true,
                bypassed: false,
            },
            Some(PlanCacheLookup::Bypass(_)) => Self {
                cacheable: false,
                hit: false,
                miss: false,
                bypassed: true,
            },
            None => Self {
                cacheable: false,
                hit: false,
                miss: false,
                bypassed: false,
            },
        }
    }
}

pub fn nowledge_mem_fast_path_classification(
    statement: &cypher::Statement,
) -> NowledgeMemFastPathClassification {
    let body = nowledge_statement_body(statement);
    let fast_path_reason = match body {
        cypher::Statement::MatchReturn(query) if is_simple_node_lookup(query) => {
            Some("simple_node_lookup")
        }
        cypher::Statement::MatchReturn(query) if is_simple_one_hop_expand(query) => {
            Some("simple_one_hop_expand")
        }
        cypher::Statement::MatchNodesReturn(query) if is_simple_two_node_lookup(query) => {
            Some("simple_two_node_lookup")
        }
        cypher::Statement::ShortestPathReturn(_) => Some("bounded_shortest_path"),
        _ => None,
    };
    NowledgeMemFastPathClassification {
        execution_path: if fast_path_reason.is_some() {
            NowledgeMemQueryExecutionPath::FastPath
        } else {
            NowledgeMemQueryExecutionPath::OptimizedPath
        },
        fast_path_reason,
    }
}

fn nowledge_statement_body(statement: &cypher::Statement) -> &cypher::Statement {
    match statement {
        cypher::Statement::CypherQuery(query) => &query.statement,
        _ => statement,
    }
}

fn statement_has_ordering(statement: &cypher::Statement) -> bool {
    match statement {
        cypher::Statement::MatchReturn(query) => {
            !query.order_by.is_empty() || !query.with_order_by.is_empty()
        }
        _ => false,
    }
}

fn statement_has_pagination(statement: &cypher::Statement) -> bool {
    match statement {
        cypher::Statement::MatchReturn(query) => {
            query.offset.is_some()
                || query.limit.is_some()
                || query.with_offset.is_some()
                || query.with_limit.is_some()
        }
        cypher::Statement::MatchNodesReturn(query) => query.limit.is_some(),
        _ => false,
    }
}

fn is_simple_node_lookup(query: &cypher::MatchReturn) -> bool {
    !query.properties.is_empty()
        && query.expand.is_none()
        && query.post_match_expand.is_none()
        && query.optional_expand.is_none()
        && query.optional_with.is_none()
        && query.collect_with.is_none()
        && query.distinct_with.is_none()
        && query.with_projection.is_none()
        && query.with_order_by.is_empty()
        && query.with_offset.is_none()
        && query.with_limit.is_none()
        && query.aggregate_with.is_none()
        && query.aggregate_with_filter.is_none()
        && query.post_with_match.is_none()
        && query.predicate.is_none()
        && !query.distinct
        && query.order_by.is_empty()
        && query.offset.is_none()
}

fn is_simple_one_hop_expand(query: &cypher::MatchReturn) -> bool {
    query.expand.as_ref().is_some_and(|expand| {
        expand.min_hops == 1
            && expand.max_hops == 1
            && !query.properties.is_empty()
            && query.post_match_expand.is_none()
            && query.optional_expand.is_none()
            && query.optional_with.is_none()
            && query.collect_with.is_none()
            && query.distinct_with.is_none()
            && query.with_projection.is_none()
            && query.with_order_by.is_empty()
            && query.with_offset.is_none()
            && query.with_limit.is_none()
            && query.aggregate_with.is_none()
            && query.aggregate_with_filter.is_none()
            && query.post_with_match.is_none()
            && query.predicate.is_none()
            && !query.distinct
            && query.order_by.is_empty()
            && query.offset.is_none()
    })
}

fn is_simple_two_node_lookup(query: &cypher::MatchNodesReturn) -> bool {
    !query.left_properties.is_empty()
        && !query.right_properties.is_empty()
        && query.predicate.is_none()
}

fn recovery_mode_name(mode: RecoveryMode) -> &'static str {
    match mode {
        RecoveryMode::Strict => "strict",
        RecoveryMode::DoctorRepairTornTail => "doctor_repair_torn_tail",
    }
}

fn missing_search_projection_evidence_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": "skein-nowledge-search-projection-evidence",
        "present": false,
        "ready": false,
        "blocker_codes": ["search_projection_not_configured"],
    })
}

fn missing_search_projection_shadow_evidence_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": "skein-nowledge-search-projection-shadow-evidence",
        "present": false,
        "ready": false,
        "blocker_codes": ["search_projection_not_configured"],
    })
}

fn missing_primary_search_projection_probe_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": "skein-nowledge-search-projection-shadow-evidence",
        "present": false,
        "ready": false,
        "blocker_codes": ["primary_search_projection_probe_missing"],
    })
}

fn missing_search_candidate_shadow_evidence_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        "route": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        "evidence_source": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
        "present": false,
        "ready": false,
        "candidate_primary_engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
        "request_count": null,
        "primary_candidate_count": null,
        "shadow_candidate_count": null,
        "matched_candidate_count": null,
        "primary_only_candidate_count": null,
        "candidate_identity": {
            "ready": false,
            "parity": false,
        },
        "filter_pushdown_ready": false,
        "filter_pushdown": {
            "ready": false,
            "required_fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
            "missing_required_fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
            "field_summary_count": 0,
            "field_summaries": [],
            "blocker_codes": ["search_candidate_shadow_evidence_missing"],
        },
        "blocker_codes": ["search_candidate_shadow_evidence_missing"],
    })
}

fn missing_workload_fixture_evidence_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
        "present": false,
        "ready": false,
        "route_count": null,
        "query_count": null,
        "failed_query_count": null,
        "bounded_expansion_probe_count": null,
        "failed_bounded_expansion_probe_count": null,
        "search_metadata_probe_count": null,
        "failed_search_metadata_probe_count": null,
        "blocker_codes": ["workload_fixture_evidence_missing"],
    })
}

fn missing_production_resource_profile_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": STORAGE_RESOURCE_PROFILE_PROTOCOL,
        "protocol_version": 2,
        "present": false,
        "resource_ready": false,
        "ready": false,
        "blocker_codes": ["production_resource_profile_missing"],
    })
}

fn query_family_replacement_evidence_json(
    replacement_readiness_by_query_family: Option<&serde_json::Value>,
) -> serde_json::Value {
    let Some(families) =
        query_family_replacement_readiness_array(replacement_readiness_by_query_family)
    else {
        return serde_json::json!({
            "protocol": "skein-nowledge-query-family-evidence-v1",
            "present": false,
            "ready": false,
            "required_query_families": REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
            "missing_required_query_families": REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
            "blocker_codes": ["query_family_evidence_missing"],
        });
    };
    let health = replacement_readiness_family_evidence_health(Some(families));
    let blocker_codes = query_family_replacement_blocker_codes(&health);
    serde_json::json!({
        "protocol": "skein-nowledge-query-family-evidence-v1",
        "present": health.present,
        "ready": health.ready,
        "min_replacement_readiness_per_million": health.min_replacement_readiness_per_million,
        "invalid_family_count": health.invalid_family_count,
        "blocked_query_families": health.blocked_query_families,
        "required_query_families": REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
        "missing_required_query_families": health.missing_required_query_families,
        "blocker_codes": blocker_codes,
        "blockers": health.blockers,
        "replacement_readiness_by_query_family": families,
    })
}

fn query_family_replacement_readiness_array(
    value: Option<&serde_json::Value>,
) -> Option<&serde_json::Value> {
    let value = value?;
    if value.is_array() {
        return Some(value);
    }
    value
        .get("replacement_readiness_by_query_family")
        .filter(|families| families.is_array())
}

fn query_family_replacement_blocker_codes(
    health: &crate::nowledge_inventory::ReplacementReadinessFamilyEvidenceHealth,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if health.invalid_family_count > 0 {
        blockers.push("invalid_family_entries");
    }
    if !health.blocked_query_families.is_empty() {
        blockers.push("blocked_query_families");
    }
    if !health.missing_required_query_families.is_empty() {
        blockers.push("missing_required_query_families");
    }
    blockers
}

fn nowledge_mem_graph_route_readiness_json(
    route_readiness: Option<&NowledgeMemRouteReadinessSummary>,
) -> serde_json::Value {
    let Some(summary) = route_readiness else {
        return serde_json::json!({
            "protocol": NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
            "present": false,
            "ready": false,
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "primary_ready_route_count": 0,
            "primary_ready_routes": [],
            "missing_required_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
            "route_primary_ready": null,
            "route_query_plan_evidence_ready": null,
            "route_query_profile_evidence_ready": null,
            "route_query_api_behavior_evidence_ready": null,
            "relationship_property_pruning_required_count": null,
            "relationship_property_pruning_report_count": null,
            "route_relationship_property_pruning_evidence_ready": null,
            "blocker_codes": ["graph_route_readiness_missing"],
        });
    };

    let missing_required_routes =
        missing_nowledge_mem_bounded_read_routes(&summary.primary_ready_routes);
    let relationship_property_pruning_count_matches = summary
        .relationship_property_pruning_required_count
        == summary.relationship_property_pruning_report_count;
    let mut blocker_codes = Vec::new();
    if !summary.route_primary_ready || !missing_required_routes.is_empty() {
        blocker_codes.push("route_primary_not_ready");
    }
    if !summary.route_query_plan_evidence_ready {
        blocker_codes.push("route_query_plan_evidence_not_ready");
    }
    if !summary.route_query_profile_evidence_ready {
        blocker_codes.push("route_query_profile_evidence_not_ready");
    }
    if !summary.route_query_api_behavior_evidence_ready {
        blocker_codes.push("route_query_api_behavior_evidence_not_ready");
    }
    if !summary.route_relationship_property_pruning_evidence_ready
        || !relationship_property_pruning_count_matches
    {
        blocker_codes.push("route_relationship_property_pruning_not_ready");
    }

    serde_json::json!({
        "protocol": NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
        "present": true,
        "ready": blocker_codes.is_empty(),
        "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "primary_ready_route_count": summary.primary_ready_routes.len(),
        "primary_ready_routes": summary.primary_ready_routes,
        "missing_required_routes": missing_required_routes,
        "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
        "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
        "route_primary_ready": summary.route_primary_ready,
        "route_query_plan_evidence_ready": summary.route_query_plan_evidence_ready,
        "route_query_profile_evidence_ready": summary.route_query_profile_evidence_ready,
        "route_query_api_behavior_evidence_ready": summary.route_query_api_behavior_evidence_ready,
        "relationship_property_pruning_required_count": summary.relationship_property_pruning_required_count,
        "relationship_property_pruning_report_count": summary.relationship_property_pruning_report_count,
        "route_relationship_property_pruning_evidence_ready": summary.route_relationship_property_pruning_evidence_ready,
        "blocker_codes": blocker_codes,
    })
}

fn missing_search_route_ownership_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL,
        "ready": false,
        "production_cutover_ready": false,
        "require_all_skein": true,
        "required_route_count": REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES.len(),
        "explicit_route_count": 0,
        "skein_route_count": 0,
        "lancedb_route_count": 0,
        "routes": [],
        "skein_routes": [],
        "lancedb_routes": [],
        "missing_required_routes": REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES,
        "unknown_routes": [],
        "duplicate_routes": [],
        "conflicting_routes": [],
        "blocker_codes": ["search_route_ownership_missing"],
    })
}

fn missing_active_search_route_ownership_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL,
        "ready": false,
        "production_cutover_ready": false,
        "require_all_skein": true,
        "required_route_count": REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len(),
        "explicit_route_count": 0,
        "skein_route_count": 0,
        "lancedb_route_count": 0,
        "routes": [],
        "skein_routes": [],
        "lancedb_routes": [],
        "missing_required_routes": REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES,
        "unknown_routes": [],
        "duplicate_routes": [],
        "invalid_projection_routes": [],
        "blocker_codes": ["active_search_route_ownership_missing"],
    })
}

fn missing_active_search_route_readiness_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL,
        "ready": false,
        "production_cutover_ready": false,
        "require_all_skein": true,
        "required_route_count": REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len(),
        "evidence_route_count": 0,
        "ready_route_count": 0,
        "skein_route_count": 0,
        "lancedb_handle_required_route_count": 0,
        "routes": [],
        "ready_routes": [],
        "missing_required_routes": REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES,
        "unknown_routes": [],
        "duplicate_routes": [],
        "invalid_projection_routes": [],
        "non_skein_routes": [],
        "lancedb_handle_required_routes": [],
        "candidate_not_ready_routes": [],
        "candidate_identity_not_ready_routes": [],
        "metadata_pushdown_not_ready_routes": [],
        "ranking_not_ready_routes": [],
        "fail_soft_not_ready_routes": [],
        "blocker_codes": ["active_search_route_readiness_missing"],
    })
}

fn search_route_ownership_ready(evidence: &serde_json::Value) -> bool {
    evidence.get("protocol").and_then(serde_json::Value::as_str)
        == Some(NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL)
        && evidence.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
        && evidence
            .get("production_cutover_ready")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        && evidence
            .get("lancedb_route_count")
            .and_then(serde_json::Value::as_u64)
            == Some(0)
        && evidence
            .get("blocker_codes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty)
}

fn active_search_route_ownership_ready(evidence: &serde_json::Value) -> bool {
    evidence.get("protocol").and_then(serde_json::Value::as_str)
        == Some(NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL)
        && evidence.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
        && evidence
            .get("production_cutover_ready")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        && evidence
            .get("lancedb_route_count")
            .and_then(serde_json::Value::as_u64)
            == Some(0)
        && evidence
            .get("blocker_codes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty)
}

fn active_search_route_readiness_ready(evidence: &serde_json::Value) -> bool {
    evidence.get("protocol").and_then(serde_json::Value::as_str)
        == Some(NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL)
        && evidence.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
        && evidence
            .get("production_cutover_ready")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        && evidence
            .get("lancedb_handle_required_route_count")
            .and_then(serde_json::Value::as_u64)
            == Some(0)
        && evidence
            .get("blocker_codes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty)
}

struct LibraryReadinessEvidence<'a> {
    bounded_read_evidence: &'a serde_json::Value,
    storage_recovery: &'a serde_json::Value,
    background_maintenance: &'a serde_json::Value,
    query_family_evidence: &'a serde_json::Value,
    graph_route_readiness: &'a serde_json::Value,
    search_route_ownership: &'a serde_json::Value,
    active_search_route_ownership: &'a serde_json::Value,
    active_search_route_readiness: &'a serde_json::Value,
    search_projection_evidence: &'a serde_json::Value,
    search_projection_shadow_evidence: &'a serde_json::Value,
    search_candidate_shadow_evidence: &'a serde_json::Value,
    workload_fixture_evidence: &'a serde_json::Value,
    production_resource_profile: &'a serde_json::Value,
}

fn library_readiness_blocker_codes(evidence: &LibraryReadinessEvidence<'_>) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if !bounded_read_evidence_ready(evidence.bounded_read_evidence) {
        blockers.push("bounded_read_evidence_not_ready");
    }
    if evidence
        .storage_recovery
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("storage_recovery_not_ready");
    }
    if !library_background_maintenance_ready(evidence.background_maintenance) {
        blockers.push("background_maintenance_not_ready");
    }
    if evidence
        .query_family_evidence
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("query_family_evidence_not_ready");
    }
    if evidence
        .graph_route_readiness
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("graph_route_readiness_not_ready");
    }
    if !search_route_ownership_ready(evidence.search_route_ownership) {
        blockers.push("search_route_ownership_not_ready");
    }
    if !active_search_route_ownership_ready(evidence.active_search_route_ownership) {
        blockers.push("active_search_route_ownership_not_ready");
    }
    if !active_search_route_readiness_ready(evidence.active_search_route_readiness) {
        blockers.push("active_search_route_readiness_not_ready");
    }
    if !search_projection_evidence_ready(evidence.search_projection_evidence) {
        blockers.push("search_projection_evidence_not_ready");
    }
    if !search_projection_shadow_evidence_ready(evidence.search_projection_shadow_evidence) {
        blockers.push("search_projection_shadow_evidence_not_ready");
    }
    if !search_candidate_shadow_evidence_ready(evidence.search_candidate_shadow_evidence) {
        blockers.push("search_candidate_shadow_evidence_not_ready");
    }
    if !workload_fixture_evidence_ready(evidence.workload_fixture_evidence) {
        blockers.push("workload_fixture_evidence_not_ready");
    }
    if !production_resource_profile_ready(evidence.production_resource_profile) {
        blockers.push("production_resource_profile_not_ready");
    }
    blockers
}

fn library_readiness_by_area(
    evidence: &LibraryReadinessEvidence<'_>,
) -> NowledgeMemReadinessAreaMap {
    NowledgeMemReadinessAreaMap {
        graph: NowledgeMemReadinessAreaSummary::new("graph", true, Vec::new()),
        query: bounded_read_readiness_area(evidence.bounded_read_evidence),
        query_family: readiness_area(
            "query_family",
            evidence.query_family_evidence,
            "query_family_evidence_not_ready",
        ),
        graph_route: readiness_area(
            "graph_route",
            evidence.graph_route_readiness,
            "graph_route_readiness_not_ready",
        ),
        search_route_ownership: search_route_ownership_readiness_area(
            evidence.search_route_ownership,
            evidence.active_search_route_ownership,
            evidence.active_search_route_readiness,
        ),
        storage: storage_readiness_area(
            evidence.storage_recovery,
            evidence.production_resource_profile,
        ),
        search_projection: search_projection_readiness_area(evidence.search_projection_evidence),
        search_projection_shadow: search_projection_shadow_readiness_area(
            evidence.search_projection_shadow_evidence,
        ),
        search_candidate_shadow: search_candidate_shadow_readiness_area(
            evidence.search_candidate_shadow_evidence,
        ),
        workload_fixture: workload_fixture_readiness_area(evidence.workload_fixture_evidence),
        background: background_maintenance_readiness_area(evidence.background_maintenance),
    }
}

fn storage_readiness_area(
    storage_recovery: &serde_json::Value,
    production_resource_profile: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let mut blocker_codes = Vec::new();
    if storage_recovery
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blocker_codes.push("storage_recovery_not_ready".to_string());
    }
    blocker_codes.extend(production_resource_profile_blocker_codes(
        production_resource_profile,
    ));
    NowledgeMemReadinessAreaSummary::new("storage", blocker_codes.is_empty(), blocker_codes)
}

pub(crate) fn production_resource_profile_ready(evidence: &serde_json::Value) -> bool {
    production_resource_profile_blocker_codes(evidence).is_empty()
}

fn production_resource_profile_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    if evidence_string(evidence, "protocol") != Some(STORAGE_RESOURCE_PROFILE_PROTOCOL)
        || evidence_u64(evidence, "protocol_version") != Some(2)
    {
        blockers.insert("production_resource_profile_protocol_mismatch".to_string());
    }
    if evidence_bool(evidence, "present") != Some(true) {
        blockers.insert("production_resource_profile_missing".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("production_resource_profile_not_ready".to_string());
    }
    if evidence_bool(evidence, "resource_ready") != Some(true) {
        blockers.insert("production_resource_profile_resource_not_ready".to_string());
    }
    if !string_array_at(evidence, &["blocker_codes"]).is_some_and(|codes| codes.is_empty()) {
        blockers.insert("production_resource_profile_has_blockers".to_string());
    }

    let binding_identity = nested_value(evidence, &["evidence_binding", "identity"]);
    let expected_identity = evidence.get("expected_identity");
    let canonical_graph_commit_epoch = evidence_u64(evidence, "canonical_graph_commit_epoch");
    let identity_valid = evidence_bool(evidence, "identity_matches_expected") == Some(true)
        && nested_u64(evidence, &["evidence_binding", "generated_at_unix_seconds"])
            .is_some_and(|generated_at| generated_at > 0)
        && binding_identity == expected_identity
        && binding_identity.is_some_and(|identity| {
            [
                "source_revision",
                "rust_toolchain",
                "target_os",
                "target_arch",
                "configuration_digest",
                "deployment_profile",
                "dataset_fingerprint",
            ]
            .into_iter()
            .all(|field| {
                identity
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty())
            }) && identity
                .get("enabled_features")
                .is_some_and(serde_json::Value::is_array)
                && identity
                    .get("durable_format_version")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|version| version > 0)
                && identity
                    .get("schema_version")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|version| version > 0)
                && identity
                    .get("policy_version")
                    .and_then(serde_json::Value::as_u64)
                    == Some(crate::PRODUCTION_QUALIFICATION_POLICY_VERSION)
                && identity
                    .get("canonical_graph_commit_epoch")
                    .and_then(serde_json::Value::as_u64)
                    == canonical_graph_commit_epoch
        });
    if !identity_valid {
        blockers.insert("production_resource_profile_identity_invalid".to_string());
    }

    let canonical_bytes = nested_u64(evidence, &["storage", "canonical_artifact_bytes"]);
    let cache_capacity = nested_u64(evidence, &["storage", "segment_cache_capacity_bytes"]);
    let cache_resident = nested_u64(evidence, &["storage", "segment_cache_resident_bytes_after"]);
    let minimum_canonical_bytes = nested_u64(evidence, &["limits", "min_canonical_artifact_bytes"]);
    let storage_bytes_within_budget = matches!(
        (
            canonical_bytes,
            cache_capacity,
            cache_resident,
            minimum_canonical_bytes,
        ),
        (Some(canonical), Some(capacity), Some(resident), Some(minimum))
            if canonical >= minimum && canonical > capacity && resident <= capacity
    );
    if nested_bool(evidence, &["storage", "durable"]) != Some(true)
        || nested_bool(evidence, &["storage", "out_of_core"]) != Some(true)
        || nested_bool(evidence, &["storage", "canonical_exceeds_cache"]) != Some(true)
        || nested_bool(evidence, &["storage", "delta_within_budget"]) != Some(true)
        || !storage_bytes_within_budget
    {
        blockers.insert("production_resource_profile_storage_budget_invalid".to_string());
    }

    let require_fully_streamed = nested_bool(evidence, &["limits", "require_fully_streamed"]);
    let fully_streamed = nested_bool(evidence, &["execution", "fully_streamed"]);
    if require_fully_streamed.is_none()
        || fully_streamed.is_none()
        || (require_fully_streamed == Some(true) && fully_streamed != Some(true))
    {
        blockers.insert("production_resource_profile_streaming_invalid".to_string());
    }
    if nested_u64(evidence, &["execution", "start_resident_bytes"]).is_none()
        || nested_u64(evidence, &["execution", "start_peak_resident_bytes"]).is_none()
        || nested_u64(evidence, &["execution", "steady_resident_growth_bytes"]).is_none()
        || nested_u64(
            evidence,
            &["execution", "lifetime_peak_resident_growth_bytes"],
        )
        .is_none()
    {
        blockers.insert("production_resource_profile_resident_growth_missing".to_string());
    }

    for (metric, limit) in [
        ("steady_resident_bytes", "max_steady_resident_bytes"),
        ("peak_resident_bytes", "max_peak_resident_bytes"),
        ("intermediate_rows", "max_intermediate_rows"),
        (
            "intermediate_payload_bytes",
            "max_intermediate_payload_bytes",
        ),
        ("output_rows", "max_output_rows"),
        ("output_payload_bytes", "max_output_payload_bytes"),
    ] {
        let measured = nested_u64(evidence, &["execution", metric]);
        let admitted = nested_u64(evidence, &["limits", limit]);
        if measured.is_none() || admitted.is_none() || measured > admitted {
            blockers.insert(format!("production_resource_profile_{metric}_invalid"));
        }
    }

    let resident_memory_supported = nested_bool(
        evidence,
        &["execution", "metric_capabilities", "resident_memory"],
    );
    let total_page_faults_supported = nested_bool(
        evidence,
        &["execution", "metric_capabilities", "total_page_faults"],
    );
    let split_page_faults_supported = nested_bool(
        evidence,
        &["execution", "metric_capabilities", "split_page_faults"],
    );
    if resident_memory_supported != Some(true)
        || total_page_faults_supported != Some(true)
        || split_page_faults_supported.is_none()
    {
        blockers.insert("production_resource_profile_metric_capabilities_invalid".to_string());
    }

    let total_page_faults = nested_u64(evidence, &["execution", "total_page_faults"]);
    let max_total_page_faults = nested_u64(evidence, &["limits", "max_total_page_faults"]);
    if total_page_faults.is_none()
        || max_total_page_faults.is_none()
        || total_page_faults > max_total_page_faults
    {
        blockers.insert("production_resource_profile_total_page_faults_invalid".to_string());
    }

    for (metric, limit) in [
        ("minor_page_faults", "max_minor_page_faults"),
        ("major_page_faults", "max_major_page_faults"),
    ] {
        let measured = nested_u64(evidence, &["execution", metric]);
        let admitted = nested_u64(evidence, &["limits", limit]);
        if admitted.is_some()
            && (split_page_faults_supported != Some(true)
                || measured.is_none()
                || measured > admitted)
        {
            blockers.insert(format!("production_resource_profile_{metric}_invalid"));
        }
    }
    blockers.into_iter().collect()
}

fn bounded_read_readiness_area(evidence: &serde_json::Value) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = bounded_read_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new("query", blocker_codes.is_empty(), blocker_codes)
}

fn search_route_ownership_readiness_area(
    evidence: &serde_json::Value,
    active_route_evidence: &serde_json::Value,
    active_read_evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let ready = search_route_ownership_ready(evidence)
        && active_search_route_ownership_ready(active_route_evidence)
        && active_search_route_readiness_ready(active_read_evidence);
    let blocker_codes = if ready {
        Vec::new()
    } else {
        let mut codes = evidence
            .get("blocker_codes")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        codes.extend(
            active_route_evidence
                .get("blocker_codes")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string),
        );
        codes.extend(
            active_read_evidence
                .get("blocker_codes")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string),
        );
        if codes.is_empty() {
            vec!["search_route_ownership_not_ready".to_string()]
        } else {
            codes
        }
    };
    NowledgeMemReadinessAreaSummary::new("search_route_ownership", ready, blocker_codes)
}

fn bounded_read_evidence_ready(evidence: &serde_json::Value) -> bool {
    bounded_read_readiness_blocker_codes(evidence).is_empty()
}

fn bounded_read_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("bounded_read_evidence_missing".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol") != Some(NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL) {
        blockers.insert("bounded_read_protocol_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("bounded_read_not_ready".to_string());
    }
    if evidence_string(evidence, "mode") != Some(NowledgeMemGraphMode::ShadowReadOnly.as_str()) {
        blockers.insert("bounded_read_not_shadow_read_only".to_string());
    }
    let max_rows = evidence_u64(evidence, "max_rows");
    if !max_rows.is_some_and(|value| value > 0) {
        blockers.insert("bounded_read_missing_max_rows".to_string());
    }
    let expected_execution_row_cap = max_rows.and_then(|value| value.checked_add(1));
    if expected_execution_row_cap.is_none()
        || evidence_u64(evidence, "execution_row_cap") != expected_execution_row_cap
    {
        blockers.insert("bounded_read_execution_row_cap_mismatch".to_string());
    }
    if evidence_u64(evidence, "estimated_payload_bytes").is_none() {
        blockers.insert("bounded_read_estimated_payload_bytes_missing".to_string());
    }
    if !evidence_u64(evidence, "max_estimated_payload_bytes").is_some_and(|value| value > 0) {
        blockers.insert("bounded_read_max_estimated_payload_bytes_missing".to_string());
    }
    if evidence_bool(evidence, "payload_budget_exceeded") != Some(false) {
        blockers.insert("bounded_read_payload_budget_exceeded".to_string());
    }
    if evidence_bool(evidence, "row_limit_enforced_before_output") != Some(true) {
        blockers.insert("bounded_read_row_limit_not_enforced_before_output".to_string());
    }
    if evidence_bool(evidence, "operator_row_cap_enabled") != Some(true) {
        blockers.insert("bounded_read_operator_row_cap_disabled".to_string());
    }
    if evidence_bool(evidence, "row_budget_exceeded") == Some(true) {
        blockers.insert("bounded_read_row_budget_exceeded".to_string());
    }
    if evidence_bool(evidence, "streaming").is_none() {
        blockers.insert("bounded_read_streaming_evidence_missing".to_string());
    }
    if evidence_bool(evidence, "blocking_operator_memory_reports_complete") != Some(true) {
        blockers.insert("bounded_read_blocking_operator_memory_report_incomplete".to_string());
    }
    if evidence_bool(evidence, "blocking_operator_memory_within_budget") != Some(true) {
        blockers.insert("bounded_read_blocking_operator_memory_budget_exceeded".to_string());
    }
    if evidence_bool(evidence, "spill_within_budget") != Some(true) {
        blockers.insert("bounded_read_blocking_operator_spill_budget_exceeded".to_string());
    }
    if !string_array_at(evidence, &["missing_covered_routes"])
        .is_some_and(|routes| routes.is_empty())
    {
        blockers.insert("bounded_read_missing_covered_routes".to_string());
    }
    if evidence_string(evidence, "route_catalog_version")
        != Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION)
        || evidence_string(evidence, "route_catalog_digest")
            != Some(nowledge_mem_graph_read_route_catalog_digest().as_str())
    {
        blockers.insert("bounded_read_route_catalog_stale".to_string());
    }
    if evidence_bool(evidence, "route_primary_ready") != Some(true)
        || evidence_bool(evidence, "route_query_plan_evidence_ready") != Some(true)
        || evidence_bool(evidence, "route_query_profile_evidence_ready") != Some(true)
        || evidence_bool(evidence, "route_query_api_behavior_evidence_ready") != Some(true)
        || evidence_bool(
            evidence,
            "route_relationship_property_pruning_evidence_ready",
        ) != Some(true)
    {
        blockers.insert("bounded_read_graph_route_readiness_not_ready".to_string());
    }
    let required_pruning_count =
        evidence_u64(evidence, "relationship_property_pruning_required_count");
    if required_pruning_count.is_none()
        || required_pruning_count
            != evidence_u64(evidence, "relationship_property_pruning_report_count")
    {
        blockers.insert("bounded_read_relationship_property_pruning_missing".to_string());
    }
    blockers.into_iter().collect()
}

fn search_projection_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = search_projection_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "search_projection",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn search_projection_shadow_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = search_projection_shadow_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "search_projection_shadow",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn search_projection_evidence_ready(evidence: &serde_json::Value) -> bool {
    search_projection_readiness_blocker_codes(evidence).is_empty()
}

fn search_projection_shadow_evidence_ready(evidence: &serde_json::Value) -> bool {
    search_projection_shadow_readiness_blocker_codes(evidence).is_empty()
}

fn search_projection_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("search_projection_not_configured".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol") != Some(NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL) {
        blockers.insert("search_projection_protocol_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("search_projection_not_ready".to_string());
    }
    if evidence_bool(evidence, "derived_projection") != Some(true) {
        blockers.insert("search_projection_not_derived".to_string());
    }
    if evidence_bool(evidence, "all_tables_covered") != Some(true)
        || !evidence_u64(evidence, "covered_table_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "covered_table_count")
            != evidence_u64(evidence, "required_table_count")
    {
        blockers.insert("search_projection_tables_not_ready".to_string());
    }
    if evidence_bool(evidence, "fts_ready") != Some(true) {
        blockers.insert("search_projection_fts_not_ready".to_string());
    }
    if evidence_bool(evidence, "vector_ready") != Some(true) {
        blockers.insert("search_projection_vector_not_ready".to_string());
    }
    if evidence_bool(evidence, "document_identity_ready") != Some(true) {
        blockers.insert("search_projection_document_identity_not_ready".to_string());
    }
    if evidence_bool(evidence, "embedding_identity_ready") != Some(true) {
        blockers.insert("search_projection_embedding_identity_not_ready".to_string());
    }
    if evidence_bool(evidence, "fail_soft_ready") != Some(true) {
        blockers.insert("search_projection_fail_soft_not_ready".to_string());
    }
    if evidence_bool(evidence, "rebuild_marker_ready") != Some(true) {
        blockers.insert("search_projection_rebuild_marker_not_ready".to_string());
    }
    if evidence_bool(evidence, "metadata_repair_marker_ready") != Some(true) {
        blockers.insert("search_projection_metadata_repair_marker_not_ready".to_string());
    }
    if evidence_bool(evidence, "incremental_update_ready") != Some(true) {
        blockers.insert("search_projection_incremental_update_not_ready".to_string());
    }
    if evidence_bool(evidence, "source_chunk_ready") != Some(true) {
        blockers.insert("search_projection_source_chunk_not_ready".to_string());
    }
    if evidence_bool(evidence, "predicate_pushdown_ready") != Some(true) {
        blockers.insert("search_projection_predicate_pushdown_not_ready".to_string());
    }
    if evidence_bool(evidence, "production_filter_pruning_ready") != Some(true) {
        blockers.insert("search_projection_production_filter_pruning_not_ready".to_string());
    }
    if evidence_bool(evidence, "compressed_vector_projection_ready") == Some(false) {
        blockers.insert("search_projection_compressed_vector_not_ready".to_string());
    }
    blockers.into_iter().collect()
}

fn search_projection_shadow_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("search_projection_not_configured".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol")
        != Some(NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL)
    {
        blockers.insert("search_projection_shadow_protocol_mismatch".to_string());
    }
    if evidence_string(evidence, "evidence_source")
        != Some(NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE)
    {
        blockers.insert("search_projection_shadow_evidence_source_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("search_projection_shadow_not_ready".to_string());
    }
    if evidence_bool(evidence, "primary_ready") != Some(true) {
        blockers.insert("search_projection_shadow_primary_not_ready".to_string());
    }
    if evidence_bool(evidence, "shadow_ready") != Some(true) {
        blockers.insert("search_projection_shadow_shadow_not_ready".to_string());
    }
    if evidence_bool(evidence, "document_count_parity") != Some(true)
        || evidence_bool(evidence, "document_identity_parity") != Some(true)
    {
        blockers.insert("search_projection_shadow_document_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["table_parity", "ready"]) != Some(true)
        && evidence_bool(evidence, "table_parity_ready") != Some(true)
    {
        blockers.insert("search_projection_shadow_table_parity_not_ready".to_string());
    }
    if evidence_bool(evidence, "embedding_identity_parity") != Some(true) {
        blockers.insert("search_projection_shadow_embedding_identity_not_ready".to_string());
    }
    if evidence_bool(evidence, "lifecycle_parity") != Some(true) {
        blockers.insert("search_projection_shadow_lifecycle_not_ready".to_string());
    }
    if evidence_bool(evidence, "incremental_watermark_parity") != Some(true) {
        blockers.insert("search_projection_shadow_incremental_watermark_not_ready".to_string());
    }
    if evidence_bool(evidence, "predicate_pushdown_parity") != Some(true) {
        blockers.insert("search_projection_shadow_predicate_pushdown_not_ready".to_string());
    }
    if nested_bool(evidence, &["pushdown_evidence", "ready"]) != Some(true) {
        blockers.insert(SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY.to_string());
    }
    if nested_bool(
        evidence,
        &[
            "pushdown_evidence",
            "shadow_persisted_segment_descriptor_ready",
        ],
    ) != Some(true)
    {
        blockers.insert(SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING.to_string());
    }
    if nested_bool(
        evidence,
        &[
            "pushdown_evidence",
            "shadow_segment_descriptor_scan_filter_fields_ready",
        ],
    ) != Some(true)
    {
        blockers.insert(SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING.to_string());
    }
    blockers.into_iter().collect()
}

fn search_candidate_shadow_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = search_candidate_shadow_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "search_candidate_shadow",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn search_candidate_shadow_evidence_ready(evidence: &serde_json::Value) -> bool {
    search_candidate_shadow_readiness_blocker_codes(evidence).is_empty()
}

fn workload_fixture_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = workload_fixture_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "workload_fixture",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn workload_fixture_evidence_ready(evidence: &serde_json::Value) -> bool {
    workload_fixture_readiness_blocker_codes(evidence).is_empty()
}

fn workload_fixture_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("workload_fixture_evidence_missing".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol") != Some(NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL)
    {
        blockers.insert("workload_fixture_protocol_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("workload_fixture_not_ready".to_string());
    }
    if !evidence_u64(evidence, "route_count").is_some_and(|count| count > 0)
        || !evidence_u64(evidence, "query_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_query_count") != Some(0)
    {
        blockers.insert("workload_fixture_route_queries_not_ready".to_string());
    }
    if !evidence_u64(evidence, "bounded_expansion_probe_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_bounded_expansion_probe_count") != Some(0)
    {
        blockers.insert("workload_fixture_bounded_expansion_not_ready".to_string());
    }
    if !evidence_u64(evidence, "search_metadata_probe_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_search_metadata_probe_count") != Some(0)
    {
        blockers.insert("workload_fixture_search_metadata_not_ready".to_string());
    }
    if !evidence_u64(evidence, "graph_rag_probe_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_graph_rag_probe_count") != Some(0)
    {
        blockers.insert("workload_fixture_graph_rag_not_ready".to_string());
    }
    if !array_at(evidence, &["graph_rag_reports"]).is_some_and(|reports| {
        reports.iter().any(|report| {
            report.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
                && report
                    .get("label_count")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|count| count > 0)
                && report
                    .get("relationship_type_count")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|count| count > 0)
                && report
                    .get("route_count")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|count| count > 0)
                && report
                    .get("parameter_requirement_count")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|count| count > 0)
                && report
                    .get("row_count")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|count| count > 0)
                && report
                    .get("row_budget_exceeded")
                    .and_then(serde_json::Value::as_bool)
                    == Some(false)
                && report
                    .get("payload_budget_exceeded")
                    .and_then(serde_json::Value::as_bool)
                    == Some(false)
                && report
                    .get("blocking_operator_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(0)
                && report.get("streaming").and_then(serde_json::Value::as_bool) == Some(false)
                && report
                    .get("error_class")
                    .and_then(serde_json::Value::as_str)
                    .is_none()
        })
    }) {
        blockers.insert("workload_fixture_graph_rag_probe_missing".to_string());
    }
    if !evidence_u64(evidence, "source_projection_probe_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_source_projection_probe_count") != Some(0)
    {
        blockers.insert("workload_fixture_source_projection_not_ready".to_string());
    }
    if !array_at(evidence, &["source_projection_reports"]).is_some_and(|reports| {
        reports.iter().any(|report| {
            report.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
                && report
                    .get("too_small_batch_failed_closed")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
                && report
                    .get("operation_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(2)
                && report
                    .get("upserted_documents")
                    .and_then(serde_json::Value::as_u64)
                    == Some(2)
                && report
                    .get("deleted_documents")
                    .and_then(serde_json::Value::as_u64)
                    == Some(0)
                && report
                    .get("source_document_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(2)
                && report
                    .get("indexed_source_document_ready")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
                && report
                    .get("source_graph_commit_epoch")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                && report
                    .get("complete_through_graph_commit_epoch")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                && report
                    .get("error_class")
                    .and_then(serde_json::Value::as_str)
                    .is_none()
        })
    }) {
        blockers.insert("workload_fixture_source_projection_probe_missing".to_string());
    }
    blockers.into_iter().collect()
}

fn search_candidate_shadow_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("search_candidate_shadow_evidence_missing".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol")
        != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL)
    {
        blockers.insert("search_candidate_shadow_protocol_mismatch".to_string());
    }
    if evidence_string(evidence, "route") != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE) {
        blockers.insert("search_candidate_shadow_route_mismatch".to_string());
    }
    if evidence_string(evidence, "evidence_source")
        != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE)
    {
        blockers.insert("search_candidate_shadow_evidence_source_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("search_candidate_shadow_not_ready".to_string());
    }
    if evidence_string(evidence, "candidate_primary_engine")
        != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE)
    {
        blockers.insert("search_candidate_primary_engine_not_skein".to_string());
    }
    if !search_candidate_shadow_counts_ready(evidence) {
        blockers.insert("search_candidate_counts_not_ready".to_string());
    }
    if evidence_bool(evidence, "text_retriever_ready") != Some(true) {
        blockers.insert("search_candidate_text_retriever_not_ready".to_string());
    }
    if evidence_bool(evidence, "vector_retriever_ready") != Some(true) {
        blockers.insert("search_candidate_vector_retriever_not_ready".to_string());
    }
    if evidence_bool(evidence, "fts_top_k_overlap_ready") != Some(true) {
        blockers.insert("search_candidate_fts_top_k_overlap_not_ready".to_string());
    }
    if evidence_bool(evidence, "vector_top_k_overlap_ready") != Some(true) {
        blockers.insert("search_candidate_vector_top_k_overlap_not_ready".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "source_chunk_identity_ready"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_source_chunk_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["candidate_readiness", "fail_soft_observed"]) != Some(true) {
        blockers.insert("search_candidate_fail_soft_not_observed".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "projection_marker_status_visible"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_projection_marker_status_missing".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "projection_watermark_ready"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_projection_watermark_missing".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "embedding_identity_ready"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_embedding_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["candidate_identity", "ready"]) != Some(true)
        || nested_bool(evidence, &["candidate_identity", "parity"]) != Some(true)
    {
        blockers.insert("search_candidate_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["filter_pushdown", "ready"]) != Some(true)
        || evidence_bool(evidence, "filter_pushdown_ready") != Some(true)
    {
        blockers.insert("search_candidate_filter_pushdown_not_ready".to_string());
    }
    if !nested_u64(evidence, &["filter_pushdown", "field_summary_count"])
        .is_some_and(|count| count > 0)
        || !string_array_at(evidence, &["filter_pushdown", "missing_required_fields"])
            .is_some_and(|fields| fields.is_empty())
    {
        blockers.insert("search_candidate_field_pruning_missing".to_string());
    }
    blockers.into_iter().collect()
}

fn evidence_string<'a>(evidence: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    evidence.get(field).and_then(serde_json::Value::as_str)
}

fn evidence_bool(evidence: &serde_json::Value, field: &str) -> Option<bool> {
    evidence.get(field).and_then(serde_json::Value::as_bool)
}

fn evidence_u64(evidence: &serde_json::Value, field: &str) -> Option<u64> {
    evidence.get(field).and_then(serde_json::Value::as_u64)
}

fn nested_value<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
}

fn nested_bool(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    nested_value(value, path).and_then(serde_json::Value::as_bool)
}

fn nested_u64(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    nested_value(value, path).and_then(serde_json::Value::as_u64)
}

fn string_array_at(value: &serde_json::Value, path: &[&str]) -> Option<Vec<String>> {
    nested_value(value, path)?
        .as_array()?
        .iter()
        .map(|item| item.as_str().map(str::to_string))
        .collect()
}

fn array_at<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a Vec<serde_json::Value>> {
    nested_value(value, path)?.as_array()
}

fn evidence_blocker_codes(evidence: &serde_json::Value) -> BTreeSet<String> {
    evidence
        .get("blocker_codes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn search_candidate_shadow_counts_ready(evidence: &serde_json::Value) -> bool {
    let request_count = evidence_u64(evidence, "request_count");
    let primary_candidate_count = evidence_u64(evidence, "primary_candidate_count");
    let shadow_candidate_count = evidence_u64(evidence, "shadow_candidate_count");
    let matched_candidate_count = evidence_u64(evidence, "matched_candidate_count");
    let primary_only_candidate_count = evidence_u64(evidence, "primary_only_candidate_count");
    request_count.is_some_and(|count| count > 0)
        && primary_candidate_count.is_some()
        && primary_candidate_count == shadow_candidate_count
        && matched_candidate_count == shadow_candidate_count
        && primary_only_candidate_count == Some(0)
}

fn readiness_area(
    name: &'static str,
    evidence: &serde_json::Value,
    fallback_blocker_code: &'static str,
) -> NowledgeMemReadinessAreaSummary {
    let ready = evidence.get("ready").and_then(serde_json::Value::as_bool) == Some(true);
    NowledgeMemReadinessAreaSummary::new(
        name,
        ready,
        readiness_blocker_codes(evidence, fallback_blocker_code, ready),
    )
}

fn background_maintenance_readiness_area(
    background_maintenance: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let health = background_maintenance_evidence_health(Some(background_maintenance), true);
    NowledgeMemReadinessAreaSummary::new("background", health.ready, health.blocker_codes)
}

fn library_background_maintenance_ready(background_maintenance: &serde_json::Value) -> bool {
    background_maintenance_evidence_health(Some(background_maintenance), true).ready
}

fn readiness_blocker_codes(
    evidence: &serde_json::Value,
    fallback_blocker_code: &'static str,
    ready: bool,
) -> Vec<String> {
    if ready {
        return Vec::new();
    }
    let codes = evidence
        .get("blocker_codes")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if codes.is_empty() {
        vec![fallback_blocker_code.to_string()]
    } else {
        codes
    }
}

fn nowledge_mem_readiness_dashboard_areas(
    library: &NowledgeMemLibraryReadinessReport,
    slow_query: &NowledgeMemSlowQueryReport,
) -> Vec<NowledgeMemReadinessAreaSummary> {
    let mut areas = library.areas();
    areas.push(NowledgeMemReadinessAreaSummary {
        name: "slow_query".to_string(),
        ready: slow_query.ready,
        blocker_codes: if slow_query.ready {
            Vec::new()
        } else {
            vec!["slow_query_report_not_ready".to_string()]
        },
    });
    areas
}

fn nowledge_mem_search_candidate_report(
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

fn search_candidate_returned_kind_counts(
    hits: &[crate::search::SearchHit],
) -> BTreeMap<String, usize> {
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
    report: &crate::search::SearchPredicatePushdownReport,
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
    report: &crate::search::SearchPredicateFieldPruningReport,
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

fn nowledge_mem_retrieval_report(
    mode: NowledgeMemGraphMode,
    compressed_vector_search_mode: CompressedVectorSearchMode,
    output: &KnowledgeRetrievalOutput,
) -> NowledgeMemRetrievalReport {
    let vector_backend = output
        .search
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
        .map(|retriever| retriever.backend.clone());
    let text_backend = output
        .search
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .map(|retriever| retriever.backend.clone());
    let search_backend = vector_backend
        .clone()
        .or_else(|| text_backend.clone())
        .or_else(|| {
            output
                .search
                .retrievers
                .first()
                .map(|retriever| retriever.backend.clone())
        });
    let knowledge_fallback_reason_codes = output
        .diagnostics
        .graph_context_fallback_reason_codes
        .iter()
        .map(|code| code.as_str().to_string())
        .collect::<Vec<_>>();
    let retriever_fallback_reason_codes = output
        .retrievers
        .iter()
        .flat_map(|retriever| retriever.fallback_reason_codes.iter())
        .map(|code| code.as_str().to_string())
        .collect::<Vec<_>>();
    let truncation_reason_codes = output
        .diagnostics
        .search_truncation_reason_codes
        .iter()
        .map(|code| code.as_str().to_string())
        .chain(
            output
                .diagnostics
                .graph_seed_truncation_reason_codes
                .iter()
                .map(|code| code.as_str().to_string()),
        )
        .chain(
            output
                .diagnostics
                .graph_context_truncation_reason_codes
                .iter()
                .map(|code| code.as_str().to_string()),
        )
        .chain(
            output
                .diagnostics
                .candidate_truncation_reason_codes
                .iter()
                .map(|code| code.as_str().to_string()),
        )
        .collect::<Vec<_>>();
    NowledgeMemRetrievalReport {
        protocol: NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL.to_string(),
        mode,
        compressed_vector_search_mode,
        graph_commit_epoch: output.graph_commit_epoch,
        projection_source_graph_commit_epoch: output.projection_freshness.source_graph_commit_epoch,
        projection_commit_lag: output.diagnostics.projection_commit_lag,
        projection_stale: output.diagnostics.projection_stale,
        search_document_count: output.search.document_count,
        search_filtered_document_count: output.search.filtered_document_count,
        search_total_hits: output.search.total_hits,
        candidate_count: output.candidates.len(),
        candidate_total_count: output.diagnostics.candidate_total_count,
        evidence_count: output.evidence.len(),
        graph_seed_count: output.graph_seeds.len(),
        graph_context_path_count: output.graph_context_paths.len(),
        search_backend,
        vector_backend,
        text_backend,
        search_fallback_reason_codes: output
            .diagnostics
            .search_fallback_reason_codes
            .iter()
            .map(|code| code.as_str().to_string())
            .collect(),
        retriever_fallback_reason_codes,
        knowledge_fallback_reason_codes,
        truncation_reason_codes,
        warning_count: output.diagnostics.warnings.len(),
        warnings: output.diagnostics.warnings.clone(),
    }
}

fn nowledge_mem_read_report(
    mode: NowledgeMemGraphMode,
    output: &QueryOutput,
    options: &NowledgeMemReadOptions,
    execution_profile: &ReadExecutionProfile,
) -> NowledgeMemReadReport {
    let estimated_payload_bytes = estimate_query_output_payload_bytes(output);
    NowledgeMemReadReport {
        protocol: NOWLEDGE_MEM_READ_REPORT_PROTOCOL.to_string(),
        mode,
        row_count: output.rows.len(),
        max_rows: options.max_rows,
        execution_row_cap: execution_profile.detection_row_cap,
        estimated_payload_bytes,
        max_estimated_payload_bytes: options.max_estimated_payload_bytes,
        row_budget_exceeded: options
            .max_rows
            .is_some_and(|max_rows| output.rows.len() > max_rows),
        payload_budget_exceeded: options
            .max_estimated_payload_bytes
            .is_some_and(|max_bytes| estimated_payload_bytes > max_bytes),
        row_limit_enforced_before_output: execution_profile.row_limit_enforced_before_output,
        operator_row_cap_enabled: execution_profile.operator_row_cap_enabled,
        blocking_operator_count: execution_profile.blocking_operator_count(),
        blocking_operator_kinds: execution_profile.blocking_operator_kinds.clone(),
        blocking_operator_memory_reports: execution_profile
            .blocking_operator_memory_reports
            .clone(),
        intermediate_rows: execution_profile.pipeline_memory_report.intermediate_rows,
        intermediate_payload_bytes: execution_profile
            .pipeline_memory_report
            .intermediate_payload_bytes,
        output_payload_bytes: execution_profile
            .pipeline_memory_report
            .output_payload_bytes,
        steady_resident_bytes: execution_profile
            .pipeline_memory_report
            .steady_resident_bytes,
        peak_resident_bytes: execution_profile.pipeline_memory_report.peak_resident_bytes,
        total_page_faults: execution_profile.pipeline_memory_report.total_page_faults,
        minor_page_faults: execution_profile.pipeline_memory_report.minor_page_faults,
        major_page_faults: execution_profile.pipeline_memory_report.major_page_faults,
        streaming: false,
    }
}

fn bounded_nowledge_mem_read_output(
    mode: NowledgeMemGraphMode,
    bounded: BoundedReadQueryOutput,
    options: &NowledgeMemReadOptions,
) -> Result<NowledgeMemReadOutput> {
    let report =
        nowledge_mem_read_report(mode, &bounded.output, options, &bounded.execution_profile);
    if report.row_budget_exceeded {
        return Err(SkeinError::Execution(format!(
            "nowledge mem read query returned {} rows, exceeding max_rows {}",
            report.row_count,
            report.max_rows.unwrap_or_default()
        )));
    }
    if report.payload_budget_exceeded {
        return Err(SkeinError::Execution(format!(
            "nowledge mem read query estimated {} payload bytes, exceeding max_estimated_payload_bytes {}",
            report.estimated_payload_bytes,
            report.max_estimated_payload_bytes.unwrap_or_default()
        )));
    }
    Ok(NowledgeMemReadOutput {
        output: bounded.output,
        report,
    })
}

fn streamed_nowledge_mem_read_output(
    mode: NowledgeMemGraphMode,
    rows: Vec<BTreeMap<String, Value>>,
    streamed: QueryStreamReport,
    options: &NowledgeMemReadOptions,
) -> Result<NowledgeMemReadOutput> {
    let output = QueryOutput { rows: rows.into() };
    let mut report = nowledge_mem_read_report(mode, &output, options, &streamed.execution_profile);
    report.streaming = true;
    debug_assert_eq!(report.row_count, streamed.output_rows);
    debug_assert_eq!(
        report.estimated_payload_bytes,
        streamed.output_payload_bytes
    );
    Ok(NowledgeMemReadOutput { output, report })
}

fn legacy_nowledge_mem_read_error(
    error: SkeinError,
    options: &NowledgeMemReadOptions,
) -> SkeinError {
    let Some(max_payload_bytes) = options.max_estimated_payload_bytes else {
        return error;
    };
    if matches!(
        &error,
        SkeinError::Execution(message)
            if message.contains(&format!("max_payload_bytes {max_payload_bytes}"))
    ) {
        return SkeinError::Execution(format!(
            "nowledge mem read query payload exceeding max_estimated_payload_bytes {max_payload_bytes}"
        ));
    }
    error
}

fn estimate_query_output_payload_bytes(output: &QueryOutput) -> usize {
    output
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|(key, value)| key.len() + estimate_value_payload_bytes(value))
                .sum::<usize>()
        })
        .sum()
}

fn estimate_value_payload_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => std::mem::size_of::<i64>(),
        Value::String(value) => value.len(),
        Value::Binary(value) => value.len(),
        Value::List(values) => values.iter().map(estimate_value_payload_bytes).sum(),
        Value::Map(values) => values
            .iter()
            .map(|(key, value)| key.len() + estimate_value_payload_bytes(value))
            .sum(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_mem_bounded_read_evidence_json,
        nowledge_mem_bounded_read_evidence_json_with_route_readiness,
        nowledge_mem_fast_path_classification, nowledge_mem_graph_config,
        nowledge_mem_graph_config_with_search_mode,
        nowledge_mem_search_candidate_shadow_evidence_json,
        nowledge_mem_source_mutation_dual_write_evidence_all_ready,
        nowledge_mem_source_mutation_dual_write_readiness,
        workload_fixture_readiness_blocker_codes, NowledgeMemCutoverControls,
        NowledgeMemEmbeddedStore, NowledgeMemEmbeddedStoreHandle, NowledgeMemGraph,
        NowledgeMemGraphMode, NowledgeMemOpenDiagnosticOptions, NowledgeMemOpenOptions,
        NowledgeMemOutOfCoreSearchProjection, NowledgeMemQualifiedOutOfCoreSearchOptions,
        NowledgeMemQueryExecutionPath, NowledgeMemQueryReportOptions, NowledgeMemReadOptions,
        NowledgeMemReadReport, NowledgeMemReadinessAreaSummary, NowledgeMemReadinessDashboard,
        NowledgeMemReadinessOptions, NowledgeMemRetrievalProjectionAdvisor,
        NowledgeMemRouteReadinessSummary, NowledgeMemSearchCandidateReadinessOptions,
        NowledgeMemSearchCandidateRequest, NowledgeMemSearchCandidateShadowAccumulator,
        NowledgeMemSearchCandidateShadowEvidence, NowledgeMemSearchProjection,
        NowledgeMemSourceMutationDualWriteEvidence, NowledgeMemStorageLifecycleActionKind,
        NowledgeMemStorageLifecycleDecision, NowledgeMemStorageRecoveryReport,
        NowledgeMemWorkControl, NowledgeQueryRuntimePreflightProbe,
        NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL, NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL,
        NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL, NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL,
        NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL, NOWLEDGE_MEM_PRODUCTION_STATUS_PROTOCOL,
        NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL, NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL,
        NOWLEDGE_MEM_READ_REPORT_PROTOCOL, NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL,
        NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
        NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE,
        NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL,
        NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL, NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES, REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES,
        REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES, SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY,
    };
    use crate::mem_integration_readiness::nowledge_mem_final_cutover_preflight;
    use crate::route_ownership::{
        nowledge_mem_route_ownership_all_legacy, nowledge_mem_route_ownership_all_skein,
        nowledge_mem_route_ownership_readiness, NowledgeMemRouteOwnershipPolicy,
    };
    use crate::search::{
        CompressedVectorSearchMode, SearchFusionWeights, SearchLexicalFeasibilityCoverage,
        SearchLexicalFeasibilityMetrics, SearchLexicalProductionQualificationReport,
        SearchOutOfCoreConfig, SearchTopKScoreParity,
    };
    use crate::search_route_ownership::{
        nowledge_mem_active_search_route_ownership_all_skein,
        nowledge_mem_active_search_route_ownership_readiness,
        nowledge_mem_active_search_route_read_evidence_all_skein_ready,
        nowledge_mem_active_search_route_readiness, nowledge_mem_search_route_ownership_all_skein,
        nowledge_mem_search_route_ownership_readiness,
        NowledgeMemActiveSearchRouteOwnershipReadinessReport,
        NowledgeMemActiveSearchRouteReadinessReport, NowledgeMemSearchRouteOwnershipPolicy,
        NowledgeMemSearchRouteOwnershipReadinessReport,
    };
    use crate::workload_fixtures::{
        nowledge_graph_route_workload_fixture_report, NowledgeGraphRouteWorkloadFixtureOptions,
    };
    use crate::AdaptiveVectorBackendPolicy;
    use crate::Value;
    use crate::{
        BackgroundMaintenanceKind, BackgroundMaintenanceOptions, BackgroundWorkHint, Database,
        DatabaseConfig, GraphRagQueryBinding, GraphRagQueryDraft, GraphRagQueryPattern,
        GraphRagQueryPredicate, GraphRagQueryPredicateOperator, GraphRagQueryProjection,
        GraphRagSchemaContextOptions, KnowledgeCandidateScoringPolicy, KnowledgeRetrievalRequest,
        LocalQosPolicy, LocalQosScheduler, LocalQosState, NowledgeGraphStatement,
        ProductionEvidenceBinding, ProductionQualificationIdentity, RecoveryMode,
        SearchEmbeddingManifest, SearchIndex, SearchMode, SearchProjectionDelta,
        SearchProjectionFreshness, SearchProjectionKind, SearchProjectionProbeOptions,
        SearchProjectionRelationalDelta, SearchProjectionRow, SkeinError,
        SkeinLightningInitialImportCheckpoint, SkeinLightningInitialImportCutoverCatchUpReport,
        SkeinLightningInitialImportDocumentIdentity, SkeinLightningInitialImportReadinessInputs,
        StorageOpenTimings, StorageRecoveryReport, StorageResidencyMode,
        StorageResourceProfileLimits, VectorRecallValidationOptions, VectorRecallValidationReport,
        WorkClass, PRODUCTION_QUALIFICATION_POLICY_VERSION, VECTOR_RECALL_VALIDATION_PROTOCOL,
    };
    use std::collections::BTreeMap;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    fn production_identity(canonical_graph_commit_epoch: u64) -> ProductionQualificationIdentity {
        ProductionQualificationIdentity {
            source_revision: "test-revision".to_string(),
            rust_toolchain: "test-toolchain".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: vec!["vector-search".to_string()],
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "test-configuration".to_string(),
            deployment_profile: "test".to_string(),
            dataset_fingerprint: "test-dataset".to_string(),
            canonical_graph_commit_epoch,
            policy_version: PRODUCTION_QUALIFICATION_POLICY_VERSION,
        }
    }

    fn ready_vector_recall_report() -> VectorRecallValidationReport {
        VectorRecallValidationReport {
            protocol: VECTOR_RECALL_VALIDATION_PROTOCOL.to_string(),
            ready: true,
            approximate_backend: "skein_turboquant_candidate_projection".to_string(),
            sample_candidate_count: 2,
            requested_sample_count: 2,
            executed_sample_count: 2,
            top_k: 1,
            candidate_limit: 1,
            minimum_recall_per_million: 950_000,
            exact_hit_count: 2,
            candidate_hit_count: 2,
            candidate_overlap_count: 2,
            candidate_recall_at_k_per_million: 1_000_000,
            approximate_hit_count: 2,
            overlap_count: 2,
            recall_at_k_per_million: 1_000_000,
            overlap_at_k_per_million: 1_000_000,
            fallback_count: 0,
            index_coverage_incomplete_count: 0,
            average_filter_selectivity_per_million: 1_000_000,
            max_filter_selectivity_per_million: 1_000_000,
            blocker_codes: Vec::new(),
        }
    }

    #[test]
    fn graph_config_tracks_shadow_vs_cutover_mode() {
        assert!(nowledge_mem_graph_config(NowledgeMemGraphMode::ShadowReadOnly).read_only);
        assert!(!nowledge_mem_graph_config(NowledgeMemGraphMode::WritableCutover).read_only);
        assert_eq!(
            nowledge_mem_graph_config(NowledgeMemGraphMode::ShadowReadOnly)
                .compressed_vector_search_mode,
            CompressedVectorSearchMode::Disabled
        );
        assert_eq!(
            nowledge_mem_graph_config_with_search_mode(
                NowledgeMemGraphMode::ShadowReadOnly,
                CompressedVectorSearchMode::Required,
            )
            .compressed_vector_search_mode,
            CompressedVectorSearchMode::Required
        );
    }

    #[test]
    fn graph_facade_executes_cypher_through_library_api() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);

        graph
            .query("CREATE (:Memory {id: 'mem-1', title: 'Library seam'})")
            .unwrap();
        let output = graph
            .query("MATCH (m:Memory {id: 'mem-1'}) RETURN m.title AS title")
            .unwrap();

        assert_eq!(output.rows.len(), 1);
        assert_eq!(graph.mode(), NowledgeMemGraphMode::WritableCutover);
    }

    #[test]
    fn embedded_handle_generates_and_executes_bounded_schema_guided_graph_rag() {
        let mut graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        graph
            .query(
                "CREATE (:Memory {id: 'memory-1'})\
                 -[:MENTIONS]->(:Entity {id: 'entity-1', name: 'Skein'})",
            )
            .unwrap();
        let handle =
            NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None));
        let context = handle
            .graph_rag_schema_context(GraphRagSchemaContextOptions::default())
            .unwrap();
        let generated = context
            .generate_query(&GraphRagQueryDraft {
                schema_fingerprint: context.fingerprint,
                pattern: GraphRagQueryPattern::Route {
                    source_label: "Memory".to_string(),
                    relationship_type: "MENTIONS".to_string(),
                    target_label: "Entity".to_string(),
                },
                predicates: vec![GraphRagQueryPredicate {
                    binding: GraphRagQueryBinding::Source,
                    property: "id".to_string(),
                    operator: GraphRagQueryPredicateOperator::Eq,
                    parameter: Some("memory_id".to_string()),
                }],
                projections: vec![GraphRagQueryProjection {
                    binding: GraphRagQueryBinding::Target,
                    property: "name".to_string(),
                    alias: "entity_name".to_string(),
                }],
                limit: 2,
            })
            .unwrap();
        let output = handle
            .read_generated_graph_rag(
                &generated,
                &BTreeMap::from([(
                    "memory_id".to_string(),
                    Value::String("memory-1".to_string()),
                )]),
                &NowledgeMemReadOptions {
                    max_rows: Some(2),
                    max_estimated_payload_bytes: Some(256),
                },
            )
            .unwrap();

        assert_eq!(
            output.output.rows[0].get("entity_name"),
            Some(&Value::String("Skein".to_string()))
        );
        assert_eq!(output.report.max_rows, Some(2));
        assert!(output.report.row_limit_enforced_before_output);

        handle
            .query_with_report("CREATE (:Source {id: 'source-1'})")
            .unwrap();
        let error = handle
            .read_generated_graph_rag(
                &generated,
                &BTreeMap::from([(
                    "memory_id".to_string(),
                    Value::String("memory-1".to_string()),
                )]),
                &NowledgeMemReadOptions::default(),
            )
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("GraphRAG schema context is stale"));
    }

    #[test]
    fn fast_path_classification_uses_ast_shape_not_query_text() {
        let compact =
            crate::cypher::parse("MATCH (m:Memory {id: $id}) RETURN m.title AS title").unwrap();
        let spaced = crate::cypher::parse(
            "  match   ( m : Memory   { id : $id } )   return   m.title   as   title  ",
        )
        .unwrap();
        let ordered = crate::cypher::parse(
            "MATCH (m:Memory {id: $id}) RETURN m.title AS title ORDER BY title LIMIT 1",
        )
        .unwrap();

        let compact_classification = nowledge_mem_fast_path_classification(&compact);
        let spaced_classification = nowledge_mem_fast_path_classification(&spaced);
        let ordered_classification = nowledge_mem_fast_path_classification(&ordered);

        assert_eq!(
            compact_classification.execution_path,
            NowledgeMemQueryExecutionPath::FastPath
        );
        assert_eq!(
            compact_classification.fast_path_reason,
            Some("simple_node_lookup")
        );
        assert_eq!(compact_classification, spaced_classification);
        assert_eq!(
            ordered_classification.execution_path,
            NowledgeMemQueryExecutionPath::OptimizedPath
        );
        assert_eq!(ordered_classification.fast_path_reason, None);
    }

    #[test]
    fn graph_query_with_report_marks_simple_lookup_fast_path() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-fast', title: 'Fast path'})")
            .unwrap();

        let query = graph
            .query_with_report("MATCH (m:Memory {id: 'mem-fast'}) RETURN m.title AS title")
            .unwrap();

        assert_eq!(query.output.rows.len(), 1);
        assert_eq!(query.report.protocol, NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL);
        assert_eq!(query.report.statement_kind, "match_return");
        assert_eq!(
            query.report.execution_path,
            NowledgeMemQueryExecutionPath::FastPath
        );
        assert_eq!(
            query.report.fast_path_reason.as_deref(),
            Some("simple_node_lookup")
        );
        assert_eq!(query.report.optimizer_decision_count, 0);
        assert!(!query.report.physical_plan_captured);
        assert!(query.report.physical_operator_counts.is_empty());
        assert!(!query.report.slow_log_candidate);
        assert_eq!(query.report.json()["execution_path"], "fast_path");
        assert_eq!(query.report.json()["statement_kind"], "match_return");
        assert_eq!(query.report.json()["fast_path_selected"], true);
        assert_eq!(query.report.json()["physical_plan_captured"], false);
        assert_eq!(query.report.optimizer_rule_event_count, 0);
        assert_eq!(query.report.json()["optimizer_rule_event_count"], 0);
        assert_eq!(query.report.json()["plan_cache"]["cacheable"], true);
        assert_eq!(query.report.output_row_shape.row_count, 1);
        assert_eq!(query.report.output_row_shape.column_count, 1);
        assert_eq!(query.report.output_row_shape.columns, vec!["title"]);
        assert_eq!(query.report.json()["output_row_shape"]["row_count"], 1);
        assert_eq!(
            query.report.json()["output_row_shape"]["columns"],
            serde_json::json!(["title"])
        );
        assert!(
            query
                .report
                .api_behavior
                .include_metadata_false_strips_metadata
        );
        assert!(query.report.api_behavior.ordering_contract_recorded);
        assert!(query.report.api_behavior.pagination_contract_recorded);
        assert!(query.report.api_behavior.error_class_stable);
        assert!(!query.report.api_behavior.statement_has_ordering);
        assert!(!query.report.api_behavior.statement_has_pagination);
        assert_eq!(
            query.report.json()["api_behavior"]["error_class_stable"],
            true
        );
    }

    #[test]
    fn graph_query_with_report_keeps_ordered_scan_on_optimized_path() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-slow-1', title: 'B'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'mem-slow-2', title: 'A'})")
            .unwrap();

        let query = graph
            .query_with_report("MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1")
            .unwrap();

        assert_eq!(query.output.rows.len(), 1);
        assert_eq!(
            query.report.execution_path,
            NowledgeMemQueryExecutionPath::OptimizedPath
        );
        assert_eq!(query.report.statement_kind, "match_return");
        assert_eq!(query.report.fast_path_reason, None);
        assert_eq!(query.report.optimizer_decision_count, 0);
        assert_eq!(query.report.optimizer_rule_event_count, 0);
        assert!(!query.report.physical_plan_captured);
        assert!(query.report.physical_operator_counts.is_empty());
        assert!(query.report.plan_cache_cacheable);
        assert!(query.report.plan_cache_miss);
        assert!(!query.report.plan_cache_hit);
        assert!(!query.report.plan_cache_bypassed);
        assert_eq!(query.report.json()["execution_path"], "optimized_path");
        assert_eq!(query.report.json()["fast_path_selected"], false);
        assert_eq!(query.report.json()["plan_cache"]["lookup"], "miss");
        assert_eq!(query.report.json()["plan_cache"]["cacheable"], true);
        assert_eq!(query.report.json()["plan_cache"]["miss"], true);
        assert!(query.report.api_behavior.statement_has_ordering);
        assert!(query.report.api_behavior.statement_has_pagination);
        assert_eq!(
            query.report.json()["api_behavior"]["statement_has_ordering"],
            true
        );
        assert_eq!(
            query.report.json()["api_behavior"]["statement_has_pagination"],
            true
        );
    }

    #[test]
    fn graph_query_with_report_can_capture_physical_plan_on_demand() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-plan-1', title: 'B'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'mem-plan-2', title: 'A'})")
            .unwrap();

        let query = graph
            .query_with_params_with_report_options(
                "MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1",
                &BTreeMap::new(),
                NowledgeMemQueryReportOptions {
                    capture_physical_plan: true,
                    slow_log_threshold_micros: Some(0),
                },
            )
            .unwrap();

        assert_eq!(query.output.rows.len(), 1);
        assert!(query.report.physical_plan_captured);
        assert_eq!(query.report.statement_kind, "match_return");
        assert!(query.report.optimizer_decision_count > 0);
        assert!(query.report.optimizer_rule_event_count > 0);
        assert!(query
            .report
            .physical_operator_counts
            .contains_key("TopNExec"));
        assert_eq!(query.report.plan_cache_lookup.as_deref(), Some("miss"));
        assert_eq!(query.report.plan_cache_bypass_reason, None);
        assert!(query.report.slow_log_candidate);
        assert_eq!(query.report.json()["physical_plan_captured"], true);
        assert!(query.report.json()["optimizer_rule_event_count"]
            .as_u64()
            .is_some_and(|count| count > 0));
        assert_eq!(query.report.json()["plan_cache_lookup"], "miss");
        assert_eq!(query.report.json()["slow_log_candidate"], true);
        assert_eq!(
            query.report.json()["physical_operator_counts"]["TopNExec"],
            1
        );
    }

    #[test]
    fn graph_query_with_report_captures_plan_without_extra_cache_lookup() {
        let db = Database::new_with_config(DatabaseConfig {
            max_plan_cache_entries: Some(8),
            ..DatabaseConfig::default()
        });
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-report-cache-1', title: 'B'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'mem-report-cache-2', title: 'A'})")
            .unwrap();
        let before = graph.database().plan_cache_stats();

        let query = graph
            .query_with_params_with_report_options(
                "MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1",
                &BTreeMap::new(),
                NowledgeMemQueryReportOptions {
                    capture_physical_plan: true,
                    slow_log_threshold_micros: None,
                },
            )
            .unwrap();
        let after = graph.database().plan_cache_stats();

        assert_eq!(query.output.rows.len(), 1);
        assert!(query.report.physical_plan_captured);
        assert_eq!(query.report.plan_cache_lookup.as_deref(), Some("miss"));
        assert_eq!(query.report.plan_cache_bypass_reason, None);
        assert!(query.report.plan_cache_cacheable);
        assert!(query.report.plan_cache_miss);
        assert!(!query.report.plan_cache_bypassed);
        assert_eq!(after.entries, before.entries + 1);
        assert_eq!(after.misses, before.misses + 1);
        assert_eq!(after.hits, before.hits);
    }

    #[test]
    fn graph_query_with_report_exposes_storage_scan_pruning() {
        let mut db = Database::new();
        db.query("CREATE INDEX ON :Memory(kind)").unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-prune-1', kind: 'note', title: 'Keep'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'mem-prune-2', kind: 'note', title: 'Also keep'})")
            .unwrap();

        let query = graph
            .query_with_report("MATCH (m:Memory) WHERE m.kind = 'note' RETURN m.title AS title")
            .unwrap();

        assert_eq!(query.output.rows.len(), 2);
        assert_eq!(query.report.scan_pruning_reports.len(), 1);
        let scan = &query.report.scan_pruning_reports[0];
        assert!(scan.pruned);
        assert_eq!(scan.candidate_count_before_filter, 2);
        assert_eq!(scan.output_count, 2);
        assert_eq!(query.report.json()["scan_pruning_report_count"], 1);
        assert_eq!(
            query.report.json()["scan_pruning_reports"][0]["strategy"]["kind"],
            "property_eq"
        );
        assert_eq!(
            query.report.json()["scan_pruning_reports"][0]["strategy"]["property"],
            "kind"
        );
    }

    #[test]
    fn graph_query_report_exposes_normalized_default_scan_pruning() {
        let mut db = Database::new();
        db.query("CREATE INDEX ON :Thread(space_id)").unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Thread {thread_id: 'thread-missing', title: 'Missing space'})")
            .unwrap();
        graph
            .query(
                "CREATE (:Thread {thread_id: 'thread-empty', space_id: '', title: 'Empty space'})",
            )
            .unwrap();
        graph
            .query("CREATE (:Thread {thread_id: 'thread-default', space_id: 'default', title: 'Default space'})")
            .unwrap();
        graph
            .query("CREATE (:Thread {thread_id: 'thread-team', space_id: 'team', title: 'Team space'})")
            .unwrap();

        let source_params = BTreeMap::from([(
            "source_space_id".to_string(),
            Value::String("default".to_string()),
        )]);
        let source_query = graph
            .query_with_params_with_report(
                "MATCH (t:Thread) WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END = $source_space_id RETURN t.thread_id AS thread_id",
                &source_params,
            )
            .unwrap();

        assert_eq!(source_query.output.rows.len(), 3);
        assert_eq!(source_query.report.scan_pruning_reports.len(), 1);
        let source_scan = &source_query.report.scan_pruning_reports[0];
        assert!(source_scan.pruned);
        assert_eq!(source_scan.candidate_count_before_pruning, 4);
        assert_eq!(source_scan.candidate_count_before_filter, 3);
        assert_eq!(source_scan.pruned_candidate_count, 1);
        assert_eq!(
            source_query.report.json()["scan_pruning_reports"][0]["strategy"]["kind"],
            "property_default_if_null_eq"
        );
        assert_eq!(
            source_query.report.json()["scan_pruning_reports"][0]["strategy"]["property"],
            "space_id"
        );

        let target_params = BTreeMap::from([(
            "target_space_id".to_string(),
            Value::String("default".to_string()),
        )]);
        let target_query = graph
            .query_with_params_with_report(
                "MATCH (t:Thread) WHERE CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END <> $target_space_id RETURN t.thread_id AS thread_id",
                &target_params,
            )
            .unwrap();

        assert_eq!(target_query.output.rows.len(), 1);
        assert_eq!(target_query.report.scan_pruning_reports.len(), 1);
        let target_scan = &target_query.report.scan_pruning_reports[0];
        assert!(target_scan.pruned);
        assert_eq!(target_scan.candidate_count_before_pruning, 4);
        assert_eq!(target_scan.candidate_count_before_filter, 1);
        assert_eq!(target_scan.pruned_candidate_count, 3);
        assert_eq!(
            target_query.report.json()["scan_pruning_reports"][0]["strategy"]["kind"],
            "property_default_if_null_not_eq"
        );
        assert_eq!(
            target_query.report.json()["scan_pruning_reports"][0]["strategy"]["property"],
            "space_id"
        );
    }

    #[test]
    fn graph_query_with_report_keeps_system_statement_out_of_plan_cache() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);

        let query = graph
            .query_with_report("SET system.work_priority = 'background'")
            .unwrap();

        assert_eq!(query.output.rows.len(), 1);
        assert_eq!(query.report.statement_kind, "set_system_variable");
        assert_eq!(
            query.report.execution_path,
            NowledgeMemQueryExecutionPath::OptimizedPath
        );
        assert_eq!(query.report.plan_cache_lookup, None);
        assert_eq!(query.report.plan_cache_bypass_reason, None);
        assert!(!query.report.plan_cache_cacheable);
        assert!(!query.report.plan_cache_hit);
        assert!(!query.report.plan_cache_miss);
        assert!(!query.report.plan_cache_bypassed);
        assert_eq!(
            query.report.json()["plan_cache"]["lookup"],
            serde_json::Value::Null
        );
        assert_eq!(query.report.json()["plan_cache"]["cacheable"], false);
    }

    #[test]
    fn embedded_store_exposes_redacted_typed_slow_query_report() {
        let db = Database::new_with_config(DatabaseConfig {
            slow_query_log_threshold_micros: 0,
            slow_query_log_capacity: 4,
            ..DatabaseConfig::default()
        });
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);

        store
            .query_with_report("CREATE (:Memory {id: 'slow-secret-id', title: 'Slow Secret'})")
            .unwrap();
        store
            .query_with_report("MATCH (m:Memory {id: 'slow-secret-id'}) RETURN m.title AS title")
            .unwrap();

        let report = store.slow_query_report();
        let json = report.json();
        let encoded = json.to_string();

        assert_eq!(report.protocol, NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL);
        assert_eq!(report.mode, NowledgeMemGraphMode::ShadowReadOnly);
        assert!(report.present);
        assert!(report.ready);
        assert_eq!(report.capacity, 4);
        assert_eq!(report.threshold_micros, 0);
        assert_eq!(report.record_count, 2);
        assert_eq!(report.latest_sequence, Some(2));
        assert_eq!(report.records.len(), 2);
        assert!(report.records.iter().all(|record| record.success));
        assert!(report
            .records
            .iter()
            .all(|record| record.slow_log_candidate));
        assert_eq!(json["protocol"], NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL);
        assert_eq!(json["record_count"], 2);
        assert_eq!(json["redaction"]["query_text_copied"], false);
        assert_eq!(json["redaction"]["parameters_copied"], false);
        assert!(json["records"][0].get("query_digest").is_some());
        assert!(!encoded.contains("Slow Secret"));
        assert!(!encoded.contains("slow-secret-id"));
        assert!(!encoded.contains("MATCH (m:Memory"));
    }

    #[test]
    fn embedded_store_handle_supports_shared_read_and_observability_access() {
        let db = Database::new_with_config(DatabaseConfig {
            max_plan_cache_entries: Some(8),
            slow_query_log_threshold_micros: 0,
            slow_query_log_capacity: 8,
            ..DatabaseConfig::default()
        });
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        store
            .query_with_report("CREATE (:Memory {id: 'shared-1', title: 'Shared State'})")
            .unwrap();
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let reader = {
            let handle = handle.clone();
            thread::spawn(move || {
                for _ in 0..4 {
                    let query = handle
                        .read_query(
                            "MATCH (m:Memory {id: 'shared-1'}) RETURN m.title AS title",
                            &NowledgeMemReadOptions::default(),
                        )
                        .unwrap();
                    assert_eq!(query.output.rows.len(), 1);
                    assert!(query.report.row_limit_enforced_before_output);
                }
            })
        };
        let observer = {
            let handle = handle.clone();
            thread::spawn(move || {
                for _ in 0..4 {
                    let slow_query = handle.slow_query_report().unwrap();
                    assert!(slow_query.ready);
                    assert!(slow_query.record_count <= slow_query.capacity);

                    let dashboard = handle
                        .readiness_dashboard(&NowledgeMemReadinessOptions::default())
                        .unwrap();
                    assert!(readiness_dashboard_area(&dashboard, "slow_query").ready);
                    assert!(dashboard.slow_query_record_count <= slow_query.capacity);
                }
            })
        };

        reader.join().unwrap();
        observer.join().unwrap();

        let final_slow_query = handle.slow_query_report().unwrap();
        assert!(final_slow_query.ready);
        assert_eq!(final_slow_query.latest_sequence, Some(1));
        assert_eq!(final_slow_query.record_count, 1);
    }

    #[test]
    fn embedded_store_exposes_typed_operations_readiness_report() {
        let db = Database::new_with_config(DatabaseConfig {
            slow_query_log_threshold_micros: 0,
            slow_query_log_capacity: 4,
            ..DatabaseConfig::default()
        });
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        store
            .query_with_report("CREATE (:Memory {id: 'ops-1', title: 'Operations'})")
            .unwrap();

        let report = store.operations_readiness(&NowledgeMemReadinessOptions::default());
        let json = report.json();
        let encoded = json.to_string();

        assert_eq!(report.protocol, NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL);
        assert!(report.present);
        assert!(!report.ready);
        assert!(report.graph_open);
        assert!(!report.graph_read_only);
        assert!(!report.search_projection_open);
        assert_eq!(report.graph_commit_epoch, 1);
        assert_eq!(report.projection_commit_lag, 1);
        assert!(!report.projection_stale);
        assert_eq!(
            report.storage_lifecycle_action,
            NowledgeMemStorageLifecycleActionKind::OpenReadOnlyInspect
        );
        assert!(!report.storage_lifecycle_ready);
        assert!(!report.storage_recovery_ready);
        assert!(report.slow_query_ready);
        assert!(report.background_maintenance_ready);
        assert!(report
            .blocker_codes
            .contains(&"storage_recovery_not_ready".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"storage_lifecycle.storage_not_durable".to_string()));
        assert_eq!(json["protocol"], NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL);
        assert_eq!(
            json["storage_lifecycle"]["action"],
            "open_read_only_inspect"
        );
        assert_eq!(json["readiness"]["storage_lifecycle_ready"], false);
        assert_eq!(
            json["storage_lifecycle_decision"]["protocol"],
            NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL
        );
        assert_eq!(json["redaction"]["query_text_copied"], false);
        assert!(!encoded.contains("Operations"));
        assert!(!encoded.contains("ops-1"));
    }

    #[test]
    fn embedded_store_operations_readiness_reports_projection_staleness() {
        let root = unique_nowledge_mem_test_dir("operations_readiness_projection_stale");
        let search_path = root.join("search");
        let mut graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'ops-stale', title: 'Projection stale'})")
            .unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let stale = store.operations_readiness(&NowledgeMemReadinessOptions::default());
        assert!(stale.search_projection_open);
        assert_eq!(stale.projection_commit_lag, 1);
        assert!(stale.projection_stale);
        assert!(stale
            .blocker_codes
            .contains(&"search_projection_stale".to_string()));

        store.catch_up_search_projection(16, 1).unwrap();
        let caught_up = store.operations_readiness(&NowledgeMemReadinessOptions::default());
        assert!(caught_up.search_projection_open);
        assert_eq!(caught_up.projection_commit_lag, 0);
        assert!(!caught_up.projection_stale);
        assert!(!caught_up
            .blocker_codes
            .contains(&"search_projection_stale".to_string()));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_handle_supports_operations_readiness_with_shared_reads() {
        let db = Database::new_with_config(DatabaseConfig {
            slow_query_log_threshold_micros: 0,
            slow_query_log_capacity: 8,
            ..DatabaseConfig::default()
        });
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        store
            .query_with_report("CREATE (:Memory {id: 'ops-shared', title: 'Shared Operations'})")
            .unwrap();
        let handle = NowledgeMemEmbeddedStoreHandle::new(store);

        let reader = {
            let handle = handle.clone();
            thread::spawn(move || {
                let read = handle
                    .read_query(
                        "MATCH (m:Memory {id: 'ops-shared'}) RETURN m.title AS title",
                        &NowledgeMemReadOptions::default(),
                    )
                    .unwrap();
                assert_eq!(read.output.rows.len(), 1);
            })
        };
        let observer = {
            let handle = handle.clone();
            thread::spawn(move || {
                let report = handle
                    .operations_readiness(&NowledgeMemReadinessOptions::default())
                    .unwrap();
                assert!(report.slow_query_ready);
                assert!(report.background_maintenance_ready);
            })
        };

        reader.join().unwrap();
        observer.join().unwrap();
    }

    #[test]
    fn embedded_store_handle_allows_overlapping_read_guards() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let handle =
            NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None));
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let first = {
            let handle = handle.clone();
            let acquired_tx = acquired_tx.clone();
            thread::spawn(move || {
                let _guard = handle.read_store().unwrap();
                acquired_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
        };
        acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let second = {
            let handle = handle.clone();
            thread::spawn(move || {
                let _guard = handle.read_store().unwrap();
                acquired_tx.send(()).unwrap();
            })
        };
        acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_tx.send(()).unwrap();

        first.join().unwrap();
        second.join().unwrap();
    }

    #[test]
    fn embedded_store_exposes_typed_query_runtime_preflight() {
        let db = Database::new_with_config(DatabaseConfig {
            max_plan_cache_entries: Some(8),
            ..DatabaseConfig::default()
        });
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        store
            .query_with_report("CREATE INDEX ON :Memory(id)")
            .unwrap();
        store
            .query_with_report("CREATE (:Memory {id: 'preflight-1', title: 'Preflight'})")
            .unwrap();
        for id in 0..8 {
            store
                .query_with_report(&format!(
                    "CREATE (:Memory {{id: 'preflight-filler-{id}', title: 'Filler {id}'}})"
                ))
                .unwrap();
        }
        let probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                NowledgeQueryRuntimePreflightProbe::new(
                    format!("probe:{route}"),
                    "MATCH (m:Memory {id: 'preflight-1'}) RETURN m.title AS title",
                )
                .with_route(*route)
                .with_query_family(super::nowledge_mem_required_query_families_for_route(route)[0])
                .require_scan_pruning(1)
                .require_pruned()
                .with_max_output_rows(1)
            })
            .collect::<Vec<_>>();

        let report = store.query_runtime_preflight(&probes);
        let json = report.json();

        assert_eq!(report.protocol, NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL);
        assert!(report.ready);
        assert!(report.database_opened);
        assert!(report.redaction.ready());
        assert!(!report.redaction.rows_copied);
        assert!(!report.redaction.parameters_copied);
        assert!(!report.redaction.local_paths_copied);
        assert!(!report.redaction.raw_errors_copied);
        assert_eq!(
            report.probe_count,
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(report.passed_probe_count, report.probe_count);
        assert_eq!(report.failed_probe_count, 0);
        assert!(report.required_routes_covered);
        assert!(report.route_coverage_ready);
        assert!(report.blocker_codes.is_empty());
        assert!(report.probes.iter().all(|probe| probe.ready));
        assert!(report
            .probes
            .iter()
            .all(|probe| probe.selected_plan_fingerprint.is_some()));
        assert!(report
            .probes
            .iter()
            .all(|probe| !probe.scan_pruning_reports.is_empty()));
        assert_eq!(json["protocol"], NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL);
        assert_eq!(json["ready"], true);
        assert_eq!(json["redaction"]["ready"], true);
        assert_eq!(json["redaction"]["rows_copied"], false);
        assert_eq!(json["redaction"]["parameters_copied"], false);
        assert_eq!(json["redaction"]["local_paths_copied"], false);
        assert_eq!(json["redaction"]["raw_errors_copied"], false);
        assert_eq!(json["probe_count"], probes.len());
        assert_eq!(json["probes"][0]["output_row_count"], 1);
        assert_eq!(
            json["probes"][0]["execution_profile"]["scan_pruning_report_count"],
            1
        );
        assert!(json["probes"][0]["selected_plan_fingerprint"]
            .as_str()
            .is_some_and(|fingerprint| fingerprint.contains("IndexNodeSeek")));
    }

    #[test]
    fn embedded_store_query_runtime_preflight_redacts_failed_probe_values() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        let probe = NowledgeQueryRuntimePreflightProbe::new(
            "probe:/graph/node-details/{node_id}",
            "MATCH (m:Memory {id: 'secret-preflight-id'}) RETURN missing(",
        )
        .with_route("/graph/node-details/{node_id}")
        .with_query_family("memory_lookup");

        let report = store.query_runtime_preflight(&[probe]);
        let json = report.json();
        let encoded = json.to_string();

        assert!(!report.ready);
        assert!(report.redaction.ready());
        assert_eq!(report.failed_probe_count, 1);
        assert_eq!(
            report.probes[0].blocker_codes,
            vec!["query_runtime_failed".to_string()]
        );
        assert_eq!(json["probes"][0]["success"], false);
        assert_eq!(json["probes"][0]["error_class"], "parse");
        assert_eq!(json["redaction"]["ready"], true);
        assert_eq!(json["redaction"]["parameters_copied"], false);
        assert_eq!(json["redaction"]["raw_errors_copied"], false);
        assert!(!encoded.contains("secret-preflight-id"));
        assert!(!encoded.contains("RETURN missing"));
    }

    #[test]
    fn graph_read_query_reports_bounded_payload() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-read', title: 'Bounded read'})")
            .unwrap();

        let read = graph
            .read_query_with_options(
                "MATCH (m:Memory {id: 'mem-read'}) RETURN m.title AS title",
                &NowledgeMemReadOptions {
                    max_rows: Some(4),
                    max_estimated_payload_bytes: Some(128),
                },
            )
            .unwrap();

        assert_eq!(read.output.rows.len(), 1);
        assert_eq!(read.report.protocol, NOWLEDGE_MEM_READ_REPORT_PROTOCOL);
        assert_eq!(read.report.mode, NowledgeMemGraphMode::ShadowReadOnly);
        assert_eq!(read.report.row_count, 1);
        assert_eq!(read.report.max_rows, Some(4));
        assert_eq!(read.report.execution_row_cap, Some(5));
        assert!(read.report.estimated_payload_bytes <= 128);
        assert!(!read.report.row_budget_exceeded);
        assert!(!read.report.payload_budget_exceeded);
        assert!(read.report.row_limit_enforced_before_output);
        assert!(read.report.operator_row_cap_enabled);
        assert_eq!(read.report.blocking_operator_count, 0);
        assert!(read.report.blocking_operator_kinds.is_empty());
        assert!(read.report.intermediate_rows >= read.report.row_count);
        assert!(read.report.intermediate_payload_bytes >= read.report.output_payload_bytes);
        assert!(read.report.output_payload_bytes > 0);
        assert!(read.report.steady_resident_bytes.is_some());
        assert!(read.report.peak_resident_bytes.is_some());
        assert!(read.report.total_page_faults.is_some());
        assert_eq!(read.report.minor_page_faults.is_some(), cfg!(unix));
        assert_eq!(read.report.major_page_faults.is_some(), cfg!(unix));
        assert!(!read.report.streaming);
        assert_eq!(read.report.json()["execution_row_cap"], 5);
        assert_eq!(read.report.json()["row_limit_enforced_before_output"], true);
        assert_eq!(read.report.json()["operator_row_cap_enabled"], true);
        assert_eq!(read.report.json()["blocking_operator_count"], 0);
        assert!(read.report.json()["intermediate_rows"].as_u64().unwrap() >= 1);
        assert!(read.report.json()["output_payload_bytes"].as_u64().unwrap() > 0);
        assert!(
            read.report.json()["steady_resident_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(read.report.json()["peak_resident_bytes"].as_u64().unwrap() > 0);
        assert_eq!(read.report.json()["streaming"], false);
        assert_eq!(
            read.report.bounded_read_evidence_json()["protocol"],
            NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL
        );
        assert_eq!(
            read.report.bounded_read_evidence_json()["mode"],
            "shadow_read_only"
        );
        assert_eq!(read.report.bounded_read_evidence_json()["ready"], false);
        assert_eq!(
            read.report.bounded_read_evidence_json()["blocker_codes"],
            serde_json::json!(["missing_covered_routes", "graph_route_readiness_missing"])
        );
        let covered_routes = full_bounded_read_routes();
        let route_readiness = ready_route_readiness_summary();
        let evidence = nowledge_mem_bounded_read_evidence_json_with_route_readiness(
            &read.report,
            &covered_routes,
            Some(&route_readiness),
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(
            evidence["covered_routes"],
            serde_json::json!(covered_routes)
        );
        assert_eq!(evidence["missing_covered_routes"], serde_json::json!([]));
    }

    #[test]
    fn graph_queries_hold_and_release_runtime_governor_permits() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);

        graph
            .query("CREATE (:Memory {id: 'admitted-mem'})")
            .unwrap();
        let read = graph
            .read_query_with_options(
                "MATCH (m:Memory {id: 'admitted-mem'}) RETURN m.id AS id",
                &NowledgeMemReadOptions {
                    max_rows: Some(1),
                    max_estimated_payload_bytes: Some(1024),
                },
            )
            .unwrap();

        assert_eq!(read.output.rows.len(), 1);
        let snapshot = graph.runtime_governor_snapshot();
        assert_eq!(snapshot.admissions, 2);
        assert_eq!(snapshot.completions, 2);
        assert_eq!(snapshot.active_foreground_tasks, 0);
        assert_eq!(snapshot.active_cpu_slots, 0);
        assert_eq!(snapshot.admitted_memory_bytes, 0);
    }

    #[test]
    fn graph_query_rejects_before_mutation_when_runtime_memory_is_unavailable() {
        let governor = skein_qos::RuntimeGovernor::detect(
            skein_qos::RuntimeGovernorConfig {
                memory_budget_bytes: Some(1),
                ..skein_qos::RuntimeGovernorConfig::default()
            },
            skein_qos::IoConcurrencyBudget::new(1, 1),
        );
        let mut graph = NowledgeMemGraph::from_database_with_runtime_governor(
            Database::new(),
            NowledgeMemGraphMode::WritableCutover,
            governor,
        );

        let error = graph
            .query("CREATE (:Memory {id: 'rejected-mem'})")
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("runtime admission memory_saturated"));
        assert!(graph
            .database_mut()
            .query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap()
            .rows
            .is_empty());
        let snapshot = graph.runtime_governor_snapshot();
        assert_eq!(snapshot.admissions, 0);
        assert_eq!(snapshot.completions, 0);
        assert_eq!(snapshot.admission_rejections, 1);
    }

    #[test]
    fn embedded_handle_transaction_rejects_before_mutation_without_runtime_memory() {
        let governor = skein_qos::RuntimeGovernor::detect(
            skein_qos::RuntimeGovernorConfig {
                memory_budget_bytes: Some(1),
                ..skein_qos::RuntimeGovernorConfig::default()
            },
            skein_qos::IoConcurrencyBudget::new(2, 1),
        );
        let graph = NowledgeMemGraph::from_database_with_runtime_governor(
            Database::new(),
            NowledgeMemGraphMode::WritableCutover,
            governor,
        );
        let handle =
            NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None));

        let error = handle
            .transaction(&[NowledgeGraphStatement {
                cypher: "CREATE (:Memory {id: 'transaction-rejected'})".to_string(),
                parameters: BTreeMap::new(),
            }])
            .unwrap_err();

        assert!(error.to_string().contains("runtime admission"));
        assert_eq!(handle.runtime_governor_snapshot().unwrap().admissions, 0);
        let mut store = handle.write_store().unwrap();
        let output = store
            .graph_mut()
            .database_mut()
            .query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap();
        assert!(output.rows.is_empty());
    }

    #[test]
    fn embedded_handle_query_transaction_commits_graph_and_relational_state_once() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let handle =
            NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None));

        handle
            .with_transaction(|transaction| {
                transaction.query("CREATE (:Memory {id: 'memory-1'})")?;
                transaction.query_sql(
                    "CREATE TABLE anchors (\
                       anchor_id TEXT PRIMARY KEY,\
                       memory_id TEXT NOT NULL\
                     )",
                )?;
                transaction.query_sql_with_params(
                    "INSERT INTO anchors (anchor_id, memory_id) VALUES ($1, $2)",
                    &[
                        Value::String("anchor-1".to_string()),
                        Value::String("memory-1".to_string()),
                    ],
                )?;
                Ok(())
            })
            .unwrap();

        assert_eq!(handle.runtime_status().unwrap().graph_commit_epoch, 1);
        let mut store = handle.write_store().unwrap();
        assert_eq!(
            store
                .graph_mut()
                .database_mut()
                .query("MATCH (m:Memory) RETURN m.id AS id")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_eq!(
            store
                .graph_mut()
                .database_mut()
                .query_sql("SELECT anchor_id, memory_id FROM anchors")
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn embedded_handle_query_transaction_rolls_back_callback_errors() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let handle =
            NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None));

        let error = handle
            .with_transaction(|transaction| {
                transaction.query("CREATE (:Memory {id: 'rolled-back'})")?;
                Err::<(), _>(crate::SkeinError::Execution(
                    "injected callback failure".to_string(),
                ))
            })
            .unwrap_err();

        assert!(error.to_string().contains("injected callback failure"));
        assert_eq!(handle.runtime_status().unwrap().graph_commit_epoch, 0);
        let mut store = handle.write_store().unwrap();
        assert!(store
            .graph_mut()
            .database_mut()
            .query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap()
            .rows
            .is_empty());
    }

    #[test]
    fn embedded_handle_checkpoints_through_admitted_maintenance() {
        let root = unique_nowledge_mem_test_dir("embedded_handle_checkpoint");
        let graph_path = root.join("graph");
        let options =
            NowledgeMemOpenOptions::graph_only(&graph_path, NowledgeMemGraphMode::WritableCutover);
        let (handle, _) = NowledgeMemEmbeddedStoreHandle::open_with_options(options).unwrap();

        handle
            .query_with_report("CREATE (:Memory {id: 'checkpointed'})")
            .unwrap();
        let before = handle.runtime_governor_snapshot().unwrap();
        handle.checkpoint().unwrap();
        let after = handle.runtime_governor_snapshot().unwrap();

        assert_eq!(after.admissions, before.admissions + 1);
        assert_eq!(after.completions, before.completions + 1);
        assert_eq!(after.active_background_tasks, 0);
        assert_eq!(after.active_blocking_tasks, 0);
        assert_eq!(after.active_cpu_slots, 0);
        assert_eq!(after.active_background_io_slots, 0);
        assert_eq!(after.admitted_memory_bytes, 0);
        drop(handle);

        let options =
            NowledgeMemOpenOptions::graph_only(&graph_path, NowledgeMemGraphMode::ShadowReadOnly);
        let (handle, _) = NowledgeMemEmbeddedStoreHandle::open_with_options(options).unwrap();
        let output = handle
            .query_with_report("MATCH (m:Memory {id: 'checkpointed'}) RETURN m.id AS id")
            .unwrap();
        assert_eq!(output.output.rows.len(), 1);
        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_handle_search_holds_governed_io_and_result_budget() {
        let governor = skein_qos::RuntimeGovernor::detect(
            skein_qos::RuntimeGovernorConfig {
                memory_budget_bytes: Some(256 * 1024 * 1024),
                result_budget_bytes: 64 * 1024 * 1024,
                ..skein_qos::RuntimeGovernorConfig::default()
            },
            skein_qos::IoConcurrencyBudget::new(1, 1),
        );
        let graph = NowledgeMemGraph::from_database_with_runtime_governor(
            Database::new(),
            NowledgeMemGraphMode::WritableCutover,
            governor,
        );
        let mut projection = NowledgeMemSearchProjection::from_index(SearchIndex::default());
        projection.set_range_read_config(crate::SearchRangeReadConfig::new(
            std::num::NonZeroUsize::MIN,
            std::num::NonZeroU64::new(1024).unwrap(),
        ));
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ));

        let output = handle
            .search_candidates_with_report(&NowledgeMemSearchCandidateRequest::text("empty", 4))
            .unwrap();

        assert!(output.result.hits.is_empty());
        let snapshot = handle.runtime_governor_snapshot().unwrap();
        assert_eq!(snapshot.admissions, 1);
        assert_eq!(snapshot.completions, 1);
        assert_eq!(snapshot.active_foreground_io_slots, 0);
        assert_eq!(snapshot.admitted_memory_bytes, 0);
    }

    #[test]
    fn embedded_handle_charges_out_of_core_buffers_to_runtime_admission() {
        let root = unique_nowledge_mem_test_dir("out_of_core_runtime_admission");
        let search_path = root.join("search");
        {
            let mut index = SearchIndex::open(&search_path).unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "admission".to_string(),
                    title: "Out of core admission".to_string(),
                    body: "Bounded buffers must be admitted".to_string(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::new(),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let governor = skein_qos::RuntimeGovernor::detect(
            skein_qos::RuntimeGovernorConfig {
                memory_budget_bytes: Some(256 * 1024 * 1024),
                result_budget_bytes: 64 * 1024 * 1024,
                ..skein_qos::RuntimeGovernorConfig::default()
            },
            skein_qos::IoConcurrencyBudget::new(1, 1),
        );
        let graph = NowledgeMemGraph::from_database_with_runtime_governor(
            Database::new(),
            NowledgeMemGraphMode::ShadowReadOnly,
            governor,
        );
        let projection = NowledgeMemOutOfCoreSearchProjection::open(&search_path).unwrap();
        let handle = NowledgeMemEmbeddedStoreHandle::new(
            NowledgeMemEmbeddedStore::new_with_out_of_core_search(
                graph,
                projection,
                NowledgeMemRetrievalProjectionAdvisor::default(),
            ),
        );

        let error = handle
            .search_candidates(&NowledgeMemSearchCandidateRequest::text("admission", 1))
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("runtime admission memory_saturated"));
        let snapshot = handle.runtime_governor_snapshot().unwrap();
        assert_eq!(snapshot.admissions, 0);
        assert_eq!(snapshot.admission_rejections, 1);
        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_clamps_search_io_depth_to_runtime_governor() {
        let governor = skein_qos::RuntimeGovernor::detect(
            skein_qos::RuntimeGovernorConfig {
                memory_budget_bytes: Some(256 * 1024 * 1024),
                result_budget_bytes: 64 * 1024 * 1024,
                ..skein_qos::RuntimeGovernorConfig::default()
            },
            skein_qos::IoConcurrencyBudget::new(2, 1),
        );
        let graph = NowledgeMemGraph::from_database_with_runtime_governor(
            Database::new(),
            NowledgeMemGraphMode::WritableCutover,
            governor,
        );
        let mut projection = NowledgeMemSearchProjection::from_index(SearchIndex::default());
        projection.set_range_read_config(crate::SearchRangeReadConfig::new(
            std::num::NonZeroUsize::new(4).unwrap(),
            std::num::NonZeroU64::new(1024).unwrap(),
        ));

        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        assert_eq!(
            store
                .search_projection()
                .unwrap()
                .index()
                .range_read_config()
                .io_depth
                .get(),
            2
        );
    }

    #[test]
    fn embedded_handle_read_transaction_runs_against_one_admitted_snapshot() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let handle =
            NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(graph, None));
        handle
            .query_with_report("CREATE (:Memory {id: 'memory-1'})")
            .unwrap();

        let (epoch, rows) = handle
            .with_read_transaction(64 * 1024, |transaction| {
                let epoch = transaction.commit_epoch();
                let rows = transaction
                    .query_with_params_bounded(
                        "MATCH (m:Memory) RETURN m.id AS id LIMIT 2",
                        &BTreeMap::new(),
                        Some(2),
                    )?
                    .rows;
                Ok((epoch, rows))
            })
            .unwrap();

        assert_eq!(epoch, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("id"),
            Some(&Value::String("memory-1".to_string()))
        );
        let snapshot = handle.runtime_governor_snapshot().unwrap();
        assert_eq!(snapshot.active_foreground_tasks, 0);
        assert_eq!(snapshot.admitted_memory_bytes, 0);
    }

    #[test]
    fn graph_streaming_query_reports_pre_execution_cancellation() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'cancelled-mem'})").unwrap();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let cancellation = skein_core::RuntimeCancellationToken::new();
        cancellation.cancel();
        let context = skein_core::RuntimeTaskContext::without_deadline(cancellation);

        let error = graph
            .read_query_with_params_streaming_context(
                "MATCH (m:Memory) RETURN m.id AS id",
                &BTreeMap::new(),
                &NowledgeMemReadOptions::default(),
                &context,
                |_| Ok(()),
            )
            .unwrap_err();

        assert!(error.to_string().contains("runtime task cancelled"));
        let snapshot = graph.runtime_governor_snapshot();
        assert_eq!(snapshot.admissions, 0);
        assert_eq!(snapshot.cancellations, 1);
    }

    #[test]
    fn graph_read_query_streaming_collect_enforces_payload_before_host_ownership() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-stream', title: 'streamed payload'})")
            .unwrap();
        let query = "MATCH (m:Memory {id: 'mem-stream'}) RETURN m.title AS title";

        let read = graph
            .read_query_with_params_streaming_collect(
                query,
                &BTreeMap::new(),
                &NowledgeMemReadOptions {
                    max_rows: Some(1),
                    max_estimated_payload_bytes: Some(1024),
                },
            )
            .unwrap();
        assert!(read.report.streaming);
        assert_eq!(read.report.row_count, 1);
        assert_eq!(
            read.output.rows[0].get("title"),
            Some(&Value::String("streamed payload".to_string()))
        );

        let error = graph
            .read_query_with_params_streaming_collect(
                query,
                &BTreeMap::new(),
                &NowledgeMemReadOptions {
                    max_rows: Some(1),
                    max_estimated_payload_bytes: Some(1),
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("max_payload_bytes 1"));
    }

    #[test]
    fn bounded_read_evidence_fails_closed_for_missing_row_cap() {
        let report = NowledgeMemReadReport {
            protocol: NOWLEDGE_MEM_READ_REPORT_PROTOCOL.to_string(),
            mode: NowledgeMemGraphMode::ShadowReadOnly,
            row_count: 2,
            max_rows: Some(512),
            execution_row_cap: None,
            estimated_payload_bytes: 128,
            max_estimated_payload_bytes: Some(4 * 1024 * 1024),
            row_budget_exceeded: false,
            payload_budget_exceeded: false,
            row_limit_enforced_before_output: false,
            operator_row_cap_enabled: false,
            blocking_operator_count: 1,
            blocking_operator_kinds: vec!["Sort".to_string()],
            blocking_operator_memory_reports: Vec::new(),
            intermediate_rows: 0,
            intermediate_payload_bytes: 0,
            output_payload_bytes: 0,
            steady_resident_bytes: None,
            peak_resident_bytes: None,
            total_page_faults: None,
            minor_page_faults: None,
            major_page_faults: None,
            streaming: false,
        };

        let evidence = nowledge_mem_bounded_read_evidence_json(&report);

        assert_eq!(
            evidence["protocol"],
            NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL
        );
        assert_eq!(evidence["present"], true);
        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["max_rows"], 512);
        assert_eq!(evidence["execution_row_cap"], serde_json::Value::Null);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "missing_execution_row_cap",
                "row_limit_not_enforced_before_output",
                "operator_row_cap_disabled",
                "blocking_operator_memory_report_incomplete",
                "missing_covered_routes",
                "graph_route_readiness_missing"
            ])
        );
    }

    #[test]
    fn bounded_read_evidence_requires_shadow_read_only_mode() {
        let report = NowledgeMemReadReport {
            protocol: NOWLEDGE_MEM_READ_REPORT_PROTOCOL.to_string(),
            mode: NowledgeMemGraphMode::WritableCutover,
            row_count: 2,
            max_rows: Some(512),
            execution_row_cap: Some(513),
            estimated_payload_bytes: 128,
            max_estimated_payload_bytes: Some(4 * 1024 * 1024),
            row_budget_exceeded: false,
            payload_budget_exceeded: false,
            row_limit_enforced_before_output: true,
            operator_row_cap_enabled: true,
            blocking_operator_count: 0,
            blocking_operator_kinds: Vec::new(),
            blocking_operator_memory_reports: Vec::new(),
            intermediate_rows: 0,
            intermediate_payload_bytes: 0,
            output_payload_bytes: 0,
            steady_resident_bytes: None,
            peak_resident_bytes: None,
            total_page_faults: None,
            minor_page_faults: None,
            major_page_faults: None,
            streaming: false,
        };

        let evidence = nowledge_mem_bounded_read_evidence_json(&report);

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["mode"], "writable_cutover");
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "not_shadow_read_only",
                "missing_covered_routes",
                "graph_route_readiness_missing"
            ])
        );
    }

    #[test]
    fn bounded_read_evidence_accepts_streaming_with_budgeted_blocking_operator() {
        let report = NowledgeMemReadReport {
            protocol: NOWLEDGE_MEM_READ_REPORT_PROTOCOL.to_string(),
            mode: NowledgeMemGraphMode::ShadowReadOnly,
            row_count: 2,
            max_rows: Some(512),
            execution_row_cap: Some(513),
            estimated_payload_bytes: 128,
            max_estimated_payload_bytes: Some(4 * 1024 * 1024),
            row_budget_exceeded: false,
            payload_budget_exceeded: false,
            row_limit_enforced_before_output: true,
            operator_row_cap_enabled: true,
            blocking_operator_count: 1,
            blocking_operator_kinds: vec!["TopNExec".to_string()],
            blocking_operator_memory_reports: vec![skein_executor::BlockingOperatorMemoryReport {
                operator: "TopNExec".to_string(),
                budget_bytes: 4096,
                peak_tracked_bytes: 2048,
                input_rows: 100,
                max_spill_bytes: 8192,
                max_spill_runs: 4,
                spilled_bytes: 4096,
                spill_run_count: 2,
                spilled_rows: 64,
            }],
            intermediate_rows: 100,
            intermediate_payload_bytes: 2048,
            output_payload_bytes: 128,
            steady_resident_bytes: Some(1024),
            peak_resident_bytes: Some(2048),
            total_page_faults: Some(1),
            minor_page_faults: Some(1),
            major_page_faults: Some(0),
            streaming: true,
        };
        let route_readiness = NowledgeMemRouteReadinessSummary {
            route_primary_ready: true,
            primary_ready_routes: REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
                .iter()
                .map(|route| (*route).to_string())
                .collect(),
            route_query_plan_evidence_ready: true,
            route_query_profile_evidence_ready: true,
            route_query_api_behavior_evidence_ready: true,
            relationship_property_pruning_required_count: 0,
            relationship_property_pruning_report_count: 0,
            route_relationship_property_pruning_evidence_ready: true,
        };
        let covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| (*route).to_string())
            .collect::<Vec<_>>();

        let evidence = nowledge_mem_bounded_read_evidence_json_with_route_readiness(
            &report,
            &covered_routes,
            Some(&route_readiness),
        );

        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["streaming"], true);
        assert_eq!(evidence["blocking_operator_memory_reports_complete"], true);
        assert_eq!(evidence["blocking_operator_memory_within_budget"], true);
        assert_eq!(evidence["spill_within_budget"], true);
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn search_candidate_shadow_evidence_reports_ready_counts() {
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_compare_candidate_ids(&["mem_1", "mem_2"], &["mem_1", "mem_2"]);
        accumulator.record_compare_candidate_ids(
            &["mem_3", "mem_4", "mem_5"],
            &["mem_3", "mem_4", "mem_5"],
        );
        accumulator.record_filter_pushdown_fields(
            2,
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
                .iter()
                .copied(),
        );
        let evidence = accumulator.json();

        assert_eq!(
            evidence["protocol"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL
        );
        assert_eq!(
            evidence["route"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE
        );
        assert_eq!(
            evidence["evidence_source"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["request_count"], 2);
        assert_eq!(evidence["primary_candidate_count"], 5);
        assert_eq!(evidence["shadow_candidate_count"], 5);
        assert_eq!(evidence["matched_candidate_count"], 5);
        assert_eq!(evidence["primary_only_candidate_count"], 0);
        assert_eq!(evidence["row_count_parity"], true);
        assert_eq!(evidence["text_retriever_ready"], false);
        assert_eq!(evidence["vector_retriever_ready"], false);
        assert_eq!(evidence["fts_top_k_overlap_ready"], false);
        assert_eq!(evidence["vector_top_k_overlap_ready"], false);
        assert_eq!(
            evidence["candidate_readiness"]["source_chunk_identity_ready"],
            false
        );
        assert_eq!(evidence["candidate_readiness"]["fail_soft_observed"], false);
        assert_eq!(
            evidence["candidate_readiness"]["projection_marker_status_visible"],
            false
        );
        assert_eq!(
            evidence["candidate_readiness"]["projection_watermark_ready"],
            false
        );
        assert_eq!(
            evidence["candidate_readiness"]["embedding_identity_ready"],
            false
        );
        assert_eq!(evidence["candidate_identity"]["ready"], true);
        assert_eq!(evidence["candidate_identity"]["parity"], true);
        assert_eq!(evidence["shadow_scan_present"], true);
        assert_eq!(evidence["shadow_scan_filter_pushdown_ready"], true);
        assert_eq!(evidence["shadow_scan_field_pruning_ready"], true);
        assert_eq!(
            evidence["shadow_scan_field_summary_count"],
            serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len())
        );
        assert_eq!(evidence["filter_pushdown_ready"], true);
        assert_eq!(evidence["filter_pushdown"]["ready"], true);
        assert_eq!(evidence["filter_pushdown"]["pushed_predicate_count"], 2);
        assert_eq!(
            evidence["filter_pushdown"]["field_capabilities_ready"],
            true
        );
        assert_eq!(
            evidence["filter_pushdown"]["missing_value_summary_fields"],
            serde_json::json!([])
        );
        assert_eq!(
            evidence["filter_pushdown"]["missing_numeric_range_fields"],
            serde_json::json!([])
        );
        assert_eq!(
            evidence["filter_pushdown"]["missing_timestamp_range_fields"],
            serde_json::json!([])
        );
        assert_eq!(
            evidence["filter_pushdown"]["field_summary_count"],
            serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len())
        );
        assert!(evidence["filter_pushdown"]["field_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary["field"] == "importance"
                && summary["source"] == "persisted_segment_descriptor_contract"
                && summary["numeric_range_summary_used"] == true));
        assert!(evidence["candidate_identity"]
            .get("candidate_ids")
            .is_none());
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn search_candidate_shadow_evidence_fails_closed_on_weak_counts() {
        let evidence = nowledge_mem_search_candidate_shadow_evidence_json(
            &NowledgeMemSearchCandidateShadowEvidence {
                request_count: 0,
                primary_candidate_count: 3,
                shadow_candidate_count: 2,
                matched_candidate_count: 1,
                primary_only_candidate_count: 1,
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
                blocker_codes: vec!["bridge_timeout".to_string()],
            },
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "bridge_timeout",
                "search_candidate_filter_pushdown_missing",
                "search_candidate_identity_missing",
                "search_candidate_mismatch",
                "search_candidate_primary_only",
                "search_candidate_shadow_no_requests"
            ])
        );
    }

    #[test]
    fn search_candidate_shadow_accumulator_generates_bridge_evidence() {
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_compare_candidate_ids(&["mem_1", "mem_2"], &["mem_1", "mem_2"]);
        accumulator.record_compare_candidate_ids(&["mem_3"], &["mem_3"]);
        accumulator.record_filter_pushdown_fields(
            1,
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
                .iter()
                .copied(),
        );

        let evidence = accumulator.json();

        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["request_count"], 2);
        assert_eq!(evidence["primary_candidate_count"], 3);
        assert_eq!(evidence["shadow_candidate_count"], 3);
        assert_eq!(evidence["matched_candidate_count"], 3);
        assert_eq!(evidence["primary_only_candidate_count"], 0);
        assert_eq!(evidence["row_count_parity"], true);
        assert_eq!(evidence["text_retriever_ready"], false);
        assert_eq!(evidence["vector_retriever_ready"], false);
        assert_eq!(evidence["fts_top_k_overlap_ready"], false);
        assert_eq!(evidence["vector_top_k_overlap_ready"], false);
        assert_eq!(
            evidence["candidate_readiness"]["source_chunk_identity_ready"],
            false
        );
        assert_eq!(
            evidence["candidate_readiness"]["projection_watermark_ready"],
            false
        );
        assert_eq!(
            evidence["candidate_readiness"]["embedding_identity_ready"],
            false
        );
        assert_eq!(evidence["candidate_identity"]["ready"], true);
        assert_eq!(evidence["shadow_scan_filter_pushdown_ready"], true);
        assert_eq!(evidence["shadow_scan_field_pruning_ready"], true);
        assert_eq!(evidence["filter_pushdown_ready"], true);
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn search_candidate_shadow_accumulator_preserves_request_blockers() {
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_compare_candidate_ids(&["mem_1", "mem_2"], &["mem_1"]);
        accumulator.record_filter_pushdown_fields(
            1,
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
                .iter()
                .copied(),
        );
        accumulator.add_blocker_code("bridge_error");

        let evidence = accumulator.json();

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["request_count"], 1);
        assert_eq!(evidence["primary_candidate_count"], 2);
        assert_eq!(evidence["shadow_candidate_count"], 1);
        assert_eq!(evidence["matched_candidate_count"], 1);
        assert_eq!(evidence["primary_only_candidate_count"], 1);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "bridge_error",
                "search_candidate_identity_mismatch",
                "search_candidate_mismatch",
                "search_candidate_primary_only"
            ])
        );
    }

    #[test]
    fn search_candidate_shadow_evidence_requires_filter_pushdown_fields() {
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_compare_candidate_ids(&["mem_1"], &["mem_1"]);

        let missing = accumulator.json();

        assert_eq!(missing["ready"], false);
        assert_eq!(missing["filter_pushdown_ready"], false);
        assert!(missing["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_candidate_filter_pushdown_missing"));

        accumulator.record_filter_pushdown_fields(1, ["unit_type"]);
        let partial = accumulator.json();

        assert_eq!(partial["ready"], false);
        assert!(!partial["filter_pushdown"]["missing_required_fields"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(partial["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_candidate_field_pruning_missing"));
    }

    #[test]
    fn graph_read_query_rejects_payload_budget_excess() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-large', title: 'Large read payload'})")
            .unwrap();

        let error = graph
            .read_query_with_options(
                "MATCH (m:Memory {id: 'mem-large'}) RETURN m.title AS title",
                &NowledgeMemReadOptions {
                    max_rows: Some(4),
                    max_estimated_payload_bytes: Some(4),
                },
            )
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("exceeding max_estimated_payload_bytes 4"));
    }

    #[test]
    fn read_transaction_rejects_rows_above_configured_limit() {
        let mut db = Database::new_with_config(DatabaseConfig {
            max_read_result_rows: Some(1),
            ..DatabaseConfig::default()
        });
        db.query("CREATE (:Memory {id: 'mem-limit-1', title: 'Limit one'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-limit-2', title: 'Limit two'})")
            .unwrap();

        let error = db
            .begin_read_transaction()
            .query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap_err();

        assert!(error.to_string().contains("more than 1 rows"));
    }

    #[test]
    fn graph_read_query_rejects_rows_before_returning_oversized_output() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-read-limit-1', title: 'Limit one'})")
            .unwrap();
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-read-limit-2', title: 'Limit two'})")
            .unwrap();

        let error = graph
            .read_query_with_options(
                "MATCH (m:Memory) RETURN m.id AS id",
                &NowledgeMemReadOptions {
                    max_rows: Some(1),
                    max_estimated_payload_bytes: Some(4096),
                },
            )
            .unwrap_err();

        assert!(error.to_string().contains("more than 1 rows"));
    }

    #[test]
    fn graph_read_query_allows_cypher_limit_within_row_budget() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-read-limit-pass-1', title: 'Limit one'})")
            .unwrap();
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-read-limit-pass-2', title: 'Limit two'})")
            .unwrap();

        let read = graph
            .read_query_with_options(
                "MATCH (m:Memory) RETURN m.id AS id LIMIT 1",
                &NowledgeMemReadOptions {
                    max_rows: Some(1),
                    max_estimated_payload_bytes: Some(4096),
                },
            )
            .unwrap();

        assert_eq!(read.output.rows.len(), 1);
        assert_eq!(read.report.row_count, 1);
        assert!(!read.report.row_budget_exceeded);
    }

    #[test]
    fn graph_read_query_reports_blocking_operators() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-sort-profile-1', title: 'B'})")
            .unwrap();
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-sort-profile-2', title: 'A'})")
            .unwrap();

        let read = graph
            .read_query_with_options(
                "MATCH (m:Memory) RETURN m.title AS title ORDER BY title LIMIT 1",
                &NowledgeMemReadOptions {
                    max_rows: Some(4),
                    max_estimated_payload_bytes: Some(4096),
                },
            )
            .unwrap();

        assert_eq!(read.output.rows.len(), 1);
        assert_eq!(read.report.blocking_operator_kinds, vec!["TopNExec"]);
        assert_eq!(read.report.blocking_operator_count, 1);
        assert_eq!(read.report.json()["blocking_operator_kinds"][0], "TopNExec");
    }

    #[test]
    fn embedded_store_read_query_does_not_require_search_projection() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-store-read', title: 'Store read'})")
            .unwrap();
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let read = store
            .read_query_with_options(
                "MATCH (m:Memory {id: 'mem-store-read'}) RETURN m.title AS title",
                &NowledgeMemReadOptions::default(),
            )
            .unwrap();

        assert_eq!(read.output.rows.len(), 1);
        assert_eq!(read.report.row_count, 1);
        assert!(!read.report.row_budget_exceeded);
        assert!(!read.report.payload_budget_exceeded);
    }

    #[test]
    fn embedded_store_library_readiness_fails_closed_without_required_evidence() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions::default());

        assert_eq!(
            readiness["protocol"],
            NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL
        );
        assert_eq!(readiness["present"], true);
        assert_eq!(readiness["ready"], false);
        assert_eq!(readiness["mode"], "shadow_read_only");
        assert_eq!(readiness["production_path"]["ready"], true);
        assert_eq!(readiness["production_path"]["in_process"], true);
        assert_eq!(readiness["production_path"]["cli_required"], false);
        assert_eq!(
            readiness["production_path"]["env_control_plane_required"],
            false
        );
        assert_eq!(
            readiness["production_path"]["spawned_helper_required"],
            false
        );
        assert_eq!(readiness["bounded_read_evidence"]["present"], false);
        assert_eq!(
            readiness["bounded_read_evidence"]["blocker_codes"],
            serde_json::json!(["bounded_read_probe_missing"])
        );
        assert_eq!(
            readiness["search_projection_evidence"]["blocker_codes"],
            serde_json::json!(["search_projection_not_configured"])
        );
        assert_eq!(
            readiness["search_projection_shadow_evidence"]["blocker_codes"],
            serde_json::json!(["primary_search_projection_probe_missing"])
        );
        assert_eq!(
            readiness["search_candidate_shadow_evidence"]["blocker_codes"],
            serde_json::json!(["search_candidate_shadow_evidence_missing"])
        );
        assert_eq!(
            readiness["query_family_evidence"]["blocker_codes"],
            serde_json::json!(["query_family_evidence_missing"])
        );
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "bounded_read_evidence_not_ready"));
        assert_eq!(readiness["readiness_by_area"]["graph"]["ready"], true);
        assert_eq!(readiness["readiness_by_area"]["query"]["ready"], false);
        assert_eq!(
            readiness["readiness_by_area"]["query"]["blocker_codes"],
            serde_json::json!(["bounded_read_probe_missing"])
        );
        assert_eq!(readiness["readiness_by_area"]["storage"]["ready"], false);
        assert_eq!(readiness["readiness_by_area"]["background"]["ready"], false);
        assert_eq!(readiness["production_resource_profile"]["present"], false);
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "production_resource_profile_not_ready"));
        assert_eq!(
            readiness["readiness_by_area"]["background"]["blocker_codes"],
            serde_json::json!(["no_candidates", "no_ranked_work"])
        );
        assert_eq!(
            readiness["readiness_by_area"]["query_family"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["query_family"]["blocker_codes"],
            serde_json::json!(["query_family_evidence_missing"])
        );
        assert_eq!(
            readiness["graph_route_readiness"]["blocker_codes"],
            serde_json::json!(["graph_route_readiness_missing"])
        );
        assert_eq!(
            readiness["readiness_by_area"]["graph_route"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["graph_route"]["blocker_codes"],
            serde_json::json!(["graph_route_readiness_missing"])
        );
        assert_eq!(
            readiness["search_route_ownership"]["blocker_codes"],
            serde_json::json!(["search_route_ownership_missing"])
        );
        assert_eq!(
            readiness["active_search_route_ownership"]["blocker_codes"],
            serde_json::json!(["active_search_route_ownership_missing"])
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_route_ownership"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_route_ownership"]["blocker_codes"],
            serde_json::json!([
                "search_route_ownership_missing",
                "active_search_route_ownership_missing",
                "active_search_route_readiness_missing"
            ])
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_projection"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_projection_shadow"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_candidate_shadow"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_candidate_shadow"]["blocker_codes"],
            serde_json::json!(["search_candidate_shadow_evidence_missing"])
        );
        assert_eq!(
            readiness["workload_fixture_evidence"]["blocker_codes"],
            serde_json::json!(["workload_fixture_evidence_missing"])
        );
        assert_eq!(
            readiness["readiness_by_area"]["workload_fixture"]["ready"],
            false
        );
        assert_eq!(
            readiness["readiness_by_area"]["workload_fixture"]["blocker_codes"],
            serde_json::json!(["workload_fixture_evidence_missing"])
        );
        assert_eq!(readiness["ready_area_count"], 1);
        assert_eq!(readiness["blocked_area_count"], 10);
        assert!(!readiness.to_string().contains("redacted"));
    }

    #[test]
    fn embedded_store_library_readiness_consumes_typed_production_resource_profile() {
        let path = unique_nowledge_mem_test_dir("production_resource_profile");
        let cache_capacity = 1024;
        let config = DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            segment_cache_capacity_bytes: cache_capacity,
            ..DatabaseConfig::default()
        };
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        let mut transaction = db.begin_transaction();
        for id in 0..32 {
            transaction
                .query_with_params(
                    "CREATE (:Memory {id: $id, body: $body})",
                    &BTreeMap::from([
                        ("id".to_string(), Value::Int(id)),
                        ("body".to_string(), Value::String("x".repeat(1024))),
                    ]),
                )
                .unwrap();
        }
        transaction.commit().unwrap();
        db.checkpoint().unwrap();
        drop(db);
        let db = Database::open_with_config(&path, config).unwrap();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let store = NowledgeMemEmbeddedStore::new(graph, None);
        let statement = NowledgeGraphStatement {
            cypher: "MATCH (m:Memory) RETURN m.id AS memory_id".to_string(),
            parameters: BTreeMap::new(),
        };
        let identity = crate::ProductionQualificationIdentity {
            source_revision: "test-revision".to_string(),
            rust_toolchain: "test-toolchain".to_string(),
            target_os: std::env::consts::OS.to_string(),
            target_arch: std::env::consts::ARCH.to_string(),
            enabled_features: Vec::new(),
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "test-config".to_string(),
            deployment_profile: "test-production-replica".to_string(),
            dataset_fingerprint: "test-dataset".to_string(),
            canonical_graph_commit_epoch: store.graph.database().commit_epoch(),
            policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        let profile = store
            .production_resource_profile(
                &statement,
                StorageResourceProfileLimits {
                    min_canonical_artifact_bytes: 4096,
                    max_steady_resident_bytes: u64::MAX,
                    max_peak_resident_bytes: u64::MAX,
                    max_total_page_faults: Some(u64::MAX),
                    max_minor_page_faults: cfg!(unix).then_some(u64::MAX),
                    max_major_page_faults: cfg!(unix).then_some(u64::MAX),
                    max_intermediate_rows: 1024,
                    max_intermediate_payload_bytes: 1024 * 1024,
                    max_output_rows: 64,
                    max_output_payload_bytes: 1024 * 1024,
                    require_fully_streamed: true,
                },
                crate::ProductionEvidenceBinding {
                    identity: identity.clone(),
                    generated_at_unix_seconds: 1,
                },
                identity,
            )
            .unwrap();

        assert!(
            profile.resource_ready,
            "unexpected blockers: {:?}",
            profile.blocker_codes
        );
        assert!(profile.production_ready());
        assert!(profile.after.canonical_artifact_bytes > cache_capacity);
        let readiness = store.library_readiness(&NowledgeMemReadinessOptions {
            production_resource_profile: Some(profile),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(readiness.production_resource_profile["ready"], true);
        assert!(
            readiness.readiness_by_area.storage.ready,
            "unexpected storage blockers: {:?}",
            readiness.readiness_by_area.storage.blocker_codes
        );
        assert!(!readiness
            .blocker_codes
            .iter()
            .any(|code| code == "production_resource_profile_not_ready"));

        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn embedded_store_exposes_typed_library_readiness_report() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let report = store.library_readiness(&NowledgeMemReadinessOptions::default());
        let json = report.json();

        assert_eq!(report.protocol, NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL);
        assert!(report.present);
        assert!(!report.ready);
        assert_eq!(report.mode, NowledgeMemGraphMode::ShadowReadOnly);
        assert!(report.redaction.ready());
        assert!(!report.redaction.query_text_copied);
        assert!(!report.redaction.parameters_copied);
        assert!(!report.redaction.local_paths_copied);
        assert!(report.graph_open);
        assert!(!report.graph_read_only);
        let areas = report.areas();
        assert_eq!(areas.len(), 11);
        assert_eq!(report.ready_area_count, 1);
        assert_eq!(report.blocked_area_count, 10);
        assert!(report.readiness_by_area.graph.ready);
        assert!(!report.readiness_by_area.query.ready);
        assert_eq!(
            report.readiness_by_area.query.blocker_codes,
            vec!["bounded_read_probe_missing".to_string()]
        );
        assert!(!report.readiness_by_area.graph_route.ready);
        assert_eq!(
            report.readiness_by_area.graph_route.blocker_codes,
            vec!["graph_route_readiness_missing".to_string()]
        );
        assert!(!report.readiness_by_area.search_route_ownership.ready);
        assert_eq!(
            report
                .readiness_by_area
                .search_route_ownership
                .blocker_codes,
            vec![
                "search_route_ownership_missing".to_string(),
                "active_search_route_ownership_missing".to_string(),
                "active_search_route_readiness_missing".to_string()
            ]
        );
        assert!(!report.readiness_by_area.workload_fixture.ready);
        assert_eq!(
            report.readiness_by_area.workload_fixture.blocker_codes,
            vec!["workload_fixture_evidence_missing".to_string()]
        );
        let graph_area = areas
            .iter()
            .find(|area| area.name == "graph")
            .expect("graph readiness area");
        let query_area = areas
            .iter()
            .find(|area| area.name == "query")
            .expect("query readiness area");
        assert!(graph_area.ready);
        assert!(!query_area.ready);
        assert_eq!(
            query_area.blocker_codes,
            vec!["bounded_read_probe_missing".to_string()]
        );
        assert!(report
            .blocker_codes
            .iter()
            .any(|code| code == "bounded_read_evidence_not_ready"));
        assert!(report
            .blocker_codes
            .iter()
            .any(|code| code == "graph_route_readiness_not_ready"));
        assert!(report
            .blocker_codes
            .iter()
            .any(|code| code == "search_route_ownership_not_ready"));
        assert!(report
            .blocker_codes
            .iter()
            .any(|code| code == "active_search_route_ownership_not_ready"));
        assert!(report
            .blocker_codes
            .iter()
            .any(|code| code == "active_search_route_readiness_not_ready"));
        assert_eq!(json["ready"], false);
        assert_eq!(json["redaction"]["ready"], true);
        assert_eq!(json["redaction"]["query_text_copied"], false);
        assert_eq!(json["redaction"]["parameters_copied"], false);
        assert_eq!(json["redaction"]["local_paths_copied"], false);
        assert_eq!(
            json["blocker_codes"],
            serde_json::json!(report.blocker_codes)
        );
        assert_eq!(json["areas"].as_array().unwrap().len(), areas.len());
        assert_eq!(
            json["areas"][0],
            serde_json::json!({
                "name": "graph",
                "ready": true,
                "blocker_codes": [],
            })
        );
        assert_eq!(json["graph"]["read_only"], false);
        assert_eq!(
            json["bounded_read_evidence"]["blocker_codes"],
            serde_json::json!(["bounded_read_probe_missing"])
        );
        assert_eq!(
            json["search_route_ownership"]["blocker_codes"],
            serde_json::json!(["search_route_ownership_missing"])
        );
        assert_eq!(
            json["active_search_route_ownership"]["blocker_codes"],
            serde_json::json!(["active_search_route_ownership_missing"])
        );
        assert_eq!(
            json["active_search_route_readiness"]["blocker_codes"],
            serde_json::json!(["active_search_route_readiness_missing"])
        );
    }

    #[test]
    fn embedded_store_library_readiness_requires_active_search_route_read_evidence() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions {
            search_route_ownership: Some(ready_search_route_ownership()),
            active_search_route_ownership: Some(ready_active_search_route_ownership()),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(
            readiness["search_route_ownership"]["ready"],
            serde_json::json!(true)
        );
        assert_eq!(
            readiness["active_search_route_ownership"]["ready"],
            serde_json::json!(true)
        );
        assert_eq!(
            readiness["active_search_route_readiness"]["blocker_codes"],
            serde_json::json!(["active_search_route_readiness_missing"])
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_route_ownership"]["ready"],
            serde_json::json!(false)
        );
        assert_eq!(
            readiness["readiness_by_area"]["search_route_ownership"]["blocker_codes"],
            serde_json::json!(["active_search_route_readiness_missing"])
        );
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "active_search_route_readiness_not_ready"));
    }

    #[test]
    fn embedded_store_library_readiness_accepts_typed_workload_fixture_evidence() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);
        let workload_fixture = nowledge_graph_route_workload_fixture_report(
            NowledgeGraphRouteWorkloadFixtureOptions {
                capture_physical_plan: true,
                include_bounded_expansion_probes: true,
                ..NowledgeGraphRouteWorkloadFixtureOptions::default()
            },
        )
        .unwrap();

        let report = store.library_readiness(&NowledgeMemReadinessOptions {
            workload_fixture_evidence: Some(workload_fixture.clone()),
            ..NowledgeMemReadinessOptions::default()
        });
        let json = report.json();

        assert!(workload_fixture.ready);
        assert!(report.readiness_by_area.workload_fixture.ready);
        assert_eq!(
            report.readiness_by_area.workload_fixture.blocker_codes,
            Vec::<String>::new()
        );
        assert!(report
            .blocker_codes
            .iter()
            .all(|code| code != "workload_fixture_evidence_not_ready"));
        assert_eq!(json["workload_fixture_evidence"]["ready"], true);
        assert_eq!(json["readiness_by_area"]["workload_fixture"]["ready"], true);
        assert_eq!(
            json["workload_fixture_evidence"]["failed_query_count"],
            serde_json::json!(0)
        );
        assert_eq!(
            json["workload_fixture_evidence"]["failed_bounded_expansion_probe_count"],
            serde_json::json!(0)
        );
        assert_eq!(
            json["workload_fixture_evidence"]["failed_search_metadata_probe_count"],
            serde_json::json!(0)
        );
        assert_eq!(
            json["workload_fixture_evidence"]["failed_graph_rag_probe_count"],
            serde_json::json!(0)
        );
        assert_eq!(
            json["workload_fixture_evidence"]["failed_source_projection_probe_count"],
            serde_json::json!(0)
        );
        assert_eq!(
            json["workload_fixture_evidence"]["graph_rag_reports"][0]["ready"],
            true
        );
        assert_eq!(
            json["workload_fixture_evidence"]["graph_rag_reports"][0]
                ["parameter_requirement_count"],
            serde_json::json!(1)
        );
        assert_eq!(
            json["workload_fixture_evidence"]["source_projection_reports"][0]["ready"],
            true
        );
        assert_eq!(
            json["workload_fixture_evidence"]["source_projection_reports"][0]
                ["too_small_batch_failed_closed"],
            true
        );
        assert_eq!(
            json["workload_fixture_evidence"]["source_projection_reports"][0]
                ["indexed_source_document_ready"],
            true
        );
    }

    #[test]
    fn library_readiness_rejects_workload_fixture_without_graph_rag_probe() {
        let workload_fixture = nowledge_graph_route_workload_fixture_report(
            NowledgeGraphRouteWorkloadFixtureOptions::default(),
        )
        .unwrap();
        let mut evidence = workload_fixture.json();
        evidence
            .as_object_mut()
            .unwrap()
            .remove("graph_rag_probe_count");
        evidence
            .as_object_mut()
            .unwrap()
            .remove("failed_graph_rag_probe_count");
        evidence
            .as_object_mut()
            .unwrap()
            .remove("graph_rag_reports");

        let blockers = workload_fixture_readiness_blocker_codes(&evidence);

        assert!(blockers.contains(&"workload_fixture_graph_rag_not_ready".to_string()));
        assert!(blockers.contains(&"workload_fixture_graph_rag_probe_missing".to_string()));
    }

    #[test]
    fn library_readiness_rejects_workload_fixture_without_source_projection_probe() {
        let workload_fixture = nowledge_graph_route_workload_fixture_report(
            NowledgeGraphRouteWorkloadFixtureOptions::default(),
        )
        .unwrap();
        let mut evidence = workload_fixture.json();
        evidence
            .as_object_mut()
            .unwrap()
            .remove("source_projection_probe_count");
        evidence
            .as_object_mut()
            .unwrap()
            .remove("failed_source_projection_probe_count");
        evidence
            .as_object_mut()
            .unwrap()
            .remove("source_projection_reports");

        let blockers = workload_fixture_readiness_blocker_codes(&evidence);

        assert!(blockers.contains(&"workload_fixture_source_projection_not_ready".to_string()));
        assert!(blockers.contains(&"workload_fixture_source_projection_probe_missing".to_string()));
    }

    #[test]
    fn embedded_store_library_readiness_recomputes_search_candidate_shadow_evidence() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions {
            search_candidate_shadow_evidence: Some(serde_json::json!({
                "ready": true,
                "blocker_codes": []
            })),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(
            readiness["readiness_by_area"]["search_candidate_shadow"]["ready"],
            false
        );
        let blocker_codes = readiness["readiness_by_area"]["search_candidate_shadow"]
            ["blocker_codes"]
            .as_array()
            .unwrap();
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_candidate_shadow_protocol_mismatch"));
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_candidate_counts_not_ready"));
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_candidate_shadow_evidence_not_ready"));
    }

    #[test]
    fn embedded_store_library_readiness_recomputes_search_projection_evidence() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions {
            search_projection_evidence: Some(serde_json::json!({
                "ready": true,
                "blocker_codes": []
            })),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(
            readiness["readiness_by_area"]["search_projection"]["ready"],
            false
        );
        let blocker_codes = readiness["readiness_by_area"]["search_projection"]["blocker_codes"]
            .as_array()
            .unwrap();
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_projection_protocol_mismatch"));
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_projection_tables_not_ready"));
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_projection_document_identity_not_ready"));
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_projection_evidence_not_ready"));
    }

    #[test]
    fn embedded_store_library_readiness_recomputes_search_projection_shadow_evidence() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions {
            search_projection_shadow_evidence: Some(serde_json::json!({
                "ready": true,
                "blocker_codes": []
            })),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(
            readiness["readiness_by_area"]["search_projection_shadow"]["ready"],
            false
        );
        let blocker_codes = readiness["readiness_by_area"]["search_projection_shadow"]
            ["blocker_codes"]
            .as_array()
            .unwrap();
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_projection_shadow_protocol_mismatch"));
        assert!(blocker_codes
            .iter()
            .any(|code| code == "search_projection_shadow_document_identity_not_ready"));
        assert!(blocker_codes
            .iter()
            .any(|code| code == SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY));
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "search_projection_shadow_evidence_not_ready"));
    }

    #[test]
    fn embedded_store_exposes_compact_readiness_dashboard() {
        let db = Database::new_with_config(DatabaseConfig {
            slow_query_log_threshold_micros: 0,
            slow_query_log_capacity: 4,
            ..DatabaseConfig::default()
        });
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        store
            .query_with_report(
                "CREATE (:Memory {id: 'dashboard-secret', title: 'Dashboard Secret'})",
            )
            .unwrap();

        let dashboard = store.readiness_dashboard(&NowledgeMemReadinessOptions::default());
        let json = dashboard.json();
        let encoded = json.to_string();

        assert_eq!(
            dashboard.protocol,
            NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL
        );
        assert_eq!(dashboard.mode, NowledgeMemGraphMode::ShadowReadOnly);
        assert!(!dashboard.ready);
        assert_eq!(dashboard.area_count, 12);
        assert_eq!(dashboard.ready_area_count, 3);
        assert_eq!(dashboard.blocked_area_count, 9);
        assert_eq!(
            dashboard.storage_lifecycle_action,
            NowledgeMemStorageLifecycleActionKind::OpenReadOnlyInspect
        );
        assert!(!dashboard.storage_lifecycle_ready);
        assert!(dashboard.slow_query_ready);
        assert_eq!(dashboard.slow_query_record_count, 1);
        assert!(readiness_dashboard_area(&dashboard, "graph").ready);
        assert!(readiness_dashboard_area(&dashboard, "background").ready);
        assert_eq!(
            readiness_dashboard_area(&dashboard, "query").blocker_codes,
            vec!["bounded_read_probe_missing".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "query_family").blocker_codes,
            vec!["query_family_evidence_missing".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "graph_route").blocker_codes,
            vec!["graph_route_readiness_missing".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "storage").blocker_codes,
            vec![
                "storage_recovery_not_ready".to_string(),
                "production_resource_profile_has_blockers".to_string(),
                "production_resource_profile_identity_invalid".to_string(),
                "production_resource_profile_intermediate_payload_bytes_invalid".to_string(),
                "production_resource_profile_intermediate_rows_invalid".to_string(),
                "production_resource_profile_metric_capabilities_invalid".to_string(),
                "production_resource_profile_missing".to_string(),
                "production_resource_profile_not_ready".to_string(),
                "production_resource_profile_output_payload_bytes_invalid".to_string(),
                "production_resource_profile_output_rows_invalid".to_string(),
                "production_resource_profile_peak_resident_bytes_invalid".to_string(),
                "production_resource_profile_resident_growth_missing".to_string(),
                "production_resource_profile_resource_not_ready".to_string(),
                "production_resource_profile_steady_resident_bytes_invalid".to_string(),
                "production_resource_profile_storage_budget_invalid".to_string(),
                "production_resource_profile_streaming_invalid".to_string(),
                "production_resource_profile_total_page_faults_invalid".to_string()
            ]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "search_projection").blocker_codes,
            vec!["search_projection_not_configured".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "search_projection_shadow").blocker_codes,
            vec!["primary_search_projection_probe_missing".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "search_candidate_shadow").blocker_codes,
            vec!["search_candidate_shadow_evidence_missing".to_string()]
        );
        assert_eq!(
            readiness_dashboard_area(&dashboard, "workload_fixture").blocker_codes,
            vec!["workload_fixture_evidence_missing".to_string()]
        );
        assert!(readiness_dashboard_area(&dashboard, "slow_query").ready);
        assert_eq!(json["protocol"], NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL);
        assert_eq!(
            json["storage_lifecycle"]["action"],
            "open_read_only_inspect"
        );
        assert_eq!(json["storage_lifecycle"]["ready"], false);
        assert_eq!(json["redaction"]["query_text_copied"], false);
        assert_eq!(json["redaction"]["parameters_copied"], false);
        assert_eq!(json["redaction"]["local_paths_copied"], false);
        assert!(!encoded.contains("Dashboard Secret"));
        assert!(!encoded.contains("dashboard-secret"));
    }

    #[test]
    fn storage_recovery_report_exposes_typed_readiness_summary() {
        let report =
            NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
                open_timings: Default::default(),
                durable: true,
                recovery_mode: RecoveryMode::Strict,
                max_wal_replay_entries: Some(16),
                max_wal_replay_bytes: Some(4096),
                max_wal_record_bytes: Some(1024),
                checkpoint_epoch: Some(3),
                checkpoint_commit_epoch: Some(11),
                wal_present: true,
                wal_replay_start_lsn: Some(4),
                next_lsn_after_replay: Some(7),
                replayed_wal_entries: 3,
                torn_tail_ignored: false,
                torn_tail_reason: None,
                recovered_commit_epoch: 14,
                ..StorageRecoveryReport::default()
            });
        let json = report.json();

        assert_eq!(report.protocol, "skein-storage-recovery-report");
        assert!(report.present);
        assert!(report.ready);
        assert!(report.durable_recovery_observed);
        assert!(report.checkpoint_boundary_present);
        assert!(report.wal_replay_bounded);
        assert!(report.replay_boundary_consistent);
        assert!(report.torn_tail_clean);
        assert!(report.blocker_codes.is_empty());
        assert_eq!(json["ready"], true);
        assert_eq!(json["readiness"]["wal_replay_bounded"], true);
        assert_eq!(json["readiness"]["replay_boundary_consistent"], true);
        assert_eq!(json["max_wal_replay_entries"], 16);
    }

    #[test]
    fn storage_recovery_report_rejects_inconsistent_open_timings() {
        let report =
            NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
                open_timings: StorageOpenTimings {
                    durable_manifest_open_micros: 2,
                    total_open_micros: 1,
                    ..StorageOpenTimings::default()
                },
                durable: true,
                recovery_mode: RecoveryMode::Strict,
                max_wal_replay_entries: Some(16),
                max_wal_replay_bytes: Some(4096),
                max_wal_record_bytes: Some(1024),
                checkpoint_epoch: Some(3),
                checkpoint_commit_epoch: Some(11),
                wal_present: true,
                wal_replay_start_lsn: Some(4),
                next_lsn_after_replay: Some(7),
                replayed_wal_entries: 3,
                torn_tail_ignored: false,
                torn_tail_reason: None,
                recovered_commit_epoch: 14,
                ..StorageRecoveryReport::default()
            });

        assert!(!report.open_timing_consistent);
        assert!(!report.ready);
        assert_eq!(
            report.blocker_codes,
            vec!["storage_open_timing_inconsistent".to_string()]
        );
        assert_eq!(report.json()["readiness"]["open_timing_consistent"], false);
    }

    #[test]
    fn storage_lifecycle_decision_reports_ready_for_clean_recovery() {
        let recovery =
            NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
                open_timings: Default::default(),
                durable: true,
                recovery_mode: RecoveryMode::Strict,
                max_wal_replay_entries: Some(16),
                max_wal_replay_bytes: Some(4096),
                max_wal_record_bytes: Some(1024),
                checkpoint_epoch: Some(3),
                checkpoint_commit_epoch: Some(11),
                wal_present: true,
                wal_replay_start_lsn: Some(4),
                next_lsn_after_replay: Some(7),
                replayed_wal_entries: 3,
                torn_tail_ignored: false,
                torn_tail_reason: None,
                recovered_commit_epoch: 14,
                ..StorageRecoveryReport::default()
            });

        let decision = NowledgeMemStorageLifecycleDecision::from_storage_recovery(recovery);
        let json = decision.json();

        assert_eq!(
            decision.protocol,
            NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL
        );
        assert_eq!(
            decision.action,
            NowledgeMemStorageLifecycleActionKind::Ready
        );
        assert!(decision.ready_for_mem_lifecycle);
        assert!(decision.storage_recovery_ready);
        assert!(!decision.checkpoint_required);
        assert!(!decision.repair_required);
        assert!(!decision.quarantine_required);
        assert!(!decision.read_only_inspection_required);
        assert!(decision.blocker_codes.is_empty());
        assert_eq!(json["action"], "ready");
        assert_eq!(json["ready_for_mem_lifecycle"], true);
    }

    #[test]
    fn storage_lifecycle_decision_recommends_wal_tail_repair() {
        let recovery =
            NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
                open_timings: Default::default(),
                durable: true,
                recovery_mode: RecoveryMode::DoctorRepairTornTail,
                max_wal_replay_entries: Some(16),
                max_wal_replay_bytes: Some(4096),
                max_wal_record_bytes: Some(1024),
                checkpoint_epoch: Some(3),
                checkpoint_commit_epoch: Some(11),
                wal_present: true,
                wal_replay_start_lsn: Some(4),
                next_lsn_after_replay: Some(7),
                replayed_wal_entries: 3,
                torn_tail_ignored: true,
                torn_tail_reason: Some("partial wal entry".to_string()),
                recovered_commit_epoch: 14,
                ..StorageRecoveryReport::default()
            });

        let decision = NowledgeMemStorageLifecycleDecision::from_storage_recovery(recovery);

        assert_eq!(
            decision.action,
            NowledgeMemStorageLifecycleActionKind::RepairWalTail
        );
        assert!(!decision.ready_for_mem_lifecycle);
        assert!(decision.repair_required);
        assert_eq!(
            decision.blocker_codes,
            vec![
                "torn_tail_observed".to_string(),
                "wal_tail_repair_required".to_string()
            ]
        );
        assert_eq!(decision.json()["action"], "repair_wal_tail");
    }

    #[test]
    fn storage_lifecycle_decision_fails_closed_for_in_memory_storage() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let decision = store.storage_lifecycle_decision();
        let json = store.storage_lifecycle_decision_json();

        assert_eq!(
            decision.action,
            NowledgeMemStorageLifecycleActionKind::OpenReadOnlyInspect
        );
        assert!(!decision.ready_for_mem_lifecycle);
        assert!(decision.read_only_inspection_required);
        assert!(decision
            .blocker_codes
            .contains(&"durable_recovery_not_observed".to_string()));
        assert!(decision
            .blocker_codes
            .contains(&"storage_not_durable".to_string()));
        assert_eq!(json["action"], "open_read_only_inspect");
    }

    #[test]
    fn storage_lifecycle_decision_recommends_checkpoint_for_missing_boundary() {
        let recovery =
            NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
                open_timings: Default::default(),
                durable: true,
                recovery_mode: RecoveryMode::Strict,
                max_wal_replay_entries: Some(16),
                max_wal_replay_bytes: Some(4096),
                max_wal_record_bytes: Some(1024),
                checkpoint_epoch: None,
                checkpoint_commit_epoch: None,
                wal_present: true,
                wal_replay_start_lsn: Some(4),
                next_lsn_after_replay: Some(4),
                replayed_wal_entries: 0,
                torn_tail_ignored: false,
                torn_tail_reason: None,
                recovered_commit_epoch: 4,
                ..StorageRecoveryReport::default()
            });

        let decision = NowledgeMemStorageLifecycleDecision::from_storage_recovery(recovery);

        assert_eq!(
            decision.action,
            NowledgeMemStorageLifecycleActionKind::RunCheckpoint
        );
        assert!(!decision.ready_for_mem_lifecycle);
        assert!(decision.checkpoint_required);
        assert_eq!(
            decision.blocker_codes,
            vec![
                "checkpoint_boundary_missing".to_string(),
                "replay_boundary_inconsistent".to_string(),
                "checkpoint_required".to_string()
            ]
        );
    }

    #[test]
    fn storage_recovery_report_recomputes_typed_readiness_from_raw_fields() {
        let report =
            NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
                open_timings: Default::default(),
                durable: true,
                recovery_mode: RecoveryMode::Strict,
                max_wal_replay_entries: Some(2),
                max_wal_replay_bytes: Some(4096),
                max_wal_record_bytes: Some(1024),
                checkpoint_epoch: Some(3),
                checkpoint_commit_epoch: None,
                wal_present: true,
                wal_replay_start_lsn: Some(4),
                next_lsn_after_replay: Some(8),
                replayed_wal_entries: 3,
                torn_tail_ignored: false,
                torn_tail_reason: Some("partial wal entry".to_string()),
                recovered_commit_epoch: 13,
                ..StorageRecoveryReport::default()
            });
        let json = report.json();

        assert!(report.durable_recovery_observed);
        assert!(!report.ready);
        assert!(!report.checkpoint_boundary_present);
        assert!(!report.wal_replay_bounded);
        assert!(!report.replay_boundary_consistent);
        assert!(!report.torn_tail_clean);
        assert_eq!(
            report.blocker_codes,
            vec![
                "checkpoint_boundary_missing".to_string(),
                "wal_replay_unbounded".to_string(),
                "replay_boundary_inconsistent".to_string(),
                "torn_tail_observed".to_string()
            ]
        );
        assert_eq!(json["readiness"]["checkpoint_boundary_present"], false);
        assert_eq!(json["readiness"]["wal_replay_bounded"], false);
        assert_eq!(json["readiness"]["replay_boundary_consistent"], false);
        assert_eq!(json["readiness"]["torn_tail_clean"], false);
    }

    #[test]
    fn storage_recovery_report_rejects_inconsistent_replay_boundary() {
        let report =
            NowledgeMemStorageRecoveryReport::from_storage_report(&StorageRecoveryReport {
                open_timings: Default::default(),
                durable: true,
                recovery_mode: RecoveryMode::Strict,
                max_wal_replay_entries: Some(16),
                max_wal_replay_bytes: Some(4096),
                max_wal_record_bytes: Some(1024),
                checkpoint_epoch: Some(3),
                checkpoint_commit_epoch: Some(11),
                wal_present: true,
                wal_replay_start_lsn: Some(4),
                next_lsn_after_replay: Some(7),
                replayed_wal_entries: 3,
                torn_tail_ignored: false,
                torn_tail_reason: None,
                recovered_commit_epoch: 13,
                ..StorageRecoveryReport::default()
            });
        let json = report.json();

        assert!(report.durable_recovery_observed);
        assert!(report.checkpoint_boundary_present);
        assert!(report.wal_replay_bounded);
        assert!(!report.replay_boundary_consistent);
        assert!(report.torn_tail_clean);
        assert!(!report.ready);
        assert_eq!(
            report.blocker_codes,
            vec!["replay_boundary_inconsistent".to_string()]
        );
        assert_eq!(json["readiness"]["replay_boundary_consistent"], false);
    }

    #[test]
    fn embedded_store_exposes_storage_recovery_report_through_library_api() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let report = store.storage_recovery_report();
        let json = store.storage_recovery_report_json();

        assert_eq!(report.protocol, "skein-storage-recovery-report");
        assert!(report.present);
        assert!(!report.ready);
        assert_eq!(
            report.blocker_codes,
            vec![
                "durable_recovery_not_observed".to_string(),
                "checkpoint_boundary_missing".to_string(),
                "wal_replay_unbounded".to_string(),
                "replay_boundary_inconsistent".to_string()
            ]
        );
        assert_eq!(json["ready"], false);
        assert_eq!(
            json["blocker_codes"],
            serde_json::json!(report.blocker_codes)
        );
    }

    #[test]
    fn embedded_store_library_readiness_runs_bounded_probe() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-readiness', title: 'Readiness'})")
            .unwrap();
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions {
            bounded_read_probe: Some(NowledgeGraphStatement {
                cypher: "MATCH (m:Memory {id: 'mem-readiness'}) RETURN m.title AS title"
                    .to_string(),
                parameters: BTreeMap::new(),
            }),
            covered_routes: full_bounded_read_routes(),
            graph_route_readiness: Some(ready_route_readiness_summary()),
            search_route_ownership: Some(ready_search_route_ownership()),
            active_search_route_ownership: Some(ready_active_search_route_ownership()),
            active_search_route_readiness: Some(ready_active_search_route_readiness()),
            replacement_readiness_by_query_family: Some(ready_query_family_replacement()),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(readiness["bounded_read_evidence"]["present"], true);
        assert_eq!(readiness["bounded_read_evidence"]["ready"], true);
        assert_eq!(
            readiness["bounded_read_evidence"]["mode"],
            "shadow_read_only"
        );
        assert_eq!(readiness["bounded_read_evidence"]["execution_row_cap"], 513);
        assert!(
            readiness["bounded_read_evidence"]["estimated_payload_bytes"]
                .as_u64()
                .is_some()
        );
        assert_eq!(
            readiness["bounded_read_evidence"]["max_estimated_payload_bytes"],
            serde_json::json!(4 * 1024 * 1024)
        );
        assert_eq!(
            readiness["bounded_read_evidence"]["payload_budget_exceeded"],
            false
        );
        assert_eq!(
            readiness["bounded_read_evidence"]["missing_covered_routes"],
            serde_json::json!([])
        );
        assert_eq!(
            readiness["background_maintenance"]["total_candidates"]
                .as_u64()
                .unwrap_or_default(),
            readiness["background_maintenance"]["ranked"]
                .as_array()
                .unwrap()
                .len() as u64
        );
        assert_eq!(readiness["readiness_by_area"]["query"]["ready"], true);
        assert_eq!(readiness["readiness_by_area"]["background"]["ready"], true);
        assert_eq!(
            readiness["readiness_by_area"]["background"]["blocker_codes"],
            serde_json::json!([])
        );
        assert_eq!(
            readiness["readiness_by_area"]["query_family"]["ready"],
            true
        );
        assert_eq!(readiness["readiness_by_area"]["graph_route"]["ready"], true);
        assert_eq!(
            readiness["readiness_by_area"]["search_route_ownership"]["ready"],
            true
        );
        assert_eq!(
            readiness["search_route_ownership"]["lancedb_route_count"],
            0
        );
        assert_eq!(
            readiness["active_search_route_ownership"]["lancedb_route_count"],
            0
        );
        assert_eq!(
            readiness["active_search_route_readiness"]["lancedb_handle_required_route_count"],
            0
        );
        assert_eq!(
            readiness["graph_route_readiness"]["primary_ready_route_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(
            readiness["query_family_evidence"]["missing_required_query_families"],
            serde_json::json!([])
        );
        assert_eq!(
            readiness["query_family_evidence"]["min_replacement_readiness_per_million"],
            1_000_000
        );
    }

    #[test]
    fn embedded_store_library_readiness_feeds_final_preflight_without_cli() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        graph
            .database_mut()
            .query("CREATE (:Memory {id: 'mem-preflight', title: 'Preflight'})")
            .unwrap();
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let library = store.library_readiness(&NowledgeMemReadinessOptions {
            bounded_read_probe: Some(NowledgeGraphStatement {
                cypher: "MATCH (m:Memory {id: 'mem-preflight'}) RETURN m.title AS title"
                    .to_string(),
                parameters: BTreeMap::new(),
            }),
            covered_routes: full_bounded_read_routes(),
            graph_route_readiness: Some(ready_route_readiness_summary()),
            search_route_ownership: Some(ready_search_route_ownership()),
            active_search_route_ownership: Some(ready_active_search_route_ownership()),
            active_search_route_readiness: Some(ready_active_search_route_readiness()),
            replacement_readiness_by_query_family: Some(ready_query_family_replacement()),
            ..NowledgeMemReadinessOptions::default()
        });
        let mut library_json = library.json();
        library_json["open_report"] = serde_json::json!({
            "protocol": NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL,
            "mode": "shadow_read_only",
            "graph_configured": true,
            "search_projection_configured": false,
            "compressed_vector_search_mode": "disabled",
            "graph_opened": true,
            "search_projection_opened": false,
        });

        let bundle = serde_json::json!({
            "library_readiness": library_json,
        });
        let preflight = nowledge_mem_final_cutover_preflight(&bundle);

        assert!(library.readiness_by_area.query.ready);
        assert!(library.readiness_by_area.graph_route.ready);
        assert!(library.readiness_by_area.search_route_ownership.ready);
        assert!(library.readiness_by_area.query_family.ready);
        assert!(library.readiness_by_area.background.ready);
        assert!(!library.readiness_by_area.storage.ready);
        assert!(!library.readiness_by_area.search_projection.ready);
        assert!(!preflight.production_cutover_ready);
        assert!(!preflight.library_only_ready);
        assert!(preflight
            .failed_checks
            .iter()
            .any(|check| check == "library_readiness"));
        assert!(preflight
            .failed_evidence_fields
            .iter()
            .any(|field| field == "library_readiness.open_report.search_projection_opened"));
        assert!(preflight
            .next_action_names
            .iter()
            .any(|action| action == "attach_library_readiness_evidence"));
    }

    #[test]
    fn embedded_store_library_readiness_recomputes_bounded_read_payload_budget() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let readiness = store.library_readiness_json(&NowledgeMemReadinessOptions {
            bounded_read_evidence: Some(serde_json::json!({
                "protocol": NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
                "present": true,
                "ready": true,
                "mode": "shadow_read_only",
                "max_rows": 512,
                "execution_row_cap": 513,
                "row_limit_enforced_before_output": true,
                "operator_row_cap_enabled": true,
                "streaming": false,
                "blocking_operator_count": 0,
                "row_budget_exceeded": false,
                "payload_budget_exceeded": false,
                "missing_covered_routes": [],
                "route_primary_ready": true,
                "route_query_plan_evidence_ready": true,
                "route_query_profile_evidence_ready": true,
                "route_query_api_behavior_evidence_ready": true,
                "relationship_property_pruning_required_count": 0,
                "relationship_property_pruning_report_count": 0,
                "route_relationship_property_pruning_evidence_ready": true,
                "blocker_codes": [],
            })),
            ..NowledgeMemReadinessOptions::default()
        });

        assert_eq!(readiness["bounded_read_evidence"]["ready"], true);
        assert_eq!(readiness["readiness_by_area"]["query"]["ready"], false);
        assert!(readiness["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "bounded_read_evidence_not_ready"));
        let query_blockers = readiness["readiness_by_area"]["query"]["blocker_codes"]
            .as_array()
            .unwrap();
        assert!(query_blockers
            .iter()
            .any(|code| code == "bounded_read_estimated_payload_bytes_missing"));
        assert!(query_blockers
            .iter()
            .any(|code| code == "bounded_read_max_estimated_payload_bytes_missing"));
    }

    #[test]
    fn embedded_store_exposes_search_projection_probe() {
        let index = SearchIndex::default();
        let projection = NowledgeMemSearchProjection::from_index(index);
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let probe = store
            .search_projection()
            .unwrap()
            .probe_json(SearchProjectionProbeOptions::default());

        assert_eq!(probe["protocol"], "skein-nowledge-search-projection-probe");
    }

    #[test]
    fn embedded_store_handle_exposes_sampled_vector_recall_validation() {
        let index = persisted_nowledge_projection_evidence_index("handle_recall_validation");
        let projection = NowledgeMemSearchProjection::from_index(index);
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ));

        let report = handle
            .validate_sampled_vector_recall(VectorRecallValidationOptions {
                max_samples: 2,
                top_k: 1,
                candidate_limit: 1,
                minimum_recall_per_million: 0,
                metadata_filters: BTreeMap::new(),
            })
            .unwrap();

        assert!(report.ready, "{:?}", report.blocker_codes);
        assert_eq!(report.protocol, VECTOR_RECALL_VALIDATION_PROTOCOL);
        assert_eq!(report.executed_sample_count, 2);
        assert_eq!(
            report.approximate_backend,
            "skein_turboquant_candidate_projection"
        );
        assert!(report.validates_required_approximate_backend());
    }

    #[test]
    fn embedded_store_recall_validation_requires_search_projection() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let error = store
            .validate_sampled_vector_recall(VectorRecallValidationOptions::default())
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "storage error: nowledge mem search projection is not configured"
        );
    }

    #[test]
    fn embedded_store_exposes_search_projection_replacement_evidence() {
        let index = persisted_nowledge_projection_evidence_index("replacement_evidence");
        let projection = NowledgeMemSearchProjection::from_index(index);
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let evidence = store
            .search_projection_evidence_json(SearchProjectionProbeOptions {
                active_embedding_model: Some("bge-m3".to_string()),
                active_embedding_dimension: Some(8),
            })
            .unwrap();

        assert_eq!(
            evidence["protocol"],
            "skein-nowledge-search-projection-evidence"
        );
        assert_eq!(evidence["ready"], true, "evidence={evidence:#}");
        assert_eq!(evidence["covered_table_count"], 6);
        assert_eq!(evidence["required_table_count"], 6);
        assert_eq!(evidence["source_chunk_ready"], true);
        assert_eq!(evidence["incremental_update_ready"], true);
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn embedded_store_exposes_typed_search_projection_replacement_evidence() {
        let index = persisted_nowledge_projection_evidence_index("typed_replacement_evidence");
        let projection = NowledgeMemSearchProjection::from_index(index);
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let report = store
            .search_projection_evidence_report(SearchProjectionProbeOptions {
                active_embedding_model: Some("bge-m3".to_string()),
                active_embedding_dimension: Some(8),
            })
            .unwrap();

        assert_eq!(report.protocol, "skein-nowledge-search-projection-evidence");
        assert!(report.ready);
        assert!(report.compressed_vector_projection_ready);
        assert!(report.derived_projection);
        assert!(report.all_tables_covered);
        assert_eq!(report.covered_table_count, 6);
        assert_eq!(report.required_table_count, 6);
        assert!(report.source_chunk_ready);
        assert!(report.incremental_update_ready);
        assert_eq!(report.json()["covered_table_count"], 6);
    }

    #[test]
    fn embedded_store_exposes_search_projection_shadow_evidence() {
        let index = persisted_nowledge_projection_evidence_index("shadow_evidence");
        let projection = NowledgeMemSearchProjection::from_index(index);
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let probe_options = SearchProjectionProbeOptions {
            active_embedding_model: Some("bge-m3".to_string()),
            active_embedding_dimension: Some(8),
        };
        let primary_probe = store
            .search_projection_probe_json(probe_options.clone())
            .unwrap();

        let evidence = store
            .search_projection_shadow_evidence_json(&primary_probe, probe_options)
            .unwrap();

        assert_eq!(
            evidence["protocol"],
            "skein-nowledge-search-projection-shadow-evidence"
        );
        assert_eq!(evidence["ready"], true, "evidence={evidence:#}");
        assert_eq!(evidence["primary_ready"], true);
        assert_eq!(evidence["shadow_ready"], true);
        assert_eq!(evidence["document_count_parity"], true);
        assert_eq!(evidence["table_parity"]["ready"], true);
        assert_eq!(evidence["embedding_identity_parity"], true);
        assert_eq!(evidence["incremental_watermark_parity"], true);
        assert_eq!(
            evidence["pushdown_evidence"]["shadow_segment_document_pruning_ready"],
            true
        );
        assert_eq!(
            evidence["pushdown_evidence"]["shadow_segment_pruning_candidate_document_count"],
            6
        );
        assert_eq!(
            evidence["pushdown_evidence"]["shadow_segment_pruned_document_count"],
            4
        );
        assert_eq!(
            evidence["pushdown_evidence"]["shadow_segment_scanned_document_count"],
            2
        );
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    fn embedded_store_search_projection_evidence_requires_projection() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let error = store
            .search_projection_evidence_json(SearchProjectionProbeOptions::default())
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "storage error: nowledge mem search projection is not configured"
        );
    }

    #[test]
    fn embedded_store_search_projection_shadow_evidence_requires_projection() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let error = store
            .search_projection_shadow_evidence_json(
                &serde_json::json!({ "engine": "lancedb" }),
                SearchProjectionProbeOptions::default(),
            )
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "storage error: nowledge mem search projection is not configured"
        );
    }

    #[test]
    fn open_options_report_is_sanitized() {
        let options = NowledgeMemOpenOptions::with_search_projection(
            "redacted_graph_path",
            "redacted_search_path",
            NowledgeMemGraphMode::ShadowReadOnly,
        );

        let report = options.sanitized_report().json();

        assert_eq!(report["protocol"], NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL);
        assert_eq!(report["mode"], "shadow_read_only");
        assert_eq!(report["graph_configured"], true);
        assert_eq!(report["search_projection_configured"], true);
        assert_eq!(report["compressed_vector_search_mode"], "disabled");
        assert!(report.get("graph_path").is_none());
        assert!(report.get("search_projection_path").is_none());
        assert!(!report.to_string().contains("redacted_graph_path"));
        assert!(!report.to_string().contains("redacted_search_path"));
    }

    #[test]
    fn open_options_diagnostics_include_local_paths_only_with_debug_flag() {
        let options = NowledgeMemOpenOptions::with_search_projection(
            "debug_graph_path",
            "debug_search_path",
            NowledgeMemGraphMode::ShadowReadOnly,
        );

        let redacted = options.diagnostic_report_json(NowledgeMemOpenDiagnosticOptions::default());
        assert_eq!(redacted["debug_local_paths_included"], false);
        assert_eq!(redacted["local_paths_redacted"], true);
        assert!(redacted.get("graph_path").is_none());
        assert!(redacted.get("search_projection_path").is_none());
        assert!(!redacted.to_string().contains("debug_graph_path"));
        assert!(!redacted.to_string().contains("debug_search_path"));

        let debug = options.diagnostic_report_json(NowledgeMemOpenDiagnosticOptions {
            include_local_paths: true,
        });
        assert_eq!(debug["debug_local_paths_included"], true);
        assert_eq!(debug["local_paths_redacted"], false);
        assert_eq!(debug["graph_path"], "debug_graph_path");
        assert_eq!(debug["search_projection_path"], "debug_search_path");
    }

    #[test]
    fn open_options_report_gates_advanced_compressed_vector_search_mode() {
        let options = NowledgeMemOpenOptions::with_search_projection(
            "redacted_graph_path",
            "redacted_search_path",
            NowledgeMemGraphMode::ShadowReadOnly,
        )
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred);

        let report = options.sanitized_report().json();

        assert_eq!(report["compressed_vector_search_mode"], "disabled");
        assert_eq!(
            report["requested_compressed_vector_search_mode"],
            "preferred"
        );
        assert_eq!(report["retrieval_projection_advisor"]["ready"], false);
        assert_eq!(
            report["retrieval_projection_advisor_blocker_codes"],
            serde_json::json!([
                "retrieval_projection_recall_evidence_missing",
                "retrieval_projection_parity_evidence_missing",
                "retrieval_projection_segment_not_advised"
            ])
        );
        assert!(!report.to_string().contains("redacted_graph_path"));
        assert!(!report.to_string().contains("redacted_search_path"));
    }

    #[test]
    fn open_options_report_allows_compressed_vector_search_with_advisor_evidence() {
        let recall_report = ready_vector_recall_report();
        let options = NowledgeMemOpenOptions::with_search_projection(
            "redacted_graph_path",
            "redacted_search_path",
            NowledgeMemGraphMode::ShadowReadOnly,
        )
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred)
        .with_retrieval_projection_advisor(
            NowledgeMemRetrievalProjectionAdvisor::cold_local_with_recall_parity(&recall_report),
        );

        let report = options.sanitized_report().json();

        assert_eq!(report["compressed_vector_search_mode"], "preferred");
        assert_eq!(
            report["requested_compressed_vector_search_mode"],
            "preferred"
        );
        assert_eq!(
            report["adaptive_vector_backend_policy"]["flat_scan_memory_budget_bytes"],
            16 * 1024 * 1024
        );
        assert_eq!(report["retrieval_projection_advisor"]["ready"], true);
        assert_eq!(
            report["retrieval_projection_advisor_blocker_codes"],
            serde_json::json!([])
        );
    }

    #[test]
    fn open_options_report_rejects_inconsistent_recall_evidence() {
        let mut recall_report = ready_vector_recall_report();
        recall_report.recall_at_k_per_million = 0;
        let options = NowledgeMemOpenOptions::with_search_projection(
            "redacted_graph_path",
            "redacted_search_path",
            NowledgeMemGraphMode::ShadowReadOnly,
        )
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Required)
        .with_retrieval_projection_advisor(
            NowledgeMemRetrievalProjectionAdvisor::cold_local_with_recall_parity(&recall_report),
        );

        let report = options.sanitized_report().json();

        assert_eq!(report["compressed_vector_search_mode"], "disabled");
        assert_eq!(
            report["retrieval_projection_advisor_blocker_codes"],
            serde_json::json!(["retrieval_projection_recall_evidence_not_ready"])
        );
    }

    #[test]
    fn qualified_out_of_core_open_recomputes_production_evidence() {
        let root = unique_nowledge_mem_test_dir("qualified_out_of_core_open");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        let graph_commit_epoch = {
            let mut db = Database::open(&graph_path).unwrap();
            db.query("CREATE (:Memory {id: 'qualified-open'})").unwrap();
            db.commit_epoch()
        };
        {
            let mut index = SearchIndex::open(&search_path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "test-embedding".to_string(),
                    version: Some("v1".to_string()),
                    dimension: 2,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "qualified-open".to_string(),
                        title: "Qualified open".to_string(),
                        body: "Production evidence is recomputed".to_string(),
                        embedding: Some(vec![1.0, 0.0]),
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(graph_commit_epoch),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemOutOfCoreSearchProjection::open(&search_path).unwrap();
        let projection_identity = projection.reader().production_qualification_identity();
        drop(projection);
        let expected_identity = production_identity(graph_commit_epoch);
        let qualification = SearchLexicalProductionQualificationReport::evaluate_for_production(
            projection_identity,
            ProductionEvidenceBinding {
                identity: expected_identity.clone(),
                generated_at_unix_seconds: 1,
            },
            expected_identity.clone(),
            SearchTopKScoreParity {
                text: true,
                vector: true,
                hybrid: true,
            },
            SearchLexicalFeasibilityCoverage::default(),
            SearchLexicalFeasibilityMetrics::default(),
        );
        let out_of_core_config = SearchOutOfCoreConfig {
            spill_directory: root.join("spill"),
            ..SearchOutOfCoreConfig::default()
        };
        let options = NowledgeMemOpenOptions::with_qualified_out_of_core_search_projection(
            &graph_path,
            &search_path,
            NowledgeMemGraphMode::ShadowReadOnly,
            NowledgeMemQualifiedOutOfCoreSearchOptions {
                config: out_of_core_config,
                qualification,
                expected_identity,
            },
        );
        let report = options.sanitized_report().json();
        assert_eq!(report["search_projection_role"], "qualified_out_of_core");
        assert_eq!(report["search_production_qualification_bound"], false);

        let error = NowledgeMemEmbeddedStore::open_with_options(options).unwrap_err();
        assert!(error.to_string().contains("dataset_too_small"));
        assert!(error.to_string().contains("workload_coverage_incomplete"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_opens_from_options_with_sanitized_report() {
        let root = unique_nowledge_mem_test_dir("open_options");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        let options = NowledgeMemOpenOptions::with_search_projection(
            graph_path,
            search_path,
            NowledgeMemGraphMode::WritableCutover,
        );

        let (mut store, report) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
        store
            .graph_mut()
            .query("CREATE (:Memory {id: 'mem-open', title: 'Open options'})")
            .unwrap();

        assert_eq!(report.protocol, NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL);
        assert_eq!(report.mode, NowledgeMemGraphMode::WritableCutover);
        assert_eq!(
            report.compressed_vector_search_mode,
            CompressedVectorSearchMode::Disabled
        );
        assert!(report.graph_opened);
        assert!(report.search_projection_opened);
        assert!(store.search_projection().is_some());
        assert_eq!(
            store
                .graph_mut()
                .query("MATCH (m:Memory {id: 'mem-open'}) RETURN m.title AS title")
                .unwrap()
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn embedded_store_applies_incremental_graph_search_projection_delta() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'new', title: 'Incremental facade', content: 'Graph changes feed search projection'})")
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let request = store
            .build_search_projection_graph_delta_request_from_freshness(Some(4))
            .unwrap()
            .expect("expected graph delta request");
        let plan = store
            .search_projection_graph_delta_background_work_plan(
                &request,
                BackgroundWorkHint::default(),
            )
            .expect("expected background work plan");

        let report = store.apply_search_projection_graph_delta(request).unwrap();

        assert_eq!(plan.request.class, WorkClass::Projection);
        assert_eq!(report.upserted_documents, 1);
        assert_eq!(
            store
                .search_projection()
                .unwrap()
                .index()
                .document("memory:new")
                .unwrap()
                .title,
            "Incremental facade"
        );
    }

    #[test]
    fn initial_import_projection_uses_local_cursor_and_preserves_legacy_provenance() {
        let root = unique_nowledge_mem_test_dir("initial_import_projection_local_cursor");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        let options = NowledgeMemOpenOptions::with_search_projection(
            graph_path,
            search_path,
            NowledgeMemGraphMode::WritableCutover,
        );
        let (mut store, _) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
        store
            .graph_mut()
            .query("CREATE (:Memory {id: 'm1', title: 'Imported'})")
            .unwrap();
        let local_graph_commit_epoch = store.graph().database().commit_epoch();

        let report = store
            .apply_initial_import_projection_delta_and_checkpoint(
                SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "m1".to_string(),
                        title: "Imported".to_string(),
                        body: "legacy import".to_string(),
                        embedding: None,
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    deletes: Vec::new(),
                    max_operations: Some(1),
                    source_graph_commit_epoch: Some(77),
                },
                77,
            )
            .unwrap();

        assert_eq!(
            report.source_graph_commit_epoch_after,
            Some(local_graph_commit_epoch)
        );
        let freshness = store
            .search_projection()
            .unwrap()
            .index()
            .projection_freshness();
        assert_eq!(freshness.import_source_graph_commit_epoch, Some(77));
        assert_eq!(
            freshness.source_graph_commit_epoch,
            Some(local_graph_commit_epoch)
        );
        assert_eq!(
            freshness.durable_source_graph_commit_epoch,
            Some(local_graph_commit_epoch)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_handle_runs_durable_projection_catch_up() {
        let root = unique_nowledge_mem_test_dir("embedded_projection_catch_up");
        let search_path = root.join("search");
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'm1', title: 'First'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'm2', title: 'Second'})")
            .unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ));

        let report = handle.catch_up_search_projection(1, 4).unwrap();

        assert!(report.complete);
        assert_eq!(report.end_applied_epoch, report.end_durable_epoch);
        drop(handle);
        let reopened = SearchIndex::open(&search_path).unwrap();
        assert!(reopened.document("memory:m1").is_some());
        assert!(reopened.document("memory:m2").is_some());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_handle_runs_unified_relational_projection_catch_up() {
        let root = unique_nowledge_mem_test_dir("embedded_relational_projection_catch_up");
        let search_path = root.join("search");
        let mut db = Database::new();
        db.query_sql("CREATE TABLE thread_messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
            .unwrap();
        db.query_sql(
            "INSERT INTO thread_messages (id, body) VALUES (1, 'Embedded relational document')",
        )
        .unwrap();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ));

        let report = handle
            .catch_up_search_projection_with_relational(1, 1, |snapshot, batch| {
                assert_eq!(batch.relational_primary_key_changes().len(), 1);
                let output = snapshot.query_sql_with_params(
                    "SELECT body FROM thread_messages WHERE id = $1",
                    &[Value::Int(1)],
                )?;
                let body = match output.rows.as_slice() {
                    [row] => match row.get("body") {
                        Some(Value::String(body)) => body.clone(),
                        value => {
                            return Err(SkeinError::Execution(format!(
                                "thread_messages body expected STRING, got {value:?}"
                            )));
                        }
                    },
                    rows => {
                        return Err(SkeinError::Execution(format!(
                            "thread_messages hydration returned {} rows",
                            rows.len()
                        )));
                    }
                };
                Ok(SearchProjectionRelationalDelta {
                    delta: SearchProjectionDelta {
                        upserts: vec![SearchProjectionRow {
                            kind: SearchProjectionKind::Message,
                            external_id: "1".to_string(),
                            title: String::new(),
                            body,
                            embedding: None,
                            source_id: None,
                            metadata: BTreeMap::new(),
                        }],
                        ..SearchProjectionDelta::default()
                    },
                    processed_primary_key_count: 1,
                })
            })
            .unwrap();

        assert!(report.complete);
        assert_eq!(report.applied_operation_count, 1);
        assert_eq!(report.end_applied_epoch, report.end_durable_epoch);
        drop(handle);
        let reopened = SearchIndex::open(&search_path).unwrap();
        assert_eq!(
            reopened
                .document("message:1")
                .map(|document| document.content.as_str()),
            Some("Embedded relational document")
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_handle_runs_unified_graph_batch_hydrator() {
        let root = unique_nowledge_mem_test_dir("embedded_graph_batch_projection_catch_up");
        let search_path = root.join("search");
        let mut db = Database::new();
        db.query("CREATE (:Thread {id: 'thread-a', title: 'Updated title'})")
            .unwrap();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ));
        handle.catch_up_search_projection(1, 1).unwrap();
        handle
            .with_transaction(|transaction| {
                transaction
                    .query("MATCH (t:Thread {id: 'thread-a'}) SET t.title = 'Current title'")?;
                Ok(())
            })
            .unwrap();
        let mut observed_graph_only_batch = false;

        let report = handle
            .catch_up_search_projection_with_batch_hydrator(1, 2, 1, |_snapshot, batch| {
                observed_graph_only_batch = !batch.has_relational_changes()
                    && batch.graph_delta().upsert_node_ids.len() == 1;
                Ok(SearchProjectionRelationalDelta {
                    delta: SearchProjectionDelta {
                        upserts: vec![SearchProjectionRow {
                            kind: SearchProjectionKind::Message,
                            external_id: "derived".to_string(),
                            title: String::new(),
                            body: "Graph-dependent document".to_string(),
                            embedding: None,
                            source_id: None,
                            metadata: BTreeMap::new(),
                        }],
                        ..SearchProjectionDelta::default()
                    },
                    processed_primary_key_count: 0,
                })
            })
            .unwrap();

        assert!(observed_graph_only_batch);
        assert!(report.complete);
        drop(handle);
        let reopened = SearchIndex::open(&search_path).unwrap();
        assert!(reopened.document("thread:thread-a").is_none());
        assert!(reopened.document("message:derived").is_some());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_handle_exposes_live_graph_and_projection_watermarks() {
        let root = unique_nowledge_mem_test_dir("embedded_runtime_watermarks");
        let search_path = root.join("search");
        let mut graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'm1', title: 'Runtime status'})")
            .unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ));

        let status_before = handle.runtime_status().unwrap();
        assert_eq!(status_before.graph_commit_epoch, 1);
        assert_eq!(status_before.changefeed.retained_mutation_count, 1);
        assert_eq!(
            status_before
                .projection_freshness
                .as_ref()
                .unwrap()
                .durable_source_graph_commit_epoch,
            None
        );
        assert_eq!(status_before.projection_commit_lag(), 1);
        assert!(status_before.projection_stale());

        handle.catch_up_search_projection(16, 1).unwrap();

        let status_after = handle.runtime_status().unwrap();
        let after = status_after.projection_freshness.as_ref().unwrap();
        assert_eq!(after.source_graph_commit_epoch, Some(1));
        assert_eq!(after.durable_source_graph_commit_epoch, Some(1));
        assert!(!after.has_uncheckpointed_changes);
        assert_eq!(status_after.projection_commit_lag(), 0);
        assert!(!status_after.projection_stale());
        assert_eq!(
            status_after.json()["projection"]["durable_source_graph_commit_epoch"],
            1
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn production_status_does_not_claim_graph_cutover_without_route_ownership() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let status = store.production_status(None);
        let json = status.json();

        assert_eq!(status.protocol, NOWLEDGE_MEM_PRODUCTION_STATUS_PROTOCOL);
        assert!(status.graph_open);
        assert!(!status.graph_read_only);
        assert!(!status.graph_route_ownership_present);
        assert!(!status.graph_skein_cutover_effective);
        assert!(!status.search_projection_open);
        assert!(!status.search_skein_cutover_effective);
        assert!(status
            .blocker_codes
            .contains(&"graph_route_ownership_missing".to_string()));
        assert!(status
            .blocker_codes
            .contains(&"search_projection_not_open".to_string()));
        assert_eq!(json["graph"]["skein_cutover_effective"], false);
        assert_eq!(json["search"]["skein_cutover_effective"], false);
        assert_eq!(json["redaction"]["local_paths_copied"], false);
    }

    #[test]
    fn production_status_reports_partial_graph_ownership_without_cutover_claim() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let store = NowledgeMemEmbeddedStore::new(graph, None);
        let route_ownership = nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_legacy(),
            Some(&ready_route_readiness_summary()),
            NowledgeMemRouteOwnershipPolicy::migration(),
        );

        let status = store.production_status(Some(&route_ownership));

        assert!(status.graph_route_ownership_present);
        assert!(status.graph_route_ownership_ready);
        assert_eq!(status.graph_skein_route_count, 0);
        assert_eq!(
            status.graph_legacy_route_count,
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert!(!status.graph_skein_cutover_effective);
        assert!(status
            .blocker_codes
            .contains(&"graph_legacy_routes_remaining".to_string()));
    }

    #[test]
    fn production_status_separates_graph_cutover_from_search_freshness() {
        let root = unique_nowledge_mem_test_dir("production_status_split_cutover");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        let mut graph =
            NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
        graph
            .query("CREATE (:Memory {id: 'status-m1', title: 'Production status'})")
            .unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let route_ownership = nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_skein(),
            Some(&ready_route_readiness_summary()),
            NowledgeMemRouteOwnershipPolicy::production_cutover(),
        );

        let stale = store.production_status(Some(&route_ownership));
        assert!(stale.graph_skein_cutover_effective);
        assert!(stale.search_projection_open);
        assert_eq!(stale.search_projection_commit_lag, 2);
        assert!(stale.search_projection_stale);
        assert!(!stale.search_skein_cutover_effective);
        assert!(stale
            .blocker_codes
            .contains(&"search_projection_stale".to_string()));

        store.catch_up_search_projection(16, 1).unwrap();
        let ready = store.production_status(Some(&route_ownership));
        assert!(ready.graph_skein_cutover_effective);
        assert!(ready.search_projection_open);
        assert_eq!(ready.search_projection_commit_lag, 0);
        assert!(!ready.search_projection_stale);
        assert!(ready.search_skein_cutover_effective);
        assert!(!ready
            .blocker_codes
            .contains(&"search_projection_stale".to_string()));
        assert_eq!(
            ready.json()["route_ownership"]["production_cutover_ready"],
            true
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cutover_controls_keep_legacy_reads_ready_without_skein_ownership() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let report = store.cutover_controls_report(NowledgeMemCutoverControls::legacy(), None);
        let json = report.json();

        assert_eq!(report.protocol, NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL);
        assert!(report.ready);
        assert!(!report.graph_read_selected_skein);
        assert!(report.graph_read_effective);
        assert!(!report.search_read_selected_skein);
        assert!(report.search_read_effective);
        assert!(!report.dual_writes_enabled);
        assert_eq!(json["controls"]["graph_reads"], "legacy");
        assert_eq!(json["controls"]["search_reads"], "legacy");
        assert_eq!(json["redaction"]["local_paths_copied"], false);
    }

    #[test]
    fn source_mutation_dual_write_readiness_accepts_complete_family_coverage() {
        let report = nowledge_mem_source_mutation_dual_write_readiness(
            &nowledge_mem_source_mutation_dual_write_evidence_all_ready(),
        );

        assert!(report.ready);
        assert_eq!(
            report.protocol,
            NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL
        );
        assert_eq!(
            report.required_family_count,
            REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len()
        );
        assert_eq!(
            report.ready_family_count,
            REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len()
        );
        assert!(report.blocker_codes.is_empty());
        assert!(report.requirements.iter().any(|requirement| {
            requirement.family == NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE
                && requirement.requires_search_projection_payload
        }));
        assert_eq!(report.json()["ready"], true);
    }

    #[test]
    fn source_mutation_dual_write_readiness_blocks_composite_source_ingest_gaps() {
        let mut evidence = nowledge_mem_source_mutation_dual_write_evidence_all_ready();
        let ingest = evidence
            .iter_mut()
            .find(|item| item.family == NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE)
            .unwrap();
        ingest.payload_frozen = false;
        ingest.skein_ack_recorded = false;
        ingest.independent_watermarks_recorded = false;
        ingest.replay_idempotent = false;
        ingest.search_projection_payload_frozen = false;
        evidence.push(NowledgeMemSourceMutationDualWriteEvidence::ready(
            "unknown_source_mutation",
        ));

        let report = nowledge_mem_source_mutation_dual_write_readiness(&evidence);

        assert!(!report.ready);
        assert_eq!(
            report.payload_not_frozen_families,
            vec![NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE.to_string()]
        );
        assert_eq!(
            report.search_projection_payload_not_frozen_families,
            vec![NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE.to_string()]
        );
        assert_eq!(report.unknown_families, vec!["unknown_source_mutation"]);
        assert!(report
            .blocker_codes
            .contains(&"source_mutation_dual_write_payload_not_frozen".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"source_mutation_dual_write_skein_ack_missing".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"source_mutation_dual_write_independent_watermarks_missing".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"source_mutation_dual_write_replay_not_idempotent".to_string()));
        assert!(report.blocker_codes.contains(
            &"source_mutation_dual_write_search_projection_payload_not_frozen".to_string()
        ));
        assert!(report
            .blocker_codes
            .contains(&"source_mutation_dual_write_unknown_families".to_string()));
    }

    #[test]
    fn cutover_controls_fail_closed_when_skein_reads_are_not_effective() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let report = store.cutover_controls_report(NowledgeMemCutoverControls::skein_reads(), None);

        assert!(!report.ready);
        assert!(report.graph_read_selected_skein);
        assert!(!report.graph_read_effective);
        assert!(report.search_read_selected_skein);
        assert!(!report.search_read_effective);
        assert!(report.projection_catch_up_enabled);
        assert!(report
            .blocker_codes
            .contains(&"graph_read_selected_skein_but_not_effective".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"search_read_selected_skein_but_not_effective".to_string()));
        assert_eq!(
            report.json()["production_status"]["graph"]["skein_cutover_effective"],
            false
        );
    }

    #[test]
    fn cutover_controls_require_dual_writes_for_initial_import() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let store = NowledgeMemEmbeddedStore::new(graph, None);
        let controls = NowledgeMemCutoverControls {
            initial_import: NowledgeMemWorkControl::Enabled,
            ..NowledgeMemCutoverControls::legacy()
        };

        let report = store.cutover_controls_report(controls, None);

        assert!(!report.ready);
        assert!(report.initial_import_enabled);
        assert!(!report.dual_writes_enabled);
        assert!(!report.initial_import_inactive_for_cutover);
        assert!(report
            .blocker_codes
            .contains(&"initial_import_enabled_without_dual_writes".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"initial_import_active_blocks_read_cutover".to_string()));
    }

    fn ready_initial_import_cutover_catch_up_report(
    ) -> SkeinLightningInitialImportCutoverCatchUpReport {
        SkeinLightningInitialImportCutoverCatchUpReport {
            ready: true,
            session_ready_for_cutover: true,
            durable_state_present: true,
            live_projection_present: true,
            import_graph_commit_epoch: Some(42),
            import_durable_search_projection_commit_epoch: Some(42),
            live_graph_commit_epoch: 42,
            live_search_projection_commit_epoch: Some(42),
            live_durable_search_projection_commit_epoch: Some(42),
            graph_watermark_caught_up: true,
            search_projection_watermark_caught_up: true,
            live_projection_checkpointed: true,
            live_projection_healthy: true,
            cutover_watermark: Some(42),
            blocker_codes: Vec::new(),
        }
    }

    fn ready_initial_import_recovery_report(
    ) -> crate::SkeinLightningInitialImportRecoveryReadinessReport {
        let mut source = Database::new();
        source
            .query("CREATE (:Memory {id: 'import-root'})-[:LINKS {id: 'import-rel'}]->(:Entity {id: 'import-entity'})")
            .unwrap();
        let export = source.prepare_skein_lightning_bootstrap_export().unwrap();
        let checkpoint = SkeinLightningInitialImportCheckpoint {
            protocol_version: 1,
            import_id: "import-controls".to_string(),
            task_id: "import-controls-task".to_string(),
            fencing_token: "import-controls-fence".to_string(),
            object_digest: "import-controls-digest".to_string(),
            schema_checksum: export.manifest.schema_checksum,
            graph_stream_checksum: export.manifest.graph_stream_checksum,
            graph_stream_byte_len: export.manifest.graph_stream_byte_len,
            relational_stream_checksum: export.manifest.relational_stream_checksum,
            relational_stream_byte_len: export.manifest.relational_stream_byte_len,
            manifest_database_commit_epoch: export.manifest.database_commit_epoch,
            manifest_graph_commit_epoch: export.manifest.graph_commit_epoch,
            applied_graph_commit_epoch: export.manifest.graph_commit_epoch,
            applied_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
            durable_search_projection_commit_epoch: Some(export.manifest.graph_commit_epoch),
            completed_batches: 1,
            total_batches: 1,
            document_identity_count: 6,
        };
        let identities = [
            SearchProjectionKind::Memory,
            SearchProjectionKind::Message,
            SearchProjectionKind::Entity,
            SearchProjectionKind::Source,
            SearchProjectionKind::SourceChunk,
            SearchProjectionKind::Community,
        ]
        .into_iter()
        .map(|kind| SkeinLightningInitialImportDocumentIdentity {
            kind,
            document_id: format!("{}:import", kind.as_str()),
        })
        .collect::<Vec<_>>();
        let durable_state = crate::skein_lightning_initial_import_durable_state_report(
            &export.manifest,
            &checkpoint,
            &identities,
        )
        .state
        .expect("expected persistable durable state");
        let durable_state_payload =
            crate::skein_lightning_initial_import_encode_durable_state(&durable_state).unwrap();
        let projection_rows = identities
            .iter()
            .map(|identity| SearchProjectionRow {
                kind: identity.kind,
                external_id: identity.document_id.clone(),
                title: "initial import".to_string(),
                body: "initial import".to_string(),
                embedding: None,
                source_id: None,
                metadata: BTreeMap::new(),
            })
            .collect::<Vec<_>>();
        let projection_delta = SearchProjectionDelta {
            upserts: projection_rows,
            deletes: Vec::new(),
            max_operations: Some(6),
            source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
        };
        let freshness = SearchProjectionFreshness {
            document_count: 6,
            import_source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
            source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
            durable_source_graph_commit_epoch: Some(export.manifest.graph_commit_epoch),
            has_uncheckpointed_changes: false,
            full_reindex_needed: false,
            full_reindex_reasons: Vec::new(),
            metadata_repair_needed: false,
            metadata_repair_reasons: Vec::new(),
            embedding_model: None,
            embedding_version: None,
            embedding_dimension: None,
        };

        let projection_batches = [projection_delta];
        let recovery = source.skein_lightning_initial_import_recovery_readiness(
            SkeinLightningInitialImportReadinessInputs {
                encoded_graph_stream: &export.graph_stream.encoded,
                encoded_relational_stream: &export.relational_stream.encoded,
                manifest: &export.manifest,
                projection_batches: &projection_batches,
                target_projection_freshness: Some(&freshness),
                live_projection_freshness: Some(&freshness),
            },
            Some(&durable_state_payload),
        );
        assert!(recovery.ready);
        recovery
    }

    #[test]
    fn cutover_controls_accept_active_initial_import_with_cutover_catch_up_proof() {
        let root = unique_nowledge_mem_test_dir("cutover_controls_initial_import_catch_up");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        let mut graph =
            NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
        graph
            .query("CREATE (:Memory {id: 'controls-import-m1', title: 'Import cutover'})")
            .unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        store.catch_up_search_projection(16, 1).unwrap();
        let route_ownership = nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_skein(),
            Some(&ready_route_readiness_summary()),
            NowledgeMemRouteOwnershipPolicy::production_cutover(),
        );
        let controls = NowledgeMemCutoverControls {
            dual_writes: NowledgeMemWorkControl::Enabled,
            initial_import: NowledgeMemWorkControl::Enabled,
            projection_catch_up: NowledgeMemWorkControl::Enabled,
            graph_reads: super::NowledgeMemReadControl::Skein,
            search_reads: super::NowledgeMemReadControl::Skein,
        };
        let catch_up = ready_initial_import_cutover_catch_up_report();

        let report = store.cutover_controls_report_with_initial_import_cutover_catch_up(
            controls,
            Some(&route_ownership),
            Some(&catch_up),
        );

        assert!(report.ready);
        assert!(report.initial_import_enabled);
        assert!(!report.initial_import_inactive_for_cutover);
        assert!(report.initial_import_cutover_catch_up_ready);
        assert!(report.initial_import_safe_for_read_cutover);
        assert!(report.blocker_codes.is_empty());
        assert_eq!(
            report.json()["work"]["initial_import_safe_for_read_cutover"],
            true
        );
    }

    #[test]
    fn cutover_controls_require_recovery_proof_for_active_initial_import() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let store = NowledgeMemEmbeddedStore::new(graph, None);
        let controls = NowledgeMemCutoverControls {
            dual_writes: NowledgeMemWorkControl::Enabled,
            initial_import: NowledgeMemWorkControl::Enabled,
            ..NowledgeMemCutoverControls::legacy()
        };

        let report =
            store.cutover_controls_report_with_initial_import_recovery(controls, None, None);

        assert!(!report.ready);
        assert!(report
            .blocker_codes
            .contains(&"initial_import_recovery_not_ready".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"initial_import_active_blocks_read_cutover".to_string()));
    }

    #[test]
    fn cutover_controls_accept_active_initial_import_with_recovery_proof() {
        let root = unique_nowledge_mem_test_dir("cutover_controls_initial_import_recovery");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        let mut graph =
            NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
        graph
            .query("CREATE (:Memory {id: 'controls-recovery-m1', title: 'Import recovery'})")
            .unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        store.catch_up_search_projection(16, 1).unwrap();
        let route_ownership = nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_skein(),
            Some(&ready_route_readiness_summary()),
            NowledgeMemRouteOwnershipPolicy::production_cutover(),
        );
        let controls = NowledgeMemCutoverControls {
            dual_writes: NowledgeMemWorkControl::Enabled,
            initial_import: NowledgeMemWorkControl::Enabled,
            projection_catch_up: NowledgeMemWorkControl::Enabled,
            graph_reads: super::NowledgeMemReadControl::Skein,
            search_reads: super::NowledgeMemReadControl::Skein,
        };
        let recovery = ready_initial_import_recovery_report();

        let report = store.cutover_controls_report_with_initial_import_recovery(
            controls,
            Some(&route_ownership),
            Some(&recovery),
        );

        assert!(report.ready);
        assert!(report.initial_import_cutover_catch_up_ready);
        assert!(report.initial_import_safe_for_read_cutover);
        assert!(report.blocker_codes.is_empty());
    }

    #[test]
    fn cutover_controls_block_read_cutover_while_initial_import_is_active() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let store = NowledgeMemEmbeddedStore::new(graph, None);
        let controls = NowledgeMemCutoverControls {
            dual_writes: NowledgeMemWorkControl::Enabled,
            initial_import: NowledgeMemWorkControl::Enabled,
            ..NowledgeMemCutoverControls::legacy()
        };

        let report = store.cutover_controls_report(controls, None);

        assert!(!report.ready);
        assert!(report.dual_writes_enabled);
        assert!(report.initial_import_enabled);
        assert!(!report.initial_import_inactive_for_cutover);
        assert_eq!(
            report.json()["work"]["initial_import_inactive_for_cutover"],
            false
        );
        assert_eq!(
            report.blocker_codes,
            vec!["initial_import_active_blocks_read_cutover".to_string()]
        );
    }

    #[test]
    fn cutover_controls_accept_independent_skein_reads_after_status_is_ready() {
        let root = unique_nowledge_mem_test_dir("cutover_controls_ready_reads");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        let mut graph =
            NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
        graph
            .query("CREATE (:Memory {id: 'controls-m1', title: 'Cutover controls'})")
            .unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        store.catch_up_search_projection(16, 1).unwrap();
        let route_ownership = nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_skein(),
            Some(&ready_route_readiness_summary()),
            NowledgeMemRouteOwnershipPolicy::production_cutover(),
        );

        let report = store.cutover_controls_report(
            NowledgeMemCutoverControls::skein_reads(),
            Some(&route_ownership),
        );

        assert!(report.ready);
        assert!(report.graph_read_selected_skein);
        assert!(report.graph_read_effective);
        assert!(report.search_read_selected_skein);
        assert!(report.search_read_effective);
        assert!(report.dual_writes_enabled);
        assert!(report.projection_catch_up_enabled);
        assert!(report.blocker_codes.is_empty());
        assert_eq!(
            report.json()["production_status"]["search"]["skein_cutover_effective"],
            true
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_catches_up_delete_after_graph_checkpoint_and_restart() {
        let root = unique_nowledge_mem_test_dir("embedded_projection_checkpoint_restart");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        {
            let graph =
                NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
            let projection =
                NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
            let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
            store
                .graph_mut()
                .query("CREATE (:Memory {id: 'm1', title: 'Removed after checkpoint'})")
                .unwrap();
            let initial_report = store.catch_up_search_projection(16, 1).unwrap();
            assert!(initial_report.complete);
            assert!(store
                .search_projection()
                .unwrap()
                .index()
                .document("memory:m1")
                .is_some());

            store
                .graph_mut()
                .query("MATCH (m:Memory {id: 'm1'}) DETACH DELETE m")
                .unwrap();
            store.graph_mut().database_mut().checkpoint().unwrap();
        }

        {
            let graph =
                NowledgeMemGraph::open(&graph_path, NowledgeMemGraphMode::WritableCutover).unwrap();
            let projection =
                NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
            let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
            let report = store.catch_up_search_projection(16, 1).unwrap();

            assert!(report.complete);
            assert_eq!(report.start_durable_epoch, Some(2));
            assert_eq!(report.end_durable_epoch, Some(3));
            assert!(store
                .search_projection()
                .unwrap()
                .index()
                .document("memory:m1")
                .is_none());
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_does_not_split_graph_commit_across_projection_batches() {
        let root = unique_nowledge_mem_test_dir("embedded_projection_commit_boundary");
        let search_path = root.join("search");
        let mut db = Database::new();
        {
            let mut transaction = db.begin_transaction();
            transaction
                .query("CREATE (:Memory {id: 'm1', title: 'First'})")
                .unwrap();
            transaction
                .query("CREATE (:Memory {id: 'm2', title: 'Second'})")
                .unwrap();
            transaction.commit().unwrap();
        }
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let error = store.catch_up_search_projection(1, 1).unwrap_err();
        assert!(error.to_string().contains(
            "search projection change at commit epoch 1 requires 2 operations, exceeding configured per-batch limit 1"
        ));
        let projection = store.search_projection().unwrap();
        assert_eq!(
            projection
                .index()
                .projection_freshness()
                .durable_source_graph_commit_epoch,
            None
        );
        assert!(projection.index().document("memory:m1").is_none());
        assert!(projection.index().document("memory:m2").is_none());

        let report = store.catch_up_search_projection(2, 1).unwrap();
        assert!(report.complete);
        assert_eq!(report.end_durable_epoch, Some(1));
        assert!(store
            .search_projection()
            .unwrap()
            .index()
            .document("memory:m1")
            .is_some());
        assert!(store
            .search_projection()
            .unwrap()
            .index()
            .document("memory:m2")
            .is_some());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_handle_reuses_open_store_for_knowledge_retrieval() {
        let root = unique_nowledge_mem_test_dir("embedded_handle_retrieval");
        let search_path = root.join("search");
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'm1', title: 'Reusable handle', content: 'embedded retrieval'})")
            .unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ));
        handle.catch_up_search_projection(16, 1).unwrap();

        let output = handle
            .retrieve_knowledge(&KnowledgeRetrievalRequest {
                query_text: "reusable handle".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 4,
                graph_context_limit: 4,
                graph_context_max_hops: 1,
            })
            .unwrap();

        assert_eq!(output.search.total_hits, 1);
        assert_eq!(output.search.hits[0].external_id.as_deref(), Some("m1"));
        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_background_delta_uses_scheduler_qos() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'new', title: 'Scheduled facade'})")
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request = store
            .build_search_projection_graph_delta_request_from_freshness(Some(4))
            .unwrap()
            .expect("expected graph delta request");
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(0),
            ..LocalQosPolicy::default()
        });

        let error = store
            .apply_scheduled_background_search_projection_graph_delta(&mut scheduler, request)
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("background search projection graph delta"));
        assert!(store
            .search_projection()
            .unwrap()
            .index()
            .document("memory:new")
            .is_none());
    }

    #[test]
    fn embedded_store_scheduled_catch_up_reports_qos_deferral_without_applying() {
        let root = unique_nowledge_mem_test_dir("embedded_scheduled_catch_up_deferred");
        let search_path = root.join("search");
        let mut graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'new', title: 'Scheduled facade'})")
            .unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_total_background_operations: Some(0),
            ..LocalQosPolicy::default()
        });

        let report = store
            .catch_up_search_projection_with_scheduler(&mut scheduler, 4, 1)
            .unwrap();

        assert_eq!(
            report.stop_reason,
            crate::SearchProjectionCatchUpStopReason::Deferred(
                crate::QosAdmissionCode::TotalBackgroundLimitExceeded
            )
        );
        assert_eq!(report.catch_up.applied_batch_count, 0);
        assert!(!report.catch_up.complete);
        let projection = store.search_projection().unwrap();
        assert_eq!(
            projection
                .index()
                .projection_freshness()
                .durable_source_graph_commit_epoch,
            None
        );
        assert!(projection.index().document("memory:new").is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_handle_scheduled_catch_up_checkpoints_before_returning() {
        let root = unique_nowledge_mem_test_dir("embedded_handle_scheduled_catch_up");
        let search_path = root.join("search");
        let mut graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'new', title: 'Scheduled facade'})")
            .unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ));
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

        let report = handle
            .catch_up_search_projection_with_scheduler(&mut scheduler, 4, 1)
            .unwrap();

        assert_eq!(
            report.stop_reason,
            crate::SearchProjectionCatchUpStopReason::CaughtUp
        );
        assert!(report.catch_up.complete);
        assert_eq!(report.catch_up.applied_batch_count, 1);
        assert_eq!(
            report.catch_up.end_applied_epoch,
            report.catch_up.end_durable_epoch
        );
        assert_eq!(scheduler.state().running_background_operations, 0);
        drop(handle);

        let reopened = SearchIndex::open(&search_path).unwrap();
        assert!(reopened.document("memory:new").is_some());
        assert_eq!(
            reopened
                .projection_freshness()
                .durable_source_graph_commit_epoch,
            report.catch_up.end_durable_epoch
        );
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_scheduled_catch_up_reports_batch_budget_exhaustion() {
        let root = unique_nowledge_mem_test_dir("embedded_scheduled_catch_up_budget");
        let search_path = root.join("search");
        let mut graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'first', title: 'First'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'second', title: 'Second'})")
            .unwrap();
        let projection =
            NowledgeMemSearchProjection::from_index(SearchIndex::open(&search_path).unwrap());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

        let report = store
            .catch_up_search_projection_with_scheduler(&mut scheduler, 1, 1)
            .unwrap();

        assert_eq!(
            report.stop_reason,
            crate::SearchProjectionCatchUpStopReason::BatchBudgetExhausted
        );
        assert!(!report.catch_up.complete);
        assert_eq!(report.catch_up.applied_batch_count, 1);
        assert_eq!(report.catch_up.applied_operation_count, 1);
        assert_eq!(report.catch_up.end_durable_epoch, Some(1));
        let projection = store.search_projection().unwrap();
        assert!(projection.index().document("memory:first").is_some());
        assert!(projection.index().document("memory:second").is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_retrieves_knowledge_through_search_projection() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-search', title: 'Facade retrieval', content: 'Skein replaces LanceDB retrieval'})")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'entity-skein', name: 'Skein'})")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'mem-search'}), (e:Entity {id: 'entity-skein'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let delta = store
            .build_search_projection_graph_delta_request_from_freshness(Some(8))
            .unwrap()
            .expect("expected search projection delta");
        store.apply_search_projection_graph_delta(delta).unwrap();

        let retrieval = store
            .retrieve_knowledge_with_report(&KnowledgeRetrievalRequest {
                query_text: "facade retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 4,
                graph_context_limit: 4,
                graph_context_max_hops: 1,
            })
            .unwrap();
        let output = retrieval.output;
        let report = retrieval.report;

        assert_eq!(output.search.total_hits, 1);
        assert_eq!(output.search.hits[0].id, "memory:mem-search");
        assert_eq!(output.diagnostics.projection_commit_lag, 0);
        assert!(!output.evidence.is_empty());
        assert_eq!(report.protocol, NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL);
        assert_eq!(report.mode, NowledgeMemGraphMode::WritableCutover);
        assert_eq!(
            report.compressed_vector_search_mode,
            CompressedVectorSearchMode::Disabled
        );
        assert_eq!(report.search_total_hits, 1);
        assert!(report.candidate_count >= 1);
        assert!(report.evidence_count >= 1);
        assert_eq!(report.text_backend, Some("bm25_text".to_string()));
        assert_eq!(
            report.json()["protocol"],
            NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL
        );
    }

    #[test]
    fn embedded_handle_serves_search_and_graph_context_without_document_residency() {
        let root = unique_nowledge_mem_test_dir("embedded_out_of_core_serving");
        let search_path = root.join("search");
        let mut graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-ooc', title: 'Bounded serving', content: 'Skein out of core retrieval'})")
            .unwrap();
        graph
            .query("CREATE (:Entity {id: 'entity-ooc', name: 'OutOfCore'})")
            .unwrap();
        graph
            .query("MATCH (m:Memory {id: 'mem-ooc'}), (e:Entity {id: 'entity-ooc'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        let graph_commit_epoch = graph.database().commit_epoch();
        {
            let mut index = SearchIndex::open(&search_path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "test-embedding".to_string(),
                    version: Some("v1".to_string()),
                    dimension: 2,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "mem-ooc".to_string(),
                        title: "Bounded serving".to_string(),
                        body: "Skein out of core retrieval".to_string(),
                        embedding: Some(vec![1.0, 0.0]),
                        source_id: Some("source-ooc".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), "active".to_string()),
                        ]),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(graph_commit_epoch),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemOutOfCoreSearchProjection::open(&search_path).unwrap();
        assert_eq!(projection.reader().resident_document_count(), 0);
        let handle = NowledgeMemEmbeddedStoreHandle::new(
            NowledgeMemEmbeddedStore::new_with_out_of_core_search(
                graph,
                projection,
                NowledgeMemRetrievalProjectionAdvisor::default(),
            ),
        );

        let candidates = handle
            .search_candidates_with_report(&NowledgeMemSearchCandidateRequest::text(
                "bounded serving",
                1,
            ))
            .unwrap();
        assert_eq!(candidates.result.hits[0].id, "memory:mem-ooc");
        assert!(candidates.out_of_core_metrics.is_some());

        let hydration = handle
            .search_projection_documents_with_report(&["memory:mem-ooc".to_string()], 1)
            .unwrap();
        assert_eq!(hydration.documents[0].id, "memory:mem-ooc");
        assert_eq!(
            hydration
                .out_of_core_metrics
                .as_ref()
                .unwrap()
                .hydrated_documents,
            1
        );

        let retrieval = handle
            .retrieve_knowledge_with_report(&KnowledgeRetrievalRequest {
                query_text: "bounded serving".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 1,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: Some(4),
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 4,
                graph_context_limit: 4,
                graph_context_max_hops: 1,
            })
            .unwrap();
        assert_eq!(retrieval.output.search.hits[0].id, "memory:mem-ooc");
        assert!(!retrieval.output.graph_context_paths.is_empty());
        assert!(retrieval.out_of_core_search_metrics.is_some());
        assert_eq!(
            retrieval
                .output
                .projection_freshness
                .source_graph_commit_epoch,
            Some(graph_commit_epoch)
        );

        let vector = handle
            .query_with_params_with_report(
                "CALL vector_search($embedding, topK := 1) RETURN id, score",
                &BTreeMap::from([(
                    "embedding".to_string(),
                    Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
                )]),
            )
            .unwrap();
        assert_eq!(vector.output.rows.len(), 1);
        assert_eq!(
            vector.output.rows[0].get("id"),
            Some(&Value::String("memory:mem-ooc".to_string()))
        );

        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_projection_candidate_api_reports_metadata_pushdown() {
        let root = unique_nowledge_mem_test_dir("search_candidate_api_pushdown");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            for (external_id, lifecycle_state) in [
                ("deleted", "deleted"),
                ("forgotten", "forgotten"),
                ("active", "active"),
            ] {
                index
                    .upsert_projection_row(SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: external_id.to_string(),
                        title: format!("{external_id} candidate"),
                        body: "metadata filtered candidate read".to_string(),
                        embedding: None,
                        source_id: Some("source-1".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), lifecycle_state.to_string()),
                        ]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request = NowledgeMemSearchCandidateRequest::text("candidate read", 10)
            .with_metadata_filters(BTreeMap::from([(
                "lifecycle_state__not_in".to_string(),
                r#"["deleted","forgotten"]"#.to_string(),
            )]));

        let output = store.search_candidates(&request).unwrap();

        assert_eq!(output.result.total_hits, 1);
        assert_eq!(output.result.hits[0].id, "memory:active");
        assert_eq!(
            output.report.protocol,
            NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL
        );
        assert_eq!(output.report.metadata_filter_count, 1);
        assert_eq!(output.report.pushed_predicate_count, 1);
        assert_eq!(output.report.residual_predicate_count, 0);
        assert_eq!(output.report.segment_count, 2);
        assert_eq!(output.report.pruned_segment_count, 1);
        assert_eq!(output.report.scanned_segment_count, 1);
        assert!(output.report.persisted_segment_descriptor_used);
        assert_eq!(output.report.physical_range_read_count, 1);
        assert!(output.report.physical_bytes_read > 0);
        assert_eq!(output.report.filtered_out_count, 2);
        assert_eq!(
            output.report.json()["candidate_set"]["metadata_predicate_pushdown"]["field_summaries"]
                [0]["field"],
            "lifecycle_state"
        );

        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_search_candidate_output(["memory:active"], &output);
        let evidence = accumulator.json();

        assert_eq!(evidence["ready"], true);
        assert_eq!(
            evidence["evidence_source"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE
        );
        assert_eq!(evidence["request_count"], 1);
        assert_eq!(evidence["candidate_primary_engine"], "skein");
        assert_eq!(evidence["candidate_identity"]["ready"], true);
        assert_eq!(evidence["filter_pushdown"]["ready"], true);
        assert_eq!(
            evidence["filter_pushdown"]["field_capabilities_ready"],
            true
        );
        assert_eq!(
            evidence["filter_pushdown"]["field_summary_count"],
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len()
        );
        assert_eq!(
            evidence["filter_pushdown"]["missing_required_fields"],
            serde_json::json!([])
        );
        assert!(evidence["filter_pushdown"]["field_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary["field"] == "lifecycle_state"
                && summary["source"] == "persisted_segment_descriptor_contract"));
        assert!(evidence["filter_pushdown"]["field_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|summary| summary["field"] == "confidence"
                && summary["numeric_range_summary_used"] == true));

        let direct_evidence = store
            .search_candidate_shadow_evidence_json(&request, ["memory:active"])
            .unwrap();
        assert_eq!(direct_evidence["ready"], true);
        assert_eq!(
            direct_evidence["evidence_source"],
            NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE
        );
        assert_eq!(direct_evidence["candidate_identity"]["ready"], true);
        assert_eq!(direct_evidence["filter_pushdown"]["ready"], true);
        assert_eq!(
            direct_evidence["filter_pushdown"]["field_capabilities_ready"],
            true
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_handle_hydrates_only_explicitly_bounded_search_documents() {
        let root = unique_nowledge_mem_test_dir("bounded_search_document_hydration");
        let mut index = SearchIndex::open(&root).unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::SourceChunk,
                external_id: "source-1-chunk-0".to_string(),
                title: "Introduction".to_string(),
                body: "bounded projection payload".to_string(),
                embedding: None,
                source_id: Some("source-1".to_string()),
                metadata: BTreeMap::from([("chunk_index".to_string(), "0".to_string())]),
            })
            .unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(NowledgeMemSearchProjection::from_index(index)),
        ));

        let documents = handle
            .search_projection_documents(&["source_chunk:source-1-chunk-0".to_string()], 1)
            .unwrap();

        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].content, "bounded projection payload");
        assert_eq!(documents[0].metadata["chunk_index"], "0");
        assert!(handle
            .search_projection_documents(
                &[
                    "source_chunk:source-1-chunk-0".to_string(),
                    "source_chunk:source-1-chunk-1".to_string(),
                ],
                1,
            )
            .is_err());
        let snapshot = handle.runtime_governor_snapshot().unwrap();
        assert_eq!(snapshot.admissions, 1);
        assert_eq!(snapshot.completions, 1);
    }

    #[test]
    fn embedded_handle_search_hydration_rejects_payload_over_budget() {
        let graph = NowledgeMemGraph::from_database(
            Database::new_with_config(DatabaseConfig {
                max_read_result_payload_bytes: Some(64),
                ..DatabaseConfig::default()
            }),
            NowledgeMemGraphMode::ShadowReadOnly,
        );
        let mut index = SearchIndex::in_memory();
        index
            .upsert(crate::SearchDocument {
                id: "source_chunk:large".to_string(),
                title: "Large".to_string(),
                content: "x".repeat(256),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(NowledgeMemSearchProjection::from_index(index)),
        ));

        let error = handle
            .search_projection_documents(&["source_chunk:large".to_string()], 1)
            .unwrap_err();

        assert!(error.to_string().contains("payload bytes"));
        let snapshot = handle.runtime_governor_snapshot().unwrap();
        assert_eq!(snapshot.admissions, 1);
        assert_eq!(snapshot.completions, 1);
    }

    #[test]
    fn embedded_store_handle_checkpoints_external_projection_delta_before_returning() {
        let root = unique_nowledge_mem_test_dir("external_projection_delta_checkpoint");
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ));

        let report = handle
            .apply_search_projection_delta_and_checkpoint(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "memory-1".to_string(),
                    title: "Imported memory".to_string(),
                    body: "Preserved legacy projection payload".to_string(),
                    embedding: Some(vec![0.25, 0.75]),
                    source_id: Some("source-1".to_string()),
                    metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
                }],
                deletes: Vec::new(),
                max_operations: Some(1),
                source_graph_commit_epoch: Some(7),
            })
            .unwrap();
        assert_eq!(report.upserted_documents, 1);
        drop(handle);

        let reopened = SearchIndex::open(&root).unwrap();
        let freshness = reopened.projection_freshness();
        assert_eq!(freshness.document_count, 1);
        assert_eq!(freshness.source_graph_commit_epoch, Some(7));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_query_runtime_executes_vector_seed_with_observability() {
        let mut index = SearchIndex::in_memory();
        for (external_id, embedding) in [("nearest", vec![1.0, 0.0]), ("farther", vec![0.0, 1.0])] {
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: external_id.to_string(),
                    title: external_id.to_string(),
                    body: String::new(),
                    embedding: Some(embedding),
                    source_id: None,
                    metadata: BTreeMap::new(),
                })
                .unwrap();
        }
        let projection = NowledgeMemSearchProjection::from_index(index);
        let graph = NowledgeMemGraph::from_database(
            Database::new_with_config(DatabaseConfig {
                slow_query_log_threshold_micros: 0,
                ..DatabaseConfig::default()
            }),
            NowledgeMemGraphMode::WritableCutover,
        );
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let parameters = BTreeMap::from([(
            "embedding".to_string(),
            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
        )]);

        let output = store
            .query_with_params_with_report_options(
                "CALL vector_search($embedding, topK := 1) RETURN id, score",
                &parameters,
                NowledgeMemQueryReportOptions {
                    capture_physical_plan: true,
                    slow_log_threshold_micros: Some(0),
                },
            )
            .unwrap();

        assert_eq!(
            output.output.rows[0].get("id"),
            Some(&Value::String("memory:nearest".to_string()))
        );
        assert_eq!(output.report.statement_kind, "vector_search");
        assert_eq!(
            output.report.physical_operator_counts.get("VectorSeedScan"),
            Some(&1)
        );
        assert_eq!(output.report.plan_cache_lookup.as_deref(), Some("bypass"));
        assert_eq!(
            output.report.vector_execution_reports[0].scalar_filtered_count,
            0
        );
        assert_eq!(output.report.vector_execution_reports.len(), 1);
        assert_eq!(
            output.report.vector_execution_reports[0].backend,
            skein_executor::VectorExecutionBackend::ScalarFlat
        );
        assert_eq!(
            output.report.vector_execution_reports[0].compression_mode,
            skein_executor::VectorCompressionMode::Disabled
        );
        assert_eq!(
            output.report.vector_execution_reports[0].candidate_source,
            skein_plan::VectorCandidateSource::Scalar
        );
        assert_eq!(
            output.report.vector_execution_reports[0].backend_selection_reason,
            Some(skein_plan::VectorBackendSelectionReason::CompressionDisabled)
        );
        assert_eq!(
            output.report.vector_execution_reports[0].estimated_raw_vector_bytes,
            Some(16)
        );
        assert_eq!(
            output.report.vector_execution_reports[0].filter_selectivity_per_million,
            Some(0)
        );
        assert_eq!(
            output.report.vector_execution_reports[0].final_score_source,
            skein_executor::VectorScoreSource::RawVector
        );
        assert_eq!(
            output.report.vector_execution_reports[0].index_covered_document_count,
            Some(2)
        );
        assert_eq!(
            output.report.vector_execution_reports[0].index_candidate_document_count,
            Some(2)
        );
        assert_eq!(
            output.report.vector_execution_reports[0].index_coverage_complete,
            Some(true)
        );
        assert!(output.report.vector_execution_reports[0]
            .fallback_reason_codes
            .is_empty());
        assert!(output.report.vector_execution_reports[0].raw_vector_bytes_read > 0);

        let explain = store
            .query_with_params_with_report(
                "EXPLAIN ANALYZE CALL vector_search($embedding, topK := 1) RETURN id, score",
                &parameters,
            )
            .unwrap();
        let Value::List(vector_reports) = explain.output.rows[0]
            .get("vector_execution_reports")
            .expect("explain analyze vector reports")
        else {
            panic!("expected vector execution report list");
        };
        assert_eq!(vector_reports.len(), 1);
        let Some(Value::String(plan)) = explain.output.rows[0].get("plan") else {
            panic!("expected explain plan");
        };
        assert!(plan.contains("VectorSeedScan embedding=$embedding"));
        assert!(plan.contains("Filter->VectorCandidateScan->RawVectorRerank->TopK"));
        assert!(!plan.contains("[1"));

        let slow_log = store.graph().database().slow_query_log_jsonl().unwrap();
        let slow_event: serde_json::Value =
            serde_json::from_str(slow_log.lines().next().unwrap()).unwrap();
        assert_eq!(slow_event["vector_execution_report_count"], 1);
        assert_eq!(
            slow_event["vector_execution_reports"][0]["backend"],
            "scalar_flat"
        );
        assert_eq!(
            slow_event["vector_execution_reports"][0]["compression_mode"],
            "disabled"
        );
        assert_eq!(
            slow_event["vector_execution_reports"][0]["backend_selection_reason"],
            "compression_disabled"
        );
        assert_eq!(
            slow_event["vector_execution_reports"][0]["estimated_raw_vector_bytes"],
            16
        );
        assert_eq!(
            slow_event["vector_execution_reports"][0]["index_coverage_complete"],
            true
        );
        assert!(slow_event.get("query_text").is_none());
        assert!(!slow_log.contains("1.0"));
    }

    #[test]
    fn embedded_query_runtime_feeds_vector_candidates_into_graph_match() {
        let mut index = SearchIndex::in_memory();
        for (external_id, embedding) in [("nearest", vec![1.0, 0.0]), ("farther", vec![0.8, 0.2])] {
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: external_id.to_string(),
                    title: external_id.to_string(),
                    body: String::new(),
                    embedding: Some(embedding),
                    source_id: None,
                    metadata: BTreeMap::from([(
                        "space_id".to_string(),
                        if external_id == "nearest" {
                            "selected"
                        } else {
                            "other"
                        }
                        .to_string(),
                    )]),
                })
                .unwrap();
        }
        let projection = NowledgeMemSearchProjection::from_index(index);
        let mut graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'nearest', space_id: 'selected'})")
            .unwrap();
        graph
            .query("CREATE (:Memory {id: 'farther', space_id: 'other'})")
            .unwrap();
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let parameters = BTreeMap::from([
            (
                "embedding".to_string(),
                Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
            ),
            (
                "space_id".to_string(),
                Value::String("selected".to_string()),
            ),
        ]);
        let query = "CALL vector_search($embedding, topK := 2) YIELD id, score \
                     MATCH (m:Memory) WHERE m.space_id = $space_id \
                     RETURN m.id AS memory_id, score";
        let logical =
            crate::planner::plan_with_params(&crate::cypher::parse(query).unwrap(), &parameters)
                .unwrap();
        let plan = crate::optimizer::CascadesOptimizer::new(Default::default())
            .optimize(&logical)
            .explain(0);
        let vector_seed_line = plan
            .lines()
            .find(|line| line.contains("VectorSeedScan"))
            .expect("vector seed plan");
        assert!(vector_seed_line.contains("metadata_filter_fields=[\"space_id\"]"));
        assert!(!vector_seed_line.contains("selected"));

        let output = store
            .query_with_params_with_report_options(
                query,
                &parameters,
                NowledgeMemQueryReportOptions {
                    capture_physical_plan: true,
                    slow_log_threshold_micros: None,
                },
            )
            .unwrap();

        assert_eq!(output.output.rows.len(), 1);
        assert_eq!(
            output.output.rows[0].get("memory_id"),
            Some(&Value::String("nearest".to_string()))
        );
        assert!(matches!(
            output.output.rows[0].get("score"),
            Some(Value::Float(score)) if *score > 0.0
        ));
        assert!(!output.output.rows[0].contains_key("external_id"));
        assert_eq!(output.report.statement_kind, "vector_graph_search");
        assert_eq!(output.report.plan_cache_lookup.as_deref(), Some("bypass"));
        assert_eq!(
            output.report.physical_operator_counts.get("VectorSeedScan"),
            Some(&1)
        );
        assert_eq!(
            output
                .report
                .physical_operator_counts
                .get("NodeColumnLookupExec"),
            Some(&1)
        );
        assert!(output.report.physical_plan_captured);
        assert_eq!(
            output.report.vector_execution_reports[0].scalar_filtered_count,
            1
        );
    }

    #[test]
    fn embedded_query_runtime_expands_from_vector_seed_candidates() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "nearest".to_string(),
                title: "nearest".to_string(),
                body: String::new(),
                embedding: Some(vec![1.0, 0.0]),
                source_id: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(index);
        let mut graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        graph.query("CREATE (:Memory {id: 'nearest'})").unwrap();
        graph.query("CREATE (:Entity {id: 'entity-rust'})").unwrap();
        graph
            .query(
                "MATCH (m:Memory {id: 'nearest'}), (e:Entity {id: 'entity-rust'}) \
                 CREATE (m)-[:MENTIONS]->(e)",
            )
            .unwrap();
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let parameters = BTreeMap::from([(
            "embedding".to_string(),
            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
        )]);

        let output = store
            .query_with_params_with_report_options(
                "CALL vector_search($embedding, topK := 1) YIELD id, score \
                 MATCH (m:Memory)-[:MENTIONS]->(e:Entity) \
                 RETURN m.id AS memory_id, e.id AS entity_id, score",
                &parameters,
                NowledgeMemQueryReportOptions {
                    capture_physical_plan: true,
                    slow_log_threshold_micros: None,
                },
            )
            .unwrap();

        assert_eq!(output.output.rows.len(), 1);
        assert_eq!(
            output.output.rows[0].get("memory_id"),
            Some(&Value::String("nearest".to_string()))
        );
        assert_eq!(
            output.output.rows[0].get("entity_id"),
            Some(&Value::String("entity-rust".to_string()))
        );
        assert!(matches!(
            output.output.rows[0].get("score"),
            Some(Value::Float(score)) if *score > 0.0
        ));
        assert_eq!(
            output
                .report
                .physical_operator_counts
                .get("AdjacencyExpandExec"),
            Some(&1)
        );
        assert_eq!(output.report.graph_expansion_reports.len(), 1);
        let graph_report = &output.report.graph_expansion_reports[0];
        assert_eq!(graph_report.seed_count, 1);
        assert_eq!(graph_report.expanded_node_count, 1);
        assert_eq!(graph_report.expanded_edge_count, 1);
        assert_eq!(graph_report.relation_types, vec!["MENTIONS".to_string()]);
        assert_eq!(graph_report.min_hops, 1);
        assert_eq!(graph_report.max_hops, 1);
        assert_eq!(graph_report.reranked_seed_count, 1);
        assert_eq!(graph_report.returned_count, 1);
        assert!(graph_report.payload_bytes_used > 0);
        assert!(!graph_report.truncated());

        let explain = store
            .query_with_params_with_report(
                "EXPLAIN ANALYZE CALL vector_search($embedding, topK := 1) YIELD id, score \
                 MATCH (m:Memory)-[:MENTIONS]->(e:Entity) \
                 RETURN m.id AS memory_id, e.id AS entity_id, score",
                &parameters,
            )
            .unwrap();
        let Value::List(graph_reports) = explain.output.rows[0]
            .get("graph_expansion_reports")
            .expect("explain analyze graph reports")
        else {
            panic!("expected graph expansion report list");
        };
        assert_eq!(graph_reports.len(), 1);
    }

    #[test]
    fn embedded_query_runtime_bounds_vector_seed_graph_fanout() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "fanout-seed".to_string(),
                title: "fanout seed".to_string(),
                body: String::new(),
                embedding: Some(vec![1.0, 0.0]),
                source_id: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(index);
        let mut graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        graph.query("CREATE (:Memory {id: 'fanout-seed'})").unwrap();
        for index in 0..40 {
            graph
                .query(&format!("CREATE (:Entity {{id: 'entity-{index}'}})"))
                .unwrap();
            graph
                .query(&format!(
                    "MATCH (m:Memory {{id: 'fanout-seed'}}), \
                     (e:Entity {{id: 'entity-{index}'}}) \
                     CREATE (m)-[:MENTIONS]->(e)"
                ))
                .unwrap();
        }
        let mut store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let parameters = BTreeMap::from([(
            "embedding".to_string(),
            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
        )]);

        let output = store
            .query_with_params_with_report(
                "CALL vector_search($embedding, topK := 1) YIELD id, score \
                 MATCH (m:Memory)-[:MENTIONS]->(e:Entity) \
                 RETURN e.id AS entity_id",
                &parameters,
            )
            .unwrap();

        assert_eq!(output.output.rows.len(), 32);
        let graph_report = &output.report.graph_expansion_reports[0];
        assert_eq!(graph_report.candidate_limit, 32);
        assert_eq!(graph_report.returned_count, 32);
        assert_eq!(graph_report.expanded_node_count, 32);
        assert_eq!(graph_report.expanded_edge_count, 32);
        assert_eq!(
            graph_report.truncation_reason,
            Some(skein_executor::GraphExpansionTruncationReason::CandidateLimit)
        );
    }

    #[test]
    fn embedded_query_runtime_rejects_vector_seed_expansion_over_two_hops() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(
            graph,
            Some(NowledgeMemSearchProjection::from_index(
                SearchIndex::in_memory(),
            )),
        );
        let parameters = BTreeMap::from([(
            "embedding".to_string(),
            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
        )]);

        let error = store
            .query_with_params_with_report(
                "CALL vector_search($embedding, topK := 1) YIELD id, score \
                 MATCH (m:Memory)-[:MENTIONS*1..3]->(e:Entity) \
                 RETURN e.id AS entity_id",
                &parameters,
            )
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("vector-seeded graph expansion supports at most 2 hops"));
    }

    #[test]
    fn graph_only_query_runtime_rejects_vector_seed_capability() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
        let mut store = NowledgeMemEmbeddedStore::new(graph, None);
        let parameters = BTreeMap::from([(
            "embedding".to_string(),
            Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
        )]);

        let error = store
            .query_with_params_with_report(
                "CALL vector_search($embedding, topK := 1) RETURN id, score",
                &parameters,
            )
            .unwrap_err();

        assert!(error.to_string().contains("search projection"));
    }

    #[test]
    fn embedded_store_reopens_search_projection_with_descriptor_pruning() {
        let root = unique_nowledge_mem_test_dir("search_candidate_library_reopen_pruning");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        {
            let mut db = Database::open(&graph_path).unwrap();
            db.query(
                "CREATE (:Memory {id: 'active', title: 'Active candidate', space_id: 'default'})",
            )
            .unwrap();
            db.checkpoint().unwrap();
        }
        {
            let mut index = SearchIndex::open(&search_path).unwrap();
            for (external_id, lifecycle_state, importance) in [
                ("deleted", "deleted", "0.95"),
                ("forgotten", "forgotten", "0.90"),
                ("active", "active", "0.80"),
            ] {
                index
                    .upsert_projection_row(SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: external_id.to_string(),
                        title: format!("{external_id} candidate"),
                        body: "checkpointed candidate read".to_string(),
                        embedding: None,
                        source_id: Some("source-1".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("unit_type".to_string(), "memory".to_string()),
                            ("lifecycle_state".to_string(), lifecycle_state.to_string()),
                            ("importance".to_string(), importance.to_string()),
                        ]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }

        let options = NowledgeMemOpenOptions::with_search_projection(
            graph_path,
            search_path,
            NowledgeMemGraphMode::ShadowReadOnly,
        );
        let (store, open_report) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
        let request = NowledgeMemSearchCandidateRequest::text("candidate read", 10)
            .with_metadata_filters(BTreeMap::from([
                (
                    "lifecycle_state__not_in".to_string(),
                    r#"["deleted","forgotten"]"#.to_string(),
                ),
                ("importance__gte".to_string(), "0.8".to_string()),
            ]));

        let output = store.search_candidates(&request).unwrap();

        assert!(open_report.graph_opened);
        assert!(open_report.search_projection_opened);
        assert_eq!(output.result.total_hits, 1);
        assert_eq!(output.result.hits[0].id, "memory:active");
        assert_eq!(output.report.metadata_filter_count, 2);
        assert_eq!(output.report.pushed_predicate_count, 2);
        assert_eq!(output.report.residual_predicate_count, 0);
        assert!(output.report.persisted_segment_descriptor_used);
        assert_eq!(output.report.segment_count, 2);
        assert_eq!(output.report.pruned_segment_count, 1);
        assert_eq!(output.report.scanned_segment_count, 1);
        assert_eq!(output.report.physical_range_read_count, 1);
        assert!(output.report.physical_bytes_read > 0);
        assert_eq!(output.report.filtered_out_count, 2);
        assert!(output
            .report
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries
            .iter()
            .any(|summary| summary.field == "lifecycle_state" && summary.value_summary_used));
        assert!(output
            .report
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries
            .iter()
            .any(|summary| summary.field == "importance" && summary.numeric_range_summary_used));

        let direct_evidence = store
            .search_candidate_shadow_evidence_json(&request, ["memory:active"])
            .unwrap();
        assert_eq!(direct_evidence["ready"], true);
        assert_eq!(
            direct_evidence["filter_pushdown"]["field_capabilities_ready"],
            true
        );
        assert_eq!(
            direct_evidence["filter_pushdown"]["missing_numeric_range_fields"],
            serde_json::json!([])
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_projection_candidate_api_reports_vector_generation_inputs() {
        let root = unique_nowledge_mem_test_dir("search_candidate_api_vector");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            for (external_id, lifecycle_state, embedding) in [
                ("aaa-deleted", "deleted", vec![1.0, 0.0]),
                ("aab-forgotten", "forgotten", vec![1.0, 0.0]),
                ("zza-active", "active", vec![1.0, 0.0]),
                ("zzb-other", "active", vec![0.6, 0.8]),
            ] {
                index
                    .upsert_projection_row(SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: external_id.to_string(),
                        title: format!("{external_id} vector candidate"),
                        body: "vector candidate read".to_string(),
                        embedding: Some(embedding),
                        source_id: Some("source-vector".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), lifecycle_state.to_string()),
                        ]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request = NowledgeMemSearchCandidateRequest::vector(vec![1.0, 0.0], 10)
            .with_offset(1)
            .with_rank_window(Some(2))
            .with_metadata_filters(BTreeMap::from([(
                "lifecycle_state__not_in".to_string(),
                r#"["deleted","forgotten"]"#.to_string(),
            )]));

        let output = store.search_candidates(&request).unwrap();

        assert_eq!(output.result.total_hits, 2);
        assert_eq!(output.result.offset, 1);
        assert_eq!(output.result.hits.len(), 1);
        assert_eq!(output.result.hits[0].id, "memory:zzb-other");
        assert_eq!(output.result.hits[0].vector_rank, Some(2));
        assert_eq!(output.report.mode, SearchMode::Vector);
        assert_eq!(output.report.query_embedding_dimension, Some(2));
        assert_eq!(output.report.offset, 1);
        assert_eq!(output.report.rank_window, Some(2));
        assert_eq!(
            output.report.retriever_backends.get("vector"),
            Some(&"scalar_vector_scan".to_string())
        );
        assert_eq!(output.report.retriever_available.get("vector"), Some(&true));
        assert_eq!(output.report.retriever_available.get("text"), Some(&false));
        assert_eq!(
            output.report.retriever_candidate_counts.get("vector"),
            Some(&2)
        );
        assert_eq!(
            output
                .report
                .retriever_candidate_score_sources
                .get("vector"),
            Some(&"raw_vector".to_string())
        );
        assert_eq!(
            output.report.retriever_final_score_sources.get("vector"),
            Some(&"raw_vector".to_string())
        );
        assert_eq!(output.report.pushed_predicate_count, 1);
        assert_eq!(output.report.pruned_segment_count, 1);
        assert_eq!(output.report.filtered_out_count, 2);
        assert!(output.report.persisted_segment_descriptor_used);
        assert_eq!(output.report.json()["query_embedding_dimension"], 2);
        assert_eq!(output.report.json()["offset"], 1);
        assert_eq!(
            output.report.json()["retriever_candidate_counts"]["vector"],
            2
        );
        assert_eq!(
            output.report.json()["retriever_final_score_sources"]["vector"],
            "raw_vector"
        );
        assert!(!output.report.json().to_string().contains("[1.0,0.0]"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_shadow_bridge_aggregates_text_and_vector_leg_evidence() {
        let root = unique_nowledge_mem_test_dir("search_candidate_bridge_retrievers");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "mem-leg".to_string(),
                        title: "Retriever leg candidate".to_string(),
                        body: "retriever leg candidate read".to_string(),
                        embedding: Some(vec![1.0, 0.0]),
                        source_id: Some("source-leg".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), "active".to_string()),
                        ]),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(31),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let text_request = NowledgeMemSearchCandidateRequest::text("retriever leg", 10)
            .with_metadata_filters(BTreeMap::from([(
                "lifecycle_state__not_in".to_string(),
                r#"["deleted","forgotten"]"#.to_string(),
            )]));
        let vector_request = NowledgeMemSearchCandidateRequest::vector(vec![1.0, 0.0], 10)
            .with_metadata_filters(BTreeMap::from([(
                "lifecycle_state__not_in".to_string(),
                r#"["deleted","forgotten"]"#.to_string(),
            )]));
        let text_output = projection.search_candidates_with_report(&text_request);
        let vector_output = projection.search_candidates_with_report(&vector_request);
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();

        accumulator.record_search_candidate_output(["memory:mem-leg"], &text_output);
        accumulator.record_search_candidate_output(["memory:mem-leg"], &vector_output);
        let evidence = accumulator.json();

        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["request_count"], 2);
        assert_eq!(evidence["text_retriever_ready"], true);
        assert_eq!(evidence["vector_retriever_ready"], true);
        assert_eq!(evidence["fts_top_k_overlap_ready"], true);
        assert_eq!(evidence["vector_top_k_overlap_ready"], true);
        assert_eq!(evidence["top_k_overlap_observed"]["fts"], true);
        assert_eq!(evidence["top_k_overlap_observed"]["vector"], true);
        assert_eq!(
            evidence["candidate_readiness"]["source_chunk_identity_ready"],
            false
        );
        assert_eq!(evidence["candidate_readiness"]["fail_soft_observed"], false);
        assert_eq!(
            evidence["candidate_readiness"]["projection_marker_status_visible"],
            true
        );
        assert_eq!(
            evidence["candidate_readiness"]["projection_watermark_ready"],
            true
        );
        assert_eq!(
            evidence["candidate_readiness"]["embedding_identity_ready"],
            true
        );
        assert_eq!(evidence["retriever_leg_candidate_counts"]["text"], 1);
        assert_eq!(evidence["retriever_leg_candidate_counts"]["vector"], 1);
        assert_eq!(
            evidence["filter_pushdown"]["field_capabilities_ready"],
            true
        );
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_projection_candidate_api_preserves_source_chunk_identity() {
        let root = unique_nowledge_mem_test_dir("search_candidate_api_source_chunk");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::SourceChunk,
                    external_id: "chunk-1".to_string(),
                    title: "Source chunk candidate".to_string(),
                    body: "source chunk identity candidate read".to_string(),
                    embedding: None,
                    source_id: Some("source-1".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "memory-1".to_string(),
                    title: "Memory candidate".to_string(),
                    body: "source chunk identity candidate read".to_string(),
                    embedding: None,
                    source_id: Some("source-1".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request = NowledgeMemSearchCandidateRequest::text("source chunk identity", 10)
            .with_metadata_filters(BTreeMap::from([(
                "kind__in".to_string(),
                r#"["source_chunk"]"#.to_string(),
            )]));

        let output = store.search_candidates(&request).unwrap();

        assert_eq!(output.result.total_hits, 1);
        let hit = &output.result.hits[0];
        assert_eq!(hit.id, "source_chunk:chunk-1");
        assert_eq!(hit.kind.as_deref(), Some("source_chunk"));
        assert_eq!(hit.external_id.as_deref(), Some("chunk-1"));
        assert_eq!(hit.source_id.as_deref(), Some("source-1"));
        assert_eq!(
            output.report.returned_kind_counts.get("source_chunk"),
            Some(&1)
        );
        assert_eq!(output.report.returned_missing_external_id_count, 0);
        assert_eq!(output.report.returned_missing_source_id_count, 0);
        assert_eq!(output.report.metadata_filter_count, 1);
        assert_eq!(output.report.pushed_predicate_count, 1);
        assert_eq!(
            output.report.json()["returned_kind_counts"]["source_chunk"],
            1
        );
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("source chunk identity candidate read"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_readiness_reports_lancedb_replacement_ready_shape() {
        let root = unique_nowledge_mem_test_dir("search_candidate_readiness_ready");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::SourceChunk,
                        external_id: "chunk-ready".to_string(),
                        title: "Ready source chunk".to_string(),
                        body: "ready candidate replacement body".to_string(),
                        embedding: Some(vec![1.0, 0.0]),
                        source_id: Some("source-ready".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), "active".to_string()),
                        ]),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(19),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let handle = NowledgeMemEmbeddedStoreHandle::new(NowledgeMemEmbeddedStore::new(
            graph,
            Some(projection),
        ));
        let request = NowledgeMemSearchCandidateRequest::text("ready source chunk", 10)
            .with_metadata_filters(BTreeMap::from([(
                "kind__in".to_string(),
                r#"["source_chunk"]"#.to_string(),
            )]));
        let options =
            NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
                .with_text_retriever(true)
                .with_source_chunk_identity(true)
                .with_embedding_identity("bge-m3", 2);

        let readiness = handle
            .search_candidate_readiness(&request, &options)
            .unwrap();

        assert!(readiness.ready);
        assert!(readiness.present);
        assert_eq!(
            readiness.protocol,
            NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL
        );
        assert!(readiness.metadata_pushdown_ready);
        assert!(readiness.segment_descriptor_ready);
        assert!(readiness.text_retriever_ready);
        assert!(readiness.source_chunk_identity_ready);
        assert!(readiness.projection_marker_status_visible);
        assert!(readiness.projection_watermark_ready);
        assert!(readiness.embedding_identity_ready);
        assert!(readiness.blocker_codes.is_empty());
        assert_eq!(
            readiness
                .candidate_report
                .projection_source_graph_commit_epoch,
            Some(19)
        );
        assert_eq!(
            readiness
                .candidate_report
                .projection_embedding_model
                .as_deref(),
            Some("bge-m3")
        );
        assert_eq!(
            readiness.candidate_report.projection_embedding_dimension,
            Some(2)
        );
        assert_eq!(
            readiness
                .candidate_report
                .returned_kind_counts
                .get("source_chunk"),
            Some(&1)
        );
        assert_eq!(readiness.json()["projection_watermark_ready"], true);
        assert_eq!(readiness.json()["embedding_identity_ready"], true);
        assert!(!readiness
            .json()
            .to_string()
            .contains("ready candidate replacement body"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_readiness_blocks_missing_source_chunk_identity() {
        let root = unique_nowledge_mem_test_dir("search_candidate_readiness_blocked");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "memory-only".to_string(),
                    title: "Memory only candidate".to_string(),
                    body: "memory only candidate replacement body".to_string(),
                    embedding: None,
                    source_id: Some("source-memory".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let request = NowledgeMemSearchCandidateRequest::text("memory only candidate", 10);
        let options =
            NowledgeMemSearchCandidateReadinessOptions::default().with_source_chunk_identity(true);

        let readiness = projection.search_candidate_readiness(&request, &options);

        assert!(!readiness.ready);
        assert_eq!(readiness.candidate_report.returned_hit_count, 1);
        assert!(!readiness.source_chunk_identity_ready);
        assert!(readiness
            .blocker_codes
            .iter()
            .any(|code| code == "search_candidate_source_chunk_identity_missing"));
        assert!(!readiness
            .json()
            .to_string()
            .contains("memory only candidate replacement body"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_readiness_blocks_missing_projection_watermark() {
        let root = unique_nowledge_mem_test_dir("search_candidate_readiness_watermark");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::SourceChunk,
                    external_id: "chunk-no-watermark".to_string(),
                    title: "No watermark source chunk".to_string(),
                    body: "candidate watermark body".to_string(),
                    embedding: Some(vec![1.0, 0.0]),
                    source_id: Some("source-no-watermark".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let request = NowledgeMemSearchCandidateRequest::text("no watermark source chunk", 10)
            .with_metadata_filters(BTreeMap::from([(
                "kind__in".to_string(),
                r#"["source_chunk"]"#.to_string(),
            )]));
        let options =
            NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
                .with_text_retriever(true)
                .with_source_chunk_identity(true)
                .with_embedding_identity("bge-m3", 2);

        let readiness = projection.search_candidate_readiness(&request, &options);

        assert!(!readiness.ready);
        assert!(readiness.text_retriever_ready);
        assert!(readiness.source_chunk_identity_ready);
        assert!(!readiness.projection_watermark_ready);
        assert!(readiness.embedding_identity_ready);
        assert!(readiness
            .blocker_codes
            .iter()
            .any(|code| code == "search_candidate_projection_watermark_missing"));
        assert!(!readiness
            .json()
            .to_string()
            .contains("candidate watermark body"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_readiness_lancedb_default_requires_embedding_identity() {
        let root = unique_nowledge_mem_test_dir("search_candidate_readiness_manifest_required");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::SourceChunk,
                        external_id: "chunk-no-manifest".to_string(),
                        title: "No manifest source chunk".to_string(),
                        body: "candidate manifest body".to_string(),
                        embedding: Some(vec![1.0, 0.0]),
                        source_id: Some("source-no-manifest".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), "active".to_string()),
                        ]),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(29),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let request = NowledgeMemSearchCandidateRequest::text("no manifest source chunk", 10)
            .with_metadata_filters(BTreeMap::from([(
                "kind__in".to_string(),
                r#"["source_chunk"]"#.to_string(),
            )]));
        let options =
            NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
                .with_text_retriever(true)
                .with_source_chunk_identity(true);

        let readiness = projection.search_candidate_readiness(&request, &options);

        assert!(!readiness.ready);
        assert!(readiness.projection_watermark_ready);
        assert!(!readiness.embedding_identity_ready);
        assert_eq!(readiness.candidate_report.projection_embedding_model, None);
        assert_eq!(
            readiness.candidate_report.projection_embedding_dimension,
            Some(2)
        );
        assert!(readiness
            .blocker_codes
            .iter()
            .any(|code| code == "search_candidate_embedding_identity_not_ready"));
        assert!(!readiness
            .json()
            .to_string()
            .contains("candidate manifest body"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_candidate_readiness_blocks_embedding_identity_mismatch() {
        let root = unique_nowledge_mem_test_dir("search_candidate_readiness_embedding");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::SourceChunk,
                        external_id: "chunk-embedding".to_string(),
                        title: "Embedding identity source chunk".to_string(),
                        body: "candidate embedding body".to_string(),
                        embedding: Some(vec![1.0, 0.0]),
                        source_id: Some("source-embedding".to_string()),
                        metadata: BTreeMap::from([
                            ("space_id".to_string(), "default".to_string()),
                            ("lifecycle_state".to_string(), "active".to_string()),
                        ]),
                    }],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(23),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let request =
            NowledgeMemSearchCandidateRequest::text("embedding identity source chunk", 10)
                .with_metadata_filters(BTreeMap::from([(
                    "kind__in".to_string(),
                    r#"["source_chunk"]"#.to_string(),
                )]));
        let options =
            NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read()
                .with_text_retriever(true)
                .with_source_chunk_identity(true)
                .with_embedding_identity("bge-m3", 3);

        let readiness = projection.search_candidate_readiness(&request, &options);

        assert!(!readiness.ready);
        assert!(readiness.projection_watermark_ready);
        assert!(!readiness.embedding_identity_ready);
        assert_eq!(
            readiness.candidate_report.projection_embedding_dimension,
            Some(2)
        );
        assert!(readiness
            .blocker_codes
            .iter()
            .any(|code| code == "search_candidate_embedding_identity_not_ready"));
        assert!(!readiness
            .json()
            .to_string()
            .contains("candidate embedding body"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_projection_candidate_api_reports_fail_soft_vector_fallback() {
        let root = unique_nowledge_mem_test_dir("search_candidate_api_fail_soft");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "mem-fallback".to_string(),
                    title: "Fallback candidate".to_string(),
                    body: "hybrid fallback candidate read".to_string(),
                    embedding: Some(vec![1.0, 0.0]),
                    source_id: Some("source-fallback".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request =
            NowledgeMemSearchCandidateRequest::hybrid("hybrid fallback", vec![1.0, 0.0, 0.0], 10);

        let output = store.search_candidates(&request).unwrap();

        assert_eq!(output.result.total_hits, 1);
        assert_eq!(output.result.hits[0].id, "memory:mem-fallback");
        assert_eq!(output.report.mode, SearchMode::Hybrid);
        assert_eq!(output.report.query_embedding_dimension, Some(3));
        assert_eq!(
            output.report.retriever_available.get("vector"),
            Some(&false)
        );
        assert_eq!(output.report.retriever_available.get("text"), Some(&true));
        assert_eq!(
            output.report.retriever_candidate_counts.get("text"),
            Some(&1)
        );
        assert!(output
            .report
            .fallback_reason_codes
            .iter()
            .any(|code| code == "vector_dimension_mismatch"));
        assert!(output.report.empty_reason_codes.is_empty());
        assert_eq!(
            output.report.json()["fallback_reason_codes"],
            serde_json::json!(["vector_dimension_mismatch"])
        );
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("hybrid fallback candidate read"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn search_projection_candidate_api_gates_compressed_vector_preference() {
        let mut index = SearchIndex::in_memory();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 2,
            })
            .unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "mem-advisor".to_string(),
                title: "Advisor gated candidate".to_string(),
                body: "advisor gated compressed candidate".to_string(),
                embedding: Some(vec![1.0, 0.0]),
                source_id: Some("source-advisor".to_string()),
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(index);
        let request = NowledgeMemSearchCandidateRequest::vector(vec![1.0, 0.0], 10)
            .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred);

        let output = projection.search_candidates_with_report(&request);

        assert_eq!(
            output.report.compressed_vector_search_mode,
            CompressedVectorSearchMode::Disabled
        );
        assert_eq!(
            output.report.requested_compressed_vector_search_mode,
            CompressedVectorSearchMode::Preferred
        );
        assert_eq!(
            output
                .report
                .retriever_backend_selection_reasons
                .get("vector")
                .map(String::as_str),
            Some("compression_disabled")
        );
        assert_eq!(
            output.report.retriever_backends.get("vector"),
            Some(&"scalar_vector_scan".to_string())
        );
        assert_eq!(
            output.report.retrieval_projection_advisor_blocker_codes,
            vec![
                "retrieval_projection_recall_evidence_missing".to_string(),
                "retrieval_projection_parity_evidence_missing".to_string(),
                "retrieval_projection_segment_not_advised".to_string()
            ]
        );
        assert_eq!(
            output.report.json()["retrieval_projection_advisor"]["ready"],
            false
        );
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("advisor gated compressed candidate"));
    }

    #[test]
    fn search_projection_candidate_api_reports_repair_markers_without_hits() {
        let root = unique_nowledge_mem_test_dir("search_candidate_api_repair_markers");
        {
            let mut index = SearchIndex::open(&root).unwrap();
            index
                .upsert_projection_row(SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "mem-marker".to_string(),
                    title: "Marker candidate".to_string(),
                    body: "repair marker candidate read".to_string(),
                    embedding: None,
                    source_id: Some("source-marker".to_string()),
                    metadata: BTreeMap::from([
                        ("space_id".to_string(), "default".to_string()),
                        ("lifecycle_state".to_string(), "active".to_string()),
                    ]),
                })
                .unwrap();
            index.mark_full_reindex_needed("stale projection").unwrap();
            index
                .mark_metadata_repair_needed("missing metadata")
                .unwrap();
            index.checkpoint().unwrap();
        }
        let projection = NowledgeMemSearchProjection::open(&root).unwrap();
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));
        let request = NowledgeMemSearchCandidateRequest::text("not present", 10);

        let output = store.search_candidates(&request).unwrap();

        assert_eq!(output.result.total_hits, 0);
        assert!(output.report.projection_full_reindex_needed);
        assert!(output.report.projection_metadata_repair_needed);
        assert!(output
            .report
            .empty_reason_codes
            .iter()
            .any(|code| code == "retriever_no_hits"));
        assert_eq!(output.report.json()["projection_full_reindex_needed"], true);
        assert_eq!(
            output.report.json()["projection_metadata_repair_needed"],
            true
        );
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("stale projection"));
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("missing metadata"));
        assert!(!output
            .report
            .json()
            .to_string()
            .contains("repair marker candidate read"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_open_options_can_prefer_compressed_vector_search() {
        let root = unique_nowledge_mem_test_dir("compressed_vector_open_options");
        let graph_path = root.join("graph");
        let search_path = root.join("search");
        {
            let mut db = Database::open(&graph_path).unwrap();
            db.query(
                "CREATE (:Memory {id: 'mem-vector', title: 'Vector facade', content: 'Compressed vector retrieval'})",
            )
            .unwrap();
            db.checkpoint().unwrap();
        }
        {
            let mut index = SearchIndex::open(&search_path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: None,
                    dimension: 8,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: vec![
                        SearchProjectionRow {
                            kind: SearchProjectionKind::Memory,
                            external_id: "mem-vector".to_string(),
                            title: "Vector facade".to_string(),
                            body: "Compressed vector retrieval".to_string(),
                            embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                            source_id: None,
                            metadata: BTreeMap::new(),
                        },
                        SearchProjectionRow {
                            kind: SearchProjectionKind::Memory,
                            external_id: "mem-vector-neighbor".to_string(),
                            title: "Vector neighbor".to_string(),
                            body: "Recall validation neighbor".to_string(),
                            embedding: Some(vec![0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                            source_id: None,
                            metadata: BTreeMap::new(),
                        },
                    ],
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(1),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let recall_report = SearchIndex::open(&search_path)
            .unwrap()
            .validate_sampled_vector_recall(VectorRecallValidationOptions {
                max_samples: 2,
                top_k: 1,
                candidate_limit: 1,
                minimum_recall_per_million: 1_000_000,
                metadata_filters: BTreeMap::new(),
            });
        assert!(recall_report.ready, "{:?}", recall_report.blocker_codes);
        let options = NowledgeMemOpenOptions::with_search_projection(
            graph_path,
            search_path,
            NowledgeMemGraphMode::ShadowReadOnly,
        )
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Preferred)
        .with_adaptive_vector_backend_policy(AdaptiveVectorBackendPolicy {
            flat_scan_max_documents: 0,
            high_filter_selectivity_per_million: u32::MAX,
            flat_scan_memory_budget_bytes: 0,
        })
        .with_retrieval_projection_advisor(
            NowledgeMemRetrievalProjectionAdvisor::cold_local_with_recall_parity(&recall_report),
        );

        let (store, report) = NowledgeMemEmbeddedStore::open_with_options(options).unwrap();
        let retrieval = store
            .retrieve_knowledge_with_report(&KnowledgeRetrievalRequest {
                query_text: String::new(),
                query_embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                mode: SearchMode::Vector,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 0,
                graph_context_limit: 0,
                graph_context_max_hops: 0,
            })
            .unwrap();
        let output = retrieval.output;

        assert_eq!(
            report.compressed_vector_search_mode,
            CompressedVectorSearchMode::Preferred
        );
        assert_eq!(
            report.requested_compressed_vector_search_mode,
            CompressedVectorSearchMode::Preferred
        );
        assert!(report.retrieval_projection_advisor.ready());
        assert_eq!(output.search.hits[0].id, "memory:mem-vector");
        assert_eq!(
            output.search.retrievers[0].backend,
            "skein_turboquant_candidate_projection"
        );
        assert_eq!(
            retrieval.report.compressed_vector_search_mode,
            CompressedVectorSearchMode::Preferred
        );
        assert_eq!(
            retrieval.report.vector_backend,
            Some("skein_turboquant_candidate_projection".to_string())
        );
        assert_eq!(
            retrieval.report.json()["vector_backend"],
            "skein_turboquant_candidate_projection"
        );

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn embedded_store_retrieval_requires_search_projection() {
        let graph =
            NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::ShadowReadOnly);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let error = store
            .retrieve_knowledge(&KnowledgeRetrievalRequest {
                query_text: "missing projection".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 4,
                graph_context_limit: 4,
                graph_context_max_hops: 1,
            })
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "storage error: nowledge mem search projection is not configured"
        );
    }

    #[test]
    fn embedded_store_reports_background_maintenance_summary() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        graph
            .query("CREATE (:Memory {id: 'mem-maintenance', title: 'Maintenance summary'})")
            .unwrap();
        let projection = NowledgeMemSearchProjection::from_index(SearchIndex::in_memory());
        let store = NowledgeMemEmbeddedStore::new(graph, Some(projection));

        let summary = store.background_maintenance_summary(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            BackgroundMaintenanceOptions {
                include_schema_maintenance: false,
                include_property_index_projection: false,
                include_search_projection_rebuild: false,
                include_search_projection_metadata_repair: false,
                include_skein_lightning_bootstrap_export: false,
                include_external_content_artifact_jobs: false,
                ..BackgroundMaintenanceOptions::default()
            },
        );

        assert_eq!(summary.total_candidates, 1);
        assert_eq!(summary.admitted_count, 1);
        assert_eq!(
            summary.top_admitted_kind,
            Some(BackgroundMaintenanceKind::SearchProjectionGraphDelta)
        );
        assert_eq!(summary.executable_search_projection_graph_delta_count, 1);
        assert_eq!(summary.admitted_search_projection_graph_delta_count, 1);
        let item = &summary.ranked[0];
        assert_eq!(
            summary.max_search_projection_graph_delta_complete_through_graph_commit_epoch,
            item.search_projection_graph_delta_complete_through_graph_commit_epoch
        );
        assert!(summary
            .max_search_projection_graph_delta_complete_through_graph_commit_epoch
            .is_some());
        assert_eq!(item.name, "search_projection_graph_delta");
        assert_eq!(item.admission_name, "admit");
        assert_eq!(
            item.search_projection_graph_delta_upsert_node_count,
            Some(1)
        );
        assert_eq!(
            item.search_projection_graph_delta_delete_document_count,
            Some(0)
        );

        let report = store.background_maintenance_report(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            BackgroundMaintenanceOptions {
                include_schema_maintenance: false,
                include_property_index_projection: false,
                include_search_projection_rebuild: false,
                include_search_projection_metadata_repair: false,
                include_skein_lightning_bootstrap_export: false,
                include_external_content_artifact_jobs: false,
                ..BackgroundMaintenanceOptions::default()
            },
        );
        let json = report.json();

        assert_eq!(report.protocol, "skein-background-maintenance-report");
        assert!(report.present);
        assert!(report.ready);
        assert_eq!(report.total_candidates, 1);
        assert_eq!(report.ranked_count, 1);
        assert_eq!(report.foreground_ranked_count, 0);
        assert_eq!(report.unknown_admission_count, 0);
        assert_eq!(report.executable_search_projection_graph_delta_count, 1);
        assert_eq!(report.admitted_search_projection_graph_delta_count, 1);
        assert_eq!(report.slow_query_ready, Some(true));
        assert_eq!(report.slow_query_record_count, Some(0));
        assert_eq!(report.slow_query_capacity, Some(256));
        assert_eq!(report.slow_query_redaction_ready, Some(true));
        assert_eq!(report.memory_pressure_ready, Some(true));
        assert!(report.memory_budget_bytes.is_some());
        assert!(report.estimated_memory_bytes.is_some());
        assert!(report.blocker_codes.is_empty());
        assert_eq!(json["protocol"], "skein-background-maintenance-report");
        assert_eq!(json["memory_pressure"]["ready"], true);
        assert!(json["memory_pressure"]["budget_bytes"].as_u64().is_some());
        assert!(json["memory_pressure"]["estimated_bytes"]
            .as_u64()
            .is_some());
        assert_eq!(json["slow_query"]["ready"], true);
        assert_eq!(json["slow_query"]["record_count"], 0);
        assert_eq!(json["slow_query"]["capacity"], 256);
        assert_eq!(json["ranked"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn embedded_store_background_maintenance_report_fails_closed_without_work() {
        let db = Database::new();
        let graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let store = NowledgeMemEmbeddedStore::new(graph, None);

        let report = store.background_maintenance_report(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            BackgroundMaintenanceOptions {
                include_schema_maintenance: false,
                include_property_index_projection: false,
                include_search_projection_graph_delta_freshness: false,
                include_search_projection_rebuild: false,
                include_search_projection_metadata_repair: false,
                include_skein_lightning_bootstrap_export: false,
                include_external_content_artifact_jobs: false,
                ..BackgroundMaintenanceOptions::default()
            },
        );

        assert!(!report.ready);
        assert_eq!(report.total_candidates, 0);
        assert_eq!(report.ranked_count, 0);
        assert_eq!(
            report.blocker_codes,
            vec!["no_candidates".to_string(), "no_ranked_work".to_string()]
        );
    }

    fn nowledge_projection_evidence_rows() -> Vec<SearchProjectionRow> {
        vec![
            nowledge_projection_evidence_row(SearchProjectionKind::Memory, "mem_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::Message, "msg_1", false),
            nowledge_projection_evidence_row(SearchProjectionKind::Community, "community_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::Entity, "entity_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::Source, "source_1", true),
            nowledge_projection_evidence_row(SearchProjectionKind::SourceChunk, "chunk_1", true),
        ]
    }

    fn persisted_nowledge_projection_evidence_index(name: &str) -> SearchIndex {
        let path = unique_nowledge_mem_test_dir(name);
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 8,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: nowledge_projection_evidence_rows(),
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(17),
            })
            .unwrap();
        index.checkpoint().unwrap();
        SearchIndex::open(path).unwrap()
    }

    fn nowledge_projection_evidence_row(
        kind: SearchProjectionKind,
        external_id: &str,
        include_embedding: bool,
    ) -> SearchProjectionRow {
        SearchProjectionRow {
            kind,
            external_id: external_id.to_string(),
            title: format!("{external_id} title"),
            body: format!("{external_id} body"),
            embedding: include_embedding.then_some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            source_id: Some("source_1".to_string()),
            metadata: BTreeMap::from([
                ("space_id".to_string(), "default".to_string()),
                ("unit_type".to_string(), "fact".to_string()),
                ("lifecycle_state".to_string(), "active".to_string()),
                ("importance".to_string(), "0.8".to_string()),
                ("confidence".to_string(), "0.9".to_string()),
                ("created_at".to_string(), "11".to_string()),
                ("updated_at".to_string(), "12".to_string()),
                ("event_start".to_string(), "10".to_string()),
                ("event_end".to_string(), "20".to_string()),
                ("is_latest".to_string(), "true".to_string()),
            ]),
        }
    }

    fn full_bounded_read_routes() -> Vec<String> {
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| (*route).to_string())
            .collect()
    }

    fn ready_route_readiness_summary() -> NowledgeMemRouteReadinessSummary {
        NowledgeMemRouteReadinessSummary {
            route_primary_ready: true,
            primary_ready_routes: full_bounded_read_routes(),
            route_query_plan_evidence_ready: true,
            route_query_profile_evidence_ready: true,
            route_query_api_behavior_evidence_ready: true,
            relationship_property_pruning_required_count: 0,
            relationship_property_pruning_report_count: 0,
            route_relationship_property_pruning_evidence_ready: true,
        }
    }

    fn ready_search_route_ownership() -> NowledgeMemSearchRouteOwnershipReadinessReport {
        nowledge_mem_search_route_ownership_readiness(
            &nowledge_mem_search_route_ownership_all_skein(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        )
    }

    fn ready_active_search_route_ownership() -> NowledgeMemActiveSearchRouteOwnershipReadinessReport
    {
        nowledge_mem_active_search_route_ownership_readiness(
            &nowledge_mem_active_search_route_ownership_all_skein(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        )
    }

    fn ready_active_search_route_readiness() -> NowledgeMemActiveSearchRouteReadinessReport {
        nowledge_mem_active_search_route_readiness(
            &nowledge_mem_active_search_route_read_evidence_all_skein_ready(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        )
    }

    fn ready_query_family_replacement() -> serde_json::Value {
        serde_json::json!(REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES
            .iter()
            .map(|family| serde_json::json!({
                "query_family": family,
                "required_checks": 1,
                "covered_checks": 1,
                "shadow_matched_checks": 1,
                "replacement_readiness_per_million": 1_000_000,
            }))
            .collect::<Vec<_>>())
    }

    fn readiness_dashboard_area<'a>(
        dashboard: &'a NowledgeMemReadinessDashboard,
        name: &str,
    ) -> &'a NowledgeMemReadinessAreaSummary {
        dashboard
            .areas
            .iter()
            .find(|area| area.name == name)
            .expect("readiness dashboard area")
    }

    fn unique_nowledge_mem_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_nowledge_mem_{name}_{}_{}",
            std::process::id(),
            nanos
        ))
    }
}
