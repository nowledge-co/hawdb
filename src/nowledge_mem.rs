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

use crate::search::{
    AdaptiveVectorSearchOptions, CompressedVectorSearchMode, SearchFallbackReasonCode,
    SearchLexicalProductionQualificationReport, SearchOutOfCoreConfig,
    SearchOutOfCoreHydrationOutput, SearchOutOfCoreMetrics, SearchOutOfCoreReader,
    SearchQueryOptions, SearchRangeReadConfig, VectorRecallProductionQualificationReport,
    VectorRecallValidationOptions, VectorRecallValidationReport, VectorSearchExecutionOptions,
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
};
use crate::search_projection_evidence::{
    nowledge_search_projection_evidence_json, nowledge_search_projection_shadow_evidence_json,
    NowledgeSearchProjectionEvidenceReport,
};
use crate::{
    cypher, BackgroundMaintenanceOptions, BackgroundMaintenanceSummary, BackgroundWorkHint,
    BackgroundWorkPlan, BoundedReadQueryOutput, Database, DatabaseConfig, DatabaseReadTransaction,
    GraphRagGeneratedQuery, GraphRagSchemaContext, GraphRagSchemaContextOptions, HawDBError,
    HawDBLightningBootstrapManifest, HawDBLightningInitialImportApplyReport,
    HawDBLightningInitialImportCheckpoint, HawDBLightningInitialImportCutoverCatchUpReport,
    HawDBLightningInitialImportDocumentIdentity,
    HawDBLightningInitialImportRecoveryReadinessReport, KnowledgeRetrievalOutput,
    KnowledgeRetrievalRequest, LocalQosPolicy, LocalQosState, NowledgeGraphStatement,
    PlanCacheLookup, QueryOutput, QueryStreamOptions, QueryStreamReport, ReadBudgetResource,
    ReadExecutionProfile, Result, ScheduledSearchProjectionCatchUpReport, SearchDocument,
    SearchIndex, SearchProjectionCatchUpReport, SearchProjectionChangeBatch,
    SearchProjectionChangefeedReadiness, SearchProjectionDelta, SearchProjectionDeltaReport,
    SearchProjectionFreshness, SearchProjectionGraphDeltaRequest, SearchProjectionProbeOptions,
    SearchProjectionRelationalDelta, SearchResultSet, StorageResourceProfileLimits,
    StorageResourceProfileReport, TelemetrySink, Value, STORAGE_RESOURCE_PROFILE_PROTOCOL,
};
use crate::{
    graph_route_readiness::NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
    nowledge_inventory::{
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
    workload_fixtures::{
        NowledgeGraphRouteWorkloadFixtureReport, NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
    },
};
use hawdb_core::RuntimeTaskContext;
mod concurrent_owner;
mod incremental_search;
mod read_execution;
mod resident_read;
mod search_read;
use resident_read::ResidentProjectionRef;
mod handle;
mod registered_search;
use handle::*;
pub use handle::{NowledgeMemEmbeddedStoreHandle, NowledgeMemReadSnapshot};
use hawdb_optimizer::AdaptiveVectorBackendPolicy;
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeGovernorSnapshot,
    RuntimePermit, RuntimeWorkKind, RuntimeWorkPriority, RuntimeWorkRequest, StorageDeviceProfile,
    WorkPriority,
};
pub use hawdb_readiness::bounded_read_evidence::{
    nowledge_mem_bounded_read_evidence_json,
    nowledge_mem_bounded_read_evidence_json_with_route_readiness,
    nowledge_mem_bounded_read_evidence_json_with_routes, NowledgeMemGraphMode,
    NowledgeMemReadReport, NowledgeMemRouteReadinessSummary,
    NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL, NOWLEDGE_MEM_READ_REPORT_PROTOCOL,
};
use hawdb_readiness::nowledge_mem_query_report::nowledge_mem_plan_cache_report;
pub use hawdb_readiness::nowledge_mem_query_report::{
    nowledge_mem_fast_path_classification, NowledgeMemFastPathClassification,
    NowledgeMemQueryApiBehavior, NowledgeMemQueryExecutionPath, NowledgeMemQueryOutput,
    NowledgeMemQueryOutputRowShape, NowledgeMemQueryReport, NowledgeMemQueryReportOptions,
    NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL,
};
pub use hawdb_readiness::nowledge_mem_reports::{
    NowledgeMemBackgroundMaintenanceReport, NowledgeMemLibraryProductionPathSummary,
    NowledgeMemLibraryReadinessReport, NowledgeMemOperationsReadinessReport,
    NowledgeMemReadinessDashboard, NowledgeMemReadinessRedactionSummary,
    NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL,
};
pub use hawdb_readiness::query_runtime_preflight::NowledgeQueryRuntimePreflightProbe;
use hawdb_readiness::query_runtime_preflight_report::{
    hawdb_error_class, nowledge_query_runtime_route_coverage,
    query_runtime_preflight_blocker_codes, query_runtime_probe_blocker_codes,
};
pub use hawdb_readiness::query_runtime_preflight_report::{
    NowledgeQueryRuntimePreflightProbeReport, NowledgeQueryRuntimePreflightRedactionSummary,
    NowledgeQueryRuntimePreflightReport,
};
pub use hawdb_readiness::slow_query::{
    NowledgeMemSlowQueryRecord, NowledgeMemSlowQueryReport, NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL,
};
pub use hawdb_readiness::{NowledgeMemReadinessAreaMap, NowledgeMemReadinessAreaSummary};

pub use incremental_search::NowledgeMemIncrementalSearchMaintenanceOptions;
pub use registered_search::{
    NowledgeMemDefinedSearchProjectionConsumer, NowledgeMemPreparedConsumerUpdate,
    NowledgeMemSearchProjectionConsumer,
};

#[cfg(test)]
mod slow_query_facade_tests {
    use super::*;
    use std::any::TypeId;

    #[test]
    fn facade_reexports_slow_query_contracts_without_conversion() {
        assert_eq!(
            TypeId::of::<NowledgeMemSlowQueryRecord>(),
            TypeId::of::<hawdb_readiness::slow_query::NowledgeMemSlowQueryRecord>(),
        );
        assert_eq!(
            TypeId::of::<NowledgeMemSlowQueryReport>(),
            TypeId::of::<hawdb_readiness::slow_query::NowledgeMemSlowQueryReport>(),
        );
        assert_eq!(
            NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL,
            hawdb_readiness::slow_query::NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL,
        );
    }
}
#[cfg(test)]
mod readiness_report_facade_tests {
    use hawdb_readiness::nowledge_mem_reports as owner;
    use std::any::TypeId;

    #[test]
    fn facade_reexports_readiness_reports_without_conversion() {
        macro_rules! same_report {
            ($report:ident) => {
                assert_eq!(
                    TypeId::of::<super::$report>(),
                    TypeId::of::<owner::$report>()
                );
                assert_eq!(
                    TypeId::of::<crate::$report>(),
                    TypeId::of::<owner::$report>()
                );
            };
        }
        same_report!(NowledgeMemLibraryReadinessReport);
        same_report!(NowledgeMemLibraryProductionPathSummary);
        same_report!(NowledgeMemReadinessRedactionSummary);
        same_report!(NowledgeMemReadinessDashboard);
        same_report!(NowledgeMemOperationsReadinessReport);
        same_report!(NowledgeMemBackgroundMaintenanceReport);
        assert_eq!(
            super::NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL,
            owner::NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL
        );
    }
}

use hawdb_search::candidate_evidence::{
    advised_compressed_vector_search_mode, effective_search_candidate_mode,
    nowledge_mem_search_candidate_report, retrieval_projection_advisor_blocker_codes,
    retrieval_projection_advisor_json,
};
pub use hawdb_search::candidate_evidence::{
    nowledge_mem_search_candidate_shadow_evidence_json, NowledgeMemRetrievalProjectionAdvisor,
    NowledgeMemSearchCandidateFieldSummary, NowledgeMemSearchCandidateFilterPushdownEvidence,
    NowledgeMemSearchCandidateOutput, NowledgeMemSearchCandidateReadinessOptions,
    NowledgeMemSearchCandidateReadinessReport, NowledgeMemSearchCandidateReport,
    NowledgeMemSearchCandidateRequest, NowledgeMemSearchCandidateShadowAccumulator,
    NowledgeMemSearchCandidateShadowEvidence, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE, NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE,
};
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

#[cfg(test)]
#[path = "nowledge_mem_app_read_snapshot_tests.rs"]
mod app_read_snapshot_tests;
#[cfg(test)]
mod candidate_facade_tests;

#[cfg(test)]
mod runtime_status_facade_tests {
    use super::{
        NowledgeMemCutoverControls, NowledgeMemCutoverControlsReport, NowledgeMemProductionStatus,
        NowledgeMemReadControl, NowledgeMemRuntimeStatus, NowledgeMemWorkControl,
    };
    use std::any::TypeId;

    #[test]
    fn root_facade_preserves_runtime_status_type_identity() {
        assert_eq!(
            TypeId::of::<NowledgeMemRuntimeStatus>(),
            TypeId::of::<hawdb_readiness::nowledge_mem_runtime_status::NowledgeMemRuntimeStatus>()
        );
        assert_eq!(
            TypeId::of::<NowledgeMemProductionStatus>(),
            TypeId::of::<hawdb_readiness::nowledge_mem_runtime_status::NowledgeMemProductionStatus>(
            )
        );
        assert_eq!(
            TypeId::of::<NowledgeMemReadControl>(),
            TypeId::of::<hawdb_readiness::nowledge_mem_cutover::NowledgeMemReadControl>()
        );
        assert_eq!(
            TypeId::of::<NowledgeMemWorkControl>(),
            TypeId::of::<hawdb_readiness::nowledge_mem_cutover::NowledgeMemWorkControl>()
        );
        assert_eq!(
            TypeId::of::<NowledgeMemCutoverControls>(),
            TypeId::of::<hawdb_readiness::nowledge_mem_cutover::NowledgeMemCutoverControls>()
        );
        assert_eq!(
            TypeId::of::<NowledgeMemCutoverControlsReport>(),
            TypeId::of::<hawdb_readiness::nowledge_mem_cutover::NowledgeMemCutoverControlsReport>()
        );
    }
}

pub use hawdb_readiness::nowledge_mem_serving_path::{
    NowledgeMemServingEntrypoint, NowledgeMemServingPathReadiness,
    NOWLEDGE_MEM_SERVING_PATH_READINESS_PROTOCOL,
};

const TYPED_CONTROL_STATEMENT_MEMORY_BYTES: u64 = 1024 * 1024;
const SEARCH_PROJECTION_CHANGEFEED_OPERATION_BYTES: usize = 1024;

pub fn nowledge_mem_graph_config(mode: NowledgeMemGraphMode) -> DatabaseConfig {
    nowledge_mem_graph_config_with_search_mode(mode, CompressedVectorSearchMode::Disabled)
}

pub fn nowledge_mem_graph_config_with_search_mode(
    mode: NowledgeMemGraphMode,
    compressed_vector_search_mode: CompressedVectorSearchMode,
) -> DatabaseConfig {
    DatabaseConfig {
        read_only: matches!(mode, NowledgeMemGraphMode::ShadowReadOnly),
        recovery_mode: if matches!(mode, NowledgeMemGraphMode::ShadowReadOnly) {
            crate::RecoveryMode::Strict
        } else {
            crate::RecoveryMode::default()
        },
        compressed_vector_search_mode,
        ..DatabaseConfig::default()
    }
}

fn default_nowledge_mem_runtime_governor(
    path: &Path,
    database_config: &DatabaseConfig,
) -> RuntimeGovernor {
    let storage_device = StorageDeviceProfile::detect(path);
    let mut governor_config = RuntimeGovernorConfig::shared_host();
    if let Some(max_payload_bytes) = database_config.max_read_result_payload_bytes {
        governor_config.result_budget_bytes = u64::try_from(max_payload_bytes).unwrap_or(u64::MAX);
    }
    RuntimeGovernor::detect(
        governor_config,
        IoConcurrencyBudget::shared_host_for_device(storage_device),
    )
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
    IncrementalMaintenance(Box<NowledgeMemIncrementalSearchMaintenanceOptions>),
    QualifiedOutOfCore(Box<NowledgeMemQualifiedOutOfCoreSearchOptions>),
}

impl NowledgeMemSearchProjectionOpenMode {
    fn role(&self) -> NowledgeMemSearchProjectionRole {
        match self {
            Self::FullResidencyMaintenance => {
                NowledgeMemSearchProjectionRole::FullResidencyMaintenance
            }
            Self::IncrementalMaintenance(_) => {
                NowledgeMemSearchProjectionRole::IncrementalMaintenance
            }
            Self::QualifiedOutOfCore(_) => NowledgeMemSearchProjectionRole::QualifiedOutOfCore,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemSearchProjectionRole {
    FullResidencyMaintenance,
    IncrementalMaintenance,
    QualifiedOutOfCore,
}

impl NowledgeMemSearchProjectionRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullResidencyMaintenance => "full_residency_maintenance",
            Self::IncrementalMaintenance => "incremental_maintenance",
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
    /// An explicit namespace durability anchor also applies to the search
    /// projection and must contain both paths. The host must already guarantee
    /// the anchor's namespace durability.
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
        if config.read_only {
            config.recovery_mode = crate::RecoveryMode::Strict;
        }
        config.compressed_vector_search_mode = self.effective_compressed_vector_search_mode();
        config.adaptive_vector_backend_policy = self.adaptive_vector_backend_policy;
        config
    }

    fn validate(&self) -> Result<()> {
        match (
            self.search_projection_path.as_ref(),
            &self.search_projection_open_mode,
        ) {
            (None, NowledgeMemSearchProjectionOpenMode::IncrementalMaintenance(_)) => {
                return Err(HawDBError::Semantic(
                    "incremental search maintenance requires search_projection_path".into(),
                ));
            }
            (Some(_), NowledgeMemSearchProjectionOpenMode::IncrementalMaintenance(options)) => {
                options.validate()?;
                if self.search_range_read_config.is_some() {
                    return Err(HawDBError::Semantic(
                        "incremental search maintenance uses its typed reader configuration".into(),
                    ));
                }
            }
            (None, NowledgeMemSearchProjectionOpenMode::QualifiedOutOfCore(_)) => {
                return Err(HawDBError::Semantic(
                    "qualified out-of-core search requires search_projection_path".to_string(),
                ));
            }
            (Some(_), NowledgeMemSearchProjectionOpenMode::QualifiedOutOfCore(qualified)) => {
                qualified.expected_identity.validate()?;
                if self.search_range_read_config.is_some() {
                    return Err(HawDBError::Semantic(
                        "search_range_read_config applies only to the full-residency maintenance projection"
                            .to_string(),
                    ));
                }
            }
            _ => {}
        }
        if self.search_projection_path.is_none() && self.search_range_read_config.is_some() {
            return Err(HawDBError::Semantic(
                "search_range_read_config requires search_projection_path".to_string(),
            ));
        }
        let mut schema_owners = BTreeSet::new();
        for registry in &self.system_schema_registries {
            if !schema_owners.insert(registry.owner()) {
                return Err(HawDBError::Semantic(format!(
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
                retrieval_projection_advisor_blocker_codes(&self.retrieval_projection_advisor)
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
            "retrieval_projection_advisor": retrieval_projection_advisor_json(&self.retrieval_projection_advisor),
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

pub use hawdb_readiness::nowledge_mem_runtime_status::{
    NowledgeMemProductionStatus, NowledgeMemRuntimeStatus, NOWLEDGE_MEM_PRODUCTION_STATUS_PROTOCOL,
    NOWLEDGE_MEM_RUNTIME_STATUS_PROTOCOL,
};

pub use hawdb_readiness::nowledge_mem_cutover::{
    NowledgeMemCutoverControls, NowledgeMemCutoverControlsReport, NowledgeMemReadControl,
    NowledgeMemWorkControl,
};

pub use hawdb_nowledge_contracts::{
    NowledgeMemReadSnapshotBudget, NowledgeMemReadSnapshotReport,
    NowledgeMemStorageLifecycleActionKind, NowledgeMemStorageLifecycleDecision,
    NowledgeMemStorageRecoveryReport, NOWLEDGE_MEM_READ_SNAPSHOT_REPORT_PROTOCOL,
    NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL,
};

#[cfg(test)]
mod storage_lifecycle_facade_tests {
    use super::*;
    use std::any::TypeId;

    #[test]
    fn facade_reexports_storage_lifecycle_contracts_without_conversion() {
        assert_eq!(
            TypeId::of::<NowledgeMemStorageRecoveryReport>(),
            TypeId::of::<hawdb_nowledge_contracts::NowledgeMemStorageRecoveryReport>(),
        );
        assert_eq!(
            TypeId::of::<NowledgeMemStorageLifecycleDecision>(),
            TypeId::of::<hawdb_nowledge_contracts::NowledgeMemStorageLifecycleDecision>(),
        );
        assert_eq!(
            TypeId::of::<NowledgeMemStorageLifecycleActionKind>(),
            TypeId::of::<hawdb_nowledge_contracts::NowledgeMemStorageLifecycleActionKind>(),
        );
    }

    #[test]
    fn facade_reexports_read_snapshot_contracts_without_conversion() {
        assert_eq!(
            TypeId::of::<NowledgeMemReadSnapshotBudget>(),
            TypeId::of::<hawdb_nowledge_contracts::NowledgeMemReadSnapshotBudget>(),
        );
        assert_eq!(
            TypeId::of::<NowledgeMemReadSnapshotReport>(),
            TypeId::of::<hawdb_nowledge_contracts::NowledgeMemReadSnapshotReport>(),
        );
        assert_eq!(
            NOWLEDGE_MEM_READ_SNAPSHOT_REPORT_PROTOCOL,
            hawdb_nowledge_contracts::NOWLEDGE_MEM_READ_SNAPSHOT_REPORT_PROTOCOL,
        );
    }
}

pub use hawdb_readiness::source_mutation::{
    nowledge_mem_source_mutation_dual_write_evidence_all_ready,
    nowledge_mem_source_mutation_dual_write_readiness,
    nowledge_mem_source_mutation_family_requirements, NowledgeMemSourceMutationDualWriteEvidence,
    NowledgeMemSourceMutationDualWriteReadinessReport, NowledgeMemSourceMutationFamilyRequirement,
    NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_CONTENT_REFRESH_REPARSE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_GRAPH_DELETE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INDEXED_TRANSITION,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_LIFECYCLE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_PATCH_DELETE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_REVISION_EDGES,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_SEARCH_PROJECTION_EFFECTS,
    REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES,
};

pub const NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL: &str = "hawdb-nowledge-mem-open-report";
pub use hawdb_readiness::previous_wrapper_preflight::{
    NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL, NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL,
    NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL, NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL,
};
pub const NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL: &str = "hawdb-nowledge-mem-retrieval-report";

pub use hawdb_route_ownership::graph::{
    nowledge_mem_graph_read_route_catalog_digest, nowledge_mem_graph_read_route_spec,
    nowledge_mem_graph_read_route_spec_json, nowledge_mem_graph_read_route_specs_json,
    nowledge_mem_required_query_families_for_route, NowledgeMemGraphReadRouteEvidenceKind,
    NowledgeMemGraphReadRouteOwner, NowledgeMemGraphReadRouteSpec,
    NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION, NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS,
    NOWLEDGE_MEM_SEARCH_ROUTE, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};

pub const DEFAULT_NOWLEDGE_MEM_READ_MAX_ROWS: usize = 512;
pub const DEFAULT_NOWLEDGE_MEM_READ_MAX_ESTIMATED_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

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
pub struct NowledgeMemReadOutput {
    pub output: QueryOutput,
    pub report: NowledgeMemReadReport,
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
        self.query_request_with_report_and_external(
            crate::QueryRequest::new(cypher)
                .with_params(parameters)
                .with_task_context(task_context),
            options,
            external,
        )
    }

    pub(crate) fn query_request_with_report(
        &mut self,
        request: crate::QueryRequest<'_>,
        options: NowledgeMemQueryReportOptions,
    ) -> Result<NowledgeMemQueryOutput> {
        self.query_request_with_report_and_external(
            request,
            options,
            &mut crate::executor::NoExternalReadOperator,
        )
    }

    fn query_request_with_report_and_external(
        &mut self,
        request: crate::QueryRequest<'_>,
        options: NowledgeMemQueryReportOptions,
        external: &mut dyn crate::executor::ExternalReadOperator,
    ) -> Result<NowledgeMemQueryOutput> {
        let default_context = RuntimeTaskContext::default();
        let task_context = request.task_context.unwrap_or(&default_context);
        let empty = BTreeMap::new();
        let parameters = request.parameters.unwrap_or(&empty);
        let cypher = request.cypher;
        self.check_runtime_context(task_context)?;
        let (permit, prepared) = self.admit_materialized_query(cypher, parameters)?;
        let is_mutation = prepared.admission().is_mutation;
        let execution_task_context = permit.bind_task_context(task_context.clone());
        let started = Instant::now();
        let result = self.db.query_prepared_request_with_trace_and_external(
            prepared,
            request.with_task_context(&execution_task_context),
            options.capture_physical_plan,
            external,
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
    ) -> Result<(RuntimePermit, crate::api::PreparedRuntimeQuery)> {
        let prepared = self.prepare_admitted_query(cypher, parameters)?;
        let admission = prepared.admission();
        let result_budget = read_execution::materialized_result_budget(
            self.db.config(),
            &self.runtime_governor,
            admission.is_mutation,
        )?;
        let request = admission
            .runtime_work_request_for_snapshot(result_budget, self.runtime_governor.snapshot())
            .with_retained_memory_bytes(prepared.planning_memory_bytes());
        read_execution::admit_reusable_prepared(prepared, &self.runtime_governor, request, || {
            self.db
                .prepare_runtime_query(cypher.to_string(), parameters)
        })
    }

    fn prepare_admitted_query(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<crate::api::PreparedRuntimeQuery> {
        let request = crate::api::runtime_planning_request(
            cypher.len(),
            hawdb_qos::RuntimeWorkPriority::Foreground,
        );
        let permit = self.try_admit_runtime(request)?;
        let prepared = self
            .db
            .prepare_runtime_query(cypher.to_string(), parameters)?;
        Ok(prepared.with_planning_memory(
            permit
                .into_retained_memory()
                .expect("control work owns a fixed memory reservation"),
        ))
    }

    fn admit_streaming_query(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        result_memory_bytes: u64,
    ) -> Result<(RuntimePermit, crate::api::PreparedRuntimeQuery)> {
        let prepared = self.prepare_admitted_query(cypher, parameters)?;
        let admission = prepared.admission();
        if admission.is_mutation {
            return Err(HawDBError::Execution(
                "admitted streaming query must be read-only".to_string(),
            ));
        }
        let request = admission
            .runtime_work_request_for_snapshot(
                result_memory_bytes,
                self.runtime_governor.snapshot(),
            )
            .with_retained_memory_bytes(prepared.planning_memory_bytes());
        read_execution::admit_reusable_prepared(prepared, &self.runtime_governor, request, || {
            self.db
                .prepare_runtime_query(cypher.to_string(), parameters)
        })
    }

    fn try_admit_runtime(&self, request: hawdb_qos::RuntimeWorkRequest) -> Result<RuntimePermit> {
        try_admit_runtime(&self.runtime_governor, request)
    }

    fn admitted_streaming_result_bytes(&self, options: &NowledgeMemReadOptions) -> Result<usize> {
        read_execution::admitted_result_bytes(self.db.config(), &self.runtime_governor, options)
    }

    fn check_runtime_context(&self, task_context: &RuntimeTaskContext) -> Result<()> {
        task_context.checkpoint().map_err(|reason| {
            self.runtime_governor.record_cancellation(reason);
            HawDBError::Execution(format!("runtime task {reason}"))
        })
    }

    fn record_runtime_cancellation(
        &self,
        error: Option<&HawDBError>,
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
        let (permit, prepared) = self.admit_streaming_query(
            cypher,
            parameters,
            read_execution::result_memory_budget(self.db.config(), &self.runtime_governor),
        )?;
        let execution_task_context = permit.bind_task_context(task_context.clone());
        let result = self
            .db
            .begin_read_transaction()?
            .query_prepared_with_params_streaming_context(
                prepared,
                parameters,
                QueryStreamOptions {
                    max_rows: options.max_rows,
                    max_payload_bytes: Some(max_payload_bytes),
                },
                crate::executor::StreamDelivery::Validated,
                &execution_task_context,
                consumer,
            );
        self.record_runtime_cancellation(result.as_ref().err(), task_context);
        result
    }

    /// Collects compact owned rows with the same limits as streaming reads.
    /// No result becomes observable before complete execution and validation.
    pub fn read_query_with_params_streaming_collect(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        let max_payload_bytes = self.admitted_streaming_result_bytes(options)?;
        let (permit, prepared) = self.admit_streaming_query(
            cypher,
            parameters,
            read_execution::result_memory_budget(self.db.config(), &self.runtime_governor),
        )?;
        let task_context = permit.bind_task_context(RuntimeTaskContext::default());
        let bounded = self
            .db
            .begin_read_transaction()?
            .query_prepared_with_params_profiled_context(
                prepared,
                parameters,
                QueryStreamOptions {
                    max_rows: options.max_rows,
                    max_payload_bytes: Some(max_payload_bytes),
                },
                &task_context,
            )?;
        let mut output = bounded_nowledge_mem_read_output(self.mode, bounded, options)?;
        output.report.streaming = true;
        Ok(output)
    }

    pub fn graph_rag_schema_context(
        &self,
        options: GraphRagSchemaContextOptions,
    ) -> Result<GraphRagSchemaContext> {
        self.db.graph_rag_schema_context(options)
    }

    pub fn read_generated_graph_rag(
        &self,
        query: &GraphRagGeneratedQuery,
        parameters: &BTreeMap<String, Value>,
        options: &NowledgeMemReadOptions,
    ) -> Result<NowledgeMemReadOutput> {
        query
            .validate_parameters(parameters)
            .map_err(|error| HawDBError::Semantic(error.to_string()))?;
        let max_payload_bytes = self.admitted_streaming_result_bytes(options)?;
        let (permit, prepared) = self.admit_streaming_query(
            query.cypher(),
            parameters,
            read_execution::result_memory_budget(self.db.config(), &self.runtime_governor),
        )?;
        let context = permit.bind_task_context(RuntimeTaskContext::default());
        let mut read = self.db.begin_read_transaction_with_context(&context)?;
        read.validate_generated_graph_rag(query, parameters)?;
        let bounded = read.query_prepared_with_params_profiled_context(
            prepared,
            parameters,
            QueryStreamOptions {
                max_rows: options.max_rows,
                max_payload_bytes: Some(max_payload_bytes),
            },
            &context,
        )?;
        bounded_nowledge_mem_read_output(self.mode, bounded, options)
    }
}

#[derive(Debug)]
pub struct NowledgeMemSearchProjection {
    index: SearchIndex,
}

#[derive(Debug)]
pub struct NowledgeMemOutOfCoreSearchProjection {
    reader: Arc<SearchOutOfCoreReader>,
    maintenance: Option<(PathBuf, NowledgeMemIncrementalSearchMaintenanceOptions)>,
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
            reader: Arc::new(SearchOutOfCoreReader::open(path)?),
            maintenance: None,
        })
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: SearchOutOfCoreConfig) -> Result<Self> {
        Ok(Self {
            reader: Arc::new(SearchOutOfCoreReader::open_with_config(path, config)?),
            maintenance: None,
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

    fn read_snapshot(&self) -> Self {
        Self {
            reader: Arc::clone(&self.reader),
            maintenance: None,
        }
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
        self.search_candidates_with_report_context(request, VectorSearchExecutionOptions::default())
    }

    pub fn search_candidates_with_report_context(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        vector_execution_options: VectorSearchExecutionOptions<'_>,
    ) -> Result<NowledgeMemOutOfCoreSearchCandidateOutput> {
        let effective_compressed_vector_search_mode = effective_search_candidate_mode(request);
        let output = self
            .reader
            .search_with_options_compressed_vector_projection_execution_options(
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
                vector_execution_options,
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
    ) -> Result<SearchResultSet> {
        self.search_candidates_with_report(request)
            .map(|output| output.result)
    }

    pub fn search_candidates_with_report(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<NowledgeMemSearchCandidateOutput> {
        let effective_compressed_vector_search_mode = effective_search_candidate_mode(request);
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

    pub fn try_search_candidates_with_report(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
    ) -> Result<NowledgeMemSearchCandidateOutput> {
        self.try_search_candidates_with_report_context(
            request,
            VectorSearchExecutionOptions::default(),
        )
    }

    pub fn try_search_candidates_with_report_context(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        vector_execution_options: VectorSearchExecutionOptions<'_>,
    ) -> Result<NowledgeMemSearchCandidateOutput> {
        let effective_compressed_vector_search_mode = effective_search_candidate_mode(request);
        let result = self
            .index
            .try_search_with_options_adaptive_vector_projection_context(
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
                vector_execution_options,
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
    ) -> Result<NowledgeMemSearchCandidateReadinessReport> {
        self.search_candidates_with_report(request)
            .map(|output| output.readiness_report(options))
    }

    pub fn search_candidate_shadow_evidence<I, S>(
        &self,
        request: &NowledgeMemSearchCandidateRequest,
        primary_candidate_ids: I,
    ) -> Result<NowledgeMemSearchCandidateShadowEvidence>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let output = self.search_candidates_with_report(request)?;
        let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
        accumulator.record_search_candidate_output(primary_candidate_ids, &output);
        Ok(accumulator.evidence())
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
        self.search_candidate_shadow_evidence(request, primary_candidate_ids)
            .map(|evidence| evidence.json())
    }
}

struct SearchProjectionExternalReadOperator<'a> {
    projection: Option<ResidentProjectionRef<'a>>,
    out_of_core_projection: Option<&'a NowledgeMemOutOfCoreSearchProjection>,
    vector_seed_execution_count: usize,
}

impl SearchProjectionExternalReadOperator<'_> {
    fn execute_text_seed_mode(
        &self,
        request: crate::executor::TextSeedExecutionRequest<'_>,
        complete: bool,
    ) -> Result<crate::executor::TextSeedExecutionOutput> {
        request.resources.checkpoint()?;
        let mut output = if complete {
            crate::executor::TextSeedExecutionOutput::new_complete(
                request.result_account,
                request.resources.result,
            )
        } else {
            crate::executor::TextSeedExecutionOutput::new(
                request.result_account,
                request.resources.result,
            )
        }?;
        if let (Some(projection), None) = (self.projection, self.out_of_core_projection) {
            let default_task = RuntimeTaskContext::default();
            let task = request.resources.task_context.unwrap_or(&default_task);
            let scored = if complete {
                projection.complete_text_seed_scores_with_context(
                    request.query_text,
                    request.metadata_filters,
                    request.resources.result.max_rows,
                    request.working_account,
                    task,
                )
            } else {
                projection.text_seed_scores_with_context(
                    request.query_text,
                    request.metadata_filters,
                    request.resources.result.max_rows,
                    request.working_account,
                    task,
                )
            }?;
            output = output.with_source_report(crate::executor::TextSeedSourceReport {
                corpus_documents: Some(scored.corpus_document_count),
                matching_documents: Some(scored.matching_document_count),
                ..Default::default()
            });
            if complete {
                for (id, score) in scored.scores() {
                    request.resources.checkpoint()?;
                    let document = projection.document(id).ok_or_else(|| {
                        HawDBError::Storage(format!("text seed document {id} is missing"))
                    })?;
                    output.push(
                        id,
                        document.metadata.get("external_id").map(String::as_str),
                        *score,
                    )?;
                }
                request.resources.checkpoint()?;
                return output.finish_complete();
            }
            let ranked_bytes = scored
                .scores()
                .len()
                .checked_mul(std::mem::size_of::<(&String, &f64)>())
                .ok_or_else(|| HawDBError::Execution("text seed rank size overflow".into()))?;
            let _ranked_memory = request.working_account.reserve(ranked_bytes)?;
            let mut ranked = scored.scores().iter().collect::<Vec<_>>();
            ranked.sort_unstable_by(|(left_id, left_score), (right_id, right_score)| {
                right_score
                    .total_cmp(left_score)
                    .then_with(|| left_id.cmp(right_id))
            });
            for (id, score) in ranked {
                request.resources.checkpoint()?;
                let document = projection.document(id).ok_or_else(|| {
                    HawDBError::Storage(format!("text seed document {id} is missing"))
                })?;
                output.push(
                    id,
                    document.metadata.get("external_id").map(String::as_str),
                    *score,
                )?;
            }
            return Ok(output);
        }
        if let (None, Some(projection)) = (self.projection, self.out_of_core_projection) {
            let default_task = RuntimeTaskContext::default();
            let task = request.resources.task_context.unwrap_or(&default_task);
            let scored = if complete {
                projection
                    .reader()
                    .complete_text_seed_scores_with_metadata_filters(
                        request.query_text,
                        request.metadata_filters,
                        request.resources.result.max_rows,
                        request.working_account,
                        task,
                    )
            } else {
                projection.reader().text_seed_scores_with_metadata_filters(
                    request.query_text,
                    request.metadata_filters,
                    request.resources.result.max_rows,
                    request.working_account,
                    task,
                )
            }?;
            output = output.with_source_report(crate::executor::TextSeedSourceReport {
                generation: Some(scored.report.generation),
                source_commit_epoch: scored.report.source_graph_commit_epoch,
                corpus_documents: Some(scored.report.corpus_document_count),
                matching_documents: Some(scored.report.matching_document_count),
                postings_visited: Some(scored.report.postings_visited),
                bytes_read: Some(scored.report.bytes_read),
                document_bytes_read: Some(scored.report.document_bytes_read),
            });
            if complete {
                for (id, score) in scored.scores() {
                    request.resources.checkpoint()?;
                    output.push(id, scored.external_id(id), *score)?;
                }
                request.resources.checkpoint()?;
                return output.finish_complete();
            }
            let ranked_bytes = scored
                .scores()
                .len()
                .checked_mul(std::mem::size_of::<(&String, &f64)>())
                .ok_or_else(|| HawDBError::Execution("text seed rank size overflow".into()))?;
            let _ranked_memory = request.working_account.reserve(ranked_bytes)?;
            let mut ranked = scored.scores().iter().collect::<Vec<_>>();
            ranked.sort_unstable_by(|(left_id, left_score), (right_id, right_score)| {
                right_score
                    .total_cmp(left_score)
                    .then_with(|| left_id.cmp(right_id))
            });
            for (id, score) in ranked {
                request.resources.checkpoint()?;
                output.push(id, scored.external_id(id), *score)?;
            }
            return Ok(output);
        }
        if self.projection.is_some() {
            return Err(HawDBError::Storage(
                "nowledge mem search projection ownership is ambiguous".to_string(),
            ));
        }
        Err(missing_search_projection_error())
    }
}

impl crate::executor::ExternalReadOperator for SearchProjectionExternalReadOperator<'_> {
    fn execute_complete_text_seed(
        &mut self,
        request: crate::executor::TextSeedExecutionRequest<'_>,
    ) -> Result<crate::executor::TextSeedExecutionOutput> {
        self.execute_text_seed_mode(request, true)
    }

    fn execute_text_seed(
        &mut self,
        request: crate::executor::TextSeedExecutionRequest<'_>,
    ) -> Result<crate::executor::TextSeedExecutionOutput> {
        self.execute_text_seed_mode(request, false)
    }

    fn execute_vector_seed(
        &mut self,
        request: crate::executor::VectorSeedExecutionRequest<'_>,
    ) -> Result<crate::executor::VectorSeedExecutionOutput> {
        request.resources.checkpoint()?;
        let top_k = vector_plan_top_k(request.vector_plan)?.min(request.resources.result.max_rows);
        let search_request =
            NowledgeMemSearchCandidateRequest::vector(request.embedding.to_vec(), top_k)
                .with_metadata_filters(request.metadata_filters.clone());
        let vector_execution_options = VectorSearchExecutionOptions::bounded(
            request.resources.max_parallelism,
            request.resources.max_working_memory_bytes.get(),
            request.resources.task_context,
        );
        let output = match (self.projection, self.out_of_core_projection) {
            (Some(projection), None) => projection.try_search_candidates_with_report_context(
                &search_request,
                vector_execution_options,
            )?,
            (None, Some(projection)) => projection
                .search_candidates_with_report_context(&search_request, vector_execution_options)?
                .into(),
            (None, None) => return Err(missing_search_projection_error()),
            (Some(_), Some(_)) => {
                return Err(HawDBError::Storage(
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
                HawDBError::Execution(
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
            report: hawdb_executor::VectorExecutionReport {
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
                    hawdb_executor::VectorCandidateScanMetrics {
                        kernel: kernel.clone(),
                        worker_count: retriever.candidate_scan_worker_count,
                        segment_count: retriever.candidate_scan_segment_count,
                        scanned_segment_count: retriever.candidate_scan_scanned_segment_count,
                        scored_document_count: retriever.candidate_scan_scored_document_count,
                        filtered_document_count: retriever.candidate_scan_filtered_document_count,
                        scanned_block_count: retriever.candidate_scan_scanned_block_count,
                        skipped_block_count: retriever.candidate_scan_skipped_block_count,
                        adaptive_exact_scan_count: retriever
                            .candidate_scan_adaptive_exact_scan_count,
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
        execution_output.validate_result_budget(request.resources.result)?;
        request.resources.checkpoint()?;
        self.vector_seed_execution_count = self
            .vector_seed_execution_count
            .checked_add(1)
            .ok_or_else(|| {
                HawDBError::Execution(
                    "bounded external vector seed execution count overflowed".to_string(),
                )
            })?;
        Ok(execution_output)
    }
}

fn vector_plan_top_k(plan: &hawdb_plan_cypher::VectorPhysicalPlan) -> Result<usize> {
    match plan {
        hawdb_plan_cypher::VectorPhysicalPlan::TopK { limit, .. } => Ok(*limit),
        _ => Err(HawDBError::Execution(
            "vector seed physical plan is missing TopK".to_string(),
        )),
    }
}

fn vector_plan_candidate_source(
    plan: &hawdb_plan_cypher::VectorPhysicalPlan,
) -> Result<hawdb_plan_cypher::VectorCandidateSource> {
    match plan {
        hawdb_plan_cypher::VectorPhysicalPlan::VectorCandidateScan { source, .. } => Ok(*source),
        hawdb_plan_cypher::VectorPhysicalPlan::ResidualFilter { input, .. }
        | hawdb_plan_cypher::VectorPhysicalPlan::RawVectorRerank { input, .. }
        | hawdb_plan_cypher::VectorPhysicalPlan::TopK { input, .. } => {
            vector_plan_candidate_source(input)
        }
        hawdb_plan_cypher::VectorPhysicalPlan::Filter { .. } => Err(HawDBError::Execution(
            "vector seed physical plan is missing VectorCandidateScan".to_string(),
        )),
    }
}

fn vector_score_source(value: &str) -> Result<hawdb_executor::VectorScoreSource> {
    match value {
        "unavailable" | "none" => Ok(hawdb_executor::VectorScoreSource::Unavailable),
        "raw_vector" => Ok(hawdb_executor::VectorScoreSource::RawVector),
        "ann_approximate" | "graph_distance" => {
            Ok(hawdb_executor::VectorScoreSource::AnnApproximate)
        }
        "quantized_approximate" | "quantized_projection" => {
            Ok(hawdb_executor::VectorScoreSource::QuantizedApproximate)
        }
        _ => Err(HawDBError::Execution(
            "vector search returned an unsupported score source".to_string(),
        )),
    }
}

fn vector_execution_backend(
    score_source: hawdb_executor::VectorScoreSource,
) -> hawdb_executor::VectorExecutionBackend {
    match score_source {
        hawdb_executor::VectorScoreSource::Unavailable => {
            hawdb_executor::VectorExecutionBackend::Unavailable
        }
        hawdb_executor::VectorScoreSource::RawVector => {
            hawdb_executor::VectorExecutionBackend::ScalarFlat
        }
        hawdb_executor::VectorScoreSource::AnnApproximate => {
            hawdb_executor::VectorExecutionBackend::AnnProjection
        }
        hawdb_executor::VectorScoreSource::QuantizedApproximate => {
            hawdb_executor::VectorExecutionBackend::QuantizedProjection
        }
    }
}

fn vector_compression_mode(
    mode: CompressedVectorSearchMode,
) -> hawdb_executor::VectorCompressionMode {
    match mode {
        CompressedVectorSearchMode::Disabled => hawdb_executor::VectorCompressionMode::Disabled,
        CompressedVectorSearchMode::Preferred => hawdb_executor::VectorCompressionMode::Preferred,
        CompressedVectorSearchMode::Required => hawdb_executor::VectorCompressionMode::Required,
    }
}

fn vector_fallback_reason_code(
    code: SearchFallbackReasonCode,
) -> Option<hawdb_executor::VectorFallbackReasonCode> {
    match code {
        SearchFallbackReasonCode::VectorDimensionMismatch => {
            Some(hawdb_executor::VectorFallbackReasonCode::VectorDimensionMismatch)
        }
        SearchFallbackReasonCode::VectorIndexEmpty => {
            Some(hawdb_executor::VectorFallbackReasonCode::VectorIndexEmpty)
        }
        SearchFallbackReasonCode::CompressedVectorProjectionUnavailable => {
            Some(hawdb_executor::VectorFallbackReasonCode::CompressedVectorProjectionUnavailable)
        }
        SearchFallbackReasonCode::CompressedVectorBudgetExceeded => {
            Some(hawdb_executor::VectorFallbackReasonCode::CompressedVectorBudgetExceeded)
        }
        SearchFallbackReasonCode::DiskAnnCacheBudgetExceeded => {
            Some(hawdb_executor::VectorFallbackReasonCode::DiskAnnCacheBudgetExceeded)
        }
        SearchFallbackReasonCode::QueryEmbeddingMissing => {
            Some(hawdb_executor::VectorFallbackReasonCode::QueryEmbeddingMissing)
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

impl crate::DatabasePublicationState for NowledgeMemEmbeddedStore {
    fn database(&self) -> &crate::Database {
        self.graph.database()
    }
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
        let namespace_anchor = graph_config.namespace_durability_anchor.clone();
        let mut graph = NowledgeMemGraph::open_with_config_and_runtime_governor(
            &options.graph_path,
            graph_config,
            runtime_governor,
        )?;
        // Project metadata can open without recovering main's data runtime.
        // Readiness requires data admission before any host schema mutation.
        graph.database().commit_epoch()?;
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
        // A sibling search projection is a separate descriptor domain. Admit
        // the same explicit host boundary before its internal component opens;
        // the returned index/reader retains that domain after this guard drops.
        // Reader modes must not create or synchronize an absent projection.
        let create_search = matches!(
            &options.search_projection_open_mode,
            NowledgeMemSearchProjectionOpenMode::FullResidencyMaintenance
        );
        let _search_namespace = namespace_anchor
            .as_deref()
            .map(|anchor| {
                hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_component_with_namespace_anchor(
                    path,
                    create_search,
                    anchor,
                )
            })
            .transpose()?;
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
            NowledgeMemSearchProjectionOpenMode::IncrementalMaintenance(maintenance) => {
                let projection = NowledgeMemOutOfCoreSearchProjection::open_incremental(
                    path,
                    (**maintenance).clone(),
                )?;
                report.search_projection_opened = true;
                Ok((
                    Self::new_with_out_of_core_search(
                        graph,
                        projection,
                        options.retrieval_projection_advisor.clone(),
                    ),
                    report,
                ))
            }
            NowledgeMemSearchProjectionOpenMode::QualifiedOutOfCore(qualified) => {
                let graph_commit_epoch = graph.database().commit_epoch()?;
                if graph_commit_epoch != qualified.expected_identity.canonical_graph_commit_epoch {
                    return Err(HawDBError::Storage(format!(
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
        if self.incremental_search_enabled() {
            let permit =
                self.admit_incremental_search_maintenance(search_projection_delta_bytes(&delta))?;
            return self.apply_incremental_search_delta(
                delta,
                permit.bind_task_context(RuntimeTaskContext::default()),
            );
        }
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
        let local_graph_commit_epoch = self.graph.database().commit_epoch()?;
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

    pub fn storage_recovery_report(&self) -> Result<NowledgeMemStorageRecoveryReport> {
        Ok({
            NowledgeMemStorageRecoveryReport::from_storage_report(
                &self.graph.database().storage_recovery_report()?,
            )
        })
    }

    pub fn storage_recovery_report_json(&self) -> Result<serde_json::Value> {
        Ok(self.storage_recovery_report()?.json())
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

    pub fn storage_lifecycle_decision(&self) -> Result<NowledgeMemStorageLifecycleDecision> {
        Ok({
            NowledgeMemStorageLifecycleDecision::from_storage_recovery(
                self.storage_recovery_report()?,
            )
        })
    }

    pub fn storage_lifecycle_decision_json(&self) -> Result<serde_json::Value> {
        Ok(self.storage_lifecycle_decision()?.json())
    }

    pub fn runtime_status(&self) -> Result<NowledgeMemRuntimeStatus> {
        Ok({
            let graph_commit_epoch = self.graph.database().commit_epoch()?;
            NowledgeMemRuntimeStatus {
                protocol: NOWLEDGE_MEM_RUNTIME_STATUS_PROTOCOL.to_string(),
                graph_commit_epoch,
                changefeed: self
                    .graph
                    .database()
                    .search_projection_changefeed_status()?,
                projection_freshness: self.search_projection_freshness(),
                file_descriptors: self.graph.database().file_descriptor_metrics(),
            }
        })
    }

    pub fn production_status(
        &self,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<NowledgeMemProductionStatus> {
        Ok({
            NowledgeMemProductionStatus::from_runtime(
                self.graph.mode(),
                self.graph.database().config().read_only,
                self.runtime_status()?,
                route_ownership.cloned(),
            )
        })
    }

    pub fn production_status_json(
        &self,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<serde_json::Value> {
        Ok(self.production_status(route_ownership)?.json())
    }

    pub fn cutover_controls_report(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<NowledgeMemCutoverControlsReport> {
        Ok({
            self.cutover_controls_report_with_initial_import_cutover_catch_up(
                controls,
                route_ownership,
                None,
            )?
        })
    }

    pub fn cutover_controls_report_with_initial_import_cutover_catch_up(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_cutover_catch_up: Option<&HawDBLightningInitialImportCutoverCatchUpReport>,
    ) -> Result<NowledgeMemCutoverControlsReport> {
        Ok({
            NowledgeMemCutoverControlsReport::from_production_status(
                controls,
                self.production_status(route_ownership)?,
                initial_import_cutover_catch_up.is_some_and(|report| report.ready),
            )
        })
    }

    pub fn cutover_controls_report_with_initial_import_recovery(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_recovery: Option<&HawDBLightningInitialImportRecoveryReadinessReport>,
    ) -> Result<NowledgeMemCutoverControlsReport> {
        Ok({
            let recovery_ready = initial_import_recovery.is_some_and(|report| {
                report.ready
                    && report.startup.readiness.ready_for_cutover
                    && report
                        .startup
                        .cutover_catch_up
                        .as_ref()
                        .is_some_and(|catch_up| catch_up.ready)
            });
            let mut report = NowledgeMemCutoverControlsReport::from_production_status(
                controls,
                self.production_status(route_ownership)?,
                recovery_ready,
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
        })
    }

    pub fn cutover_controls_report_json(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Result<serde_json::Value> {
        Ok({
            self.cutover_controls_report(controls, route_ownership)?
                .json()
        })
    }

    pub fn cutover_controls_report_json_with_initial_import_cutover_catch_up(
        &self,
        controls: NowledgeMemCutoverControls,
        route_ownership: Option<&NowledgeMemRouteOwnershipReadinessReport>,
        initial_import_cutover_catch_up: Option<&HawDBLightningInitialImportCutoverCatchUpReport>,
    ) -> Result<serde_json::Value> {
        Ok({
            self.cutover_controls_report_with_initial_import_cutover_catch_up(
                controls,
                route_ownership,
                initial_import_cutover_catch_up,
            )?
            .json()
        })
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
        if self.incremental_search_enabled() {
            let freshness = self.search_projection_freshness().ok_or_else(|| {
                HawDBError::Storage("incremental search reader is detached".into())
            })?;
            return Ok(self
                .graph
                .database()
                .search_projection_changefeed_status()?
                .readiness_after(
                    freshness.source_graph_commit_epoch,
                    freshness.durable_source_graph_commit_epoch,
                    require_restart_recoverable,
                    max_operations,
                ));
        }
        let search_projection = self.require_search_projection()?;
        self.graph
            .database()
            .search_projection_changefeed_readiness(
                search_projection.index(),
                require_restart_recoverable,
                max_operations,
            )
    }

    pub fn search_projection_graph_delta_background_work_plan(
        &self,
        request: &SearchProjectionGraphDeltaRequest,
        hint: BackgroundWorkHint,
    ) -> Result<Option<BackgroundWorkPlan>> {
        let Some(search_projection) = self.search_projection.as_ref() else {
            return Ok(None);
        };
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
        if self.incremental_search_enabled() {
            let operations = max_change_operations_per_batch
                .max(max_projection_operations_per_batch)
                .saturating_mul(max_batches);
            let permit = self.admit_incremental_search_maintenance(
                operations.saturating_mul(SEARCH_PROJECTION_CHANGEFEED_OPERATION_BYTES),
            )?;
            return self.catch_up_incremental_search_with_batch_hydrator(
                max_change_operations_per_batch,
                max_projection_operations_per_batch,
                max_batches,
                batch_hydrator,
                permit.bind_task_context(RuntimeTaskContext::default()),
            );
        }
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

    pub fn catch_up_search_projection_scheduled(
        &mut self,
        max_operations_per_batch: usize,
        max_batches: usize,
    ) -> Result<ScheduledSearchProjectionCatchUpReport> {
        let Self {
            graph,
            search_projection,
            ..
        } = self;
        let search_projection = require_search_projection_mut(search_projection)?;
        graph.database().catch_up_search_projection_scheduled(
            search_projection.index_mut(),
            max_operations_per_batch,
            max_batches,
        )
    }

    pub fn apply_scheduled_background_search_projection_graph_delta(
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
            .apply_scheduled_background_search_projection_graph_delta(
                search_projection.index_mut(),
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
        self.require_search_projection()?
            .search_candidate_shadow_evidence_json(request, primary_candidate_ids)
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
            projection: search_projection.as_ref().map(ResidentProjectionRef::Live),
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
            projection: search_projection.as_ref().map(ResidentProjectionRef::Live),
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
    ) -> Result<GraphRagSchemaContext> {
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
    ) -> Result<BackgroundMaintenanceSummary> {
        Ok({
            self.graph.database().background_maintenance_summary(
                self.search_projection
                    .as_ref()
                    .map(NowledgeMemSearchProjection::index),
                policy,
                state,
                options,
            )?
        })
    }

    pub fn background_maintenance_report(
        &self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> Result<NowledgeMemBackgroundMaintenanceReport> {
        Ok({
            let summary = self.background_maintenance_summary(policy, state, options)?;
            let slow_query = self.slow_query_report();
            NowledgeMemBackgroundMaintenanceReport::from_summary_with_slow_query(
                &summary,
                Some(&slow_query),
            )
        })
    }

    pub fn background_maintenance_report_json(
        &self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> Result<serde_json::Value> {
        Ok({
            self.background_maintenance_report(policy, state, options)?
                .json()
        })
    }

    pub fn library_readiness_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.library_readiness(options)?.json())
    }

    pub fn library_readiness(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<NowledgeMemLibraryReadinessReport> {
        Ok({
            let bounded_read_evidence = options
                .bounded_read_evidence
                .clone()
                .unwrap_or_else(|| self.bounded_read_probe_evidence_json(options));
            let storage_recovery = self.storage_recovery_report_json()?;
            let background_maintenance = self.background_maintenance_report_json(
                &options.qos_policy,
                &options.qos_state,
                options.background_maintenance_options.clone(),
            )?;
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
            let search_projection_evidence = options
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
            let assessment =
                hawdb_readiness::library_readiness::assess_nowledge_mem_library_readiness(
                    &hawdb_readiness::library_readiness::NowledgeMemLibraryReadinessEvidence {
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
                    },
                );
            let readiness_by_area = assessment.readiness_by_area;
            let areas = readiness_by_area.areas();
            let ready_area_count = areas.iter().filter(|area| area.ready).count();
            let blocked_area_count = areas.len().saturating_sub(ready_area_count);
            let blocker_codes = assessment.blocker_codes;
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
        })
    }

    pub fn readiness_dashboard(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<NowledgeMemReadinessDashboard> {
        Ok({
            let library = self.library_readiness(options)?;
            let storage_lifecycle = self.storage_lifecycle_decision()?;
            let slow_query = self.slow_query_report();
            NowledgeMemReadinessDashboard::from_reports(&library, &storage_lifecycle, &slow_query)
        })
    }

    pub fn readiness_dashboard_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.readiness_dashboard(options)?.json())
    }

    pub fn operations_readiness(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<NowledgeMemOperationsReadinessReport> {
        Ok({
            let runtime_status = self.runtime_status()?;
            let storage_recovery = self.storage_recovery_report()?;
            let slow_query = self.slow_query_report();
            let background_maintenance = self.background_maintenance_report(
                &options.qos_policy,
                &options.qos_state,
                options.background_maintenance_options.clone(),
            )?;
            NowledgeMemOperationsReadinessReport::from_reports(
                self.graph.mode(),
                self.graph.database().config().read_only,
                runtime_status,
                storage_recovery,
                slow_query,
                background_maintenance,
            )
        })
    }

    pub fn operations_readiness_json(
        &self,
        options: &NowledgeMemReadinessOptions,
    ) -> Result<serde_json::Value> {
        Ok(self.operations_readiness(options)?.json())
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

    fn external_seed_io_slots(&self) -> usize {
        self.search_projection
            .as_ref()
            .map_or(0, |projection| {
                projection.index().range_read_config().io_depth.get()
            })
            .max(usize::from(self.out_of_core_search_projection.is_some()))
    }

    fn search_request_for_retrieval(
        &self,
        request: &KnowledgeRetrievalRequest,
    ) -> NowledgeMemSearchCandidateRequest {
        search_read::retrieval_request(
            self.graph.database().config(),
            &self.retrieval_projection_advisor,
            request,
        )
    }

    fn require_search_projection(&self) -> Result<&NowledgeMemSearchProjection> {
        self.search_projection
            .as_ref()
            .ok_or_else(missing_search_projection_error)
    }
}

fn missing_search_projection_error() -> HawDBError {
    HawDBError::Storage("nowledge mem search projection is not configured".to_string())
}

fn ambiguous_search_projection_error() -> HawDBError {
    HawDBError::Storage("nowledge mem search projection ownership is ambiguous".to_string())
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
    let statement_kind = crate::api::statement_kind(
        hawdb_cypher::read_route::query_statement_body(input.statement),
    );
    let decision = nowledge_mem_fast_path_classification(input.statement);
    hawdb_readiness::nowledge_mem_query_report::nowledge_mem_query_report(
        hawdb_readiness::nowledge_mem_query_report::NowledgeMemQueryReportInput {
            mode: input.mode,
            statement_kind,
            statement: input.statement,
            execution_path: decision.execution_path,
            fast_path_reason: decision.fast_path_reason,
            physical_plan_captured: input.trace.is_some(),
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
            plan_cache_lookup: input.plan_cache_lookup,
            execution_profile: input.execution_profile,
            output: input.output,
            options: input.options,
            elapsed_micros: input.elapsed_micros,
        },
    )
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
            let plan_cache = nowledge_mem_plan_cache_report(Some(plan_cache_lookup));
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
            error_class: Some(hawdb_error_class(&error).to_string()),
            blocker_codes: vec!["query_runtime_failed".to_string()],
        },
    }
}

fn missing_search_projection_evidence_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": "hawdb-nowledge-search-projection-evidence",
        "present": false,
        "ready": false,
        "blocker_codes": ["search_projection_not_configured"],
    })
}

fn missing_search_projection_shadow_evidence_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": "hawdb-nowledge-search-projection-shadow-evidence",
        "present": false,
        "ready": false,
        "blocker_codes": ["search_projection_not_configured"],
    })
}

fn missing_primary_search_projection_probe_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": "hawdb-nowledge-search-projection-shadow-evidence",
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
            "protocol": "hawdb-nowledge-query-family-evidence-v1",
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
        "protocol": "hawdb-nowledge-query-family-evidence-v1",
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
        hawdb_readiness::bounded_read_evidence::missing_nowledge_mem_bounded_read_routes(
            &summary.primary_ready_routes,
        );
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
        "require_all_hawdb": true,
        "required_route_count": REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES.len(),
        "explicit_route_count": 0,
        "hawdb_route_count": 0,
        "lancedb_route_count": 0,
        "routes": [],
        "hawdb_routes": [],
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
        "require_all_hawdb": true,
        "required_route_count": REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len(),
        "explicit_route_count": 0,
        "hawdb_route_count": 0,
        "lancedb_route_count": 0,
        "routes": [],
        "hawdb_routes": [],
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
        "require_all_hawdb": true,
        "required_route_count": REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len(),
        "evidence_route_count": 0,
        "ready_route_count": 0,
        "hawdb_route_count": 0,
        "lancedb_handle_required_route_count": 0,
        "routes": [],
        "ready_routes": [],
        "missing_required_routes": REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES,
        "unknown_routes": [],
        "duplicate_routes": [],
        "invalid_projection_routes": [],
        "non_hawdb_routes": [],
        "lancedb_handle_required_routes": [],
        "candidate_not_ready_routes": [],
        "candidate_identity_not_ready_routes": [],
        "metadata_pushdown_not_ready_routes": [],
        "ranking_not_ready_routes": [],
        "fail_soft_not_ready_routes": [],
        "blocker_codes": ["active_search_route_readiness_missing"],
    })
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
        return Err(HawDBError::read_budget_exceeded(
            ReadBudgetResource::Rows,
            report.max_rows.unwrap_or_default(),
            format!(
                "nowledge mem read query returned {} rows, exceeding max_rows {}",
                report.row_count,
                report.max_rows.unwrap_or_default()
            ),
        ));
    }
    if report.payload_budget_exceeded {
        return Err(HawDBError::read_budget_exceeded(ReadBudgetResource::PayloadBytes, report.max_estimated_payload_bytes.unwrap_or_default(), format!(
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

fn legacy_nowledge_mem_read_error(
    error: HawDBError,
    options: &NowledgeMemReadOptions,
) -> HawDBError {
    let Some(max_payload_bytes) = options.max_estimated_payload_bytes else {
        return error;
    };
    if matches!(
        &error,
        HawDBError::ReadBudgetExceeded(cause)
            if cause.resource == ReadBudgetResource::PayloadBytes && cause.limit == max_payload_bytes
    ) {
        return HawDBError::read_budget_exceeded(ReadBudgetResource::PayloadBytes, max_payload_bytes, format!(
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
        Value::Vector(value) => value.encoded().len(),
        Value::Uuid(_) => 16,
        Value::List(values) => values.iter().map(estimate_value_payload_bytes).sum(),
        Value::Map(values) => values
            .iter()
            .map(|(key, value)| key.len() + estimate_value_payload_bytes(value))
            .sum(),
    }
}

#[cfg(test)]
mod tests;
