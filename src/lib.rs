pub mod analytics;
pub mod api;
pub mod background_maintenance_evidence;
pub mod blackbox;
pub mod bounded_read_evidence;
pub mod compat;
mod compiled_capabilities;
pub mod crash_recovery_evidence;
pub mod cypher;
pub mod embedded;
#[cfg(feature = "tokio-runtime")]
pub mod embedded_tokio;
pub mod executor;
pub mod expression {
    pub use skein_expression::*;
}
pub mod graph_route_evidence;
pub mod graph_route_readiness;
pub mod mem_integration_bundle;
pub mod mem_integration_readiness;
pub mod mem_library_readiness;
pub mod nowledge_fuzz;
pub mod nowledge_inventory;
pub mod nowledge_mem;
pub mod optimizer;
pub mod planner;
pub mod previous_wrapper_preflight;
pub mod production_evidence;
pub mod qos;
pub mod query {
    pub use skein_query::*;
}
pub mod query_family_evidence;
pub mod query_runtime_preflight;
mod relational_sql;
pub mod replacement_summary;
pub mod route_ownership;
pub mod search;
pub mod search_candidate_shadow_evidence;
pub use skein_route_ownership as search_route_ownership;
pub mod storage_recovery_evidence;
pub mod store;
pub mod telemetry;
pub mod workload_fixtures;

pub mod search_projection_evidence;

pub mod error {
    pub use skein_core::error::*;
}

pub mod schema {
    pub use skein_core::schema::*;
}

pub mod value {
    pub use skein_core::value::*;
}

pub mod sql {
    pub use skein_sql::*;
}

pub use analytics::{
    CommunityAssignment, GraphAlgorithmMemoryEstimate, LouvainOptions, PageRankOptions,
    PageRankScore, ProjectedGraph, ProjectionLayout, ProjectionMemoryAdmissionError,
    ProjectionMemoryBudget, ProjectionMemoryEstimate,
};
pub use api::{
    parse_skein_lightning_graph_stream_export, skein_lightning_initial_import_advance_checkpoint,
    skein_lightning_initial_import_advance_durable_state_streaming,
    skein_lightning_initial_import_advance_durable_state_with_search_projection_batch,
    skein_lightning_initial_import_checkpoint_readiness,
    skein_lightning_initial_import_cutover_catch_up_report,
    skein_lightning_initial_import_decode_durable_state,
    skein_lightning_initial_import_document_identity_coverage,
    skein_lightning_initial_import_durable_state_report,
    skein_lightning_initial_import_encode_durable_state, skein_lightning_initial_import_plan,
    skein_lightning_initial_import_plan_with_document_identities,
    skein_lightning_initial_import_readiness, skein_lightning_initial_import_recovery_readiness,
    skein_lightning_initial_import_resume_action,
    skein_lightning_initial_import_search_projection_batch_report,
    skein_lightning_initial_import_search_projection_batch_report_with_document_identities,
    skein_lightning_initial_import_session_bundle_readiness,
    skein_lightning_initial_import_session_report,
    skein_lightning_initial_import_source_bundle_readiness,
    skein_lightning_initial_import_source_fingerprint,
    skein_lightning_initial_import_startup_readiness, validate_skein_lightning_graph_stream,
    validate_skein_lightning_relational_stream, AccessControlPolicyReadiness,
    BackgroundMaintenanceCandidate, BackgroundMaintenanceKind, BackgroundMaintenanceOptions,
    BackgroundMaintenanceSummary, BackgroundMaintenanceSummaryItem, BoundedReadQueryOutput,
    CanonicalGraphSnapshotExport, CanonicalGraphSnapshotValidation,
    CanonicalSnapshotEndpointViolation, CanonicalSnapshotIdentityAudit, CanonicalSnapshotNode,
    CanonicalSnapshotRelationship, CanonicalStableIdMapping, ConcurrentDatabase,
    ConcurrentDatabaseTransaction, ConcurrentTransactionMode, ConcurrentTransactionOptions,
    Database, DatabaseConfig, DatabaseReadTransaction, DatabaseTransaction, DerivedArtifactJob,
    DerivedArtifactJobReport, DerivedArtifactJobStatus, ExplainAnalyzeOutput,
    ExternalContentArtifactJobCompletion, ExternalContentArtifactJobSummary,
    ExternalContentArtifactRuntimeManifest, KnowledgeCandidate, KnowledgeCandidateScoreBreakdown,
    KnowledgeCandidateScoringPolicy, KnowledgeCandidateSource, KnowledgeEntityDeleteBatchOutput,
    KnowledgeEntityDeleteBatchRequest, KnowledgeEvidence, KnowledgeFallbackReasonCode,
    KnowledgeFanoutReasonCode, KnowledgeFanoutReasonDetail, KnowledgeGraphContextPath,
    KnowledgeGraphPathDirection, KnowledgeGraphSeed, KnowledgeMemoryEvolvesCreate,
    KnowledgeMemoryEvolvesCreateBatchOutput, KnowledgeMemoryEvolvesCreateBatchRequest,
    KnowledgeMemoryEvolvesCreateBatchRow, KnowledgeMemoryLifecycleBatchOutput,
    KnowledgeMemoryLifecycleBatchRequest, KnowledgeMemoryLifecycleBatchRow,
    KnowledgeMemoryLifecycleUpdate, KnowledgeRetrievalDiagnostics,
    KnowledgeRetrievalEmptyReasonCode, KnowledgeRetrievalOutput, KnowledgeRetrievalRequest,
    KnowledgeRetrieverCandidate, KnowledgeRetrieverReport, KnowledgeSourceCandidateRow,
    KnowledgeSourceCandidateScanOrigin, KnowledgeSourceCandidateScanOutput,
    KnowledgeSourceCandidateScanRequest, KnowledgeTruncationReasonCode, NowledgeGraphAdapter,
    NowledgeGraphExplainOutput, NowledgeGraphStatement, NowledgeGraphTransactionOutput,
    PlanCacheBypassReason, PlanCacheLookup, PlanCacheStats, QueryAccessControlContext, QueryOutput,
    QueryStreamOptions, QueryStreamReport, QuerySystemVariables, RankedBackgroundMaintenance,
    ScheduledSearchProjectionCatchUpReport, SearchProjectionCatchUpReport,
    SearchProjectionCatchUpStopReason, SearchProjectionGraphDeltaRequest,
    SkeinLightningBootstrapExport, SkeinLightningBootstrapManifest, SkeinLightningGraphStream,
    SkeinLightningGraphStreamValidation, SkeinLightningInitialImportApplyReport,
    SkeinLightningInitialImportCheckpoint, SkeinLightningInitialImportCheckpointProgress,
    SkeinLightningInitialImportCheckpointProgressReport,
    SkeinLightningInitialImportCheckpointReadiness,
    SkeinLightningInitialImportCutoverCatchUpReport, SkeinLightningInitialImportDocumentIdentity,
    SkeinLightningInitialImportDocumentIdentityCoverage,
    SkeinLightningInitialImportDocumentIdentityKindReport,
    SkeinLightningInitialImportDurableBatchAdvanceReport, SkeinLightningInitialImportDurableState,
    SkeinLightningInitialImportDurableStateCodecReport,
    SkeinLightningInitialImportDurableStateReport, SkeinLightningInitialImportIdempotencyKey,
    SkeinLightningInitialImportPlan, SkeinLightningInitialImportReadiness,
    SkeinLightningInitialImportReadinessInputs, SkeinLightningInitialImportRecoveryReadinessReport,
    SkeinLightningInitialImportResumeAction, SkeinLightningInitialImportResumeActionKind,
    SkeinLightningInitialImportSearchProjectionBatchReport,
    SkeinLightningInitialImportSessionBundleReadiness, SkeinLightningInitialImportSessionReport,
    SkeinLightningInitialImportSourceBundleReadiness, SkeinLightningInitialImportSourceFingerprint,
    SkeinLightningInitialImportStartupReadinessReport,
    SkeinLightningInitialImportStreamingBatchAdvanceReport, SkeinLightningRelationalStream,
    SkeinLightningRelationalStreamValidation, SlowQueryLogExportOptions, SlowQueryLogRecordSummary,
    StorageResourceProfileLimits, StorageResourceProfileReport, SystemSchemaMigration,
    SystemSchemaRegistry, SystemSchemaUpgradeReport, WalGroupCommitActivation,
    WalGroupCommitAdaptiveColdStartEvidence, WalGroupCommitAdaptivePolicyEvidence,
    WalGroupCommitAdaptiveSteadyStateEvidence, WalGroupCommitConfig, WalGroupCommitDelayPolicy,
    WalGroupCommitEvidence, WalGroupCommitSnapshot, WalGroupCommitTailLatencyEvidence,
    WalGroupCommitWaitDecision, DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES,
    DEFAULT_MAX_READ_RESULT_ROWS, DEFAULT_PESSIMISTIC_LOCK_TIMEOUT,
    DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES, DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
    DEFAULT_WAL_GROUP_COMMIT_MAX_ENTRIES, SKEIN_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION,
    SKEIN_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION,
    SKEIN_LIGHTNING_INITIAL_IMPORT_DURABLE_STATE_PROTOCOL,
    SKEIN_LIGHTNING_RELATIONAL_STREAM_FORMAT_VERSION, SLOW_QUERY_LOG_EVENT_PROTOCOL,
    STORAGE_RESOURCE_PROFILE_PROTOCOL,
};
pub use background_maintenance_evidence::nowledge_background_maintenance_evidence_json;
pub use blackbox::{
    blackbox_readiness_from_manifest_json, blackbox_report, blackbox_report_json,
    write_blackbox_report, write_blackbox_report_typed, BlackboxArtifactReport,
    BlackboxBackgroundQosSummary, BlackboxEventReport, BlackboxJsonArtifactSummary,
    BlackboxJsonlArtifactSummary, BlackboxReadinessReport, BlackboxRedactionReport, BlackboxReport,
    BlackboxReportOptions, BlackboxRunStatus, BLACKBOX_EVENT_PROTOCOL, BLACKBOX_REPORT_PROTOCOL,
};
pub use bounded_read_evidence::{
    parse_covered_routes_json, parse_graph_route_readiness_json, parse_read_report_json,
};
pub use compat::{
    assess_compatibility_cutover, assess_compatibility_cypher_migration_gate_bundle,
    assess_compatibility_cypher_migration_gate_bundle_with_rollback,
    assess_compatibility_migration_gate, assess_compatibility_migration_gate_bundle,
    assess_compatibility_migration_gate_with_rollback, assess_query_inventory_coverage,
    assess_query_inventory_cypher_coverage, assess_query_inventory_gate,
    build_compatibility_query_inventory, build_compatibility_query_inventory_from_json,
    build_compatibility_query_inventory_from_json_str, compatibility_cutover_report_to_json,
    compatibility_inventory_coverage_report_to_json, compatibility_inventory_gate_report_to_json,
    compatibility_migration_gate_bundle_to_json, compatibility_migration_gate_report_to_json,
    compatibility_query_inventory_to_json, external_shadow_json_from_value,
    external_shadow_ready_missing_capabilities, external_shadow_trace_health_from_bundle,
    external_shadow_trace_report_json, external_shadow_value_from_json,
    nowledge_memory_core_fixture, nowledge_memory_core_inventory, run_compatibility_fixture,
    run_compatibility_fixture_with_shadow, CompatibilityCheck, CompatibilityCheckReport,
    CompatibilityCutoverDecision, CompatibilityCutoverPolicy, CompatibilityCutoverReport,
    CompatibilityFixture, CompatibilityInventoryCoveragePolicy,
    CompatibilityInventoryCoverageReport, CompatibilityInventoryGateReport,
    CompatibilityMigrationGateBundle, CompatibilityMigrationGateReport, CompatibilityQueryCallSite,
    CompatibilityQueryInventory, CompatibilityQueryInventoryItem, CompatibilityReport,
    CompatibilityRollbackEvidence, CompatibilityShadowCheckReport, CompatibilityShadowEngine,
    CompatibilityShadowReport, CompatibilityShadowStatus, CypherFixtureCheck,
    CypherFixtureStatement, ExpectedErrorClass, ExpectedRows, ExternalShadowCommand,
    ExternalShadowProjectGraphReply, ExternalShadowProjectGraphRequest,
    ExternalShadowProtocolBackend, ExternalShadowProtocolServer, ExternalShadowReady,
    ExternalShadowStatementRequest, ExternalShadowTraceHealth, ExternalShadowTraceSummary,
    ProjectedGraphFixtureCheck, ProjectedGraphShadowOutput, EXTERNAL_SHADOW_PROTOCOL_VERSION,
    REQUIRED_EXTERNAL_SHADOW_CAPABILITIES,
};
pub use compiled_capabilities::compiled_runtime_capabilities;
pub use crash_recovery_evidence::{
    StorageCrashCaseEvidence, StorageCrashPoint, StorageCrashRecoveryEvidence,
    STORAGE_CRASH_RECOVERY_EVIDENCE_PROTOCOL,
};
pub use cypher::RelationshipDirection;
pub use embedded::{
    EmbeddedDeploymentProfile, EmbeddedQueryEntrypoint, EmbeddedQueryError,
    EmbeddedQueryPathReadiness, EmbeddedRuntimeResources, SkeinEmbedded, SkeinEmbeddedOpenOptions,
    EMBEDDED_QUERY_PATH_READINESS_PROTOCOL,
};
#[cfg(feature = "tokio-runtime")]
pub use embedded_tokio::{
    SkeinTokioEmbedded, SkeinTokioEmbeddedError, TokioQueryBatchStream, TokioQueryStreamOptions,
};
pub use error::{Result, SkeinError};
pub use executor::{ReadExecutionProfile, Row};
pub use graph_route_evidence::{
    nowledge_graph_route_evidence_json, nowledge_mem_graph_augmentation_state_route_query,
    nowledge_mem_graph_community_members_route_query,
    nowledge_mem_graph_community_recent_memories_route_query,
    nowledge_mem_graph_community_subgraph_route_query, nowledge_mem_graph_node_details_route_query,
    nowledge_mem_graph_orphans_route_query, nowledge_mem_graph_overview_route_query,
    nowledge_mem_graph_pagerank_plan_route_query, nowledge_mem_graph_sample_route_query,
    parse_route_parity_evidence, parse_route_query_inventory, RouteCypherQuery,
    RouteParityEvidence, RouteParityEvidenceRoute, RouteQuery,
    NMEM_GRAPH_ROUTE_PARITY_EVIDENCE_PROTOCOL,
};
pub use graph_route_readiness::{
    nowledge_graph_route_readiness_json, NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL,
    NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
};
pub use mem_integration_bundle::{nowledge_mem_integration_bundle_json, IntegrationBundleInputs};
pub use mem_integration_readiness::{
    background_maintenance_cutover_readiness, bounded_read_alignment_cutover_readiness,
    bounded_read_cutover_readiness, content_store_boundary_cutover_readiness,
    graph_replacement_cutover_readiness, graph_route_alignment_cutover_readiness,
    graph_route_cutover_readiness, graph_route_parity_alignment_cutover_readiness,
    integration_bundle_protocol_cutover_readiness, legacy_coexistence_cutover_readiness,
    library_readiness_cutover_readiness, nowledge_mem_final_cutover_preflight,
    nowledge_mem_final_cutover_preflight_json, nowledge_mem_integration_readiness,
    nowledge_mem_integration_readiness_json, previous_wrapper_preflight_cutover_readiness,
    query_family_replacement_cutover_readiness,
    query_runtime_preflight_alignment_cutover_readiness, query_runtime_preflight_cutover_readiness,
    replacement_summary_protocol_cutover_readiness, route_ownership_cutover_readiness,
    search_candidate_cutover_readiness, search_projection_cutover_readiness,
    skein_submodule_cutover_readiness, storage_recovery_cutover_readiness,
    BackgroundMaintenanceCutoverReadiness, BoundedReadAlignmentCutoverReadiness,
    BoundedReadCutoverReadiness, ContentStoreBoundaryCutoverReadiness,
    GraphReplacementCutoverReadiness, GraphRouteAlignmentCutoverReadiness,
    GraphRouteCutoverReadiness, GraphRouteParityAlignmentCutoverReadiness,
    IntegrationBundleProtocolCutoverReadiness, LegacyCoexistenceCutoverReadiness,
    LibraryReadinessCutoverReadiness, NowledgeMemFinalCutoverPreflightReport,
    NowledgeMemIntegrationCheckReport, NowledgeMemIntegrationNextAction,
    NowledgeMemIntegrationReadinessReport, PreviousWrapperPreflightCutoverReadiness,
    QueryFamilyReplacementCutoverReadiness, QueryRuntimePreflightAlignmentCutoverReadiness,
    QueryRuntimePreflightCutoverReadiness, ReplacementSummaryProtocolCutoverReadiness,
    RouteOwnershipCutoverReadiness, SearchCandidateCutoverReadiness,
    SearchProjectionCutoverReadiness, SkeinSubmoduleCutoverReadiness,
    StorageRecoveryCutoverReadiness, NOWLEDGE_MEM_FINAL_CUTOVER_PREFLIGHT_PROTOCOL,
    NOWLEDGE_MEM_INTEGRATION_READINESS_PROTOCOL, NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL,
};
pub use mem_library_readiness::{
    parse_bounded_probe_json, parse_mem_library_covered_routes_json,
    parse_mem_library_graph_route_readiness_json, parse_mem_library_readiness_mode,
    parse_parameters_json, value_from_json, NowledgeMemLibraryReadinessRunReport,
};
pub use nowledge_fuzz::{
    nowledge_query_fuzz_harness, NowledgeQueryFuzzCaseReport, NowledgeQueryFuzzHarnessOptions,
    NowledgeQueryFuzzHarnessReport, NOWLEDGE_QUERY_FUZZ_HARNESS_PROTOCOL,
};
pub use nowledge_inventory::{
    background_maintenance_evidence_health, background_maintenance_evidence_health_from_bundle,
    replacement_readiness_family_evidence_health,
    replacement_readiness_family_evidence_health_from_bundle, scan_nowledge_query_inventory,
    scan_nowledge_query_inventory_cypher_coverage_detail_to_json,
    scan_nowledge_query_inventory_cypher_coverage_to_json,
    scan_nowledge_query_inventory_cypher_migration_gate_to_json,
    scan_nowledge_query_inventory_cypher_migration_gate_with_options_to_json,
    scan_nowledge_query_inventory_to_json, scan_nowledge_query_inventory_with_options,
    storage_recovery_evidence_health, storage_recovery_evidence_health_from_bundle,
    BackgroundMaintenanceEvidenceHealth, NowledgeCypherMigrationGateJsonOptions,
    NowledgeInventoryScanOptions, ReplacementReadinessFamilyEvidenceHealth,
    StorageRecoveryEvidenceHealth, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};
pub use nowledge_mem::{
    nowledge_mem_bounded_read_evidence_json,
    nowledge_mem_bounded_read_evidence_json_with_route_readiness,
    nowledge_mem_bounded_read_evidence_json_with_routes, nowledge_mem_graph_config,
    nowledge_mem_graph_config_with_search_mode, nowledge_mem_graph_read_route_catalog_digest,
    nowledge_mem_graph_read_route_spec, nowledge_mem_graph_read_route_spec_json,
    nowledge_mem_graph_read_route_specs_json, nowledge_mem_required_query_families_for_route,
    nowledge_mem_search_candidate_shadow_evidence_json,
    nowledge_mem_source_mutation_dual_write_evidence_all_ready,
    nowledge_mem_source_mutation_dual_write_readiness,
    nowledge_mem_source_mutation_family_requirements, NowledgeMemBackgroundMaintenanceReport,
    NowledgeMemCutoverControls, NowledgeMemCutoverControlsReport, NowledgeMemEmbeddedStore,
    NowledgeMemEmbeddedStoreHandle, NowledgeMemGraph, NowledgeMemGraphAugmentationStateOptions,
    NowledgeMemGraphAugmentationStateOutput, NowledgeMemGraphAugmentationStateRouteReport,
    NowledgeMemGraphAugmentationStateRow, NowledgeMemGraphCanvasEdge, NowledgeMemGraphCanvasMode,
    NowledgeMemGraphCanvasNode, NowledgeMemGraphCanvasOptions, NowledgeMemGraphCanvasOutput,
    NowledgeMemGraphCommunityMembersOptions, NowledgeMemGraphCommunityMembersOutput,
    NowledgeMemGraphCommunityMembersRouteReport, NowledgeMemGraphCommunityRecentMemoriesOptions,
    NowledgeMemGraphCommunityRecentMemoriesOutput,
    NowledgeMemGraphCommunityRecentMemoriesRouteReport, NowledgeMemGraphCommunityRecentMemoryRow,
    NowledgeMemGraphCommunitySubgraphEdgeRow, NowledgeMemGraphCommunitySubgraphEntityRow,
    NowledgeMemGraphCommunitySubgraphOptions, NowledgeMemGraphCommunitySubgraphOutput,
    NowledgeMemGraphCommunitySubgraphRouteReport, NowledgeMemGraphMode,
    NowledgeMemGraphNodeDetailsOptions, NowledgeMemGraphNodeDetailsOutput,
    NowledgeMemGraphNodeDetailsRouteReport, NowledgeMemGraphNodeDetailsRow,
    NowledgeMemGraphOrphanEntityRow, NowledgeMemGraphOrphansOptions, NowledgeMemGraphOrphansOutput,
    NowledgeMemGraphOrphansRouteReport, NowledgeMemGraphOverviewOptions,
    NowledgeMemGraphOverviewOutput, NowledgeMemGraphOverviewRouteReport,
    NowledgeMemGraphOverviewRow, NowledgeMemGraphPageRankPlanMetaRow,
    NowledgeMemGraphPageRankPlanOptions, NowledgeMemGraphPageRankPlanOutput,
    NowledgeMemGraphPageRankPlanRouteReport, NowledgeMemGraphReadRouteEvidenceKind,
    NowledgeMemGraphReadRouteOwner, NowledgeMemGraphReadRouteSpec, NowledgeMemGraphSampleOptions,
    NowledgeMemGraphSampleOutput, NowledgeMemGraphSampleRouteReport,
    NowledgeMemLibraryProductionPathSummary, NowledgeMemLibraryReadinessReport,
    NowledgeMemOpenOptions, NowledgeMemOpenReport, NowledgeMemOperationsReadinessReport,
    NowledgeMemOutOfCoreSearchCandidateOutput, NowledgeMemOutOfCoreSearchProjection,
    NowledgeMemProductionStatus, NowledgeMemQualifiedOutOfCoreSearchOptions,
    NowledgeMemQueryExecutionPath, NowledgeMemQueryOutput, NowledgeMemQueryReport,
    NowledgeMemQueryReportOptions, NowledgeMemReadControl, NowledgeMemReadOptions,
    NowledgeMemReadOutput, NowledgeMemReadReport, NowledgeMemReadSnapshot,
    NowledgeMemReadSnapshotBudget, NowledgeMemReadSnapshotReport, NowledgeMemReadinessAreaMap,
    NowledgeMemReadinessAreaSummary, NowledgeMemReadinessDashboard, NowledgeMemReadinessOptions,
    NowledgeMemReadinessRedactionSummary, NowledgeMemRetrievalOutput, NowledgeMemRetrievalReport,
    NowledgeMemRouteReadinessSummary, NowledgeMemRuntimeStatus,
    NowledgeMemSearchCandidateFieldSummary, NowledgeMemSearchCandidateFilterPushdownEvidence,
    NowledgeMemSearchCandidateOutput, NowledgeMemSearchCandidateReadinessOptions,
    NowledgeMemSearchCandidateReadinessReport, NowledgeMemSearchCandidateReport,
    NowledgeMemSearchCandidateRequest, NowledgeMemSearchCandidateShadowAccumulator,
    NowledgeMemSearchCandidateShadowEvidence, NowledgeMemSearchHydrationOutput,
    NowledgeMemSearchProjection, NowledgeMemSearchProjectionOpenMode,
    NowledgeMemSearchProjectionRole, NowledgeMemServingEntrypoint, NowledgeMemServingPathReadiness,
    NowledgeMemSlowQueryRecord, NowledgeMemSlowQueryReport,
    NowledgeMemSourceMutationDualWriteEvidence, NowledgeMemSourceMutationDualWriteReadinessReport,
    NowledgeMemSourceMutationFamilyRequirement, NowledgeMemStorageLifecycleActionKind,
    NowledgeMemStorageLifecycleDecision, NowledgeMemStorageRecoveryReport, NowledgeMemWorkControl,
    NowledgeQueryRuntimePreflightProbe, NowledgeQueryRuntimePreflightProbeReport,
    NowledgeQueryRuntimePreflightReport, NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
    NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL, NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_QUERY,
    NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE,
    NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE_REPORT_PROTOCOL,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_MEMORY_QUERY, NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE_REPORT_PROTOCOL,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_QUERY,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE_REPORT_PROTOCOL,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_EDGE_QUERY,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ENTITY_QUERY,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE_REPORT_PROTOCOL,
    NOWLEDGE_MEM_GRAPH_NODE_DETAILS_MEMORY_QUERY, NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE,
    NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE_REPORT_PROTOCOL, NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE,
    NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE_REPORT_PROTOCOL, NOWLEDGE_MEM_GRAPH_ORPHAN_ENTITIES_QUERY,
    NOWLEDGE_MEM_GRAPH_OVERVIEW_MEMORY_RANKING_QUERY, NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE,
    NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE_REPORT_PROTOCOL,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ACTIVE_MEMORY_RELATION_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_RELATION_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_RELATION_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MENTION_EDGE_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_RELATION_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_GRAPH_META_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MEMORY_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MENTION_EDGE_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE, NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE_REPORT_PROTOCOL,
    NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION, NOWLEDGE_MEM_GRAPH_SAMPLE_MEMORY_QUERY,
    NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE, NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE_REPORT_PROTOCOL,
    NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL, NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL,
    NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL, NOWLEDGE_MEM_PRODUCTION_STATUS_PROTOCOL,
    NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL, NOWLEDGE_MEM_READINESS_DASHBOARD_PROTOCOL,
    NOWLEDGE_MEM_READ_REPORT_PROTOCOL, NOWLEDGE_MEM_RETRIEVAL_REPORT_PROTOCOL,
    NOWLEDGE_MEM_RUNTIME_STATUS_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE, NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE, NOWLEDGE_MEM_SEARCH_ROUTE,
    NOWLEDGE_MEM_SERVING_PATH_READINESS_PROTOCOL, NOWLEDGE_MEM_SLOW_QUERY_REPORT_PROTOCOL,
    NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_CONTENT_REFRESH_REPARSE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_GRAPH_DELETE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INDEXED_TRANSITION,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_INGEST_CREATE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_LIFECYCLE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_PATCH_DELETE,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_REVISION_EDGES,
    NOWLEDGE_MEM_SOURCE_MUTATION_FAMILY_SEARCH_PROJECTION_EFFECTS,
    NOWLEDGE_MEM_STORAGE_LIFECYCLE_DECISION_PROTOCOL, NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL,
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES, REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES,
};
pub use previous_wrapper_preflight::{
    nowledge_previous_wrapper_preflight_check, nowledge_previous_wrapper_preflight_check_json,
    IntoNowledgePreviousWrapperPreflightInputs, NowledgePreviousWrapperPreflightCheckReport,
    NowledgePreviousWrapperPreflightInputs, NowledgePreviousWrapperPreflightReport,
    NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT_PROTOCOL,
};
pub use production_evidence::{
    ProductionEvidenceBinding, ProductionQualificationIdentity,
    PRODUCTION_QUALIFICATION_POLICY_VERSION,
};
pub use qos::{
    BackgroundWorkDecision, BackgroundWorkHint, BackgroundWorkPlan, BackgroundWorkReasonCode,
    LocalQosClassSnapshot, LocalQosPermit, LocalQosPolicy, LocalQosScheduler, LocalQosSnapshot,
    LocalQosState, QosAdmission, QosAdmissionCode, QosSnapshotBlockerCode, QosTelemetryEvent,
    QosTelemetryOutcome, QosTelemetryPhase, QosTelemetrySink, RankedBackgroundWork, WorkClass,
    WorkPriority, WorkRequest, WORK_CLASS_COUNT,
};
pub use query_family_evidence::nowledge_query_family_evidence_json;
pub use query_runtime_preflight::{
    parse_query_runtime_preflight_probes, query_runtime_preflight_json,
};
pub use replacement_summary::{
    nowledge_graph_route_readiness_summary, nowledge_graph_route_readiness_summary_from_bundle,
    nowledge_replacement_summary_json, nowledge_replacement_summary_json_with_options,
    GraphRouteReadinessSummary, NowledgeReplacementSummaryOptions,
};
pub use route_ownership::{
    nowledge_mem_route_ownership_all_legacy, nowledge_mem_route_ownership_all_skein,
    nowledge_mem_route_ownership_for_engine, nowledge_mem_route_ownership_readiness,
    NowledgeMemRouteOwnership, NowledgeMemRouteOwnershipPolicy,
    NowledgeMemRouteOwnershipReadinessReport, NowledgeMemRouteReadEngine,
    NOWLEDGE_MEM_ROUTE_OWNERSHIP_PROTOCOL,
};
pub use schema::{
    BasicGraphStatistics, CompositeIndexDescriptor, ConstraintDescriptor, ConstraintId,
    ConstraintKind, ConstraintSubject, GraphStatistics, IndexDescriptor, IndexId, IndexKind,
    PropertyDescriptor, PropertyId, PropertyType, SchemaObjectState, TableDescriptor, TableId,
    TableKind,
};
#[cfg(feature = "vector-search")]
pub use search::turboquant_projection::{
    TurboQuantCandidate, TurboQuantCandidateOutput, TurboQuantCandidateProjection,
    TurboQuantCandidateProjectionBuildOptions, TurboQuantCandidateScanOptions,
};
pub use search::{
    AdaptiveVectorSearchOptions, CompressedVectorSearchMode, MetadataRepairOptions,
    MetadataRepairSummary, SearchAccessControlContext, SearchAnalyzerLexicon,
    SearchCandidateSetReport, SearchCheckpointReport, SearchDerivedArtifactReport, SearchDocument,
    SearchEmbeddingManifest, SearchEmptyReasonCode, SearchFallbackReasonCode, SearchFusionWeights,
    SearchHit, SearchIndex, SearchLexicalFeasibilityCoverage, SearchLexicalFeasibilityMetrics,
    SearchLexicalProductionQualificationReport, SearchMode, SearchOutOfCoreConfig,
    SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationBuildReport,
    SearchOutOfCoreGenerationUpdate, SearchOutOfCoreGenerationWriter,
    SearchOutOfCoreHydrationOutput, SearchOutOfCoreMetrics, SearchOutOfCoreOutput,
    SearchOutOfCoreReader, SearchPredicateFieldPruningReport, SearchPredicatePushdownReport,
    SearchProjectionCleanupOptions, SearchProjectionCleanupReport, SearchProjectionDelta,
    SearchProjectionDeltaReport, SearchProjectionFreshness, SearchProjectionKind,
    SearchProjectionProbeOptions, SearchProjectionQualificationIdentity, SearchProjectionRow,
    SearchQueryOptions, SearchRangeReadConfig, SearchRebuildOptions, SearchRebuildSummary,
    SearchResultSet, SearchRetrieverCandidateSetReport, SearchTopKScoreParity,
    SearchTruncationReasonCode, VectorProjectionQualificationIdentity,
    VectorProjectionResourceEvidence, VectorRecallProductionQualificationReport,
    VectorRecallValidationBlocker, VectorRecallValidationOptions, VectorRecallValidationReport,
    VectorSearchExecutionOptions, VectorSearchKernelPreference,
    DEFAULT_VECTOR_SEARCH_WORKING_BYTES, MAX_VECTOR_RECALL_VALIDATION_CANDIDATE_LIMIT,
    MAX_VECTOR_RECALL_VALIDATION_SAMPLES, MAX_VECTOR_RECALL_VALIDATION_TOP_K,
    MINIMUM_VECTOR_QUALIFICATION_DOCUMENT_COUNT, NOWLEDGE_MEMORY_MATERIALIZED_METADATA_PATHS,
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS, SEARCH_LEXICAL_QUALIFICATION_PROTOCOL,
    SEARCH_LEXICAL_QUALIFICATION_PROTOCOL_VERSION, SEARCH_PROJECTION_CLEANUP_PROTOCOL,
    VECTOR_RECALL_PRODUCTION_QUALIFICATION_PROTOCOL, VECTOR_RECALL_VALIDATION_PROTOCOL,
};
pub use search_candidate_shadow_evidence::parse_search_candidate_shadow_probe;
pub use search_projection_evidence::{
    nowledge_search_projection_probe_contract_json, NowledgeSearchProjectionEvidenceReport,
};
pub use search_route_ownership::{
    active_search_route_read_requirement, nowledge_mem_active_search_route_ownership_all_lancedb,
    nowledge_mem_active_search_route_ownership_all_skein,
    nowledge_mem_active_search_route_ownership_for_engine,
    nowledge_mem_active_search_route_ownership_readiness,
    nowledge_mem_active_search_route_read_evidence_all_skein_ready,
    nowledge_mem_active_search_route_read_requirements, nowledge_mem_active_search_route_readiness,
    nowledge_mem_search_route_ownership_all_lancedb, nowledge_mem_search_route_ownership_all_skein,
    nowledge_mem_search_route_ownership_for_engine, nowledge_mem_search_route_ownership_readiness,
    required_projection_route_for_active_search_route, NowledgeMemActiveSearchRouteOwnership,
    NowledgeMemActiveSearchRouteOwnershipReadinessReport, NowledgeMemActiveSearchRouteReadEvidence,
    NowledgeMemActiveSearchRouteReadRequirement, NowledgeMemActiveSearchRouteReadinessReport,
    NowledgeMemSearchReadEngine, NowledgeMemSearchRouteOwnership,
    NowledgeMemSearchRouteOwnershipPolicy, NowledgeMemSearchRouteOwnershipReadinessReport,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_COMMUNITY_DISCOVERY,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_DEEP_SEARCH_GRAPH_EXPANSION,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_ENTITY_DISCOVERY, NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_FS_RECALL,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_MCP_SEARCH,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_SOURCE_CHUNK_RECALL,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_SOURCE_RECALL,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_THREAD_MESSAGE_FTS, NOWLEDGE_MEM_SEARCH_ROUTE_COMMUNITY,
    NOWLEDGE_MEM_SEARCH_ROUTE_ENTITY, NOWLEDGE_MEM_SEARCH_ROUTE_MEMORY,
    NOWLEDGE_MEM_SEARCH_ROUTE_MESSAGE, NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_ROUTE_SOURCE, NOWLEDGE_MEM_SEARCH_ROUTE_SOURCE_CHUNK,
    REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES, REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES,
};
pub use skein_core::{
    GraphRagCommonPathSummary, GraphRagGeneratedQuery, GraphRagLabelSummary,
    GraphRagPropertySubject, GraphRagPropertySummary, GraphRagQueryBinding, GraphRagQueryDraft,
    GraphRagQueryGenerationError, GraphRagQueryParameterCardinality, GraphRagQueryParameterError,
    GraphRagQueryParameterRequirement, GraphRagQueryPattern, GraphRagQueryPredicate,
    GraphRagQueryPredicateOperator, GraphRagQueryProjection, GraphRagRelationshipTypeSummary,
    GraphRagRouteSummary, GraphRagSchemaContext, GraphRagSchemaContextOptions,
    GraphRagSchemaContextTruncation, RuntimeCancellationReason, RuntimeCancellationToken,
    RuntimeCapabilities, RuntimeCapability, RuntimeTaskContext, DEFAULT_GRAPH_RAG_MAX_COMMON_PATHS,
    DEFAULT_GRAPH_RAG_MAX_LABELS, DEFAULT_GRAPH_RAG_MAX_PROPERTIES_PER_SUBJECT,
    DEFAULT_GRAPH_RAG_MAX_RELATIONSHIP_TYPES, DEFAULT_GRAPH_RAG_MAX_ROUTES,
    GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL, MAX_GRAPH_RAG_QUERY_LIMIT,
};
pub use skein_optimizer::{
    AdaptiveVectorBackendPolicy, Distribution, GroupId, Memo as OptimizerMemo,
    MemoGroup as OptimizerMemoGroup, PhysicalProperties, RequiredProperties,
};
pub use skein_qos::{
    IoConcurrencyBudget, ProcessMemoryCapabilities, ProcessMemoryProfile, ProcessMemorySnapshot,
    RuntimeAdmissionCode, RuntimeAdmissionError, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeGovernorLimits, RuntimeGovernorSnapshot, RuntimeMemoryPressure, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeTelemetryEvent,
    RuntimeTelemetryEventKind, RuntimeTelemetrySink, RuntimeWorkKind, RuntimeWorkPriority,
    RuntimeWorkRequest, StorageDeviceDiscoverySource, StorageDeviceProfile, StorageMediaKind,
};
#[cfg(feature = "tokio-runtime")]
pub use skein_runtime_tokio::{
    TokioRuntimeAdapter, TokioRuntimeConfig, TokioRuntimeError, TokioRuntimeOwnership,
    TokioTaskError,
};
pub use skein_storage::ScanPredicate;
#[cfg(feature = "vector-search")]
pub use skein_vector_projection::{
    KernelPreference as TurboQuantKernelPreference,
    ProjectionBuildReport as TurboQuantCandidateProjectionBuildReport,
    ProjectionManifest as TurboQuantCandidateProjectionManifest,
    ProjectionSearchReport as TurboQuantCandidateScanReport, ScanKernel as TurboQuantScanKernel,
};
pub use storage_recovery_evidence::nowledge_storage_recovery_evidence_json;
pub use store::{
    restore_storage_backup, AdjacencyConsistencyReport, AdjacencyConsolidationPlan,
    AdjacencyConsolidationReport, AdjacencyDirection, AdjacencyGroupConsistencyMismatch,
    AdjacencyGroupKey, AdjacencyGroupStats, AdjacencyLayout, BasicStatisticsConsistencyReport,
    DatabaseDoctor, DegreeStatisticsConsistencyReport, DegreeStatisticsEntry, DegreeStatisticsKey,
    DerivedArtifactHealth, DerivedArtifactHealthReport, DerivedArtifactHealthState,
    DerivedArtifactKind, DerivedArtifactRebuildOptions, DerivedArtifactRepairPlan,
    DerivedArtifactRepairReport, DistinctValueStatisticsConsistencyReport, DurabilityPolicy,
    FileSegmentRangeReader, OptimizerStatisticsRefreshOptions, OptimizerStatisticsRefreshReport,
    OrderedAdjacencyEntry, PropertyIndexConsistencyReport, PublishedReadView, RecoveryMode,
    SearchProjectionChangefeedReadiness, SearchProjectionChangefeedStatus,
    SearchProjectionMutationId, SegmentCacheSnapshot, SegmentRangeReader, SegmentReadError,
    SegmentReadExecutionError, SegmentReadExecutionReport, SegmentReadExecutor, SegmentReadPayload,
    SegmentReadRange, SegmentReadSchedule, SegmentReadScheduler, SegmentReadWave,
    StorageBackupReport, StorageDebtController, StoragePressureReasonCode, StoragePressureSignals,
    StoragePressureSnapshot, StoragePressureState, StorageReclamationWatermark,
    StorageRecoveryReport, StorageResidencyMode, StorageResidencyReport, StorageRestoreReport,
    StorageScrubReport, WalDoctorOptions, WalRepairAcknowledgement, WalReplayConfig,
    WalTailRepairPlan, WalTailRepairReason, WalTailRepairReport, DENSE_ADJACENCY_DEGREE_THRESHOLD,
    DERIVED_ARTIFACT_REPAIR_PROTOCOL, STORAGE_PRESSURE_DELAY_RATIO_PER_MILLION,
    STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION, WAL_DOCTOR_REPAIR_PROTOCOL,
};
#[cfg(feature = "opentelemetry")]
pub use telemetry::OpenTelemetryMetrics;
pub use telemetry::{
    operations_telemetry_readiness, qos_telemetry_sink, KernelTelemetry, KernelTelemetryOperation,
    OperationsTelemetryReadiness, QueryTelemetry, TelemetrySink, REQUIRED_OPERATIONS_TELEMETRY,
};
pub use value::Value;
pub use workload_fixtures::{
    nowledge_graph_route_workload_fixture_queries, nowledge_graph_route_workload_fixture_report,
    NowledgeGraphRouteWorkloadBoundedExpansionReport, NowledgeGraphRouteWorkloadFixtureOptions,
    NowledgeGraphRouteWorkloadFixtureReport, NowledgeGraphRouteWorkloadQueryReport,
    NowledgeGraphRouteWorkloadRouteReport, NowledgeSearchMetadataWorkloadReport,
    NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
};

#[cfg(test)]
mod tests {
    use super::{Database, NowledgeGraphAdapter, NowledgeGraphStatement, Value};
    use std::collections::BTreeMap;

    #[test]
    fn crate_root_exports_query_runtime_front_door() {
        let mut db = Database::new();
        let mut adapter = NowledgeGraphAdapter::new(&mut db);
        let create = NowledgeGraphStatement {
            cypher: "CREATE (:Memory {id: $id, title: $title})".to_string(),
            parameters: BTreeMap::from([
                ("id".to_string(), Value::String("root".to_string())),
                ("title".to_string(), Value::String("Root".to_string())),
            ]),
        };
        adapter.query(&create).unwrap();

        let read = NowledgeGraphStatement {
            cypher: "MATCH (m:Memory {id: $id}) RETURN m.title AS title".to_string(),
            parameters: BTreeMap::from([("id".to_string(), Value::String("root".to_string()))]),
        };
        let output = adapter.query(&read).unwrap();

        assert_eq!(
            output.rows[0].get("title"),
            Some(&Value::String("Root".to_string()))
        );
    }
}
