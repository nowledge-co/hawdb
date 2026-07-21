pub mod analytics;
pub mod api;
pub mod compat;
pub mod cypher;
pub mod executor;
pub mod nowledge_inventory;
pub mod nowledge_mem;
pub mod optimizer;
pub mod planner;
pub mod qos;
pub mod search;
pub mod store;

mod regex_cache;
mod search_filter;
#[path = "cli_search_projection_evidence.rs"]
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

pub use analytics::{
    CommunityAssignment, LouvainOptions, PageRankOptions, PageRankScore, ProjectedGraph,
};
pub use api::{
    validate_graph_lightning_graph_stream, BackgroundMaintenanceCandidate,
    BackgroundMaintenanceKind, BackgroundMaintenanceOptions, BackgroundMaintenanceSummary,
    BackgroundMaintenanceSummaryItem, BoundedReadQueryOutput, CanonicalGraphSnapshotExport,
    CanonicalGraphSnapshotValidation, CanonicalSnapshotEndpointViolation,
    CanonicalSnapshotIdentityAudit, CanonicalSnapshotNode, CanonicalSnapshotRelationship,
    CanonicalStableIdMapping, Database, DatabaseConfig, DatabaseReadTransaction,
    DatabaseTransaction, DerivedArtifactJob, DerivedArtifactJobReport, DerivedArtifactJobStatus,
    ExternalContentArtifactJobCompletion, ExternalContentArtifactJobSummary,
    ExternalContentArtifactRuntimeManifest, GraphLightningBootstrapExport,
    GraphLightningBootstrapManifest, GraphLightningGraphStream,
    GraphLightningGraphStreamValidation, KnowledgeAugmentationJob,
    KnowledgeAugmentationJobInterruptOutput, KnowledgeAugmentationJobInterruptRequest,
    KnowledgeAugmentationJobInterruptRow, KnowledgeAugmentationJobLifecycleBatchOutput,
    KnowledgeAugmentationJobLifecycleBatchRequest, KnowledgeAugmentationJobLifecycleBatchRow,
    KnowledgeAugmentationJobLifecycleTransition, KnowledgeAugmentationJobLifecycleUpdate,
    KnowledgeAugmentationJobListOrder, KnowledgeAugmentationJobListOutput,
    KnowledgeAugmentationJobListRequest, KnowledgeAugmentationJobOutput,
    KnowledgeAugmentationJobRequest, KnowledgeCandidate, KnowledgeCandidateScoreBreakdown,
    KnowledgeCandidateScoringPolicy, KnowledgeCandidateSource,
    KnowledgeCommunityAssignmentClearOutput, KnowledgeCommunityAssignmentClearRequest,
    KnowledgeCommunityAssignmentClearRow, KnowledgeCommunityCleanupOutput,
    KnowledgeCommunityCleanupRequest, KnowledgeCommunityCleanupRow, KnowledgeCommunityCreate,
    KnowledgeCommunityCreateBatchRow, KnowledgeCommunityEntityVisibilityOutput,
    KnowledgeCommunityEntityVisibilityRequest, KnowledgeCommunityEntityVisibilityRow,
    KnowledgeCommunityLifecycleBatchOutput, KnowledgeCommunityLifecycleBatchRequest,
    KnowledgeCommunityListOrder, KnowledgeCommunityListOutput, KnowledgeCommunityListRequest,
    KnowledgeCommunityLookupKey, KnowledgeCommunityMembershipCreate,
    KnowledgeCommunityMembershipCreateBatchOutput, KnowledgeCommunityMembershipCreateBatchRequest,
    KnowledgeCommunityMembershipCreateBatchRow, KnowledgeCommunityMemoryCrystalFilter,
    KnowledgeCommunityMemoryListOrder, KnowledgeCommunityMemoryListOutput,
    KnowledgeCommunityMemoryListRequest, KnowledgeCommunityMemoryRow,
    KnowledgeCommunityMemoryRowSource, KnowledgeCommunityMemorySource, KnowledgeCommunityOutput,
    KnowledgeCommunityRequest, KnowledgeCommunityRow, KnowledgeCommunitySummaryUpdate,
    KnowledgeCommunitySummaryUpdateBatchRow, KnowledgeContextMemoryLatestFilter,
    KnowledgeContextMemoryPreviewOutput, KnowledgeContextMemoryPreviewRequest,
    KnowledgeContextMemoryPreviewRow, KnowledgeCrystalCommunityListOrder,
    KnowledgeCrystalCommunityListOutput, KnowledgeCrystalCommunityListRequest,
    KnowledgeCrystalCommunityRow, KnowledgeCrystalCommunityScope, KnowledgeCrystalListOrder,
    KnowledgeCrystalListOutput, KnowledgeCrystalListRequest, KnowledgeCrystalRow,
    KnowledgeCrystalSourceMergeOutput, KnowledgeCrystalSourceMergeRequest,
    KnowledgeCrystalSourceVisibilityOutput, KnowledgeCrystalSourceVisibilityRequest,
    KnowledgeCrystalSourceVisibilityRow, KnowledgeEntity, KnowledgeEntityBatchOutput,
    KnowledgeEntityBatchRequest, KnowledgeEntityCreateBatchOutput,
    KnowledgeEntityCreateBatchRequest, KnowledgeEntityCreateBatchRow, KnowledgeEntityCreateOutput,
    KnowledgeEntityCreateRequest, KnowledgeEntityDeleteBatchOutput,
    KnowledgeEntityDeleteBatchRequest, KnowledgeEntityDeleteBatchRow,
    KnowledgeEntityDeleteGuardOutput, KnowledgeEntityDeleteGuardRequest,
    KnowledgeEntityDeleteOutput, KnowledgeEntityDeleteRequest, KnowledgeEntityLabelGroup,
    KnowledgeEntityLabelListOutput, KnowledgeEntityLabelListRequest,
    KnowledgeEntityLabelProjectedGroup, KnowledgeEntityLabelProjectedListOutput,
    KnowledgeEntityLabelProjectedListRequest, KnowledgeEntityLabelProjectedRow,
    KnowledgeEntityLabelRow, KnowledgeEntityMentionCountCursor,
    KnowledgeEntityMentionCountListOutput, KnowledgeEntityMentionCountListRequest,
    KnowledgeEntityMentionCountRow, KnowledgeEntityOutput, KnowledgeEntityRequest,
    KnowledgeEntityUpsertBatchOutput, KnowledgeEntityUpsertBatchRequest,
    KnowledgeEntityUpsertBatchRow, KnowledgeEntityUpsertOutput, KnowledgeEntityUpsertRequest,
    KnowledgeEvidence, KnowledgeFallbackReasonCode, KnowledgeFanoutReasonCode,
    KnowledgeFanoutReasonDetail, KnowledgeGraphContextPath, KnowledgeGraphMeta,
    KnowledgeGraphMetaDeleteOutput, KnowledgeGraphMetaOutput, KnowledgeGraphMetaProjected,
    KnowledgeGraphMetaProjectedOutput, KnowledgeGraphMetaProjectedRequest,
    KnowledgeGraphMetaRequest, KnowledgeGraphMetaStamp, KnowledgeGraphMetaStampBatchOutput,
    KnowledgeGraphMetaStampBatchRequest, KnowledgeGraphMetaStampBatchRow, KnowledgeGraphPath,
    KnowledgeGraphPathDirection, KnowledgeGraphSeed, KnowledgeInducedEdgeListOutput,
    KnowledgeInducedEdgeListRequest, KnowledgeInducedEdgeRow, KnowledgeLabelBackfillScanRequest,
    KnowledgeLabelCanonicalLookupRequest, KnowledgeLabelLifecycleBatchOutput,
    KnowledgeLabelLifecycleBatchRequest, KnowledgeLabelLifecycleBatchRow,
    KnowledgeLabelLifecycleUpdate, KnowledgeLabelMemoryDistributionOutput,
    KnowledgeLabelMemoryDistributionRequest, KnowledgeLabelMemoryDistributionRow,
    KnowledgeLabelMemoryTransferOutput, KnowledgeLabelMemoryTransferRequest,
    KnowledgeLabelMemoryTransferRow, KnowledgeLabelRegexMemoryConnectionRow,
    KnowledgeLabelRegexMemoryConnectionsOutput, KnowledgeLabelRegexMemoryConnectionsRequest,
    KnowledgeLabelUsageListOutput, KnowledgeLabelUsageListRequest, KnowledgeLabelUsageOutput,
    KnowledgeLabelUsageRequest, KnowledgeLabelUsageRow, KnowledgeMemoryAccessBatchOutput,
    KnowledgeMemoryAccessBatchRequest, KnowledgeMemoryAccessBatchRow, KnowledgeMemoryAccessTouch,
    KnowledgeMemoryCleanupFingerprintOutput, KnowledgeMemoryCleanupFingerprintRequest,
    KnowledgeMemoryCleanupFingerprintRow, KnowledgeMemoryCompactingThreadListOutput,
    KnowledgeMemoryCompactingThreadListRequest, KnowledgeMemoryCompactingThreadProjectedListOutput,
    KnowledgeMemoryCompactingThreadProjectedListRequest,
    KnowledgeMemoryCompactingThreadProjectedRow, KnowledgeMemoryCompactingThreadRow,
    KnowledgeMemoryContentBatchOutput, KnowledgeMemoryContentBatchRequest,
    KnowledgeMemoryContentBatchRow, KnowledgeMemoryContentUpdate,
    KnowledgeMemoryCrystalSynthesisCountOutput, KnowledgeMemoryCrystalSynthesisCountRequest,
    KnowledgeMemoryCrystalSynthesisCountRow, KnowledgeMemoryDecayDetail,
    KnowledgeMemoryDecayDetailOutput, KnowledgeMemoryDecayDetailRequest,
    KnowledgeMemoryDecayRefreshBatchOutput, KnowledgeMemoryDecayRefreshBatchRequest,
    KnowledgeMemoryDecayRefreshBatchRow, KnowledgeMemoryDecayRefreshUpdate,
    KnowledgeMemoryDedupReviewedBatchOutput, KnowledgeMemoryDedupReviewedBatchRequest,
    KnowledgeMemoryDedupReviewedBatchRow, KnowledgeMemoryEntityGroup,
    KnowledgeMemoryEntityListOutput, KnowledgeMemoryEntityListRequest, KnowledgeMemoryEntityRow,
    KnowledgeMemoryEvolvesCreate, KnowledgeMemoryEvolvesCreateBatchOutput,
    KnowledgeMemoryEvolvesCreateBatchRequest, KnowledgeMemoryEvolvesCreateBatchRow,
    KnowledgeMemoryEvolvesLatestOutput, KnowledgeMemoryEvolvesLatestRequest,
    KnowledgeMemoryEvolvesLatestRow, KnowledgeMemoryEvolvesNeighborOutput,
    KnowledgeMemoryEvolvesNeighborRequest, KnowledgeMemoryEvolvesNeighborRow,
    KnowledgeMemoryEvolvesProjectedSuccessorCursor, KnowledgeMemoryEvolvesProjectedSuccessorGroup,
    KnowledgeMemoryEvolvesProjectedSuccessorOrder, KnowledgeMemoryEvolvesProjectedSuccessorOutput,
    KnowledgeMemoryEvolvesProjectedSuccessorPageCursor,
    KnowledgeMemoryEvolvesProjectedSuccessorRequest, KnowledgeMemoryEvolvesProjectedSuccessorRow,
    KnowledgeMemoryEvolvesRelationCountOutput, KnowledgeMemoryEvolvesRelationCountRequest,
    KnowledgeMemoryEvolvesRelationCountRow, KnowledgeMemoryLabelDeleteOutput,
    KnowledgeMemoryLabelDeleteRequest, KnowledgeMemoryLabelTransferOutput,
    KnowledgeMemoryLabelTransferRequest, KnowledgeMemoryLabelTransferRow,
    KnowledgeMemoryLatestBatchOutput, KnowledgeMemoryLatestBatchRequest,
    KnowledgeMemoryLatestBatchRow, KnowledgeMemoryLatestUpdate,
    KnowledgeMemoryLifecycleBatchOutput, KnowledgeMemoryLifecycleBatchRequest,
    KnowledgeMemoryLifecycleBatchRow, KnowledgeMemoryLifecycleUpdate, KnowledgeMemoryListOrder,
    KnowledgeMemoryListOutput, KnowledgeMemoryListRequest, KnowledgeMemoryListRow,
    KnowledgeMemoryMetadataBatchOutput, KnowledgeMemoryMetadataBatchRequest,
    KnowledgeMemoryMetadataBatchRow, KnowledgeMemoryMetadataRelatedProjectedListOutput,
    KnowledgeMemoryMetadataRelatedProjectedListRequest, KnowledgeMemoryMetadataUpdate,
    KnowledgeMemoryPrefixOwnershipOutput, KnowledgeMemoryPrefixOwnershipRequest,
    KnowledgeMemoryPrefixOwnershipRow, KnowledgeMemoryProjectedListOutput,
    KnowledgeMemoryProjectedListRequest, KnowledgeMemoryProjectedRow,
    KnowledgeMemorySourceAttributionOutput, KnowledgeMemorySourceAttributionRequest,
    KnowledgeMemorySourceAttributionRow, KnowledgeMemoryTitleContentOutput,
    KnowledgeMemoryTitleContentRequest, KnowledgeMemoryTitleContentRow, KnowledgeNeighborDirection,
    KnowledgeNeighborsOutput, KnowledgeNeighborsRequest, KnowledgeNormalizedSpaceMoveBatchOutput,
    KnowledgeNormalizedSpaceMoveBatchRequest, KnowledgeNormalizedSpaceMoveBatchRow,
    KnowledgePageRankCentralEntityOutput, KnowledgePageRankCentralEntityRequest,
    KnowledgePageRankClearOutput, KnowledgePageRankClearRequest, KnowledgePageRankClearRow,
    KnowledgePageRankMembershipOutput, KnowledgePageRankMembershipRequest,
    KnowledgePageRankMembershipRow, KnowledgePageRankMemoryVisibilityOutput,
    KnowledgePageRankMemoryVisibilityRequest, KnowledgePageRankMemoryVisibilityRow,
    KnowledgePageRankPlanOutput, KnowledgePageRankPlanRequest, KnowledgePageRankScoreBatchOutput,
    KnowledgePageRankScoreBatchRequest, KnowledgePageRankScoreBatchRow,
    KnowledgePageRankScoreUpdate, KnowledgePathOutput, KnowledgePathRequest,
    KnowledgePropertyBatchOutput, KnowledgePropertyBatchRequest, KnowledgePropertyRow,
    KnowledgePropertyUpdateBatchOutput, KnowledgePropertyUpdateBatchRequest,
    KnowledgePropertyUpdateBatchRow, KnowledgePropertyUpdateOutput, KnowledgePropertyUpdateRequest,
    KnowledgeRelatedEntityNameListOutput, KnowledgeRelatedEntityNameListRequest,
    KnowledgeRelatedEntityNameScope, KnowledgeRelationshipCreateBatchOutput,
    KnowledgeRelationshipCreateBatchRequest, KnowledgeRelationshipCreateBatchRow,
    KnowledgeRelationshipCreateOutput, KnowledgeRelationshipCreateRequest,
    KnowledgeRelationshipDeleteBatchOutput, KnowledgeRelationshipDeleteBatchRequest,
    KnowledgeRelationshipDeleteBatchRow, KnowledgeRelationshipDeleteOutput,
    KnowledgeRelationshipDeleteRequest, KnowledgeRelationshipGroup,
    KnowledgeRelationshipUpdateBatchOutput, KnowledgeRelationshipUpdateBatchRequest,
    KnowledgeRelationshipUpdateBatchRow, KnowledgeRelationshipUpdateOutput,
    KnowledgeRelationshipUpdateRequest, KnowledgeRelationshipUpsertBatchOutput,
    KnowledgeRelationshipUpsertBatchRequest, KnowledgeRelationshipUpsertBatchRow,
    KnowledgeRelationshipUpsertOutput, KnowledgeRelationshipUpsertRequest,
    KnowledgeRelationshipsOutput, KnowledgeRelationshipsRequest, KnowledgeRetrievalDiagnostics,
    KnowledgeRetrievalEmptyReasonCode, KnowledgeRetrievalOutput, KnowledgeRetrievalRequest,
    KnowledgeRetrieverCandidate, KnowledgeRetrieverReport, KnowledgeSchemaMigrationApply,
    KnowledgeSchemaMigrationApplyBatchOutput, KnowledgeSchemaMigrationApplyBatchRequest,
    KnowledgeSchemaMigrationApplyBatchRow, KnowledgeSchemaMigrationListOutput,
    KnowledgeSchemaMigrationListRequest, KnowledgeSchemaMigrationRow,
    KnowledgeScopedEntityBatchRequest, KnowledgeScopedEntityDeleteBatchRequest,
    KnowledgeScopedEntityDeleteRequest, KnowledgeScopedEntityRequest,
    KnowledgeScopedNeighborsRequest, KnowledgeScopedPathRequest,
    KnowledgeScopedPropertyBatchRequest, KnowledgeScopedPropertyUpdateBatchRequest,
    KnowledgeScopedPropertyUpdateRequest, KnowledgeScopedRelationshipCreateBatchRequest,
    KnowledgeScopedRelationshipCreateRequest, KnowledgeScopedRelationshipDeleteBatchRequest,
    KnowledgeScopedRelationshipDeleteRequest, KnowledgeScopedRelationshipUpdateBatchRequest,
    KnowledgeScopedRelationshipUpdateRequest, KnowledgeScopedRelationshipUpsertBatchRequest,
    KnowledgeScopedRelationshipUpsertRequest, KnowledgeScopedRelationshipsRequest,
    KnowledgeScopedSubgraphRequest, KnowledgeSkillDeleteBatchOutput,
    KnowledgeSkillDeleteBatchRequest, KnowledgeSkillDeleteBatchRow,
    KnowledgeSkillDetailLookupOutput, KnowledgeSkillDetailLookupRequest,
    KnowledgeSkillLifecycleBatchOutput, KnowledgeSkillLifecycleBatchRequest,
    KnowledgeSkillLifecycleBatchRow, KnowledgeSkillLifecycleUpdate, KnowledgeSkillListOrder,
    KnowledgeSkillListOutput, KnowledgeSkillListRequest, KnowledgeSkillMemoryListOrder,
    KnowledgeSkillMemoryListOutput, KnowledgeSkillMemoryListRequest, KnowledgeSkillMemoryRow,
    KnowledgeSkillMetadataBatchOutput, KnowledgeSkillMetadataBatchRequest,
    KnowledgeSkillMetadataBatchRow, KnowledgeSkillMetadataUpdate,
    KnowledgeSkillProjectedListOutput, KnowledgeSkillProjectedListRequest,
    KnowledgeSkillProjectedRow, KnowledgeSkillRow, KnowledgeSkillSourceMergeOutput,
    KnowledgeSkillSourceMergeRequest, KnowledgeSkillStateOutput, KnowledgeSkillStateRequest,
    KnowledgeSkillThreadSourceListOutput, KnowledgeSkillThreadSourceListRequest,
    KnowledgeSkillThreadSourceRow, KnowledgeSkillUsageStatsBatchOutput,
    KnowledgeSkillUsageStatsBatchRequest, KnowledgeSkillUsageStatsBatchRow,
    KnowledgeSkillUsageStatsUpdate, KnowledgeSourceCountOutput, KnowledgeSourceDeleteBatchOutput,
    KnowledgeSourceDeleteBatchRequest, KnowledgeSourceDeleteBatchRow, KnowledgeSourceIdListOutput,
    KnowledgeSourceIdListRequest, KnowledgeSourceLabelAssignment,
    KnowledgeSourceLabelAssignmentBatchOutput, KnowledgeSourceLabelAssignmentBatchRequest,
    KnowledgeSourceLabelAssignmentRow, KnowledgeSourceLabelDelete,
    KnowledgeSourceLabelDeleteBatchOutput, KnowledgeSourceLabelDeleteBatchRequest,
    KnowledgeSourceLabelDeleteRow, KnowledgeSourceLifecycleBatchOutput,
    KnowledgeSourceLifecycleBatchRequest, KnowledgeSourceLifecycleBatchRow,
    KnowledgeSourceLifecycleUpdate, KnowledgeSourceListOrder, KnowledgeSourceListOutput,
    KnowledgeSourceListRequest, KnowledgeSourceListRow, KnowledgeSourceMemoryCountAdjustment,
    KnowledgeSourceMemoryCountBatchOutput, KnowledgeSourceMemoryCountBatchRequest,
    KnowledgeSourceMemoryCountBatchRow, KnowledgeSourceMemoryListOutput,
    KnowledgeSourceMemoryListRequest, KnowledgeSourceMemoryProjectedListOutput,
    KnowledgeSourceMemoryProjectedListRequest, KnowledgeSourceMemoryProjectedRow,
    KnowledgeSourceMemoryRow, KnowledgeSourceMetadataBatchOutput,
    KnowledgeSourceMetadataBatchRequest, KnowledgeSourceMetadataBatchRow,
    KnowledgeSourceMetadataUpdate, KnowledgeSourceOutput, KnowledgeSourceParsedCreate,
    KnowledgeSourceParsedCreateBatchOutput, KnowledgeSourceParsedCreateBatchRequest,
    KnowledgeSourceParsedCreateBatchRow, KnowledgeSourceParsedMetadataBatchOutput,
    KnowledgeSourceParsedMetadataBatchRequest, KnowledgeSourceParsedMetadataBatchRow,
    KnowledgeSourceParsedMetadataUpdate, KnowledgeSourceProjectedListOutput,
    KnowledgeSourceProjectedListRequest, KnowledgeSourceProjectedRow,
    KnowledgeSourceReferenceEntityListOutput, KnowledgeSourceReferenceEntityListRequest,
    KnowledgeSourceReferenceEntityRow, KnowledgeSourceReferenceRelationshipCleanupOutput,
    KnowledgeSourceReferenceRelationshipCleanupRequest,
    KnowledgeSourceReferenceRelationshipCleanupRow,
    KnowledgeSourceReferenceRelationshipCountOutput,
    KnowledgeSourceReferenceRelationshipCountRequest, KnowledgeSourceRequest,
    KnowledgeSourceRevisionCreate, KnowledgeSourceRevisionCreateBatchOutput,
    KnowledgeSourceRevisionCreateBatchRequest, KnowledgeSourceRevisionCreateBatchRow,
    KnowledgeSourceRow, KnowledgeSourceSourcedMemoryCountOutput,
    KnowledgeSourceSourcedMemoryCountRequest, KnowledgeSourceVersionLookupOutput,
    KnowledgeSourceVersionLookupRequest, KnowledgeSubgraphOutput, KnowledgeSubgraphRequest,
    KnowledgeSynthesizedSourceCoverageOutput, KnowledgeSynthesizedSourceCoverageRequest,
    KnowledgeSynthesizedSourceCoverageRow, KnowledgeSynthesizedSourceIdsOutput,
    KnowledgeSynthesizedSourceIdsRequest, KnowledgeSynthesizedSourceIdsRow,
    KnowledgeThreadCompactedMemoryListOutput, KnowledgeThreadCompactedMemoryListRequest,
    KnowledgeThreadCompactedMemoryProjectedListOutput,
    KnowledgeThreadCompactedMemoryProjectedListRequest, KnowledgeThreadCompactedMemoryProjectedRow,
    KnowledgeThreadCompactedMemoryRow, KnowledgeThreadCompactionLinkOutput,
    KnowledgeThreadCompactionLinkRequest, KnowledgeThreadDeleteBatchOutput,
    KnowledgeThreadDeleteBatchRequest, KnowledgeThreadDeleteBatchRow,
    KnowledgeThreadDistillationCandidateOutput, KnowledgeThreadDistillationCandidateRequest,
    KnowledgeThreadDistillationCandidateRow, KnowledgeThreadIdentityCascadeDeleteKeys,
    KnowledgeThreadIdentityDeleteOutput, KnowledgeThreadIdentityDeleteRequest,
    KnowledgeThreadIdentityOutput, KnowledgeThreadIdentityRequest, KnowledgeThreadListOrder,
    KnowledgeThreadListOutput, KnowledgeThreadListRequest, KnowledgeThreadListRow,
    KnowledgeThreadMessageCountBatchOutput, KnowledgeThreadMessageCountBatchRequest,
    KnowledgeThreadMessageCountBatchRow, KnowledgeThreadMessageCountUpdate,
    KnowledgeThreadMessageDeleteOutput, KnowledgeThreadMessageDeleteRequest,
    KnowledgeThreadMessageListOutput, KnowledgeThreadMessageListRequest,
    KnowledgeThreadMessageLookupOutput, KnowledgeThreadMessageLookupRequest,
    KnowledgeThreadMessageRow, KnowledgeThreadMetaLookupOutput, KnowledgeThreadMetaLookupRequest,
    KnowledgeThreadMetadataBatchOutput, KnowledgeThreadMetadataBatchRequest,
    KnowledgeThreadMetadataBatchRow, KnowledgeThreadMetadataUpdate,
    KnowledgeThreadSourceListOutput, KnowledgeThreadSourceListRequest,
    KnowledgeThreadSourceLookupOutput, KnowledgeThreadSourceLookupRequest,
    KnowledgeThreadSyncMetadataOutput, KnowledgeThreadSyncMetadataRequest,
    KnowledgeThreadTitleLookupOutput, KnowledgeThreadTitleLookupRequest,
    KnowledgeTraversalDiagnostics, KnowledgeTraversalFallbackReasonCode,
    KnowledgeTruncationReasonCode, NowledgeGraphAdapter, NowledgeGraphExplainOutput,
    NowledgeGraphStatement, NowledgeGraphTransactionOutput, PlanCacheStats, QueryOutput,
    QuerySystemVariables, RankedBackgroundMaintenance, SearchProjectionGraphDeltaRequest,
    GRAPH_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION, GRAPH_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION,
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
pub use cypher::RelationshipDirection;
pub use error::{Result, SkeinError};
pub use executor::ReadExecutionProfile;
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
    StorageRecoveryEvidenceHealth,
};
pub use nowledge_mem::{
    nowledge_mem_bounded_read_evidence_json, nowledge_mem_graph_config, NowledgeMemEmbeddedStore,
    NowledgeMemGraph, NowledgeMemGraphMode, NowledgeMemOpenOptions, NowledgeMemOpenReport,
    NowledgeMemReadOptions, NowledgeMemReadOutput, NowledgeMemReadReport,
    NowledgeMemSearchProjection, NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
    NOWLEDGE_MEM_OPEN_REPORT_PROTOCOL, NOWLEDGE_MEM_READ_REPORT_PROTOCOL,
};
pub use qos::{
    BackgroundWorkDecision, BackgroundWorkHint, BackgroundWorkPlan, BackgroundWorkReasonCode,
    LocalQosPermit, LocalQosPolicy, LocalQosScheduler, LocalQosState, QosAdmission,
    QosAdmissionCode, RankedBackgroundWork, WorkClass, WorkPriority, WorkRequest, WORK_CLASS_COUNT,
};
pub use schema::{
    BasicGraphStatistics, CompositeIndexDescriptor, ConstraintDescriptor, ConstraintId,
    ConstraintKind, ConstraintSubject, GraphStatistics, IndexDescriptor, IndexId, IndexKind,
    PropertyDescriptor, PropertyId, PropertyType, SchemaObjectState, TableDescriptor, TableId,
    TableKind,
};
pub use search::{
    MetadataRepairOptions, MetadataRepairSummary, SearchAnalyzerLexicon,
    SearchDerivedArtifactReport, SearchDocument, SearchEmbeddingManifest, SearchEmptyReasonCode,
    SearchFallbackReasonCode, SearchHit, SearchIndex, SearchMode, SearchProjectionDelta,
    SearchProjectionDeltaReport, SearchProjectionFreshness, SearchProjectionKind,
    SearchProjectionProbeOptions, SearchProjectionRow, SearchRebuildOptions, SearchRebuildSummary,
    SearchResultSet, SearchRetrieverCandidateSetReport, SearchTruncationReasonCode,
};
pub use skein_optimizer::{
    Distribution, GroupId, Memo as OptimizerMemo, MemoGroup as OptimizerMemoGroup,
    PhysicalProperties, RequiredProperties,
};
pub use store::{
    AdjacencyDirection, AdjacencyGroupStats, AdjacencyLayout, DurabilityPolicy,
    OrderedAdjacencyEntry, RecoveryMode, StorageReclamationWatermark, StorageRecoveryReport,
    WalReplayConfig, DENSE_ADJACENCY_DEGREE_THRESHOLD,
};
pub use value::Value;

#[cfg(test)]
mod tests {
    use super::{
        Database, KnowledgeEntityRequest, KnowledgeNeighborDirection, KnowledgeNeighborsRequest,
        KnowledgePathRequest, KnowledgeSubgraphRequest,
    };

    #[test]
    fn crate_root_exports_typed_knowledge_navigation_api() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'root', title: 'Root'})-[:LINKS]->(:Entity {id: 'leaf', name: 'Leaf'})",
        )
        .unwrap();

        let entity = db.knowledge_entity(&KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
        });
        assert!(entity.entity.is_some());

        let neighbors = db.knowledge_neighbors(&KnowledgeNeighborsRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            limit: 4,
            max_hops: 1,
        });
        assert_eq!(neighbors.diagnostics.path_count, 1);

        let paths = db.knowledge_paths(&KnowledgePathRequest {
            source_label: "Memory".to_string(),
            source_external_id: "root".to_string(),
            target_label: "Entity".to_string(),
            target_external_id: "leaf".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            limit: 4,
        });
        assert_eq!(paths.diagnostics.target_found, Some(true));

        let subgraph = db.knowledge_subgraph(&KnowledgeSubgraphRequest {
            label: "Memory".to_string(),
            external_id: "root".to_string(),
            relationship_type: Some("LINKS".to_string()),
            direction: KnowledgeNeighborDirection::Outgoing,
            max_hops: 1,
            node_limit: 4,
            relationship_limit: 4,
        });
        assert_eq!(subgraph.diagnostics.node_count, 2);
    }
}
