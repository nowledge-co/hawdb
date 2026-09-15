pub mod adjacency;
pub mod append_table;
#[doc(hidden)]
pub mod artifact_binding;
#[doc(hidden)]
pub mod artifact_files;
pub mod background;
pub mod backup;
pub mod cache;
pub mod canonical;
pub mod canonical_adjacency;
pub mod column_group;
pub mod config;
#[doc(hidden)]
pub mod cow;
pub mod doctor;
pub mod durability;
#[doc(hidden)]
pub mod durable_manifest;
#[doc(hidden)]
pub mod graph_constraints;
pub mod graph_descriptor_page;
pub mod graph_descriptor_tree;
#[doc(hidden)]
pub mod graph_index_metrics;
#[doc(hidden)]
pub mod graph_overlay;
pub mod ids;
pub mod index_page;
#[doc(hidden)]
pub mod io;
pub mod mutation;
mod ownership;
pub mod pressure;
pub mod projection;
pub mod projection_generation;
pub mod property_projection;
pub mod property_spill;
#[doc(hidden)]
pub mod read_view;
pub mod relational;
#[doc(hidden)]
pub mod relational_index_view;
#[doc(hidden)]
pub mod relational_row_workspace;
pub mod scan;
pub mod snapshot;
#[doc(hidden)]
pub mod source_scan;
pub mod stable_identity;
#[doc(hidden)]
pub mod statistics_refresh;
pub mod telemetry;
#[doc(hidden)]
pub mod text;
#[doc(hidden)]
pub mod transaction_locks;
#[doc(hidden)]
pub mod wal;
pub mod wire;

#[cfg(test)]
mod file_lock_tests;
#[cfg(test)]
mod hex_test_support;

pub use adjacency::{
    AdjacencyDirection, AdjacencyGroupConsistencyMismatch, AdjacencyGroupKey, AdjacencyGroupStats,
    AdjacencyLayout, AdjacencyPostingIter, AdjacencyPostingList, OrderedAdjacencyEntry,
    ADJACENCY_DELTA_CONSOLIDATION_ENTRIES, ADJACENCY_DELTA_HARD_MAX_ENTRIES,
    ADJACENCY_MINI_DELTA_MAX_ENTRIES, ADJACENCY_PIVOT_MIN_DEGREE,
};
pub use append_table::{
    append_generation_manifest_file, append_segment_file, decode_append_wal_batch,
    encode_append_wal_batch, AppendCommitPreparation, AppendDecodeLimits, AppendGeneratedRow,
    AppendGenerationArtifacts, AppendGenerationManifest, AppendGenerationReader, AppendLiveBatch,
    AppendMutationLimits, AppendMutationOutcome, AppendOrderMode, AppendPublicationConfig,
    AppendPublicationPhase, AppendPublicationReport, AppendPublicationState, AppendPublisher,
    AppendSegmentArtifactMetadata, AppendSegmentBinding, AppendSegmentBlockDescriptor,
    AppendSegmentConfig, AppendSegmentReadOutput, AppendSegmentReadReport, AppendSegmentReader,
    AppendSegmentWriteOutput, AppendSegmentWriter, AppendState, AppendStorageResidencyReport,
    AppendTableError, AppendTableRow, AppendTableSchema, AppendTransaction, AppendWalBatch,
    AppendWrite, DEFAULT_MAX_APPEND_MUTATION_BYTES, DEFAULT_MAX_APPEND_MUTATION_ROWS,
};
pub use background::{BackgroundWorkAdmission, BackgroundWorkPermit, BackgroundWorkRequest};
pub use backup::{
    validate_backup_file_name, BackupFileEntry, BackupManifest, StorageBackupReport,
    StorageRestoreReport, StorageScrubReport, BACKUP_HEADER_V1, BACKUP_MANIFEST_FILE,
    STABLE_ID_MAPPING_FILE, STORAGE_MANIFEST_FILE,
};
pub use cache::{
    content_digest, ContentDigest, ManifestGeneration, RepresentationKind, SegmentBytes,
    SegmentCache, SegmentCacheAdmissionError, SegmentCacheError, SegmentCacheKey,
    SegmentCacheLease, SegmentCacheSnapshot, StoreId,
};
pub use canonical::{
    canonical_segment_descriptor_page_file, canonical_segment_descriptor_root_file,
    decode_residual_row_properties, encode_residual_row_properties,
    residual_row_properties_encoded_len, write_residual_row_properties, CanonicalEndpointBloom,
    CanonicalEndpointDirection, CanonicalNodeIterator, CanonicalReadReport,
    CanonicalRelationshipIterator, CanonicalScanControl, CanonicalSegmentConfig,
    CanonicalSegmentDescriptor, CanonicalSegmentError, CanonicalSegmentKind,
    CanonicalSegmentManifest, CanonicalSegmentReader, CanonicalSegmentScrubReport,
    CanonicalSegmentWriter, PersistentCanonicalSegmentDescriptorTree,
    CANONICAL_SEGMENT_DESCRIPTOR_ARTIFACT_ID,
};
pub use canonical_adjacency::{
    canonical_adjacency_descriptor_page_file, canonical_adjacency_descriptor_root_file,
    CanonicalAdjacencyArtifactMetadata, CanonicalAdjacencyBlockDescriptor,
    CanonicalAdjacencyBuildReport, CanonicalAdjacencyConfig, CanonicalAdjacencyEntry,
    CanonicalAdjacencyError, CanonicalAdjacencyGenerationArtifacts, CanonicalAdjacencyManifest,
    CanonicalAdjacencyReadReport, CanonicalAdjacencyReader, CanonicalAdjacencyScrubReport,
    CanonicalAdjacencyWriteOutput, CanonicalAdjacencyWriter,
    CANONICAL_ADJACENCY_DESCRIPTOR_ARTIFACT_ID,
};
pub use column_group::{
    deletion::DeletionVector,
    encoding::{ChunkEncoding, EncodedChunk},
    group::{
        ColumnChunkDescriptor, ColumnGroupByteSource, ColumnGroupConfig, ColumnGroupDirectory,
        ColumnGroupPruneDecision, ColumnGroupPruneReason, ColumnGroupReader, ColumnGroupWriter,
        ColumnPredicate, FileByteSource, IdChunkDescriptor, StreamedBlob,
        DEFAULT_GROUP_ROW_CAPACITY,
    },
    manifest::{
        ColumnGroupArtifactDescriptor, ColumnGroupManifest, ColumnGroupTableDirectory,
        ColumnGroupTableDirectoryRef, ColumnGroupTableKey, ColumnGroupTableKind,
        PublishedColumnGroupCatalog, COLUMN_GROUP_MANIFEST_FILE,
    },
    zone::{ChunkZoneMap, StringPrefixMinMax},
    ColumnGroupError, DeletionVectorBinding, COLUMN_GROUP_MAGIC, DELETION_VECTOR_MAGIC,
};
pub use config::{
    DurabilityPolicy, DurableCompression, RecoveryMode, RelationalIndexMode, StorageResidencyMode,
    WalReplayConfig, DEFAULT_AUTO_MATERIALIZE_CHECKPOINT_BYTES,
    DEFAULT_MAX_CHECKPOINT_DECODED_BYTES, DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES,
    DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES, DEFAULT_MAX_OUT_OF_CORE_DELTA_BYTES,
    DEFAULT_MAX_WAL_BATCH_OPERATIONS, DEFAULT_MAX_WAL_QUARANTINE_BYTES,
    DEFAULT_MAX_WAL_RECORD_BYTES, DEFAULT_MAX_WAL_REPLAY_BYTES, DEFAULT_MAX_WAL_REPLAY_ENTRIES,
    DEFAULT_SEGMENT_CACHE_CAPACITY_BYTES,
};
pub use cow::{CowPageWeight, CowSegment, CowSegmentedMap, COW_MAP_TARGET_SEGMENT_BYTES};
pub use doctor::{
    WalDoctorOptions, WalRepairAcknowledgement, WalTailRepairPlan, WalTailRepairReason,
    WalTailRepairReport, WAL_DOCTOR_REPAIR_PROTOCOL,
};
pub use durability::{
    durable_replace_file, sync_directory, sync_parent_directory, WalSyncGroupFlush,
    WalSyncGroupProgress, WalSyncGroupState,
};
pub use graph_descriptor_page::{
    GraphDescriptorInteriorEntry, GraphDescriptorKind, GraphDescriptorLeafEntry,
    GraphDescriptorPageError, GraphDescriptorPageId, GraphDescriptorPageLimits,
    GraphDescriptorPageRef, ImmutableGraphDescriptorPage, ImmutableGraphDescriptorPageBody,
    DEFAULT_GRAPH_DESCRIPTOR_KEY_BYTES, DEFAULT_GRAPH_DESCRIPTOR_PAGE_BYTES,
    DEFAULT_GRAPH_DESCRIPTOR_PAGE_ENTRIES, DEFAULT_GRAPH_DESCRIPTOR_VALUE_BYTES,
};
pub use graph_descriptor_tree::{
    GraphDescriptorTreeArtifactMetadata, GraphDescriptorTreeBuildConfig,
    GraphDescriptorTreeBuildReport, GraphDescriptorTreeBuilder, GraphDescriptorTreeError,
    GraphDescriptorTreeGenerationArtifacts, GraphDescriptorTreeOpenReport,
    GraphDescriptorTreePaths, GraphDescriptorTreeRoot, GraphDescriptorTreeRootReader,
    GraphDescriptorTreeWriteOutput, PreparedGraphDescriptorTree,
};
pub use graph_index_metrics::{
    GraphIndexReadMetrics, GraphIndexReadMetricsSnapshot, PersistentGraphIndexClass,
};
pub use ids::{NodeId, NodeRecord, ProjectedNodeRecord, RelId, RelRecord};
pub use index_page::{
    ImmutableIndexPage, ImmutableIndexPageBody, ImmutableIndexPageError, ImmutableIndexPageLimits,
    IndexIdentity, IndexInteriorEntry, IndexInteriorPage, IndexLeafEntry, IndexLeafPage,
    IndexLeafPosting, IndexPageId, IndexPostingPage, IndexRootPage, IndexRowId,
    DEFAULT_IMMUTABLE_INDEX_IDENTITY_BYTES, DEFAULT_IMMUTABLE_INDEX_INLINE_POSTINGS,
    DEFAULT_IMMUTABLE_INDEX_KEY_BYTES, DEFAULT_IMMUTABLE_INDEX_PAGE_BYTES,
    DEFAULT_IMMUTABLE_INDEX_PAGE_ENTRIES, DEFAULT_IMMUTABLE_INDEX_ROW_ID_BYTES,
};
pub use mutation::{
    ConnectedNodesCreate, GraphMutation, MatchedRelationshipCopyMerge, MatchedRelationshipCreate,
    MatchedRelationshipMerge, MatchedRelationshipRetargetMerge,
    MatchedRelationshipSourceRetargetMerge, MutationLimits, MutationSummary, NodeSetAssignment,
    NodeSetValue, PropertyFilter, RelationshipDeleteRequest, RelationshipOnCreatePropertyValue,
    RelationshipPropertiesUpdate, RelationshipPropertyUpdate, RelationshipSetAssignment,
    RelationshipTargetNodeDelete, DEFAULT_MAX_MUTATION_AFFECTED_ROWS,
    DEFAULT_MAX_MUTATION_OPERATIONS, DEFAULT_MAX_MUTATION_RESULT_PAYLOAD_BYTES,
    DEFAULT_MAX_MUTATION_RESULT_ROWS,
};
pub use ownership::{
    DatabaseDirectoryLease, DatabaseDirectoryLeaseError, DATABASE_DIRECTORY_LOCK_FILE,
};
pub use pressure::{
    available_storage_space, StorageDebtController, StoragePressureReasonCode,
    StoragePressureSignals, StoragePressureSnapshot, StoragePressureState,
    STORAGE_PRESSURE_DEFER_RATIO_PER_MILLION, STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION,
};
pub use projection::{
    projection_document_id_for_label_and_properties, projection_document_id_for_node,
    ProjectedGraphArtifact, ProjectedGraphArtifactData, ProjectedGraphDefinition,
    ProjectedGraphStatus, PropertyIndexProjectionRebuildAction, SchemaMaintenanceAction,
    SchemaMaintenancePlanItem, SearchProjectionChange, SearchProjectionChangefeedReadiness,
    SearchProjectionChangefeedStatus, SearchProjectionGraphChange, SearchProjectionMutationId,
    StorageOpenTimings, StorageReclamationWatermark, StorageRecoveryReport, StoreStableIdMapping,
};
pub use projection_generation::{
    decode_projection_relational_member, encode_projection_relational_member,
    ProjectionGenerationBatchLimits, ProjectionGenerationBegin, ProjectionGenerationCursor,
    ProjectionGenerationDigestBuilder, ProjectionGenerationError, ProjectionGenerationGcLimits,
    ProjectionGenerationGcReport, ProjectionGenerationIdentity, ProjectionGenerationManifest,
    ProjectionGenerationMember, ProjectionGenerationPage, ProjectionGenerationPublishReport,
    ProjectionGenerationReadLimits, ProjectionGenerationReadReport, ProjectionGenerationReader,
    ProjectionGenerationSeal, ProjectionGenerationSeekFence, ProjectionGenerationState,
    ProjectionGenerationStatus, ProjectionGenerationStore, ProjectionGenerationWriter,
    SealedProjectionGeneration,
};
pub use property_projection::{
    persistent_composite_property_identity, property_projection_descriptor_page_file,
    property_projection_descriptor_root_file, PersistentPropertyProjectionBlockDescriptor,
    PersistentPropertyProjectionBuildReport, PersistentPropertyProjectionConfig,
    PersistentPropertyProjectionDefinition, PersistentPropertyProjectionDefinitionAdmission,
    PersistentPropertyProjectionDescriptorTree, PersistentPropertyProjectionError,
    PersistentPropertyProjectionKind, PersistentPropertyProjectionManifest,
    PersistentPropertyProjectionReadReport, PersistentPropertyProjectionReader,
    PersistentPropertyProjectionRecord, PersistentPropertyProjectionScrubReport,
    PersistentPropertyProjectionWriteOutput, PersistentPropertyProjectionWriter,
};
pub use property_spill::{
    property_spill_descriptor_page_file, property_spill_descriptor_root_file,
    PersistentPropertySpillDescriptorTree, PropertySpillBlockDescriptor, PropertySpillConfig,
    PropertySpillError, PropertySpillManifest, PropertySpillReadOutput, PropertySpillReadReport,
    PropertySpillReader, PropertySpillScrubReport, PropertySpillWriteOptions,
    PropertySpillWriteOutput, PropertySpillWriter,
};
pub use read_view::PublishedReadView;
pub use relational::RelationalMutationOutcome;
pub use relational::{
    decode_relational_checkpoint, decode_relational_checkpoint_file,
    decode_relational_checkpoint_file_with_index_load,
    decode_relational_checkpoint_with_index_load, decode_relational_primary_key,
    decode_relational_wal_batch, encode_relational_checkpoint,
    encode_relational_checkpoint_to_writer, encode_relational_primary_key,
    encode_relational_wal_batch, encode_relational_wal_batch_with_captures,
    encode_relational_wal_batch_with_replay_access, relational_foreign_key_index_name,
    relational_index_recovery_delta_file, relational_index_shadow_artifact_file,
    relational_index_shadow_manifest_generation_file, relational_overflow_descriptor_file,
    relational_overflow_extent_file, relational_overflow_manifest_generation_file,
    relational_row_delta_manifest_generation_file, relational_row_delta_run_file,
    relational_row_page_artifact_file, relational_row_page_manifest_generation_file,
    relational_row_page_root_descriptor_file, relational_row_page_root_key_file,
    relational_unique_index_name, EncodedRelationalWalBatch, ImmutableRelationalRowPage,
    RelationalBigIntArithmeticOperator, RelationalBigIntOperand, RelationalCheckpoint,
    RelationalCheckpointIndexLoad, RelationalColumnDefault, RelationalColumnSchema,
    RelationalComparisonOp, RelationalConflictAction, RelationalConstraintIndex,
    RelationalDecodeLimits, RelationalError, RelationalForeignKeySchema, RelationalHydrationBudget,
    RelationalIndexArtifactMetadata, RelationalIndexChange, RelationalIndexChangeCapture,
    RelationalIndexChangeCaptureLimits, RelationalIndexChangeKind, RelationalIndexDefinition,
    RelationalIndexGenerationArtifacts, RelationalIndexGenerationIdentity,
    RelationalIndexPrefixStatistics, RelationalIndexRangeScan, RelationalIndexReadLimits,
    RelationalIndexReadReport, RelationalIndexRecoveryBuilder, RelationalIndexRecoveryConfig,
    RelationalIndexRecoveryManifest, RelationalIndexRecoveryReadReport,
    RelationalIndexRecoveryReader, RelationalIndexRecoveryReport, RelationalIndexRole,
    RelationalIndexRootDescriptor, RelationalIndexRowSource, RelationalIndexScanDirection,
    RelationalIndexSchema, RelationalIndexShadowBuildReport, RelationalIndexShadowConfig,
    RelationalIndexShadowError, RelationalIndexShadowManifest, RelationalIndexShadowReader,
    RelationalIndexShadowWriter, RelationalIndexStatistics, RelationalInsertMode, RelationalKey,
    RelationalMonotonicAppendHydration, RelationalMutationLimits,
    RelationalOverflowArtifactMetadata, RelationalOverflowConfig,
    RelationalOverflowExactGenerationRequest, RelationalOverflowExactPublicationReport,
    RelationalOverflowExtentDescriptor, RelationalOverflowExtentInput,
    RelationalOverflowGenerationArtifacts, RelationalOverflowPublicationConfig,
    RelationalOverflowPublicationError, RelationalOverflowPublicationPhase,
    RelationalOverflowPublicationReport, RelationalOverflowPublisher, RelationalOverflowRef,
    RelationalOverflowReferenceSet, RelationalOverflowReferenceSetBuilder,
    RelationalOverflowReferenceSortConfig, RelationalOverflowReferenceSortReport,
    RelationalOverflowRootBinding, RelationalOverflowRootManifest, RelationalOverflowRootReader,
    RelationalPredicate, RelationalPrimaryKeyChangeCapture,
    RelationalPrimaryKeyChangeCaptureLimits, RelationalPrimaryKeyChangeRebuildReason,
    RelationalProjectedField, RelationalProjectedFieldRef, RelationalProjectedRow,
    RelationalProjectedRowRef, RelationalProjectedRowView, RelationalRecoveryFence,
    RelationalRecoverySourceBuilder, RelationalRecoverySourceIdentity, RelationalReferentialAction,
    RelationalReplayAccess, RelationalReplayAccessSet, RelationalRow, RelationalRowChange,
    RelationalRowChangeCapture, RelationalRowChangeCaptureLimits, RelationalRowDeltaBaseBinding,
    RelationalRowDeltaBuilder, RelationalRowDeltaConfig, RelationalRowDeltaError,
    RelationalRowDeltaGeneration, RelationalRowDeltaManifest, RelationalRowDeltaPublicationPhase,
    RelationalRowDeltaReadReport, RelationalRowDeltaReader, RelationalRowDeltaReport,
    RelationalRowDeltaTableMetadata, RelationalRowPageArtifactMetadata, RelationalRowPageBootstrap,
    RelationalRowPageBootstrapReport, RelationalRowPageCheckpointError,
    RelationalRowPageDemandReadError, RelationalRowPageDemandReadLimits,
    RelationalRowPageDemandReadReport, RelationalRowPageDemandReader, RelationalRowPageEntry,
    RelationalRowPageError, RelationalRowPageGenerationArtifacts,
    RelationalRowPageGenerationRequest, RelationalRowPageId, RelationalRowPageIdAllocator,
    RelationalRowPageLimits, RelationalRowPageLiveError, RelationalRowPageMutationError,
    RelationalRowPageMutationPlan, RelationalRowPageMutationPlanner,
    RelationalRowPagePhysicalGeneration, RelationalRowPageProjectedFields,
    RelationalRowPageProjectedRange, RelationalRowPageProjectedRangeFields,
    RelationalRowPagePublicationConfig, RelationalRowPagePublicationError,
    RelationalRowPagePublicationPhase, RelationalRowPagePublicationReport,
    RelationalRowPagePublisher, RelationalRowPageReadView, RelationalRowPageReadViewIdentity,
    RelationalRowPageRecoveredValue, RelationalRowPageRewriteConfig,
    RelationalRowPageRootDescriptor, RelationalRowPageRootManifest, RelationalRowPageRootReader,
    RelationalRowPageSlotIntegrity, RelationalRowPageSnapshotPointReport,
    RelationalRowPageSnapshotPointsReport, RelationalRowPageSnapshotRangeReport,
    RelationalRowPageSnapshotReadError, RelationalRowPageSnapshotReadLimits,
    RelationalRowPageSnapshotReader, RelationalRowPageSnapshotRowSource,
    RelationalRowPageTableDelta, RelationalRowPageTableRoot, RelationalRowPageView,
    RelationalScalarType, RelationalSparseIndexProbe, RelationalSparseLivePreparation,
    RelationalSparseLivePreparationStage, RelationalSparseLiveStage,
    RelationalSparseMutationHydrationPlan, RelationalSparseRecoveryRow,
    RelationalSparseRecoveryStage, RelationalSparseWorkspaceBuilder, RelationalState,
    RelationalStore, RelationalTablePrimaryKeyChanges, RelationalTableSchema,
    RelationalTransaction, RelationalTransactionStageResult, RelationalUpdateAssignment,
    RelationalUpdateValue, RelationalUpsertAssignment, RelationalUpsertValue, RelationalValue,
    RelationalValueRef, RelationalWalBatch, RelationalWrite,
    DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES, DEFAULT_MAX_RELATIONAL_INDEX_CHANGES,
    DEFAULT_MAX_RELATIONAL_INDEX_CHANGE_BYTES, DEFAULT_MAX_RELATIONAL_MUTATION_BYTES,
    DEFAULT_MAX_RELATIONAL_MUTATION_ROWS, DEFAULT_MAX_RELATIONAL_PRIMARY_KEY_CHANGES,
    DEFAULT_MAX_RELATIONAL_PRIMARY_KEY_CHANGE_BYTES, DEFAULT_MAX_RELATIONAL_ROW_CHANGES,
    DEFAULT_MAX_RELATIONAL_ROW_CHANGE_BYTES, DEFAULT_RELATIONAL_INDEX_READ_BYTES,
    DEFAULT_RELATIONAL_INDEX_READ_PAGES, DEFAULT_RELATIONAL_INDEX_READ_ROWS,
    DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT, DEFAULT_RELATIONAL_INDEX_RECOVERY_DIRTY_BYTES,
    DEFAULT_RELATIONAL_INDEX_RECOVERY_DIRTY_ENTRIES,
    DEFAULT_RELATIONAL_INDEX_RECOVERY_MANIFEST_BYTES, DEFAULT_RELATIONAL_INDEX_RECOVERY_PAGES,
    DEFAULT_RELATIONAL_INDEX_SHADOW_BUILD_METADATA_BYTES,
    DEFAULT_RELATIONAL_INDEX_SHADOW_MANIFEST_BYTES, DEFAULT_RELATIONAL_INDEX_SHADOW_ROOTS,
    DEFAULT_RELATIONAL_INDEX_SORT_MEMORY_BYTES, DEFAULT_RELATIONAL_INDEX_SORT_MERGE_FAN_IN,
    DEFAULT_RELATIONAL_INDEX_SORT_RUNS, DEFAULT_RELATIONAL_INDEX_SORT_SPILL_BYTES,
    DEFAULT_RELATIONAL_OVERFLOW_EXTENTS, DEFAULT_RELATIONAL_OVERFLOW_MANIFEST_BYTES,
    DEFAULT_RELATIONAL_OVERFLOW_NEW_EXTENT_BYTES,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_OCCURRENCES, DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_RUNS,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SORT_MEMORY_BYTES,
    DEFAULT_RELATIONAL_OVERFLOW_REFERENCE_SPILL_BYTES, DEFAULT_RELATIONAL_OVERFLOW_THRESHOLD_BYTES,
    DEFAULT_RELATIONAL_ROW_DELTA_CHECKPOINT_RUNS, DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_BYTES,
    DEFAULT_RELATIONAL_ROW_DELTA_DIRTY_ENTRIES, DEFAULT_RELATIONAL_ROW_DELTA_MANIFEST_BYTES,
    DEFAULT_RELATIONAL_ROW_DELTA_RUNS, DEFAULT_RELATIONAL_ROW_DELTA_RUN_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_COLUMNS,
    DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_DIRTY_PAGES,
    DEFAULT_RELATIONAL_ROW_PAGE_INLINE_VALUE_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_KEY_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_MANIFEST_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_READ_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_READ_PAGES, DEFAULT_RELATIONAL_ROW_PAGE_READ_PINS,
    DEFAULT_RELATIONAL_ROW_PAGE_READ_ROWS, DEFAULT_RELATIONAL_ROW_PAGE_READ_TREE_HEIGHT,
    DEFAULT_RELATIONAL_ROW_PAGE_ROOT_KEY_BYTES, DEFAULT_RELATIONAL_ROW_PAGE_ROOT_PAGES,
    DEFAULT_RELATIONAL_ROW_PAGE_ROWS, DEFAULT_RELATIONAL_ROW_PAGE_ROW_BYTES,
    DEFAULT_RELATIONAL_ROW_PAGE_TABLES, DEFAULT_RELATIONAL_ROW_PAGE_VALUE_BYTES,
    DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_BYTES, DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_ENTRIES,
    RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE, RELATIONAL_INDEX_SHADOW_MANIFEST_FILE,
    RELATIONAL_OVERFLOW_MANIFEST_FILE, RELATIONAL_PRIMARY_INDEX_NAME,
    RELATIONAL_ROW_DELTA_MANIFEST_FILE, RELATIONAL_ROW_PAGE_MANIFEST_FILE,
};
pub use scan::{
    CandidateCursor, DateTimeMinMax, EnumDictionaryStats, FieldSummary, FileSegmentRangeReader,
    MembershipFilterSummary, MembershipVerdict, NumericMinMax, PersistedScanSegment,
    PlannedScanSegment, PruningDecision, PruningReason, RangeBound, ReadySegmentScan,
    ScanPredicate, ScanPruningReport, ScanPruningStrategy, ScanPruningTargetKind, ScanScalar,
    ScanSegmentAccessPlan, ScanSegmentFallback, ScanSegmentManifest, ScanSegmentManifestError,
    SegmentPayloadRange, SegmentPruner, SegmentRangeRead, SegmentRangeReader, SegmentReadControl,
    SegmentReadError, SegmentReadExecutionError, SegmentReadExecutionReport, SegmentReadExecutor,
    SegmentReadPayload, SegmentReadPool, SegmentReadPoolError, SegmentReadRange,
    SegmentReadSchedule, SegmentReadScheduler, SegmentReadWave, SegmentSummary,
};
pub use snapshot::{
    SnapshotCommitError, SnapshotCoordinator, SnapshotReadGuard, VersionedSnapshot,
};
// These are internal ownership seams. Hosts continue to use the embedded facade.
#[doc(hidden)]
pub use source_scan::{
    render_source_candidate_page, select_source_candidate, validate_source_candidate_scan_request,
    SourceCandidateCursor, SourceCandidateRow, SourceCandidateScanOrigin,
    SourceCandidateScanOutput, SourceCandidateScanRequest,
};
pub use stable_identity::{
    stable_identity_generation_artifact_path, StableIdentityKey, StableIdentityKind,
    StableIdentityMappingConfig, StableIdentityMappingError, StableIdentityMappingHeader,
    StableIdentityMappingReader, StableIdentityMappingWriteOutput, StableIdentityMappingWriter,
    StableIdentityMaterializeLimits, StableIdentityReadLimits, StableIdentityReadReport,
    StableIdentityScrubReport, DEFAULT_STABLE_IDENTITY_ARTIFACT_BYTES,
    DEFAULT_STABLE_IDENTITY_LOOKUP_BYTES, DEFAULT_STABLE_IDENTITY_LOOKUP_PAGES,
    DEFAULT_STABLE_IDENTITY_MATERIALIZED_BYTES, DEFAULT_STABLE_IDENTITY_MATERIALIZED_ENTRIES,
    DEFAULT_STABLE_IDENTITY_PAGE_BYTES, DEFAULT_STABLE_IDENTITY_PAGE_ENTRIES,
    DEFAULT_STABLE_IDENTITY_VALUE_BYTES,
};
pub use telemetry::{StorageTelemetrySink, WalAppendTelemetry};
pub use wal::{
    WalGroupCommitActivation, WalGroupCommitAdaptiveColdStartEvidence,
    WalGroupCommitAdaptivePolicyEvidence, WalGroupCommitAdaptiveSteadyStateEvidence,
    WalGroupCommitConfig, WalGroupCommitDelayPolicy, WalGroupCommitEvidence,
    WalGroupCommitSnapshot, WalGroupCommitTailLatencyEvidence, WalGroupCommitWaitDecision,
    DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES, DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
    DEFAULT_WAL_GROUP_COMMIT_MAX_ENTRIES,
};
