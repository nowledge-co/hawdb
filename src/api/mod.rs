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

use crate::analytics::ProjectedGraph;
use crate::cypher;
use crate::error::{HawDBError, Result};
use crate::executor::{self, Row, RowRef};
use crate::optimizer::{
    CascadesOptimizer, OptimizerConfig, OptimizerContext, OptimizerSearchDirective, OptimizerTrace,
    PhysicalPlan, ResourceHints,
};
use crate::qos::{
    BackgroundWorkHint, BackgroundWorkPlan, LocalQosPolicy, LocalQosScheduler, LocalQosState,
    QosAdmission, WorkClass, WorkRequest,
};
#[cfg(test)]
use crate::schema::GraphStatistics;
use crate::schema::{Catalog, SchemaObjectState};
#[cfg(test)]
use crate::schema::{
    CompositeIndexDescriptor, ConstraintDescriptor, IndexDescriptor, PropertyDescriptor,
    TableDescriptor,
};
use crate::search::{
    projection_row_from_node_with_graph_metadata, search_metadata_predicate_pushdown,
    AdaptiveVectorSearchOptions, CompressedVectorSearchMode, MetadataRepairOptions,
    MetadataRepairSummary, SearchCandidateSetReport, SearchDerivedArtifactReport,
    SearchEmptyReasonCode, SearchFusionWeights, SearchIndex, SearchMatchedSpan,
    SearchPredicatePushdownReport, SearchProjectionDelta, SearchProjectionDeltaReport,
    SearchProjectionFreshness, SearchQueryOptions, SearchRebuildOptions, SearchRebuildSummary,
    SearchResultSet, SearchRetrieverCandidateSetReport,
};
use crate::store::{
    restore_storage_backup, AdjacencyConsistencyReport, AdjacencyConsolidationPlan,
    AdjacencyConsolidationReport, AdjacencyDirection, AdjacencyLayout, AppendSegmentReadOutput,
    AppendTableSchema, AppendTransaction, BasicStatisticsConsistencyReport,
    DegreeStatisticsConsistencyReport, DistinctValueStatisticsConsistencyReport, DurabilityPolicy,
    GraphMutationLockFootprint, GraphMutationSavepoint, GraphMutationTransaction,
    GraphSnapshotNodeImport, GraphSnapshotRelationshipImport, GraphStore, HawDBSnapshotRowsImport,
    KernelWriteBatch, MutationSummary, NodeId, NodeRecord, OptimizerStatisticsRefreshWork,
    PreparedCheckpoint, PropertyIndexConsistencyReport, PropertyIndexProjectionRebuildAction,
    PublishedReadView, RecoveryMode, RelId, RelRecord, SchemaMaintenanceAction,
    SegmentCacheSnapshot, StorageBackupReport, StoragePressureSnapshot,
    StorageReclamationWatermark, StorageRecoveryReport, StorageRestoreReport, StorageScrubReport,
    StoreStableIdMapping, WalReplayConfig,
};
use crate::telemetry::{
    operations_telemetry_readiness, qos_telemetry_sink, runtime_telemetry_sink, KernelTelemetry,
    KernelTelemetryOperation, OperationsTelemetryReadiness, TelemetrySink,
};
use crate::value::Value;
use canonical_snapshot::export_canonical_graph_snapshot_for;
use explain::{empty_read_execution_profile, explain_analyze_output_row, explain_output_row};
use hawdb_optimizer::{
    search_field_is_enum_like, SearchPredicate, SearchPredicateOp, SearchPredicateSet,
};
use plan_cache::{
    optimized_query_plan_for, statement_uses_plan_cache, OptimizedQueryPlan,
    OptimizerEnvironmentKey, OptimizerPlanningCache, PlanCache, PlanCacheContext, PlanCacheMode,
    PlanTraceMode, DEFAULT_PLAN_CACHE_MAX_ENTRIES,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use system_variables::{
    apply_set_system_variable, query_statement_variables_for_statement,
    query_work_request_for_statement, reject_system_variable_parameters,
};

mod access_control;
mod database_lifecycle;
mod database_maintenance;
mod database_projection;
mod database_query;
mod database_transactions;
mod retrieval;
mod transactions;
use retrieval::*;
use transactions::*;
pub(crate) use transactions::{
    commit_database_transaction_state, execute_concurrent_graph_transaction_request,
    execute_database_transaction_sql_request,
};
mod graph_adapter;
mod read;

mod artifact_jobs;
mod automatic_checkpoint;
pub use automatic_checkpoint::AutomaticCheckpointReport;
mod branch_lifecycle;
mod canonical_snapshot;
mod concurrent;
#[cfg(test)]
pub(crate) use concurrent::AutocommitMutationPhase;
pub(crate) use concurrent::{
    ExclusiveDatabaseLease, PublishedConcurrentRead, SharedDatabaseGuard, SharedDatabaseOwner,
};
mod vector_index;
pub use vector_index::{
    VectorIndexBuildError, VectorIndexBuildOptions, VectorIndexField, VectorIndexLanguageBudget,
    VectorIndexLanguageReport, VectorIndexReadError, VectorIndexReadTransaction,
    VectorIndexSqlReport, VectorIndexSqlRequest,
};
mod transaction_read_dependencies;
pub use transaction_read_dependencies::{
    TransactionIsolation, TransactionReadDependencyLimits, TransactionReadDependencyUsage,
};
mod dependency_read;
pub use dependency_read::{
    PreparedProjectionFeed, PreparedProjectionFullSource, PreparedRead, PreparedReadDependencies,
    PreparedReadLimits, ProjectionBaselineComparisonConfig, ProjectionBaselineComparisonSession,
    ProjectionBaselineComparisonStep, ProjectionBaselineMappingContext,
    ProjectionBaselineMappingFunction, ProjectionBaselineMappingLimits, ProjectionCoverageConfig,
    ProjectionCoverageSession, ProjectionCoverageStep, ProjectionCoveredUpdate,
    ProjectionFeedError, ProjectionFeedItem, ProjectionFeedLimits, ProjectionFeedRead,
    ProjectionFeedResource, ProjectionFeedStep, ProjectionFeedUsage, ProjectionFullRebuildConfig,
    ProjectionFullRebuildSession, ProjectionFullRebuildStep, ProjectionFullSourceItem,
    ProjectionFullSourceLimits, ProjectionMappingContext, ProjectionMappingFunction,
    ProjectionMappingInput, ProjectionMappingLimits, ProjectionMappingStatus,
    ProjectionMappingUsage, ProjectionMatchedBaseline, ProjectionOutputVerificationContext,
    ProjectionOutputVerificationFunction, ProjectionOutputVerificationSession,
    ProjectionPreparedFullRebuild, ProjectionPublicationError, ProjectionSourceDurability,
    ProjectionSourceScope, ProjectionStagedInputs, ProjectionStagingSession,
    ProjectionStagingSessionConfig, ProjectionStagingSessionStep, ReadDependencyValidationFailures,
};
mod explain;
mod graph_analytics;
mod knowledge_graph_filters;
mod knowledge_graph_read;
pub use graph_analytics::{
    GraphAnalyticsAlgorithm, GraphAnalyticsFreshness, GraphAnalyticsPublicationStatus,
    GraphAnalyticsRequest, PreparedGraphAnalytics,
};
#[cfg(test)]
mod explain_format_tests;
mod observability;
mod plan_cache;
pub(crate) mod publication;
pub use publication::{DatabasePublicationOwner, DatabasePublicationState};
#[cfg(feature = "qualification")]
pub use publication::{PublicationLockDurationSnapshot, PublicationLockMetricsSnapshot};
#[cfg(feature = "qualification")]
mod query_preparation_metrics;
mod query_request;
mod query_runtime;
#[cfg(feature = "qualification")]
pub use query_preparation_metrics::{query_preparation_metrics, QueryPreparationMetricsSnapshot};
mod sql_runtime;
#[cfg(feature = "tokio-runtime")]
pub(crate) use sql_runtime::PreparedRuntimeSql;
mod retained_query;
pub use retained_query::{
    ArrowArray, ArrowArrayStream, ArrowSchema, RetainedArrowCodeOwner, RetainedArrowExport,
    RetainedArrowSchema, RetainedArrowStream, RetainedColumnRole, RetainedColumnSchema,
    RetainedColumnType, RetainedColumnValues, RetainedNodeCursor, RetainedNodeField,
    RetainedNodeLabels, RetainedNodeSnapshotCursor, RetainedNodeTaskCursor, RetainedNodeView,
    RetainedQueryBatch, RetainedQueryCursor, RetainedQueryError, RetainedQueryOptions,
    RetainedQueryProfile, RetainedQueryStatus,
};
mod resource_profile;
mod runtime_cell;
mod schema_guidance;
mod search_projection_catch_up;
mod search_projection_consumer;
mod statement_shape;
pub use search_projection_consumer::*;
mod source_candidates;
mod system_schema;
mod system_variables;
mod transaction_locks;
mod types;

pub(crate) use hawdb_system_sql as system_sql;

pub use branch_lifecycle::{
    BranchAbandonmentAcknowledgement, BranchAbandonmentLimits, BranchAbandonmentPlan,
    BranchAbandonmentReport, BranchCreateRequest, BranchInfo, BranchLifecycleError,
    BranchLifecycleState, BranchReclamationLimits, BranchReclamationReport, BranchSelector,
};
pub use hawdb_core::graph_rag::{
    MissingScoringFeature, ScoringCombination, ScoringProgram, ScoringProgramError,
    ScoringProgramShape,
};
pub(crate) use hawdb_executor::runtime_admission::runtime_planning_request;
#[cfg(feature = "tokio-runtime")]
pub(crate) use hawdb_executor::runtime_admission::RuntimeAdmissionPlan;
pub use hawdb_storage::branch_create_recovery::{
    BranchCreateRecoveryEntry, BranchCreateRecoveryLimits, BranchCreateRecoveryReport,
    BranchCreateRecoveryStatus,
};
use query_request::BoundScoringRequest;
pub use query_request::{
    BudgetedReadSnapshot, HostScoringRequest, OwnedHostScoringRequest, OwnedQueryRequest,
    OwnedSqlQueryRequest, QueryRequest, ReadSnapshotBudget, ReadSnapshotUsage, ScoringRequest,
    SqlQueryRequest,
};
#[cfg(feature = "tokio-runtime")]
pub(crate) use query_runtime::RuntimePlanningSnapshot;
pub(crate) use query_runtime::{runtime_result_memory_budget, PreparedRuntimeQuery};
pub use types::*;

const DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_ENTRIES: usize = 4096;
const DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_BYTES: usize = 64 * 1024 * 1024;

pub use artifact_jobs::{
    DerivedArtifactJob, DerivedArtifactJobReport, DerivedArtifactJobStatus,
    ExternalContentArtifactJobCompletion, ExternalContentArtifactJobSummary,
    ExternalContentArtifactRuntimeManifest,
};
pub use canonical_snapshot::{
    hawdb_lightning_initial_import_advance_checkpoint,
    hawdb_lightning_initial_import_advance_durable_state_streaming,
    hawdb_lightning_initial_import_advance_durable_state_with_search_projection_batch,
    hawdb_lightning_initial_import_checkpoint_readiness,
    hawdb_lightning_initial_import_cutover_catch_up_report,
    hawdb_lightning_initial_import_decode_durable_state,
    hawdb_lightning_initial_import_document_identity_coverage,
    hawdb_lightning_initial_import_durable_state_report,
    hawdb_lightning_initial_import_encode_durable_state, hawdb_lightning_initial_import_plan,
    hawdb_lightning_initial_import_plan_with_document_identities,
    hawdb_lightning_initial_import_readiness, hawdb_lightning_initial_import_recovery_readiness,
    hawdb_lightning_initial_import_resume_action,
    hawdb_lightning_initial_import_search_projection_batch_report,
    hawdb_lightning_initial_import_search_projection_batch_report_with_document_identities,
    hawdb_lightning_initial_import_session_bundle_readiness,
    hawdb_lightning_initial_import_session_report,
    hawdb_lightning_initial_import_source_bundle_readiness,
    hawdb_lightning_initial_import_source_fingerprint,
    hawdb_lightning_initial_import_startup_readiness, parse_hawdb_lightning_graph_stream_export,
    validate_hawdb_lightning_graph_stream, validate_hawdb_lightning_relational_stream,
    CanonicalGraphSnapshotExport, CanonicalGraphSnapshotValidation,
    CanonicalSnapshotEndpointViolation, CanonicalSnapshotIdentityAudit, CanonicalSnapshotNode,
    CanonicalSnapshotRelationship, CanonicalStableIdMapping, HawDBLightningBootstrapExport,
    HawDBLightningBootstrapManifest, HawDBLightningGraphStream,
    HawDBLightningGraphStreamValidation, HawDBLightningInitialImportApplyReport,
    HawDBLightningInitialImportCheckpoint, HawDBLightningInitialImportCheckpointProgress,
    HawDBLightningInitialImportCheckpointProgressReport,
    HawDBLightningInitialImportCheckpointReadiness,
    HawDBLightningInitialImportCutoverCatchUpReport, HawDBLightningInitialImportDocumentIdentity,
    HawDBLightningInitialImportDocumentIdentityCoverage,
    HawDBLightningInitialImportDocumentIdentityKindReport,
    HawDBLightningInitialImportDurableBatchAdvanceReport, HawDBLightningInitialImportDurableState,
    HawDBLightningInitialImportDurableStateCodecReport,
    HawDBLightningInitialImportDurableStateReport, HawDBLightningInitialImportIdempotencyKey,
    HawDBLightningInitialImportPlan, HawDBLightningInitialImportReadiness,
    HawDBLightningInitialImportReadinessInputs, HawDBLightningInitialImportRecoveryReadinessReport,
    HawDBLightningInitialImportResumeAction, HawDBLightningInitialImportResumeActionKind,
    HawDBLightningInitialImportSearchProjectionBatchReport,
    HawDBLightningInitialImportSessionBundleReadiness, HawDBLightningInitialImportSessionReport,
    HawDBLightningInitialImportSourceBundleReadiness, HawDBLightningInitialImportSourceFingerprint,
    HawDBLightningInitialImportStartupReadinessReport,
    HawDBLightningInitialImportStreamingBatchAdvanceReport, HawDBLightningRelationalStream,
    HawDBLightningRelationalStreamValidation, HAWDB_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION,
    HAWDB_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION,
    HAWDB_LIGHTNING_INITIAL_IMPORT_DURABLE_STATE_PROTOCOL,
    HAWDB_LIGHTNING_RELATIONAL_STREAM_FORMAT_VERSION,
};
pub use concurrent::{
    ConcurrentDatabase, ConcurrentDatabaseMetrics, ConcurrentDatabaseTransaction,
    ConcurrentPhaseMetrics, ConcurrentTransactionMode, ConcurrentTransactionOptions,
    WalGroupCommitActivation, WalGroupCommitAdaptiveColdStartEvidence,
    WalGroupCommitAdaptivePolicyEvidence, WalGroupCommitAdaptiveSteadyStateEvidence,
    WalGroupCommitConfig, WalGroupCommitDelayPolicy, WalGroupCommitEvidence,
    WalGroupCommitSnapshot, WalGroupCommitTailLatencyEvidence, WalGroupCommitWaitDecision,
    DEFAULT_PESSIMISTIC_LOCK_TIMEOUT, DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES,
    DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY, DEFAULT_WAL_GROUP_COMMIT_MAX_ENTRIES,
};
pub use hawdb_core::QueryAccessControlContext;
pub use hawdb_evidence::AccessControlPolicyReadiness;
pub use hawdb_executor::{BoundedReadQueryOutput, QueryStreamOptions, QueryStreamReport};
pub use hawdb_explain::{ExplainAnalyzeOutput, ExplainOutput, NowledgeGraphExplainOutput};
pub use hawdb_system_sql::{
    SlowQueryLogExportOptions, SlowQueryLogRecordSummary, SLOW_QUERY_LOG_EVENT_PROTOCOL,
};
pub use plan_cache::{PlanCacheBypassReason, PlanCacheLookup, PlanCacheStats};
pub use resource_profile::{
    StorageResourceProfileLimits, StorageResourceProfileReport, STORAGE_RESOURCE_PROFILE_PROTOCOL,
};
pub use search_projection_catch_up::{
    ScheduledSearchProjectionCatchUpReport, SearchProjectionCatchUpReport,
    SearchProjectionCatchUpStopReason,
};
pub use source_candidates::{
    KnowledgeSourceCandidateRow, KnowledgeSourceCandidateScanOrigin,
    KnowledgeSourceCandidateScanOutput, KnowledgeSourceCandidateScanRequest,
};
pub use system_schema::{SystemSchemaMigration, SystemSchemaRegistry, SystemSchemaUpgradeReport};
pub use system_variables::QuerySystemVariables;

fn hawdb_lightning_initial_import_source_fingerprint_key(
    manifest: &HawDBLightningBootstrapManifest,
) -> String {
    let fingerprint = hawdb_lightning_initial_import_source_fingerprint(manifest);
    format!(
        "v{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
        fingerprint.protocol_version,
        fingerprint.database_commit_epoch,
        fingerprint.graph_commit_epoch,
        fingerprint.logical_checksum,
        fingerprint.graph_stream_checksum,
        fingerprint.graph_stream_byte_len,
        fingerprint.relational_stream_checksum,
        fingerprint.relational_stream_byte_len,
        fingerprint.schema_checksum,
        fingerprint.node_count,
        fingerprint.relationship_count,
        fingerprint.relational_table_count,
        fingerprint.relational_row_count,
    )
}

/// Equality-only identity of one admitted database runtime in this process.
///
/// Clones retain only a marker, not database resources. Reopening or switching
/// to another branch creates a different identity, even at equal contents/epochs.
/// This token is not a persistent identity or evidence of a retained fence.
#[derive(Debug, Clone)]
pub struct DatabaseInstanceIdentity(Arc<()>);

impl DatabaseInstanceIdentity {
    fn new() -> Self {
        Self(Arc::new(()))
    }
}

impl PartialEq for DatabaseInstanceIdentity {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for DatabaseInstanceIdentity {}

#[derive(Debug)]
pub struct Database {
    runtime: runtime_cell::BranchRuntimeCell,
    project_root_path: Option<PathBuf>,
    branch_create_recovery: Option<BranchCreateRecoveryReport>,
    durability: DurabilityPolicy,
    slow_query_log: Arc<SharedState<system_sql::SlowQueryLog>>,
    statement_summary: Arc<SharedState<system_sql::StatementSummary>>,
    config: DatabaseConfig,
    local_qos_scheduler: LocalQosScheduler,
    system_variables: QuerySystemVariables,
    derived_artifact_jobs: hawdb_artifact::DerivedArtifactJobQueue,
    derived_artifact_root_pins:
        BTreeMap<u64, Option<Arc<hawdb_storage::branch_root_pins::RootPin>>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    runtime_governor: Option<hawdb_qos::RuntimeGovernor>,
    retained_runtime: Arc<retained_query::RetainedRuntime>,
}

pub(crate) struct DatabaseCheckpointSource {
    catalog: Catalog,
    store: GraphStore,
    suspension: Option<automatic_checkpoint::Suspension>,
}

pub(crate) struct PreparedDatabaseCheckpoint {
    checkpoint: PreparedCheckpoint,
    _suspension: Option<automatic_checkpoint::Suspension>,
}

impl DatabaseCheckpointSource {
    pub(crate) fn prepare(self) -> Result<Option<PreparedDatabaseCheckpoint>> {
        Ok(self
            .store
            .prepare_checkpoint(&self.catalog)?
            .map(|checkpoint| PreparedDatabaseCheckpoint {
                checkpoint,
                _suspension: self.suspension,
            }))
    }
}

#[derive(Debug)]
pub(super) struct SharedState<T>(Mutex<T>);

impl<T> SharedState<T> {
    fn new(value: T) -> Self {
        Self(Mutex::new(value))
    }

    pub(super) fn borrow(&self) -> MutexGuard<'_, T> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(super) fn borrow_mut(&self) -> MutexGuard<'_, T> {
        self.borrow()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseConfig {
    pub read_only: bool,
    /// Maximum monotonic age of uncheckpointed data in an admitted persistent
    /// writer. The background-maintenance capability controls worker creation.
    pub automatic_checkpoint_max_age: std::time::Duration,
    /// Bounded pending-child recovery during a writable project open.
    pub branch_create_recovery_limits: BranchCreateRecoveryLimits,
    /// Optional host-wide read row ceiling. `Some(0)` and `None` (the default) mean unlimited.
    /// Session settings may lower a positive ceiling, but cannot raise it.
    pub max_read_result_rows: Option<usize>,
    pub max_read_result_payload_bytes: Option<usize>,
    pub execution_memory: executor::ExecutionMemoryConfig,
    pub mutation_limits: hawdb_storage::mutation::MutationLimits,
    /// Maximum memo groups available to graph and relational optimizer search.
    pub max_optimizer_groups: Option<usize>,
    /// Maximum memo expressions available to relational join enumeration.
    pub max_relational_join_expressions: Option<usize>,
    pub recovery_mode: RecoveryMode,
    pub max_wal_replay_entries: Option<usize>,
    pub max_wal_replay_bytes: Option<u64>,
    /// Maximum sealed WAL generations recovered for one branch admission.
    /// Checkpoint compaction resets the chain; default is 256.
    pub max_branch_sealed_wal_intervals: usize,
    /// Shared capacity target for all engine-owned project file descriptors.
    /// Defaults to 1024 without pre-opening files. Native Unix acquisition limits
    /// effective admission to the OS soft limit minus 64 host-owned handles.
    /// Process limits remain under host control; idle immutable files use LRU.
    /// Independent contexts for one canonical project must request the same value.
    pub max_open_files: usize,
    /// Reject foreground descriptor admission when `max_open_files` cannot be
    /// met by evicting idle handles. Defaults to false. Internal control I/O,
    /// recovery and maintenance admission always use a non-strict policy.
    pub strict_capacity_limit: bool,
    pub max_wal_quarantine_bytes: u64,
    pub max_wal_record_bytes: Option<usize>,
    pub max_wal_batch_operations: Option<usize>,
    pub max_checkpoint_encoded_bytes: Option<u64>,
    pub max_checkpoint_decoded_bytes: Option<u64>,
    pub segment_cache_capacity_bytes: u64,
    /// Aggregate encoded graph-manifest bytes allowed during database open.
    /// Payload pages remain governed separately by the segment cache.
    pub max_graph_manifest_open_bytes: u64,
    /// Maximum physical relational-index file bytes fetched by one query.
    /// Cache hits consume the separately derived logical traversal budget only.
    pub max_relational_index_read_bytes: NonZeroUsize,
    pub max_relational_hydration_bytes: NonZeroUsize,
    pub storage_residency_mode: hawdb_storage::config::StorageResidencyMode,
    pub auto_materialize_checkpoint_bytes: u64,
    pub max_out_of_core_delta_bytes: Option<u64>,
    /// Derived columnar shadow double-write: every checkpoint also
    /// publishes a column-group catalog under `column-groups/`, and recovery
    /// validates it. Off by default; with the flag off checkpoints are
    /// byte-for-byte unchanged and no shadow directory exists. Reads are
    /// never served from the shadow.
    pub graph_columnar_shadow_checkpoint: bool,
    /// Persistent relational-index publication and read activation mode.
    pub relational_index_mode: hawdb_storage::config::RelationalIndexMode,
    /// Enables the metadata-only monotonic INSERT fast path for RowPage tables.
    /// Disabled by default until workload qualification explicitly activates it.
    pub relational_monotonic_append_fast_path: bool,
    pub max_search_projection_change_log_entries: Option<usize>,
    pub max_search_projection_change_log_bytes: Option<usize>,
    pub search_projection_relational_change_limits:
        hawdb_storage::relational::RelationalPrimaryKeyChangeCaptureLimits,
    /// Maximum entries per graph physical-plan or relational SQL-template cache.
    pub max_plan_cache_entries: Option<usize>,
    /// Commit-epoch lag tolerated before the inline query path recomputes
    /// optimizer statistics. `None` or `Some(0)` refreshes on every committed
    /// data change (freshest plans). Larger values amortize statistics
    /// recomputation over write-heavy workloads; stale statistics only affect
    /// plan choice, never correctness.
    pub optimizer_statistics_inline_commit_lag: Option<u64>,
    pub slow_query_log_capacity: usize,
    pub slow_query_log_threshold_micros: u128,
    pub statement_summary_capacity: usize,
    /// Background-work policy used for this database's full runtime lifetime
    /// by its shared local QoS scheduler.
    pub local_qos_policy: LocalQosPolicy,
    pub runtime_capabilities: hawdb_core::RuntimeCapabilities,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub adaptive_vector_backend_policy: hawdb_optimizer::AdaptiveVectorBackendPolicy,
}

/// Default host row-count ceiling: no ceiling. Byte budgets remain active.
pub const DEFAULT_MAX_READ_RESULT_ROWS: Option<usize> = None;
pub const DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

fn restrictive_query_limit(configured: Option<usize>, requested: Option<usize>) -> Option<usize> {
    match (configured, requested) {
        (Some(configured), Some(requested)) => Some(configured.min(requested)),
        (Some(configured), None) => Some(configured),
        (None, Some(requested)) => Some(requested),
        (None, None) => None,
    }
}

fn relational_query_limits_with_payload(
    config: &DatabaseConfig,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
) -> crate::relational_sql::RelationalQueryLimits {
    let max_output_rows =
        restrictive_query_limit(config.read_result_row_limit(), max_rows).unwrap_or(usize::MAX);
    let max_output_payload_bytes =
        restrictive_query_limit(config.max_read_result_payload_bytes, max_payload_bytes)
            .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES);
    let max_intermediate_rows = config.read_result_row_limit().unwrap_or(usize::MAX);
    // A blocking relational query first reads the bounded scan projection and
    // then re-reads at most `max_output_rows` selected locators with the final
    // output projection. Budget both phases explicitly so a scan that exactly
    // reaches its intermediate-row limit can still produce an admitted result.
    let max_row_read_rows = max_intermediate_rows.saturating_add(max_output_rows).max(1);
    let max_scan_pages = max_intermediate_rows
        .div_ceil(hawdb_storage::relational::DEFAULT_RELATIONAL_ROW_PAGE_ROWS)
        .max(1);
    let max_row_read_pages = max_scan_pages.saturating_add(max_output_rows).max(1);
    let max_row_read_bytes = max_row_read_pages
        .saturating_mul(hawdb_storage::relational::DEFAULT_RELATIONAL_ROW_PAGE_BYTES)
        .max(1);
    // Logical index traversal includes cache hits and must scale with the
    // statement's admitted intermediate rows. Physical index reads retain a
    // separate fixed I/O ceiling so a warm cache cannot disable query bounds.
    let max_index_read_pages = max_intermediate_rows
        .saturating_mul(
            hawdb_storage::relational::DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT as usize,
        )
        .max(1);
    let max_index_read_bytes = max_index_read_pages
        .saturating_mul(hawdb_storage::index_page::DEFAULT_IMMUTABLE_INDEX_PAGE_BYTES)
        .max(1);
    crate::relational_sql::RelationalQueryLimits {
        max_output_rows,
        max_output_payload_bytes,
        max_intermediate_rows,
        max_candidate_work: max_intermediate_rows,
        hydration: hawdb_storage::relational::RelationalHydrationBudget {
            max_rows: max_row_read_rows,
            max_compressed_bytes: config.max_relational_hydration_bytes.get(),
            max_decompressed_bytes: config.max_relational_hydration_bytes.get(),
            max_memory_bytes: config.max_relational_hydration_bytes.get(),
            ..hawdb_storage::relational::RelationalHydrationBudget::default()
        },
        index_read: hawdb_storage::relational::RelationalIndexReadLimits {
            max_pages: NonZeroUsize::new(max_index_read_pages)
                .expect("relational index query page budget is non-zero"),
            max_rows: NonZeroUsize::new(max_intermediate_rows.max(1))
                .expect("relational index query row budget is non-zero"),
            max_bytes: NonZeroUsize::new(max_index_read_bytes)
                .expect("relational index query byte budget is non-zero"),
            max_file_bytes: config.max_relational_index_read_bytes.get(),
            ..hawdb_storage::relational::RelationalIndexReadLimits::default()
        },
        row_read: hawdb_storage::relational::RelationalRowPageSnapshotReadLimits {
            demand: hawdb_storage::relational::RelationalRowPageDemandReadLimits {
                max_pages: NonZeroUsize::new(max_row_read_pages)
                    .expect("relational row query page budget is non-zero"),
                max_rows: NonZeroUsize::new(max_row_read_rows)
                    .expect("relational row query row budget is non-zero"),
                max_bytes: NonZeroUsize::new(max_row_read_bytes)
                    .expect("relational row query byte budget is non-zero"),
                ..hawdb_storage::relational::RelationalRowPageDemandReadLimits::default()
            },
            ..hawdb_storage::relational::RelationalRowPageSnapshotReadLimits::default()
        },
    }
}

fn relational_join_enumeration_config_from_database_config(
    config: &DatabaseConfig,
) -> hawdb_optimizer::RelationalJoinEnumerationConfig {
    let defaults = hawdb_optimizer::RelationalJoinEnumerationConfig::default();
    hawdb_optimizer::RelationalJoinEnumerationConfig {
        max_groups: optimizer_config_from_database_config(config).max_groups,
        max_expressions: config
            .max_relational_join_expressions
            .unwrap_or(defaults.max_expressions),
    }
}

fn relational_query_resource_context<'a>(
    config: &'a DatabaseConfig,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
    task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
) -> crate::relational_sql::RelationalQueryResourceContext<'a> {
    crate::relational_sql::RelationalQueryResourceContext::new(
        relational_join_enumeration_config_from_database_config(config),
        relational_query_limits_with_payload(config, max_rows, max_payload_bytes),
        &config.execution_memory,
        task_context,
    )
}

fn relational_plan_template_cache_from_database_config(
    config: &DatabaseConfig,
) -> Arc<crate::relational_sql::RelationalPlanTemplateCache> {
    Arc::new(crate::relational_sql::RelationalPlanTemplateCache::new(
        config.max_plan_cache_entries,
    ))
}

fn relational_index_read_mode<
    'a,
    R: hawdb_relational::index_runtime::RelationalIndexStoreReader,
>(
    config: &DatabaseConfig,
    store: &'a R,
) -> hawdb_relational::index_runtime::RelationalIndexReadMode<'a, R> {
    match config.relational_index_mode {
        hawdb_storage::config::RelationalIndexMode::Materialized => {
            hawdb_relational::index_runtime::RelationalIndexReadMode::Materialized
        }
        hawdb_storage::config::RelationalIndexMode::Shadow => {
            hawdb_relational::index_runtime::RelationalIndexReadMode::Shadow(store)
        }
        hawdb_storage::config::RelationalIndexMode::DemandPaged => {
            hawdb_relational::index_runtime::RelationalIndexReadMode::DemandPaged(store)
        }
        hawdb_storage::config::RelationalIndexMode::Authoritative => {
            hawdb_relational::index_runtime::RelationalIndexReadMode::Authoritative(store)
        }
    }
}

/// Per-session result policy. Independent sessions start with no row-count limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionConfig {
    /// Maximum result rows; zero means no session row-count limit.
    /// Payload, execution-memory and explicit host ceilings remain enforced.
    /// A positive ceiling at or above usize::MAX imposes no additional bound:
    /// no in-process result can exceed platform row capacity. On wasm32 this
    /// includes values such as 5,000,000,000; positive host ceilings still apply.
    pub max_result_rows: u64,
}

impl SessionConfig {
    fn row_limit(self) -> Option<usize> {
        // No representable result can exceed usize::MAX rows. Avoid asking
        // the execution detector to add one to that effective infinity.
        (self.max_result_rows != 0)
            .then(|| usize::try_from(self.max_result_rows).unwrap_or(usize::MAX))
            .filter(|rows| *rows != usize::MAX)
    }
}

impl DatabaseConfig {
    /// Effective host row-count ceiling. Both zero and `None` disable it.
    pub fn read_result_row_limit(&self) -> Option<usize> {
        self.max_read_result_rows.filter(|rows| *rows != 0)
    }

    fn wal_replay_config(&self) -> WalReplayConfig {
        WalReplayConfig {
            max_open_files: self.max_open_files,
            strict_capacity_limit: self.strict_capacity_limit,
            recovery_mode: self.recovery_mode,
            max_entries: self.max_wal_replay_entries,
            max_bytes: self.max_wal_replay_bytes,
            max_branch_sealed_wal_intervals: self.max_branch_sealed_wal_intervals,
            max_quarantine_bytes: self.max_wal_quarantine_bytes,
            max_record_bytes: self.max_wal_record_bytes,
            max_batch_operations: self.max_wal_batch_operations,
            max_checkpoint_encoded_bytes: self.max_checkpoint_encoded_bytes,
            max_checkpoint_decoded_bytes: self.max_checkpoint_decoded_bytes,
            segment_cache_capacity_bytes: self.segment_cache_capacity_bytes,
            max_graph_manifest_open_bytes: self.max_graph_manifest_open_bytes,
            residency_mode: self.storage_residency_mode,
            auto_materialize_checkpoint_bytes: self.auto_materialize_checkpoint_bytes,
            max_out_of_core_delta_bytes: self.max_out_of_core_delta_bytes,
            graph_columnar_shadow_checkpoint: self.graph_columnar_shadow_checkpoint,
            relational_index_mode: self.relational_index_mode,
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            read_only: false,
            automatic_checkpoint_max_age: std::time::Duration::from_secs(60),
            branch_create_recovery_limits: BranchCreateRecoveryLimits::default(),
            max_read_result_rows: DEFAULT_MAX_READ_RESULT_ROWS,
            max_read_result_payload_bytes: Some(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
            execution_memory: executor::ExecutionMemoryConfig::default(),
            mutation_limits: hawdb_storage::mutation::MutationLimits::default(),
            max_optimizer_groups: None,
            max_relational_join_expressions: None,
            recovery_mode: RecoveryMode::default(),
            max_wal_replay_entries: Some(hawdb_storage::config::DEFAULT_MAX_WAL_REPLAY_ENTRIES),
            max_wal_replay_bytes: Some(hawdb_storage::config::DEFAULT_MAX_WAL_REPLAY_BYTES),
            max_branch_sealed_wal_intervals:
                hawdb_storage::config::DEFAULT_MAX_BRANCH_SEALED_WAL_INTERVALS,
            max_open_files: hawdb_storage::file_descriptors::DEFAULT_MAX_OPEN_FILES,
            strict_capacity_limit: false,
            max_wal_quarantine_bytes: hawdb_storage::config::DEFAULT_MAX_WAL_QUARANTINE_BYTES,
            max_wal_record_bytes: Some(hawdb_storage::config::DEFAULT_MAX_WAL_RECORD_BYTES),
            max_wal_batch_operations: Some(hawdb_storage::config::DEFAULT_MAX_WAL_BATCH_OPERATIONS),
            max_checkpoint_encoded_bytes: Some(
                hawdb_storage::config::DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES,
            ),
            max_checkpoint_decoded_bytes: Some(
                hawdb_storage::config::DEFAULT_MAX_CHECKPOINT_DECODED_BYTES,
            ),
            segment_cache_capacity_bytes:
                hawdb_storage::config::DEFAULT_SEGMENT_CACHE_CAPACITY_BYTES,
            max_graph_manifest_open_bytes:
                hawdb_storage::config::DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES,
            max_relational_index_read_bytes: NonZeroUsize::new(
                hawdb_storage::relational::DEFAULT_RELATIONAL_INDEX_READ_BYTES,
            )
            .expect("default relational index read byte budget is non-zero"),
            max_relational_hydration_bytes: NonZeroUsize::new(
                hawdb_storage::relational::DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            )
            .expect("default relational hydration byte budget is non-zero"),
            storage_residency_mode: hawdb_storage::config::StorageResidencyMode::Auto,
            auto_materialize_checkpoint_bytes:
                hawdb_storage::config::DEFAULT_AUTO_MATERIALIZE_CHECKPOINT_BYTES,
            max_out_of_core_delta_bytes: Some(
                hawdb_storage::config::DEFAULT_MAX_OUT_OF_CORE_DELTA_BYTES,
            ),
            graph_columnar_shadow_checkpoint: false,
            relational_index_mode: hawdb_storage::config::RelationalIndexMode::default(),
            relational_monotonic_append_fast_path: false,
            max_search_projection_change_log_entries: Some(
                DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_ENTRIES,
            ),
            max_search_projection_change_log_bytes: Some(
                DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_BYTES,
            ),
            search_projection_relational_change_limits: Default::default(),
            max_plan_cache_entries: Some(DEFAULT_PLAN_CACHE_MAX_ENTRIES),
            optimizer_statistics_inline_commit_lag: None,
            slow_query_log_capacity: system_sql::DEFAULT_SLOW_QUERY_LOG_CAPACITY,
            slow_query_log_threshold_micros: system_sql::DEFAULT_SLOW_QUERY_LOG_THRESHOLD_MICROS,
            statement_summary_capacity: system_sql::DEFAULT_STATEMENT_SUMMARY_CAPACITY,
            local_qos_policy: LocalQosPolicy::default(),
            runtime_capabilities: crate::compiled_runtime_capabilities(),
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            adaptive_vector_backend_policy: hawdb_optimizer::AdaptiveVectorBackendPolicy::default(),
        }
    }
}

pub(crate) fn system_runtime_snapshot(
    config: &DatabaseConfig,
) -> system_sql::SystemRuntimeSnapshot {
    system_sql::SystemRuntimeSnapshot::new(
        config.read_only,
        config.read_result_row_limit(),
        config.max_read_result_payload_bytes,
        config.storage_residency_mode,
        config.relational_index_mode,
        config.runtime_capabilities,
    )
}

pub use hawdb_executor::QueryOutput;

/// Deterministic outcome for one relational INSERT, UPDATE, or DELETE statement.
///
/// A staged statement is provisional until its enclosing transaction commits.
/// Conflict no-ops increment `conflict_rows`, never `affected_rows`, and never
/// contribute a `RETURNING` row. In the v1 contract, `rows` contains values for
/// supported `INSERT ... RETURNING` statements and remains empty for count-only
/// UPDATE and DELETE outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalMutationResult {
    pub affected_rows: usize,
    pub conflict_rows: usize,
    pub rows: executor::QueryRows,
    pub provisional: bool,
}

/// Result of one SQL statement executed inside a database transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlStatementResult {
    pub output: QueryOutput,
    pub mutation: Option<RelationalMutationResult>,
}

/// Confirmed results produced by the durable transaction commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionCommitResult {
    pub output: QueryOutput,
    pub mutations: Vec<RelationalMutationResult>,
    pub append_mutations: Vec<hawdb_storage::append_table::AppendMutationOutcome>,
}

/// Confirmed generated order-key assignments from one durable append commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendCommitResult {
    pub commit_epoch: u64,
    pub mutations: Vec<hawdb_storage::append_table::AppendMutationOutcome>,
}

/// Declares the relational tables exposed by one owner-scoped projection
/// generation inside a pinned read transaction.
///
/// The binding is generic rather than route-specific. The application owns the
/// PostgreSQL DDL for the named tables and the projection version. HawDB owns
/// active-generation resolution, pin lifetime, row decoding, and query
/// admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionRelationalReadBinding {
    projection: String,
    owner_key: Vec<u8>,
    projection_version: u64,
    tables: BTreeSet<String>,
}

impl ProjectionRelationalReadBinding {
    pub fn new(
        projection: impl Into<String>,
        owner_key: impl Into<Vec<u8>>,
        projection_version: u64,
        tables: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        let projection = projection.into();
        let owner_key = owner_key.into();
        let tables = tables.into_iter().map(Into::into).collect::<BTreeSet<_>>();
        if projection.trim().is_empty() {
            return Err(HawDBError::Semantic(
                "projection relational binding requires a non-empty projection name".to_string(),
            ));
        }
        if owner_key.is_empty() {
            return Err(HawDBError::Semantic(
                "projection relational binding requires a non-empty owner key".to_string(),
            ));
        }
        if projection_version == 0 {
            return Err(HawDBError::Semantic(
                "projection relational binding requires a non-zero projection version".to_string(),
            ));
        }
        if tables.is_empty() || tables.iter().any(|table| table.trim().is_empty()) {
            return Err(HawDBError::Semantic(
                "projection relational binding requires non-empty table names".to_string(),
            ));
        }
        Ok(Self {
            projection,
            owner_key,
            projection_version,
            tables,
        })
    }

    pub fn projection(&self) -> &str {
        &self.projection
    }

    pub fn owner_key(&self) -> &[u8] {
        &self.owner_key
    }

    pub const fn projection_version(&self) -> u64 {
        self.projection_version
    }

    pub fn tables(&self) -> &BTreeSet<String> {
        &self.tables
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct StatementExecutionContext<'a> {
    execution_profile: Option<&'a executor::ReadExecutionProfile>,
    retained_profile: Option<&'a RetainedQueryProfile>,
    access_control: Option<&'a QueryAccessControlContext>,
    parse_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphStatement {
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphTransactionOutput {
    pub statement_outputs: Vec<QueryOutput>,
    pub commit_output: QueryOutput,
}

#[derive(Debug)]
pub struct DatabaseTransaction<'a> {
    db: &'a mut Database,
    runtime: DatabaseTransactionRuntime,
    state: DatabaseTransactionState,
    task_context: Option<hawdb_core::RuntimeTaskContext>,
}

#[derive(Debug)]
pub(super) struct DatabaseTransactionRuntime {
    instance_identity: DatabaseInstanceIdentity,
    session_config: SessionConfig,
    optimizer: CascadesOptimizer,
    plan_cache: SharedState<PlanCache>,
    relational_plan_template_cache: Arc<crate::relational_sql::RelationalPlanTemplateCache>,
    optimizer_planning_cache: SharedState<OptimizerPlanningCache>,
    config: DatabaseConfig,
    system_variables: QuerySystemVariables,
    branch_catalog_path: Option<PathBuf>,
    current_branch: Option<branch_lifecycle::CurrentBranch>,
}

#[derive(Debug)]
pub(super) struct DatabaseTransactionState {
    read_dependencies: Option<transaction_read_dependencies::TransactionReadDependencies>,
    graph_transaction: Option<GraphMutationTransaction>,
    relational_transaction: hawdb_storage::relational::RelationalTransaction,
    relational_state: hawdb_storage::relational::RelationalState,
    append_transaction: hawdb_storage::append_table::AppendTransaction,
    append_state: hawdb_storage::append_table::AppendState,
    pending_generated_append_tables: BTreeSet<String>,
    relational_returning: Vec<Option<crate::relational_sql::RelationalReturningProjection>>,
    relational_index:
        std::result::Result<Option<crate::store::RelationalTransactionIndexView>, String>,
    relational_rows:
        std::result::Result<Option<crate::store::RelationalTransactionRowView>, String>,
    // Moves with the private workspace into a queued commit. Checkpoint
    // reclamation must retain its physical generation until the state retires.
    snapshot_pin: Option<ReaderPin>,
}

struct SparseRelationalStatementStage {
    state: hawdb_storage::relational::RelationalState,
    mutation_outcomes: Vec<hawdb_storage::relational::RelationalMutationOutcome>,
    index_capture: hawdb_storage::relational::RelationalIndexChangeCapture,
    row_capture: hawdb_storage::relational::RelationalRowChangeCapture,
}

pub(crate) struct GraphTransactionStatementOutcome {
    pub(crate) output: QueryOutput,
    pub(crate) savepoint: Option<GraphMutationSavepoint>,
    pub(crate) lock_footprint: GraphMutationLockFootprint,
}

#[derive(Debug)]
pub struct DatabaseSession<'a> {
    config: SessionConfig,
    db: &'a mut Database,
    graph_transaction: Option<GraphMutationTransaction>,
    transaction_runtime: Option<DatabaseTransactionRuntime>,
    system_variables: QuerySystemVariables,
}

#[derive(Debug)]
enum ReadPlanCache {
    Private(SharedState<PlanCache>),
    Shared(Arc<SharedState<PlanCache>>),
}

impl std::ops::Deref for ReadPlanCache {
    type Target = SharedState<PlanCache>;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Private(cache) => cache,
            Self::Shared(cache) => cache,
        }
    }
}

#[derive(Debug)]
pub struct DatabaseReadTransaction<S: crate::executor::ExecutionStore = GraphStore> {
    session_config: SessionConfig,
    system_variables: QuerySystemVariables,
    catalog: Catalog,
    store: S,
    published_read_view: PublishedReadView,
    optimizer: CascadesOptimizer,
    plan_cache: ReadPlanCache,
    relational_plan_template_cache: Arc<crate::relational_sql::RelationalPlanTemplateCache>,
    optimizer_planning_cache: Arc<SharedState<OptimizerPlanningCache>>,
    slow_query_snapshot: Vec<system_sql::SlowQueryRecord>,
    statement_summary_snapshot: Vec<system_sql::StatementSummaryRecord>,
    config: DatabaseConfig,
    branch_catalog_path: Option<PathBuf>,
    current_branch: Option<branch_lifecycle::CurrentBranch>,
    projection_relational: Option<ProjectionRelationalReadSnapshot>,
    task_context: Option<hawdb_core::RuntimeTaskContext>,
    _pin: Arc<ReaderPin>,
    retained_runtime: Arc<retained_query::RetainedRuntime>,
}

/// An immutable canonical view that can create independent query contexts.
/// The shared pin survives publication replacement until the last query drops.
#[derive(Debug)]
pub(crate) struct DatabaseReadSnapshot(DatabaseReadTransaction);

impl DatabaseReadSnapshot {
    pub(crate) fn config(&self) -> &DatabaseConfig {
        &self.0.config
    }

    pub(crate) fn ensure_usable(&self) -> Result<()> {
        self.0.store.ensure_usable()
    }

    pub(crate) fn begin_read_transaction(
        &self,
        task_context: &hawdb_core::RuntimeTaskContext,
    ) -> Result<DatabaseReadTransaction> {
        self.fork(Some(task_context.clone()))
    }

    fn fork(
        &self,
        task_context: Option<hawdb_core::RuntimeTaskContext>,
    ) -> Result<DatabaseReadTransaction> {
        self.ensure_usable()?;
        let source = &self.0;
        Ok(DatabaseReadTransaction {
            session_config: source.session_config,
            system_variables: source.system_variables.clone(),
            catalog: source.catalog.clone(),
            store: source.store.snapshot_for_read(),
            published_read_view: source.published_read_view,
            optimizer: source.optimizer.clone(),
            plan_cache: ReadPlanCache::Private(SharedState::new(PlanCache::new(
                source.config.max_plan_cache_entries,
            ))),
            relational_plan_template_cache: Arc::clone(&source.relational_plan_template_cache),
            optimizer_planning_cache: Arc::clone(&source.optimizer_planning_cache),
            slow_query_snapshot: source.slow_query_snapshot.clone(),
            statement_summary_snapshot: source.statement_summary_snapshot.clone(),
            config: source.config.clone(),
            branch_catalog_path: source.branch_catalog_path.clone(),
            current_branch: source.current_branch.clone(),
            projection_relational: None,
            task_context,
            _pin: Arc::clone(&source._pin),
            retained_runtime: Arc::clone(&source.retained_runtime),
        })
    }
}

#[derive(Debug)]
struct ProjectionRelationalReadSnapshot {
    binding: ProjectionRelationalReadBinding,
    reader: hawdb_storage::projection_generation::ProjectionGenerationReader,
}

struct ReadStreamingExecutionContext<'a> {
    task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
    external: Option<&'a mut dyn executor::ExternalReadOperator>,
    delivery: executor::StreamDelivery,
    scoring: Option<BoundScoringRequest<'a>>,
    access_control: Option<&'a QueryAccessControlContext>,
}

#[derive(Debug)]
pub struct NowledgeGraphAdapter<'a> {
    db: &'a mut Database,
}

#[derive(Debug, Default)]
struct ReaderPins {
    next_reader_id: u64,
    active_views: BTreeMap<u64, PublishedReadView>,
}

#[derive(Debug)]
struct ReaderPin {
    id: u64,
    pins: Arc<Mutex<ReaderPins>>,
}

fn configure_search_projection_changefeed(store: &mut GraphStore, config: &DatabaseConfig) {
    store.set_search_projection_primary_key_capture_limits(
        config.search_projection_relational_change_limits,
    );
    store.set_max_search_projection_change_log_entries(
        config.max_search_projection_change_log_entries,
    );
    store.set_max_search_projection_change_log_bytes(config.max_search_projection_change_log_bytes);
}

fn configure_relational_fast_paths(store: &mut GraphStore, config: &DatabaseConfig) {
    store.set_relational_monotonic_append_fast_path_enabled(
        config.relational_monotonic_append_fast_path,
    );
}

impl Default for Database {
    fn default() -> Self {
        let config = effective_database_config(DatabaseConfig::default());
        let local_qos_scheduler = LocalQosScheduler::new(config.local_qos_policy);
        let mut store = GraphStore::default();
        configure_search_projection_changefeed(&mut store, &config);
        configure_relational_fast_paths(&mut store, &config);
        Self {
            runtime: runtime_cell::BranchRuntimeCell::admitted(
                runtime_cell::AdmittedBranchRuntime {
                    catalog: Catalog::default(),
                    branch_selection: None,
                    projection_consumers: search_projection_consumer::ConsumerRegistry::load(
                        store.search_projection_registry_root(),
                        store.search_projection_database_identity(),
                    ),
                    store,
                    optimizer: optimizer_from_database_config(&config),
                    plan_cache: Arc::new(SharedState::new(PlanCache::new(
                        config.max_plan_cache_entries,
                    ))),
                    relational_plan_template_cache:
                        relational_plan_template_cache_from_database_config(&config),
                    optimizer_planning_cache: Arc::new(SharedState::new(
                        OptimizerPlanningCache::default(),
                    )),
                    reader_pins: Arc::new(Mutex::new(ReaderPins::default())),
                },
            ),
            project_root_path: None,

            durability: DurabilityPolicy::default(),

            slow_query_log: Arc::new(SharedState::new(system_sql::SlowQueryLog::new(
                config.slow_query_log_capacity,
            ))),
            statement_summary: Arc::new(SharedState::new(system_sql::StatementSummary::new(
                config.statement_summary_capacity,
            ))),
            config,
            local_qos_scheduler,
            system_variables: QuerySystemVariables::default(),

            derived_artifact_jobs: hawdb_artifact::DerivedArtifactJobQueue::default(),
            derived_artifact_root_pins: BTreeMap::new(),
            telemetry: None,
            runtime_governor: None,
            retained_runtime: Arc::new(retained_query::RetainedRuntime::default()),
            branch_create_recovery: None,
        }
    }
}

impl Database {
    fn ensure_writable(&self) -> Result<()> {
        self.runtime.get()?.store.ensure_usable()?;
        if self.config.read_only {
            return Err(HawDBError::Execution(
                "database is opened in read-only mode".to_string(),
            ));
        }
        Ok(())
    }

    pub fn project_graph(&self, rel_type: Option<&str>) -> Result<ProjectedGraph> {
        Ok({
            let runtime = self.runtime.get()?;
            match rel_type {
                Some(name) => runtime
                    .catalog
                    .rel_type_id(name)
                    .map(|rel_type_id| {
                        ProjectedGraph::from_store(&runtime.store, Some(rel_type_id))
                    })
                    .unwrap_or_else(|| ProjectedGraph::from_store_without_edges(&runtime.store)),
                None => ProjectedGraph::from_store(&runtime.store, None),
            }
        })
    }
}

fn effective_database_config(mut config: DatabaseConfig) -> DatabaseConfig {
    config.runtime_capabilities =
        crate::compiled_capabilities::effective_runtime_capabilities(config.runtime_capabilities);
    config
}

fn require_search_projection_source_epoch(expected: Option<u64>, actual: u64) -> Result<()> {
    if let Some(expected) = expected
        && expected != actual
    {
        if expected > actual {
            return Err(HawDBError::Storage(format!(
                "search projection graph delta complete-through epoch {expected} is ahead of graph commit epoch {actual}"
            )));
        }
        return Err(HawDBError::Execution(format!(
            "search projection complete-through epoch {expected} differs from source snapshot epoch {actual}; prepare the complete source interval before publishing its watermark"
        )));
    }
    Ok(())
}

fn search_projection_graph_delta_for(
    catalog: &Catalog,
    store: &GraphStore,
    request: &SearchProjectionGraphDeltaRequest,
) -> Result<SearchProjectionDelta> {
    let operation_count = request.operation_count();
    if let Some(limit) = request.max_operations
        && operation_count > limit
    {
        return Err(HawDBError::Storage(format!(
                "search projection graph delta operation count {operation_count} exceeded configured limit {limit}"
            )));
    }
    // A changefeed prefix describes IDs, not a historical read view.
    // Hydrating it from a newer head would publish future data under an old watermark.
    require_search_projection_source_epoch(
        request.complete_through_graph_commit_epoch,
        store.commit_epoch(),
    )?;

    let mut upserts = Vec::new();
    for node_id in &request.upsert_node_ids {
        let Some(node) = store.node_owned(NodeId(*node_id))? else {
            continue;
        };
        if let Some(row) = projection_row_from_node_with_graph_metadata(catalog, store, &node)? {
            upserts.push(row);
        }
    }

    Ok(SearchProjectionDelta {
        upserts,
        deletes: request.delete_document_ids.clone(),
        max_operations: request.max_operations,
        source_graph_commit_epoch: request.complete_through_graph_commit_epoch,
    })
}

fn search_projection_commit_lag(
    search_index: &SearchIndex,
    required_projection_commit_epoch: u64,
) -> u64 {
    search_projection_freshness_commit_lag(
        &search_index.projection_freshness(),
        required_projection_commit_epoch,
    )
}

fn knowledge_entity_from_node(catalog: &Catalog, node: &NodeRecord) -> KnowledgeEntity {
    KnowledgeEntity {
        node_id: node.id.0,
        labels: node_label_names(catalog, node),
        external_id: Some(projected_node_external_id(node)),
        properties: node.properties.clone(),
    }
}

impl ReaderPins {
    fn oldest_epoch(&self) -> Option<u64> {
        self.oldest_epoch_excluding(None)
    }

    fn oldest_epoch_excluding(&self, excluded: Option<u64>) -> Option<u64> {
        self.active_views
            .iter()
            .filter_map(|(&id, view)| (Some(id) != excluded).then_some(view.visible_commit_epoch()))
            .min()
    }

    fn pinned_physical_generations(&self) -> BTreeSet<u64> {
        self.active_views
            .values()
            .filter_map(|view| view.physical_generation())
            .map(|generation| generation.0)
            .collect()
    }
}

fn elapsed_micros(started: hawdb_core::time::Instant) -> u64 {
    started.elapsed().as_micros().min(u64::MAX as u128) as u64
}

impl ReaderPin {
    fn new(id: u64, pins: Arc<Mutex<ReaderPins>>) -> Self {
        Self { id, pins }
    }
}

impl Drop for ReaderPin {
    fn drop(&mut self) {
        self.pins
            .lock()
            .expect("database reader pins lock should not be poisoned")
            .active_views
            .remove(&self.id);
    }
}

fn search_kind_to_label(kind: &str) -> Option<&'static str> {
    match kind {
        "Memory" | "memory" => Some("Memory"),
        "Message" | "message" => Some("Message"),
        "Entity" | "entity" => Some("Entity"),
        "Source" | "source" => Some("Source"),
        "SourceChunk" | "source_chunk" | "sourcechunk" | "chunk" => Some("SourceChunk"),
        "Community" | "community" => Some("Community"),
        _ => None,
    }
}

fn search_label_to_kind(label: &str) -> Option<&'static str> {
    match label {
        "Memory" | "memory" => Some("memory"),
        "Message" | "message" => Some("message"),
        "Entity" | "entity" => Some("entity"),
        "Source" | "source" => Some("source"),
        "SourceChunk" | "source_chunk" | "sourcechunk" | "chunk" => Some("source_chunk"),
        "Community" | "community" => Some("community"),
        _ => None,
    }
}

fn node_label_names(catalog: &Catalog, node: &NodeRecord) -> Vec<String> {
    node.labels
        .iter()
        .filter_map(|label_id| catalog.label_name(*label_id))
        .map(str::to_string)
        .collect()
}

fn node_external_id(node: &NodeRecord) -> Option<String> {
    node.properties
        .get("id")
        .map(value_to_external_id)
        .filter(|external_id| !external_id.is_empty())
}

fn projected_node_external_id(node: &NodeRecord) -> String {
    node_external_id(node).unwrap_or_else(|| node.id.0.to_string())
}

fn value_to_external_id(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Binary(value) => format!(
            "\\x{}",
            value
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
        Value::Uuid(value) => value.to_string(),
        Value::Vector(_) => value.to_string(),
        Value::List(values) => values
            .iter()
            .map(value_to_external_id)
            .collect::<Vec<_>>()
            .join(","),
        Value::Map(values) => values
            .iter()
            .map(|(key, value)| format!("{key}:{}", value_to_external_id(value)))
            .collect::<Vec<_>>()
            .join(","),
    }
}

fn schema_state_value(state: SchemaObjectState) -> Value {
    let value = match state {
        SchemaObjectState::DeleteOnly => "delete_only",
        SchemaObjectState::WriteOnly => "write_only",
        SchemaObjectState::Backfill => "backfill",
        SchemaObjectState::Validating => "validating",
        SchemaObjectState::Public => "public",
        SchemaObjectState::Gc => "gc",
    };
    Value::String(value.to_string())
}

fn schema_maintenance_actions_output(actions: Vec<SchemaMaintenanceAction>) -> QueryOutput {
    let rows = actions
        .into_iter()
        .map(|action| {
            BTreeMap::from([
                ("object_type".to_string(), Value::String(action.object_type)),
                ("object".to_string(), Value::String(action.object)),
                (
                    "from_state".to_string(),
                    schema_state_value(action.from_state),
                ),
                (
                    "to_state".to_string(),
                    action
                        .to_state
                        .map(schema_state_value)
                        .unwrap_or(Value::Null),
                ),
                ("action".to_string(), Value::String(action.action)),
            ])
        })
        .collect();
    QueryOutput { rows }
}

fn property_index_projection_rebuild_output(
    actions: Vec<PropertyIndexProjectionRebuildAction>,
) -> QueryOutput {
    let rows = actions
        .into_iter()
        .map(|action| {
            BTreeMap::from([
                ("index_kind".to_string(), Value::String(action.index_kind)),
                ("label".to_string(), Value::String(action.label)),
                (
                    "properties".to_string(),
                    Value::List(action.properties.into_iter().map(Value::String).collect()),
                ),
                (
                    "estimated_operations".to_string(),
                    Value::Int(i64::try_from(action.estimated_operations).unwrap_or(i64::MAX)),
                ),
                (
                    "indexed_entries".to_string(),
                    Value::Int(i64::try_from(action.indexed_entries).unwrap_or(i64::MAX)),
                ),
            ])
        })
        .collect();
    QueryOutput { rows }
}

fn optional_u64_value(value: Option<u64>) -> Value {
    value
        .map(|value| Value::Int(value as i64))
        .unwrap_or(Value::Null)
}

fn optional_usize_value(value: Option<usize>) -> Value {
    value
        .map(|value| Value::Int(value as i64))
        .unwrap_or(Value::Null)
}

fn statement_body(statement: &cypher::Statement) -> &cypher::Statement {
    match statement {
        cypher::Statement::CypherQuery(query) => &query.statement,
        _ => statement,
    }
}

pub(crate) fn statement_kind(statement: &cypher::Statement) -> &'static str {
    match statement {
        cypher::Statement::AlterPropertyState(_) => "alter_property_state",
        cypher::Statement::AlterTableState(_) => "alter_table_state",
        cypher::Statement::BeginTransaction => "begin_transaction",
        cypher::Statement::Checkpoint => "checkpoint",
        cypher::Statement::Commit => "commit",
        cypher::Statement::CreateCompositeIndex(_) => "create_composite_index",
        cypher::Statement::CreateFullTextIndex(_) => "create_full_text_index",
        cypher::Statement::CreateIndex(_) => "create_index",
        cypher::Statement::CreateNode(_) => "create_node",
        cypher::Statement::CreateNodeLabel(_) => "create_node_label",
        cypher::Statement::CreateNodePropertyExistsConstraint(_) => {
            "create_node_property_exists_constraint"
        }
        cypher::Statement::CreateNodeTable(_) => "create_node_table",
        cypher::Statement::CreateProperty(_) => "create_property",
        cypher::Statement::CreateRangeIndex(_) => "create_range_index",
        cypher::Statement::CreateRelationship(_) => "create_relationship",
        cypher::Statement::CreateRelationshipPropertyExistsConstraint(_) => {
            "create_relationship_property_exists_constraint"
        }
        cypher::Statement::CreateRelationshipTable(_) => "create_relationship_table",
        cypher::Statement::CreateRelationshipType(_) => "create_relationship_type",
        cypher::Statement::CreateRelationshipUniqueConstraint(_) => {
            "create_relationship_unique_constraint"
        }
        cypher::Statement::CreateUniqueConstraint(_) => "create_unique_constraint",
        cypher::Statement::CypherQuery(query) => statement_kind(&query.statement),
        cypher::Statement::Explain(explain) => {
            if explain.analyze {
                "explain_analyze"
            } else {
                "explain"
            }
        }
        cypher::Statement::GraphAlgorithm(_) => "graph_algorithm",
        cypher::Statement::VectorSearch(_) => "vector_search",
        cypher::Statement::Pipeline(query) => statement_shape::pipeline_statement_kind(query),
        cypher::Statement::MergeNode(_) => "merge_node",
        cypher::Statement::MergeRelationship(_) => "merge_relationship",
        cypher::Statement::UnwindMutation(_) => "unwind_mutation",
        cypher::Statement::ProjectGraph(_) => "project_graph",
        cypher::Statement::Rollback => "rollback",
        cypher::Statement::SetSystemVariable(_) => "set_system_variable",
    }
}

fn optimizer_config_from_database_config(config: &DatabaseConfig) -> OptimizerConfig {
    let mut optimizer = OptimizerConfig::default();
    if let Some(max_groups) = config.max_optimizer_groups {
        optimizer.max_groups = max_groups;
    }
    optimizer
}

fn optimizer_from_database_config(config: &DatabaseConfig) -> CascadesOptimizer {
    CascadesOptimizer::with_context(
        OptimizerContext::from_config(optimizer_config_from_database_config(config))
            .with_resource_hints(ResourceHints {
                priority: ResourceHints::default().priority,
                max_memory_bytes: Some(
                    u64::try_from(config.execution_memory.blocking_operator_bytes.get())
                        .unwrap_or(u64::MAX),
                ),
                max_parallelism: executor::MAX_MORSEL_PARALLELISM,
            }),
    )
}

fn single_import_label(node: &CanonicalSnapshotNode) -> Result<String> {
    match node.labels.as_slice() {
        [label] => Ok(label.clone()),
        [] => Err(HawDBError::Storage(
            "HawDB Lightning initial import node has no label".to_string(),
        )),
        _ => Err(HawDBError::Storage(
            "HawDB Lightning initial import multi-label node is unsupported".to_string(),
        )),
    }
}

fn relational_state_counts(state: &hawdb_storage::relational::RelationalState) -> (usize, usize) {
    let table_count = state.table_schemas().count();
    let row_count = state
        .table_schemas()
        .map(|schema| state.row_count(&schema.name))
        .sum();
    (table_count, row_count)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod legacy_graph_seed_budget_tests;
