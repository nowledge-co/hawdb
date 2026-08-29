use crate::analytics::ProjectedGraph;
use crate::cypher;
use crate::error::{Result, SkeinError};
use crate::executor::{self, Row, RowRef};
use crate::optimizer::{
    CascadesOptimizer, OptimizerCatalog, OptimizerCatalogIndexes, OptimizerCatalogStatistics,
    OptimizerConfig, OptimizerContext, OptimizerIndexStatistics, OptimizerSearchDirective,
    OptimizerTrace, PhysicalPlan, ResourceHints,
};
use crate::qos::{
    BackgroundWorkDecision, BackgroundWorkHint, BackgroundWorkPlan, LocalQosPolicy,
    LocalQosScheduler, LocalQosSnapshot, LocalQosState, QosAdmission, QosAdmissionCode, WorkClass,
    WorkPriority, WorkRequest,
};
#[cfg(test)]
use crate::schema::LabelId;
use crate::schema::{
    AdvancedStatisticsFreshness, Catalog, GraphStatistics, IndexKind, SchemaObjectState,
};
#[cfg(test)]
use crate::schema::{
    CompositeIndexDescriptor, ConstraintDescriptor, IndexDescriptor, PropertyDescriptor,
    TableDescriptor,
};
use crate::search::{
    projection_row_from_node_with_graph_metadata, search_metadata_predicate_pushdown,
    AdaptiveVectorSearchOptions, CompressedVectorSearchMode, MetadataRepairOptions,
    MetadataRepairSummary, SearchCandidateSetReport, SearchDerivedArtifactReport,
    SearchEmptyReasonCode, SearchFallbackReasonCode, SearchFusionWeights, SearchIndex,
    SearchMatchedSpan, SearchMode, SearchPredicatePushdownReport, SearchProjectionDelta,
    SearchProjectionDeltaReport, SearchProjectionFreshness, SearchQueryOptions,
    SearchRebuildOptions, SearchRebuildSummary, SearchResultSet, SearchRetrieverCandidateSetReport,
    SearchTruncationReasonCode,
};
use crate::store::{
    restore_storage_backup, AdjacencyConsistencyReport, AdjacencyConsolidationPlan,
    AdjacencyConsolidationReport, AdjacencyDirection, AdjacencyLayout, AppendSegmentReadOutput,
    AppendTableSchema, AppendTransaction, BasicStatisticsConsistencyReport,
    DegreeStatisticsConsistencyReport, DistinctValueStatisticsConsistencyReport, DurabilityPolicy,
    GraphMutationLockFootprint, GraphMutationSavepoint, GraphMutationTransaction,
    GraphSnapshotNodeImport, GraphSnapshotRelationshipImport, GraphStore, KernelWriteBatch,
    MutationSummary, NodeId, NodeRecord, OptimizerStatisticsRefreshWork, PreparedCheckpoint,
    PropertyIndexConsistencyReport, PropertyIndexProjectionRebuildAction, PublishedReadView,
    RecoveryMode, RelId, RelRecord, SchemaMaintenanceAction, SegmentCacheSnapshot,
    SkeinSnapshotRowsImport, StorageBackupReport, StoragePressureSnapshot,
    StorageReclamationWatermark, StorageRecoveryReport, StorageRestoreReport, StorageScrubReport,
    StoreStableIdMapping, WalReplayConfig,
};
use crate::telemetry::{
    operations_telemetry_readiness, qos_telemetry_sink, KernelTelemetry, KernelTelemetryOperation,
    OperationsTelemetryReadiness, TelemetrySink,
};
use crate::value::Value;
use canonical_snapshot::export_canonical_graph_snapshot_for;
use explain::{empty_read_execution_profile, explain_analyze_output_row, explain_output_row};
use plan_cache::{
    optimized_query_plan_for, statement_uses_plan_cache, OptimizedQueryPlan,
    OptimizerEnvironmentKey, OptimizerPlanningCache, PlanCache, PlanCacheContext, PlanCacheMode,
    DEFAULT_PLAN_CACHE_MAX_ENTRIES,
};
use skein_optimizer::{
    normalize_search_enum_value, search_field_is_enum_like, SearchPredicate, SearchPredicateOp,
    SearchPredicateSet,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KnowledgeNeighborDirection {
    #[cfg(test)]
    Outgoing,
    #[cfg(test)]
    Incoming,
    Both,
}
use system_variables::{
    query_statement_variables_for_statement, query_work_request_for_statement,
    reject_system_variable_parameters,
};

mod access_control;
mod artifact_jobs;
mod canonical_snapshot;
mod concurrent;
mod explain;
mod explain_format;
mod observability;
mod plan_cache;
mod query_runtime;
mod resource_profile;
mod retrieval_pipeline;
mod schema_guidance;
mod search_projection_catch_up;
mod source_candidates;
mod system_schema;
mod system_sql;
mod system_variables;
mod transaction_locks;
mod types;

pub(crate) use query_runtime::PreparedRuntimeQuery;
pub use types::*;

const DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_ENTRIES: usize = 4096;
const DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_BYTES: usize = 64 * 1024 * 1024;
pub const SLOW_QUERY_LOG_EVENT_PROTOCOL: &str = "skein-slow-query-log-event-v1";

pub use access_control::AccessControlPolicyReadiness;
pub use artifact_jobs::{
    DerivedArtifactJob, DerivedArtifactJobReport, DerivedArtifactJobStatus,
    ExternalContentArtifactJobCompletion, ExternalContentArtifactJobSummary,
    ExternalContentArtifactRuntimeManifest,
};
pub use canonical_snapshot::{
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
    validate_skein_lightning_relational_stream, CanonicalGraphSnapshotExport,
    CanonicalGraphSnapshotValidation, CanonicalSnapshotEndpointViolation,
    CanonicalSnapshotIdentityAudit, CanonicalSnapshotNode, CanonicalSnapshotRelationship,
    CanonicalStableIdMapping, SkeinLightningBootstrapExport, SkeinLightningBootstrapManifest,
    SkeinLightningGraphStream, SkeinLightningGraphStreamValidation,
    SkeinLightningInitialImportApplyReport, SkeinLightningInitialImportCheckpoint,
    SkeinLightningInitialImportCheckpointProgress,
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
    SkeinLightningRelationalStreamValidation, SKEIN_LIGHTNING_BOOTSTRAP_PROTOCOL_VERSION,
    SKEIN_LIGHTNING_GRAPH_STREAM_FORMAT_VERSION,
    SKEIN_LIGHTNING_INITIAL_IMPORT_DURABLE_STATE_PROTOCOL,
    SKEIN_LIGHTNING_RELATIONAL_STREAM_FORMAT_VERSION,
};
pub use concurrent::{
    ConcurrentDatabase, ConcurrentDatabaseTransaction, ConcurrentTransactionMode,
    ConcurrentTransactionOptions, WalGroupCommitActivation,
    WalGroupCommitAdaptiveColdStartEvidence, WalGroupCommitAdaptivePolicyEvidence,
    WalGroupCommitAdaptiveSteadyStateEvidence, WalGroupCommitConfig, WalGroupCommitDelayPolicy,
    WalGroupCommitEvidence, WalGroupCommitSnapshot, WalGroupCommitTailLatencyEvidence,
    WalGroupCommitWaitDecision, DEFAULT_PESSIMISTIC_LOCK_TIMEOUT,
    DEFAULT_WAL_GROUP_COMMIT_MAX_BYTES, DEFAULT_WAL_GROUP_COMMIT_MAX_DELAY,
    DEFAULT_WAL_GROUP_COMMIT_MAX_ENTRIES,
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

fn skein_lightning_initial_import_source_fingerprint_key(
    manifest: &SkeinLightningBootstrapManifest,
) -> String {
    let fingerprint = skein_lightning_initial_import_source_fingerprint(manifest);
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

#[derive(Debug)]
pub struct Database {
    catalog: Catalog,
    store: GraphStore,
    optimizer: CascadesOptimizer,
    plan_cache: SharedState<PlanCache>,
    optimizer_planning_cache: SharedState<OptimizerPlanningCache>,
    slow_query_log: SharedState<system_sql::SlowQueryLog>,
    statement_summary: SharedState<system_sql::StatementSummary>,
    config: DatabaseConfig,
    system_variables: QuerySystemVariables,
    reader_pins: Arc<Mutex<ReaderPins>>,
    next_derived_artifact_job_id: u64,
    derived_artifact_jobs: Vec<DerivedArtifactJob>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
}

pub(crate) struct DatabaseCheckpointSource {
    catalog: Catalog,
    store: GraphStore,
}

impl DatabaseCheckpointSource {
    pub(crate) fn prepare(self) -> Result<Option<PreparedCheckpoint>> {
        self.store.prepare_checkpoint(&self.catalog)
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
    pub max_read_result_rows: Option<usize>,
    pub max_read_result_payload_bytes: Option<usize>,
    pub execution_memory: executor::ExecutionMemoryConfig,
    pub mutation_limits: skein_storage::MutationLimits,
    pub max_optimizer_groups: Option<usize>,
    pub recovery_mode: RecoveryMode,
    pub max_wal_replay_entries: Option<usize>,
    pub max_wal_replay_bytes: Option<u64>,
    pub max_wal_record_bytes: Option<usize>,
    pub max_wal_batch_operations: Option<usize>,
    pub max_checkpoint_encoded_bytes: Option<u64>,
    pub max_checkpoint_decoded_bytes: Option<u64>,
    pub segment_cache_capacity_bytes: u64,
    /// Aggregate encoded graph-manifest bytes allowed during database open.
    /// Payload pages remain governed separately by the segment cache.
    pub max_graph_manifest_open_bytes: u64,
    pub max_relational_index_read_bytes: NonZeroUsize,
    pub max_relational_hydration_bytes: NonZeroUsize,
    pub storage_residency_mode: skein_storage::StorageResidencyMode,
    pub auto_materialize_checkpoint_bytes: u64,
    pub max_out_of_core_delta_bytes: Option<u64>,
    /// Derived columnar shadow double-write: every checkpoint also
    /// publishes a column-group catalog under `column-groups/`, and recovery
    /// validates it. Off by default; with the flag off checkpoints are
    /// byte-for-byte unchanged and no shadow directory exists. Reads are
    /// never served from the shadow.
    pub graph_columnar_shadow_checkpoint: bool,
    /// Persistent relational-index publication and read activation mode.
    pub relational_index_mode: skein_storage::RelationalIndexMode,
    /// Enables the metadata-only monotonic INSERT fast path for RowPage tables.
    /// Disabled by default until workload qualification explicitly activates it.
    pub relational_monotonic_append_fast_path: bool,
    pub max_search_projection_change_log_entries: Option<usize>,
    pub max_search_projection_change_log_bytes: Option<usize>,
    pub search_projection_relational_change_limits:
        skein_storage::RelationalPrimaryKeyChangeCaptureLimits,
    pub max_plan_cache_entries: Option<usize>,
    pub slow_query_log_capacity: usize,
    pub slow_query_log_threshold_micros: u128,
    pub statement_summary_capacity: usize,
    pub runtime_capabilities: skein_core::RuntimeCapabilities,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub adaptive_vector_backend_policy: skein_optimizer::AdaptiveVectorBackendPolicy,
}

pub const DEFAULT_MAX_READ_RESULT_ROWS: usize = 100_000;
pub const DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

fn restrictive_query_limit(configured: Option<usize>, requested: Option<usize>) -> Option<usize> {
    match (configured, requested) {
        (Some(configured), Some(requested)) => Some(configured.min(requested)),
        (Some(configured), None) => Some(configured),
        (None, Some(requested)) => Some(requested),
        (None, None) => None,
    }
}

fn relational_query_limits(
    config: &DatabaseConfig,
    max_rows: Option<usize>,
) -> crate::relational_sql::RelationalQueryLimits {
    relational_query_limits_with_payload(config, max_rows, config.max_read_result_payload_bytes)
}

fn relational_query_limits_with_payload(
    config: &DatabaseConfig,
    max_rows: Option<usize>,
    max_payload_bytes: Option<usize>,
) -> crate::relational_sql::RelationalQueryLimits {
    let max_output_rows = max_rows.unwrap_or(DEFAULT_MAX_READ_RESULT_ROWS);
    let max_output_payload_bytes =
        restrictive_query_limit(config.max_read_result_payload_bytes, max_payload_bytes)
            .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES);
    let max_intermediate_rows = config
        .max_read_result_rows
        .unwrap_or(DEFAULT_MAX_READ_RESULT_ROWS);
    // A blocking relational query first reads the bounded scan projection and
    // then re-reads at most `max_output_rows` selected locators with the final
    // output projection. Budget both phases explicitly so a scan that exactly
    // reaches its intermediate-row limit can still produce an admitted result.
    let max_row_read_rows = max_intermediate_rows.saturating_add(max_output_rows).max(1);
    let max_scan_pages = max_intermediate_rows
        .div_ceil(skein_storage::DEFAULT_RELATIONAL_ROW_PAGE_ROWS)
        .max(1);
    let max_row_read_pages = max_scan_pages.saturating_add(max_output_rows).max(1);
    let max_row_read_bytes = max_row_read_pages
        .saturating_mul(skein_storage::DEFAULT_RELATIONAL_ROW_PAGE_BYTES)
        .max(1);
    let max_index_read_bytes = config.max_relational_index_read_bytes;
    crate::relational_sql::RelationalQueryLimits {
        max_output_rows,
        max_output_payload_bytes,
        max_intermediate_rows,
        batch_rows: config.execution_memory.batch_rows,
        blocking_operator_bytes: config.execution_memory.blocking_operator_bytes,
        hydration: skein_storage::RelationalHydrationBudget {
            max_rows: max_row_read_rows,
            max_compressed_bytes: config.max_relational_hydration_bytes.get(),
            max_decompressed_bytes: config.max_relational_hydration_bytes.get(),
            max_memory_bytes: config.max_relational_hydration_bytes.get(),
            ..skein_storage::RelationalHydrationBudget::default()
        },
        index_read: skein_storage::RelationalIndexReadLimits {
            max_rows: NonZeroUsize::new(
                max_intermediate_rows.clamp(1, skein_storage::DEFAULT_RELATIONAL_INDEX_READ_ROWS),
            )
            .expect("relational index query row budget is non-zero"),
            max_bytes: max_index_read_bytes,
            ..skein_storage::RelationalIndexReadLimits::default()
        },
        row_read: skein_storage::RelationalRowPageSnapshotReadLimits {
            demand: skein_storage::RelationalRowPageDemandReadLimits {
                max_pages: NonZeroUsize::new(max_row_read_pages)
                    .expect("relational row query page budget is non-zero"),
                max_rows: NonZeroUsize::new(max_row_read_rows)
                    .expect("relational row query row budget is non-zero"),
                max_bytes: NonZeroUsize::new(max_row_read_bytes)
                    .expect("relational row query byte budget is non-zero"),
                ..skein_storage::RelationalRowPageDemandReadLimits::default()
            },
            ..skein_storage::RelationalRowPageSnapshotReadLimits::default()
        },
    }
}

fn relational_index_read_mode<'a>(
    config: &DatabaseConfig,
    store: &'a GraphStore,
) -> crate::relational_sql::RelationalIndexReadMode<'a> {
    match config.relational_index_mode {
        skein_storage::RelationalIndexMode::Materialized
        | skein_storage::RelationalIndexMode::Shadow => {
            crate::relational_sql::RelationalIndexReadMode::Materialized
        }
        skein_storage::RelationalIndexMode::DemandPaged => {
            crate::relational_sql::RelationalIndexReadMode::DemandPaged(store)
        }
        skein_storage::RelationalIndexMode::Authoritative => {
            crate::relational_sql::RelationalIndexReadMode::Authoritative(store)
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            read_only: false,
            max_read_result_rows: Some(DEFAULT_MAX_READ_RESULT_ROWS),
            max_read_result_payload_bytes: Some(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
            execution_memory: executor::ExecutionMemoryConfig::default(),
            mutation_limits: skein_storage::MutationLimits::default(),
            max_optimizer_groups: None,
            recovery_mode: RecoveryMode::default(),
            max_wal_replay_entries: Some(skein_storage::DEFAULT_MAX_WAL_REPLAY_ENTRIES),
            max_wal_replay_bytes: Some(skein_storage::DEFAULT_MAX_WAL_REPLAY_BYTES),
            max_wal_record_bytes: Some(skein_storage::DEFAULT_MAX_WAL_RECORD_BYTES),
            max_wal_batch_operations: Some(skein_storage::DEFAULT_MAX_WAL_BATCH_OPERATIONS),
            max_checkpoint_encoded_bytes: Some(skein_storage::DEFAULT_MAX_CHECKPOINT_ENCODED_BYTES),
            max_checkpoint_decoded_bytes: Some(skein_storage::DEFAULT_MAX_CHECKPOINT_DECODED_BYTES),
            segment_cache_capacity_bytes: skein_storage::DEFAULT_SEGMENT_CACHE_CAPACITY_BYTES,
            max_graph_manifest_open_bytes: skein_storage::DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES,
            max_relational_index_read_bytes: NonZeroUsize::new(
                skein_storage::DEFAULT_RELATIONAL_INDEX_READ_BYTES,
            )
            .expect("default relational index read byte budget is non-zero"),
            max_relational_hydration_bytes: NonZeroUsize::new(
                skein_storage::DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES,
            )
            .expect("default relational hydration byte budget is non-zero"),
            storage_residency_mode: skein_storage::StorageResidencyMode::Auto,
            auto_materialize_checkpoint_bytes:
                skein_storage::DEFAULT_AUTO_MATERIALIZE_CHECKPOINT_BYTES,
            max_out_of_core_delta_bytes: Some(skein_storage::DEFAULT_MAX_OUT_OF_CORE_DELTA_BYTES),
            graph_columnar_shadow_checkpoint: false,
            relational_index_mode: skein_storage::RelationalIndexMode::default(),
            relational_monotonic_append_fast_path: false,
            max_search_projection_change_log_entries: Some(
                DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_ENTRIES,
            ),
            max_search_projection_change_log_bytes: Some(
                DEFAULT_SEARCH_PROJECTION_CHANGE_LOG_MAX_BYTES,
            ),
            search_projection_relational_change_limits: Default::default(),
            max_plan_cache_entries: Some(DEFAULT_PLAN_CACHE_MAX_ENTRIES),
            slow_query_log_capacity: system_sql::DEFAULT_SLOW_QUERY_LOG_CAPACITY,
            slow_query_log_threshold_micros: system_sql::DEFAULT_SLOW_QUERY_LOG_THRESHOLD_MICROS,
            statement_summary_capacity: system_sql::DEFAULT_STATEMENT_SUMMARY_CAPACITY,
            runtime_capabilities: crate::compiled_runtime_capabilities(),
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            adaptive_vector_backend_policy: skein_optimizer::AdaptiveVectorBackendPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOutput {
    pub rows: executor::QueryRows,
}

#[cfg(test)]
pub(super) trait QueryRowLookup: Copy {
    fn get(&self, column: &str) -> Option<&Value>;
}

#[cfg(test)]
impl QueryRowLookup for &Row {
    fn get(&self, column: &str) -> Option<&Value> {
        (*self).get(column)
    }
}

#[cfg(test)]
impl QueryRowLookup for executor::QueryRowRef<'_> {
    fn get(&self, column: &str) -> Option<&Value> {
        (*self).get(column)
    }
}

impl QueryOutput {
    pub fn from_rows(rows: Vec<Row>) -> Self {
        Self { rows: rows.into() }
    }

    pub fn schema(&self) -> &executor::QuerySchema {
        self.rows.schema()
    }

    pub fn value_rows(&self) -> executor::QueryValueRows<'_> {
        self.rows.value_rows()
    }

    /// Returns the deterministic payload accounting used by query result
    /// admission. Container allocation overhead is intentionally excluded.
    pub fn payload_bytes(&self) -> usize {
        self.rows.payload_bytes()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryStreamOptions {
    pub max_rows: Option<usize>,
    pub max_payload_bytes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryStreamReport {
    pub fully_streamed: bool,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    pub execution_profile: executor::ReadExecutionProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryAccessControlContext {
    policy_epoch: u64,
    visibility_property: String,
    allowed_visibility_values: BTreeSet<String>,
}

impl QueryAccessControlContext {
    pub fn visibility_scope(
        policy_epoch: u64,
        visibility_property: impl Into<String>,
        allowed_visibility_value: impl Into<String>,
    ) -> Self {
        Self::visibility_scopes(
            policy_epoch,
            visibility_property,
            std::iter::once(allowed_visibility_value),
        )
    }

    pub fn visibility_scopes(
        policy_epoch: u64,
        visibility_property: impl Into<String>,
        allowed_visibility_values: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            policy_epoch,
            visibility_property: visibility_property.into(),
            allowed_visibility_values: allowed_visibility_values
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }

    pub fn policy_epoch(&self) -> u64 {
        self.policy_epoch
    }

    pub fn visibility_property(&self) -> &str {
        &self.visibility_property
    }

    pub fn allowed_visibility_values(&self) -> &BTreeSet<String> {
        &self.allowed_visibility_values
    }

    fn validate(&self) -> Result<()> {
        if self.policy_epoch == 0 {
            return Err(SkeinError::Semantic(
                "access control policy epoch must be non-zero".to_string(),
            ));
        }
        if self.visibility_property.trim().is_empty() {
            return Err(SkeinError::Semantic(
                "access control visibility property must be non-empty".to_string(),
            ));
        }
        if self.allowed_visibility_values.is_empty() {
            return Err(SkeinError::Semantic(
                "access control visibility scope must not be empty".to_string(),
            ));
        }
        if self
            .allowed_visibility_values
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(SkeinError::Semantic(
                "access control visibility scope values must be non-empty".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlowQueryLogExportOptions {
    pub include_query_text: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlowQueryLogRecordSummary {
    pub sequence: u64,
    pub query_language: String,
    pub statement_kind: String,
    pub query_digest: String,
    pub started_unix_micros: i64,
    pub elapsed_micros: i64,
    pub row_count: i64,
    pub success: bool,
    pub slow_log_candidate: bool,
    pub access_control_policy_epoch: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct StatementExecutionContext<'a> {
    execution_profile: Option<&'a executor::ReadExecutionProfile>,
    access_control: Option<&'a QueryAccessControlContext>,
    parse_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedReadQueryOutput {
    pub output: QueryOutput,
    pub execution_profile: executor::ReadExecutionProfile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeGraphStatement {
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeGraphExplainOutput {
    pub plan: String,
    pub trace: OptimizerTrace,
    pub work_request: WorkRequest,
    pub plan_cache_lookup: PlanCacheLookup,
    pub statement_kind: &'static str,
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
}

#[derive(Debug)]
pub(super) struct DatabaseTransactionRuntime {
    optimizer: CascadesOptimizer,
    plan_cache: SharedState<PlanCache>,
    optimizer_planning_cache: SharedState<OptimizerPlanningCache>,
    config: DatabaseConfig,
    system_variables: QuerySystemVariables,
}

#[derive(Debug)]
pub(super) struct DatabaseTransactionState {
    graph_transaction: Option<GraphMutationTransaction>,
    relational_transaction: skein_storage::RelationalTransaction,
    relational_state: skein_storage::RelationalState,
    append_transaction: skein_storage::AppendTransaction,
    append_state: skein_storage::AppendState,
    relational_index:
        std::result::Result<Option<crate::store::RelationalTransactionIndexView>, String>,
    relational_rows:
        std::result::Result<Option<crate::store::RelationalTransactionRowView>, String>,
}

pub(super) struct GraphTransactionStatementOutcome {
    pub(crate) output: QueryOutput,
    pub(crate) savepoint: Option<GraphMutationSavepoint>,
    pub(crate) lock_footprint: GraphMutationLockFootprint,
}

#[derive(Debug)]
pub struct DatabaseSession<'a> {
    db: &'a mut Database,
    graph_transaction: Option<GraphMutationTransaction>,
    transaction_runtime: Option<DatabaseTransactionRuntime>,
    system_variables: QuerySystemVariables,
}

#[derive(Debug)]
pub struct DatabaseReadTransaction {
    catalog: Catalog,
    store: GraphStore,
    published_read_view: PublishedReadView,
    optimizer: CascadesOptimizer,
    plan_cache: SharedState<PlanCache>,
    optimizer_planning_cache: SharedState<OptimizerPlanningCache>,
    slow_query_snapshot: Vec<system_sql::SlowQueryRecord>,
    statement_summary_snapshot: Vec<system_sql::StatementSummaryRecord>,
    config: DatabaseConfig,
    _pin: ReaderPin,
}

struct ReadStreamingExecutionContext<'a> {
    task_context: Option<&'a skein_core::RuntimeTaskContext>,
    external: &'a mut dyn executor::ExternalReadOperator,
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
        let mut store = GraphStore::default();
        configure_search_projection_changefeed(&mut store, &config);
        configure_relational_fast_paths(&mut store, &config);
        Self {
            catalog: Catalog::default(),
            store,
            optimizer: optimizer_from_database_config(&config),
            plan_cache: SharedState::new(PlanCache::new(config.max_plan_cache_entries)),
            optimizer_planning_cache: SharedState::new(OptimizerPlanningCache::default()),
            slow_query_log: SharedState::new(system_sql::SlowQueryLog::new(
                config.slow_query_log_capacity,
            )),
            statement_summary: SharedState::new(system_sql::StatementSummary::new(
                config.statement_summary_capacity,
            )),
            config,
            system_variables: QuerySystemVariables::default(),
            reader_pins: Arc::new(Mutex::new(ReaderPins::default())),
            next_derived_artifact_job_id: 1,
            derived_artifact_jobs: Vec::new(),
            telemetry: None,
        }
    }
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_concurrent(self) -> ConcurrentDatabase {
        ConcurrentDatabase::new(self)
    }

    pub fn new_with_config(config: DatabaseConfig) -> Self {
        let config = effective_database_config(config);
        let mut store = GraphStore::default();
        configure_search_projection_changefeed(&mut store, &config);
        configure_relational_fast_paths(&mut store, &config);
        let optimizer = optimizer_from_database_config(&config);
        Self {
            catalog: Catalog::default(),
            store,
            optimizer,
            plan_cache: SharedState::new(PlanCache::new(config.max_plan_cache_entries)),
            optimizer_planning_cache: SharedState::new(OptimizerPlanningCache::default()),
            slow_query_log: SharedState::new(system_sql::SlowQueryLog::new(
                config.slow_query_log_capacity,
            )),
            statement_summary: SharedState::new(system_sql::StatementSummary::new(
                config.statement_summary_capacity,
            )),
            config,
            system_variables: QuerySystemVariables::default(),
            reader_pins: Arc::new(Mutex::new(ReaderPins::default())),
            next_derived_artifact_job_id: 1,
            derived_artifact_jobs: Vec::new(),
            telemetry: None,
        }
    }

    pub fn commit_epoch(&self) -> u64 {
        self.store.commit_epoch()
    }

    pub fn published_read_view(&self) -> PublishedReadView {
        self.store.published_read_view()
    }

    pub(crate) fn begin_wal_sync_group(&mut self) -> Result<bool> {
        self.store.begin_wal_sync_group()
    }

    pub(crate) fn wal_sync_group_progress(&self) -> crate::store::WalSyncGroupProgress {
        self.store.wal_sync_group_progress()
    }

    pub(crate) fn finish_wal_sync_group(&mut self) -> Result<crate::store::WalSyncGroupFlush> {
        self.store.finish_wal_sync_group()
    }

    pub(crate) fn search_projection_changefeed_status(
        &self,
    ) -> skein_storage::SearchProjectionChangefeedStatus {
        self.store.search_projection_changefeed_status()
    }

    pub fn search_projection_changefeed_readiness(
        &self,
        search_index: &SearchIndex,
        require_restart_recoverable: bool,
        max_operations: Option<usize>,
    ) -> skein_storage::SearchProjectionChangefeedReadiness {
        let freshness = search_index.projection_freshness();
        self.store
            .search_projection_changefeed_status()
            .readiness_after(
                freshness.source_graph_commit_epoch,
                freshness.durable_source_graph_commit_epoch,
                require_restart_recoverable,
                max_operations,
            )
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_durability(path, DurabilityPolicy::default())
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: DatabaseConfig) -> Result<Self> {
        Self::open_with_durability_and_config(path, DurabilityPolicy::default(), config)
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        durability: DurabilityPolicy,
    ) -> Result<Self> {
        Self::open_with_durability_and_config(path, durability, DatabaseConfig::default())
    }

    pub fn open_with_durability_and_config(
        path: impl AsRef<Path>,
        durability: DurabilityPolicy,
        config: DatabaseConfig,
    ) -> Result<Self> {
        let config = effective_database_config(config);
        let mut catalog = Catalog::default();
        let replay_config = WalReplayConfig {
            recovery_mode: config.recovery_mode,
            max_entries: config.max_wal_replay_entries,
            max_bytes: config.max_wal_replay_bytes,
            max_record_bytes: config.max_wal_record_bytes,
            max_batch_operations: config.max_wal_batch_operations,
            max_checkpoint_encoded_bytes: config.max_checkpoint_encoded_bytes,
            max_checkpoint_decoded_bytes: config.max_checkpoint_decoded_bytes,
            segment_cache_capacity_bytes: config.segment_cache_capacity_bytes,
            max_graph_manifest_open_bytes: config.max_graph_manifest_open_bytes,
            residency_mode: config.storage_residency_mode,
            auto_materialize_checkpoint_bytes: config.auto_materialize_checkpoint_bytes,
            max_out_of_core_delta_bytes: config.max_out_of_core_delta_bytes,
            graph_columnar_shadow_checkpoint: config.graph_columnar_shadow_checkpoint,
            relational_index_mode: config.relational_index_mode,
        };
        let mut store = if config.read_only {
            GraphStore::open_read_only_with_durability_and_replay_config(
                path,
                &mut catalog,
                durability,
                replay_config,
            )?
        } else {
            GraphStore::open_with_durability_and_replay_config(
                path,
                &mut catalog,
                durability,
                replay_config,
            )?
        };
        configure_search_projection_changefeed(&mut store, &config);
        configure_relational_fast_paths(&mut store, &config);
        let mut database = Self {
            catalog,
            store,
            optimizer: optimizer_from_database_config(&config),
            plan_cache: SharedState::new(PlanCache::new(config.max_plan_cache_entries)),
            optimizer_planning_cache: SharedState::new(OptimizerPlanningCache::default()),
            slow_query_log: SharedState::new(system_sql::SlowQueryLog::new(
                config.slow_query_log_capacity,
            )),
            statement_summary: SharedState::new(system_sql::StatementSummary::new(
                config.statement_summary_capacity,
            )),
            config,
            system_variables: QuerySystemVariables::default(),
            reader_pins: Arc::new(Mutex::new(ReaderPins::default())),
            next_derived_artifact_job_id: 1,
            derived_artifact_jobs: Vec::new(),
            telemetry: None,
        };
        if !database.config.read_only {
            database.complete_required_relational_row_checkpoint("writable recovery")?;
        }
        database.apply_engine_system_schema()?;
        Ok(database)
    }

    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    pub(crate) fn runtime_capabilities(&self) -> skein_core::RuntimeCapabilities {
        self.config.runtime_capabilities
    }

    pub(super) fn ensure_runtime_capability(
        &self,
        capability: skein_core::RuntimeCapability,
    ) -> Result<()> {
        self.config.runtime_capabilities.require(capability)
    }

    pub fn set_telemetry_sink(&mut self, telemetry: Option<Arc<dyn TelemetrySink>>) {
        if let Some(telemetry) = &telemetry {
            let recovery = self.store.storage_recovery_report();
            if recovery.durable {
                telemetry.record_kernel(KernelTelemetry {
                    operation: KernelTelemetryOperation::Recovery,
                    success: true,
                    elapsed_micros: 0,
                    item_count: recovery.replayed_wal_entries,
                    byte_count: recovery.replayed_wal_bytes,
                    fsync_micros: 0,
                    generation: recovery.wal_generation,
                });
            }
        }
        self.store.set_telemetry_sink(telemetry.clone());
        self.telemetry = telemetry;
    }

    pub fn telemetry_sink_configured(&self) -> bool {
        self.telemetry.is_some()
    }

    pub fn operations_telemetry_readiness(
        &self,
        search_index: Option<&SearchIndex>,
    ) -> OperationsTelemetryReadiness {
        operations_telemetry_readiness(
            self.telemetry_sink_configured(),
            search_index
                .map(SearchIndex::telemetry_sink_configured)
                .unwrap_or(false),
        )
    }

    fn configure_qos_scheduler_telemetry(&self, scheduler: &mut LocalQosScheduler) {
        if let Some(telemetry) = &self.telemetry {
            scheduler.set_telemetry_sink(Some(qos_telemetry_sink(telemetry.clone())));
        }
    }

    pub fn system_variables(&self) -> &QuerySystemVariables {
        &self.system_variables
    }

    #[cfg(test)]
    fn query_read_only_with_params_bounded(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
    ) -> Result<QueryOutput> {
        let started = std::time::Instant::now();
        let statement = cypher::parse(cypher_text)?;
        let body = statement_body(&statement);
        let query_result = (|| {
            if matches!(body, cypher::Statement::Checkpoint) {
                reject_transaction_control_parameters("CHECKPOINT", parameters)?;
                return Err(SkeinError::Execution(
                    "CHECKPOINT is not allowed inside a read-only query runtime".to_string(),
                ));
            }
            if matches!(body, cypher::Statement::SetSystemVariable(_)) {
                reject_system_variable_parameters(parameters)?;
                return Err(SkeinError::Execution(
                    "SET system variable is not allowed inside a read-only query runtime"
                        .to_string(),
                ));
            }
            query_work_request_for_statement(&QuerySystemVariables::default(), &statement)?;
            let optimized = self.optimized_query_plan(cypher_text, &statement, parameters)?;
            if executor::is_mutation_plan(&optimized.physical_plan)? {
                return Err(SkeinError::Execution(
                    "read-only query runtime must not execute a mutation".to_string(),
                ));
            }
            let mut catalog = self.catalog.clone();
            let mut store = self.store.snapshot();
            let rows = executor::execute_with_row_limit(
                &optimized.physical_plan,
                &mut catalog,
                &mut store,
                max_rows,
            )?;
            Ok(QueryOutput { rows: rows.into() })
        })();
        self.store.poison_on_storage_error(&query_result);
        self.record_statement_execution(
            "cypher",
            cypher_text,
            statement_kind(body),
            started,
            query_result.as_ref(),
            StatementExecutionContext::default(),
        );
        query_result
    }

    pub fn begin_transaction(&mut self) -> DatabaseTransaction<'_> {
        let runtime = DatabaseTransactionRuntime::from_database(self);
        let state = DatabaseTransactionState::from_database(self);
        DatabaseTransaction {
            db: self,
            runtime,
            state,
        }
    }

    pub fn session(&mut self) -> DatabaseSession<'_> {
        let system_variables = self.system_variables.clone();
        DatabaseSession {
            db: self,
            graph_transaction: None,
            transaction_runtime: None,
            system_variables,
        }
    }

    pub fn begin_read_transaction(&self) -> DatabaseReadTransaction {
        let published_read_view = self.store.published_read_view();
        let pin = {
            let mut pins = self
                .reader_pins
                .lock()
                .expect("database reader pins lock should not be poisoned");
            let id = pins.next_reader_id;
            pins.next_reader_id += 1;
            pins.active_views.insert(id, published_read_view);
            ReaderPin::new(id, Arc::clone(&self.reader_pins))
        };
        DatabaseReadTransaction {
            catalog: self.catalog.clone(),
            store: self.store.snapshot(),
            published_read_view,
            optimizer: self.optimizer.clone(),
            plan_cache: SharedState::new(PlanCache::new(self.config.max_plan_cache_entries)),
            optimizer_planning_cache: SharedState::new(
                self.optimizer_planning_cache.borrow().clone(),
            ),
            slow_query_snapshot: self.slow_query_log.borrow().snapshot(),
            statement_summary_snapshot: self.statement_summary.borrow().snapshot(),
            config: self.config.clone(),
            _pin: pin,
        }
    }

    pub fn explain_query(&self, cypher_text: &str) -> Result<ExplainOutput> {
        self.explain_query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn explain_query_with_params(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<ExplainOutput> {
        self.explain_query_with_params_access_control_internal(cypher_text, parameters, None)
    }

    pub fn explain_query_with_params_access_control(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: QueryAccessControlContext,
    ) -> Result<ExplainOutput> {
        self.explain_query_with_params_access_control_internal(
            cypher_text,
            parameters,
            Some(access_control),
        )
    }

    fn explain_query_with_params_access_control_internal(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: Option<QueryAccessControlContext>,
    ) -> Result<ExplainOutput> {
        self.store.ensure_usable()?;
        let statement = cypher::parse(cypher_text)?;
        let work_request = query_work_request_for_statement(&self.system_variables, &statement)?;
        let optimized = self.optimized_query_plan_with_access_control(
            cypher_text,
            &statement,
            parameters,
            access_control.as_ref(),
        )?;
        Ok(ExplainOutput {
            physical_plan: optimized.physical_plan,
            trace: optimized.trace,
            work_request,
            plan_cache_lookup: optimized.plan_cache_lookup,
            statement_kind: statement_kind(statement_body(&statement)),
        })
    }

    pub fn explain_analyze_query(&mut self, cypher_text: &str) -> Result<ExplainAnalyzeOutput> {
        self.explain_analyze_query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn explain_analyze_query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<ExplainAnalyzeOutput> {
        self.explain_analyze_query_with_params_access_control_internal(
            cypher_text,
            parameters,
            None,
        )
    }

    pub fn explain_analyze_query_with_params_access_control(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: QueryAccessControlContext,
    ) -> Result<ExplainAnalyzeOutput> {
        self.explain_analyze_query_with_params_access_control_internal(
            cypher_text,
            parameters,
            Some(access_control),
        )
    }

    fn explain_analyze_query_with_params_access_control_internal(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: Option<QueryAccessControlContext>,
    ) -> Result<ExplainAnalyzeOutput> {
        self.store.ensure_usable()?;
        let statement = cypher::parse(cypher_text)?;
        let work_request = query_work_request_for_statement(&self.system_variables, &statement)?;
        let optimized = self.optimized_query_plan_with_access_control(
            cypher_text,
            &statement,
            parameters,
            access_control.as_ref(),
        )?;
        if executor::is_mutation_plan(&optimized.physical_plan)? {
            return Err(SkeinError::Execution(
                "EXPLAIN ANALYZE only supports read queries".to_string(),
            ));
        }
        let mut external = executor::NoExternalReadOperator;
        let profiled = executor::execute_with_output_limits_profile_and_external_and_memory(
            &optimized.physical_plan,
            &mut self.catalog,
            &mut self.store,
            parameters,
            &mut external,
            self.config.max_read_result_rows,
            self.config.max_read_result_payload_bytes,
            &self.config.execution_memory,
        );
        self.store.poison_on_storage_error(&profiled);
        let profiled = profiled?;
        Ok(ExplainAnalyzeOutput {
            output: QueryOutput {
                rows: profiled.rows,
            },
            execution_profile: profiled.profile,
            physical_plan: optimized.physical_plan,
            trace: optimized.trace,
            work_request,
            plan_cache_lookup: optimized.plan_cache_lookup,
            statement_kind: statement_kind(statement_body(&statement)),
        })
    }

    pub fn plan_cache_stats(&self) -> PlanCacheStats {
        self.plan_cache.borrow().stats()
    }

    fn optimized_query_plan(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<OptimizedQueryPlan> {
        self.optimized_query_plan_with_access_control(cypher_text, statement, parameters, None)
    }

    fn optimized_query_plan_with_access_control(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
        access_control: Option<&QueryAccessControlContext>,
    ) -> Result<OptimizedQueryPlan> {
        let optimizer_search =
            query_statement_variables_for_statement(&self.system_variables, statement)?
                .optimizer_search;
        let cache_mode = if optimizer_search != OptimizerSearchDirective::Auto {
            PlanCacheMode::Bypass(PlanCacheBypassReason::OptimizerDirective)
        } else if statement_uses_plan_cache(statement) {
            PlanCacheMode::Use
        } else {
            PlanCacheMode::Bypass(PlanCacheBypassReason::StatementNotCacheable)
        };
        optimized_query_plan_for(
            cypher_text,
            statement,
            parameters,
            cache_mode,
            PlanCacheContext {
                catalog: &self.catalog,
                store: &self.store,
                optimizer: &self.optimizer,
                config: &self.config,
                cache: &self.plan_cache,
                planning_cache: &self.optimizer_planning_cache,
                access_control,
                optimizer_search,
            },
        )
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        self.checkpoint_internal(None)
    }

    pub fn append_transaction(&mut self, transaction: AppendTransaction) -> Result<u64> {
        self.ensure_writable()?;
        self.store.commit_kernel_write_batch(
            &mut self.catalog,
            KernelWriteBatch {
                append: transaction,
                ..KernelWriteBatch::default()
            },
            self.config.mutation_limits,
        )?;
        Ok(self.store.commit_epoch())
    }

    pub fn commit_kernel_write_batch(
        &mut self,
        batch: KernelWriteBatch,
    ) -> Result<MutationSummary> {
        self.ensure_writable()?;
        self.store
            .commit_kernel_write_batch(&mut self.catalog, batch, self.config.mutation_limits)
    }

    pub fn read_append_partition(
        &self,
        table: &str,
        partition: &skein_storage::RelationalKey,
        after: Option<&skein_storage::RelationalKey>,
        max_rows: usize,
    ) -> Result<AppendSegmentReadOutput> {
        let max_rows = self
            .config
            .max_read_result_rows
            .map_or(max_rows, |configured| configured.min(max_rows));
        let max_payload_bytes = self
            .config
            .max_read_result_payload_bytes
            .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES);
        self.read_append_partition_bounded(table, partition, after, max_rows, max_payload_bytes)
    }

    pub fn read_append_partition_bounded(
        &self,
        table: &str,
        partition: &skein_storage::RelationalKey,
        after: Option<&skein_storage::RelationalKey>,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> Result<AppendSegmentReadOutput> {
        let max_rows = self
            .config
            .max_read_result_rows
            .map_or(max_rows, |configured| configured.min(max_rows));
        let max_payload_bytes = self
            .config
            .max_read_result_payload_bytes
            .map_or(max_payload_bytes, |configured| {
                configured.min(max_payload_bytes)
            });
        self.store.read_append_partition_bounded(
            table,
            partition,
            after,
            max_rows,
            max_payload_bytes,
        )
    }

    pub fn append_table_schema(&self, table: &str) -> Option<&AppendTableSchema> {
        self.store.append_table_schema(table)
    }

    pub fn append_storage_residency_report(&self) -> crate::AppendStorageResidencyReport {
        self.store.append_storage_residency_report()
    }

    /// Runs an explicitly admitted full relational-row closure scan and
    /// publishes an exact overflow root through the ordinary manifest-last
    /// checkpoint boundary. Large overflow payloads are not hydrated.
    pub fn compact_relational_overflow(
        &mut self,
        config: crate::store::RelationalOverflowCompactionConfig,
    ) -> Result<crate::store::RelationalOverflowCompactionReport> {
        self.compact_relational_overflow_context(config, &skein_core::RuntimeTaskContext::default())
    }

    pub fn compact_relational_overflow_context(
        &mut self,
        config: crate::store::RelationalOverflowCompactionConfig,
        task: &skein_core::RuntimeTaskContext,
    ) -> Result<crate::store::RelationalOverflowCompactionReport> {
        self.ensure_writable()?;
        let oldest_reader_epoch = self
            .reader_pins
            .lock()
            .expect("database reader pins lock should not be poisoned")
            .oldest_epoch();
        self.store
            .compact_relational_overflow(&self.catalog, oldest_reader_epoch, config, task)
    }

    /// Checkpoint entry carrying an explicit pre-admitted columnar-shadow
    /// context, for callers that already hold a governor permit and
    /// extended it by [`Database::columnar_shadow_admission_bytes`]. The
    /// shadow build then draws only against the passed token — it never
    /// touches the governor, so nested admission cannot deadlock a
    /// constrained configuration.
    pub(crate) fn checkpoint_with_shadow_admission(
        &mut self,
        shadow_admission: crate::store::ColumnarShadowAdmission,
    ) -> Result<()> {
        self.checkpoint_internal(Some(shadow_admission))
    }

    /// The builder-lifetime byte reservation one shadow build needs; zero
    /// when `graph_columnar_shadow_checkpoint` is off.
    pub fn columnar_shadow_admission_bytes(&self) -> u64 {
        self.store.columnar_shadow_admission_bytes()
    }

    fn checkpoint_internal(
        &mut self,
        shadow_admission: Option<crate::store::ColumnarShadowAdmission>,
    ) -> Result<()> {
        self.ensure_writable()?;
        let started = std::time::Instant::now();
        let durable = self.store.storage_recovery_report().durable;
        let prepared = self.checkpoint_source()?.prepare()?;
        let result = match prepared {
            Some(prepared) => {
                let oldest_reader_epoch = self
                    .reader_pins
                    .lock()
                    .expect("database reader pins lock should not be poisoned")
                    .oldest_epoch();
                self.store
                    .publish_prepared_checkpoint_with_shadow_admission(
                        prepared,
                        oldest_reader_epoch,
                        shadow_admission,
                    )
            }
            None => Ok(()),
        };
        if durable && let Some(telemetry) = &self.telemetry {
            telemetry.record_kernel(KernelTelemetry {
                operation: KernelTelemetryOperation::Checkpoint,
                success: result.is_ok(),
                elapsed_micros: elapsed_micros(started),
                item_count: 1,
                byte_count: 0,
                fsync_micros: 0,
                generation: self.storage_reclamation_watermark().checkpoint_epoch,
            });
        }
        result
    }

    fn complete_required_relational_row_checkpoint(&mut self, context: &str) -> Result<()> {
        if !self.relational_row_schema_checkpoint_required() {
            return Ok(());
        }
        if self.store.wal_sync_group_active() {
            return Ok(());
        }
        let checkpoint = self.checkpoint_internal(None);
        let result = match checkpoint {
            Ok(()) if !self.relational_row_schema_checkpoint_required() => Ok(()),
            Ok(()) => Err(SkeinError::StorageIntegrity(format!(
                "canonical relational row schema checkpoint remained required after {context}"
            ))),
            Err(error) => Err(SkeinError::StorageIntegrity(format!(
                "canonical relational row schema checkpoint failed after {context}; the durable WAL remains authoritative and writable reopen will retry: {error}"
            ))),
        };
        self.store.poison_on_storage_error(&result);
        result
    }

    fn relational_row_schema_checkpoint_required(&self) -> bool {
        self.store.relational_row_schema_checkpoint_required()
    }

    pub(crate) fn checkpoint_source(&self) -> Result<DatabaseCheckpointSource> {
        self.ensure_writable()?;
        Ok(DatabaseCheckpointSource {
            catalog: self.catalog.clone(),
            store: self.store.checkpoint_source(),
        })
    }

    pub(crate) fn publish_prepared_checkpoint(
        &mut self,
        prepared: PreparedCheckpoint,
    ) -> Result<()> {
        let oldest_reader_epoch = self
            .reader_pins
            .lock()
            .expect("database reader pins lock should not be poisoned")
            .oldest_epoch();
        self.store
            .publish_prepared_checkpoint(prepared, oldest_reader_epoch)
    }

    pub fn backup_to(&mut self, destination: impl AsRef<Path>) -> Result<StorageBackupReport> {
        self.ensure_writable()?;
        self.store.backup_to(&self.catalog, destination)
    }

    pub fn scrub_storage(&mut self) -> Result<StorageScrubReport> {
        self.store.scrub_storage()
    }

    pub fn restore_backup(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<StorageRestoreReport> {
        restore_storage_backup(backup, destination)
    }

    pub fn storage_reclamation_watermark(&self) -> StorageReclamationWatermark {
        let oldest_reader_epoch = self
            .reader_pins
            .lock()
            .expect("database reader pins lock should not be poisoned")
            .oldest_epoch();
        self.store
            .storage_reclamation_watermark(oldest_reader_epoch)
    }

    pub fn storage_recovery_report(&self) -> StorageRecoveryReport {
        self.store.storage_recovery_report()
    }

    pub fn storage_handle_poisoned(&self) -> bool {
        self.store.storage_handle_poisoned()
    }

    pub fn storage_residency_report(&self) -> crate::store::StorageResidencyReport {
        self.store.storage_residency_report()
    }

    /// Threads the engine's runtime governor into the storage layer so
    /// background columnar-shadow work can request admission. Called by the
    /// embedding layers that own the governor (`SkeinEmbedded`,
    /// `NowledgeMemGraph`); a second governor is never constructed here.
    pub fn set_runtime_governor(&mut self, governor: skein_qos::RuntimeGovernor) {
        self.store.set_runtime_governor(governor);
    }

    /// Shadow write-amplification evidence of the most recent checkpoint,
    /// `None` while `graph_columnar_shadow_checkpoint` is off or before the first
    /// derived shadow checkpoint.
    pub fn columnar_shadow_checkpoint_report(
        &self,
    ) -> Option<crate::store::ColumnarShadowCheckpointReport> {
        self.store.columnar_shadow_checkpoint_report()
    }

    /// What recovery observed about the columnar shadow catalog.
    pub fn columnar_shadow_recovery_status(&self) -> crate::store::ColumnarShadowRecoveryStatus {
        self.store.columnar_shadow_recovery_status()
    }

    /// Publication evidence for the derived relational index-page store.
    /// `None` while publication is disabled or before its first checkpoint
    /// attempt.
    pub fn relational_index_shadow_checkpoint_report(
        &self,
    ) -> Option<&crate::store::RelationalIndexShadowCheckpointReport> {
        self.store.relational_index_shadow_checkpoint_report()
    }

    /// Recovery's bounded manifest-only assessment of the relational index
    /// store. Selected pages remain lazily validated on first SQL access.
    pub fn relational_index_shadow_recovery_status(
        &self,
    ) -> &crate::store::RelationalIndexShadowRecoveryStatus {
        self.store.relational_index_shadow_recovery_status()
    }

    /// Resource and publication evidence for WAL index deltas derived during
    /// the most recent open.
    pub fn relational_index_recovery_report(
        &self,
    ) -> Option<&skein_storage::RelationalIndexRecoveryReport> {
        self.store.relational_index_recovery_report()
    }

    pub fn storage_pressure_snapshot(&self) -> StoragePressureSnapshot {
        let oldest_reader_epoch = self
            .reader_pins
            .lock()
            .expect("database reader pins lock should not be poisoned")
            .oldest_epoch();
        self.store.storage_pressure_snapshot(oldest_reader_epoch)
    }

    pub fn segment_cache_snapshot(&self) -> Option<SegmentCacheSnapshot> {
        self.store.segment_cache_snapshot()
    }

    pub fn export_canonical_graph_snapshot(&self) -> CanonicalGraphSnapshotExport {
        export_canonical_graph_snapshot_for(&self.catalog, &self.store)
    }

    pub fn try_export_canonical_graph_snapshot(&self) -> Result<CanonicalGraphSnapshotExport> {
        canonical_snapshot::try_export_canonical_graph_snapshot_for(&self.catalog, &self.store)
    }

    pub fn export_canonical_graph_snapshot_with_persisted_stable_ids(
        &mut self,
    ) -> Result<CanonicalGraphSnapshotExport> {
        self.ensure_writable()?;
        let snapshot = self.try_export_canonical_graph_snapshot()?;
        let duplicate_node_stable_ids = snapshot
            .stable_identity
            .duplicate_node_stable_ids
            .iter()
            .collect::<BTreeSet<_>>();
        let duplicate_relationship_stable_ids = snapshot
            .stable_identity
            .duplicate_relationship_stable_ids
            .iter()
            .collect::<BTreeSet<_>>();
        let required_node_ids = snapshot
            .nodes
            .iter()
            .filter(|node| {
                node.stable_id
                    .as_ref()
                    .is_none_or(|stable_id| duplicate_node_stable_ids.contains(stable_id))
            })
            .map(|node| NodeId(node.node_id))
            .collect::<BTreeSet<_>>();
        let required_relationship_ids = snapshot
            .relationships
            .iter()
            .filter(|relationship| {
                relationship
                    .stable_id
                    .as_ref()
                    .is_none_or(|stable_id| duplicate_relationship_stable_ids.contains(stable_id))
            })
            .map(|relationship| RelId(relationship.relationship_id))
            .collect::<BTreeSet<_>>();
        let mapping = CanonicalStableIdMapping::from(
            self.store
                .ensure_stable_id_mapping(&required_node_ids, &required_relationship_ids)?,
        );
        Ok(snapshot.with_stable_id_mapping(&mapping))
    }

    pub fn prepare_skein_lightning_bootstrap_export(
        &mut self,
    ) -> Result<SkeinLightningBootstrapExport> {
        let snapshot = self.export_canonical_graph_snapshot_with_persisted_stable_ids()?;
        let relational_state = self.skein_lightning_relational_state()?;
        let relational_stream = SkeinLightningRelationalStream::from_state(
            self.store.commit_epoch(),
            &relational_state,
        )?;
        let manifest = snapshot.skein_lightning_bootstrap_manifest(&relational_stream);
        let graph_stream = snapshot.skein_lightning_graph_stream();
        Ok(SkeinLightningBootstrapExport {
            snapshot,
            manifest,
            graph_stream,
            relational_stream,
        })
    }

    pub fn skein_lightning_initial_import_readiness(
        &self,
        manifest: &SkeinLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
    ) -> SkeinLightningInitialImportReadiness {
        skein_lightning_initial_import_readiness(
            manifest,
            self.store.commit_epoch(),
            projection_freshness,
        )
    }

    pub fn skein_lightning_initial_import_cutover_catch_up_report(
        &self,
        session: &SkeinLightningInitialImportSessionReport,
        live_projection_freshness: Option<&SearchProjectionFreshness>,
    ) -> SkeinLightningInitialImportCutoverCatchUpReport {
        skein_lightning_initial_import_cutover_catch_up_report(
            session,
            self.store.commit_epoch(),
            live_projection_freshness,
        )
    }

    pub fn skein_lightning_initial_import_session_bundle_readiness(
        &self,
        source_bundle: &SkeinLightningInitialImportSourceBundleReadiness,
        session: &SkeinLightningInitialImportSessionReport,
        catch_up: Option<&SkeinLightningInitialImportCutoverCatchUpReport>,
    ) -> SkeinLightningInitialImportSessionBundleReadiness {
        skein_lightning_initial_import_session_bundle_readiness(source_bundle, session, catch_up)
    }

    pub fn skein_lightning_initial_import_decode_durable_state(
        &self,
        manifest: &SkeinLightningBootstrapManifest,
        raw: &str,
    ) -> Result<SkeinLightningInitialImportDurableStateCodecReport> {
        skein_lightning_initial_import_decode_durable_state(manifest, raw)
    }

    pub fn skein_lightning_initial_import_startup_readiness(
        &self,
        inputs: SkeinLightningInitialImportReadinessInputs<'_>,
        durable_state: Option<&SkeinLightningInitialImportDurableState>,
    ) -> SkeinLightningInitialImportStartupReadinessReport {
        skein_lightning_initial_import_startup_readiness(
            inputs,
            self.store.commit_epoch(),
            durable_state,
        )
    }

    pub fn skein_lightning_initial_import_recovery_readiness(
        &self,
        inputs: SkeinLightningInitialImportReadinessInputs<'_>,
        durable_state_payload: Option<&str>,
    ) -> SkeinLightningInitialImportRecoveryReadinessReport {
        skein_lightning_initial_import_recovery_readiness(
            inputs,
            self.store.commit_epoch(),
            durable_state_payload,
        )
    }

    pub fn skein_lightning_initial_import_plan(
        &self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &SkeinLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&SkeinLightningInitialImportCheckpoint>,
    ) -> SkeinLightningInitialImportPlan {
        skein_lightning_initial_import_plan(
            encoded_graph_stream,
            encoded_relational_stream,
            manifest,
            self.store.commit_epoch(),
            projection_freshness,
            checkpoint,
        )
    }

    pub fn skein_lightning_initial_import_plan_with_document_identities(
        &self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &SkeinLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&SkeinLightningInitialImportCheckpoint>,
        document_identities: &[SkeinLightningInitialImportDocumentIdentity],
    ) -> SkeinLightningInitialImportPlan {
        skein_lightning_initial_import_plan_with_document_identities(
            encoded_graph_stream,
            encoded_relational_stream,
            manifest,
            self.store.commit_epoch(),
            projection_freshness,
            checkpoint,
            Some(document_identities),
        )
    }

    pub fn skein_lightning_initial_import_apply(
        &mut self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &SkeinLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&SkeinLightningInitialImportCheckpoint>,
    ) -> Result<SkeinLightningInitialImportApplyReport> {
        self.skein_lightning_initial_import_apply_internal(
            encoded_graph_stream,
            encoded_relational_stream,
            manifest,
            projection_freshness,
            checkpoint,
            None,
        )
    }

    pub fn skein_lightning_initial_import_apply_with_document_identities(
        &mut self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &SkeinLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&SkeinLightningInitialImportCheckpoint>,
        document_identities: &[SkeinLightningInitialImportDocumentIdentity],
    ) -> Result<SkeinLightningInitialImportApplyReport> {
        self.skein_lightning_initial_import_apply_internal(
            encoded_graph_stream,
            encoded_relational_stream,
            manifest,
            projection_freshness,
            checkpoint,
            Some(document_identities),
        )
    }

    fn skein_lightning_initial_import_apply_internal(
        &mut self,
        encoded_graph_stream: &str,
        encoded_relational_stream: &[u8],
        manifest: &SkeinLightningBootstrapManifest,
        projection_freshness: Option<&SearchProjectionFreshness>,
        checkpoint: Option<&SkeinLightningInitialImportCheckpoint>,
        document_identities: Option<&[SkeinLightningInitialImportDocumentIdentity]>,
    ) -> Result<SkeinLightningInitialImportApplyReport> {
        self.ensure_writable()?;
        let mut blocker_codes = BTreeSet::new();
        let plan = skein_lightning_initial_import_plan_with_document_identities(
            encoded_graph_stream,
            encoded_relational_stream,
            manifest,
            self.store.commit_epoch(),
            projection_freshness,
            checkpoint,
            document_identities,
        );
        if !plan.ready_for_database_import {
            blocker_codes.insert("skein_lightning_database_streams_not_import_ready".to_string());
        }
        let source_fingerprint = skein_lightning_initial_import_source_fingerprint_key(manifest);
        if let Some(imported_source_fingerprint) = self.store.initial_import_source_fingerprint() {
            if imported_source_fingerprint == source_fingerprint && plan.ready_for_database_import {
                let (relational_table_count, relational_row_count) =
                    relational_state_counts(self.store.relational_state());
                return Ok(SkeinLightningInitialImportApplyReport {
                    applied: false,
                    ready_for_cutover: plan.ready_for_cutover,
                    database_commit_epoch: self.store.commit_epoch(),
                    node_count: self.store.basic_statistics().node_count as usize,
                    relationship_count: self.store.basic_statistics().relationship_count as usize,
                    relational_table_count,
                    relational_row_count,
                    plan,
                    blocker_codes: Vec::new(),
                });
            }
            if imported_source_fingerprint != source_fingerprint {
                blocker_codes.insert(
                    "skein_lightning_initial_import_source_fingerprint_mismatch".to_string(),
                );
            }
        }
        let statistics = self.store.basic_statistics();
        let target_has_only_engine_bootstrap = self.has_only_engine_system_schema_bootstrap()?;
        let relational_target_empty =
            self.store.relational_state().is_empty() || target_has_only_engine_bootstrap;
        let target_empty = statistics.node_count == 0
            && statistics.relationship_count == 0
            && relational_target_empty
            && self.catalog.is_empty();
        if !target_empty {
            blocker_codes.insert("skein_lightning_initial_import_target_not_empty".to_string());
        }
        let snapshot = if blocker_codes.is_empty() {
            Some(parse_skein_lightning_graph_stream_export(
                encoded_graph_stream,
                Some(manifest),
            )?)
        } else {
            None
        };
        let relational_state = if blocker_codes.is_empty() {
            Some(
                skein_storage::decode_relational_checkpoint(
                    encoded_relational_stream,
                    skein_storage::RelationalDecodeLimits::checkpoint(),
                )
                .map_err(|error| SkeinError::Storage(error.to_string()))?
                .state,
            )
        } else {
            None
        };
        if let Some(snapshot) = &snapshot {
            for node in &snapshot.nodes {
                if node.labels.len() != 1 {
                    blocker_codes.insert(
                        "skein_lightning_initial_import_multi_label_node_unsupported".to_string(),
                    );
                    break;
                }
            }
        }
        if !blocker_codes.is_empty() {
            let (relational_table_count, relational_row_count) =
                relational_state_counts(self.store.relational_state());
            return Ok(SkeinLightningInitialImportApplyReport {
                applied: false,
                ready_for_cutover: false,
                database_commit_epoch: self.store.commit_epoch(),
                node_count: self.store.basic_statistics().node_count as usize,
                relationship_count: self.store.basic_statistics().relationship_count as usize,
                relational_table_count,
                relational_row_count,
                plan,
                blocker_codes: blocker_codes.into_iter().collect(),
            });
        }
        let snapshot = snapshot.expect("snapshot should be available without import blockers");
        let relational_state =
            relational_state.expect("relational state should be available without import blockers");
        Self::validate_skein_lightning_system_schema(&relational_state)?;
        let stable_id_mapping = StoreStableIdMapping {
            node_stable_ids: snapshot
                .nodes
                .iter()
                .filter_map(|node| {
                    node.stable_id
                        .as_ref()
                        .map(|stable_id| (NodeId(node.node_id), stable_id.clone()))
                })
                .collect(),
            relationship_stable_ids: snapshot
                .relationships
                .iter()
                .filter_map(|relationship| {
                    relationship
                        .stable_id
                        .as_ref()
                        .map(|stable_id| (RelId(relationship.relationship_id), stable_id.clone()))
                })
                .collect(),
        };
        let node_rows = snapshot
            .nodes
            .iter()
            .map(|node| {
                Ok((
                    NodeId(node.node_id),
                    single_import_label(node)?,
                    node.properties.clone(),
                ))
            })
            .collect::<Result<Vec<GraphSnapshotNodeImport>>>()?;
        let relationship_rows = snapshot
            .relationships
            .iter()
            .map(|relationship| {
                Ok((
                    RelId(relationship.relationship_id),
                    NodeId(relationship.source_node_id),
                    NodeId(relationship.target_node_id),
                    relationship.rel_type.clone(),
                    relationship.properties.clone(),
                ))
            })
            .collect::<Result<Vec<GraphSnapshotRelationshipImport>>>()?;
        self.store
            .import_skein_snapshot_rows_with_source_fingerprint(
                &mut self.catalog,
                SkeinSnapshotRowsImport {
                    stable_id_mapping,
                    source_fingerprint,
                    nodes: node_rows,
                    relationships: relationship_rows,
                    relational_state,
                    target_has_only_engine_bootstrap,
                },
            )?;
        let updated_plan = skein_lightning_initial_import_plan_with_document_identities(
            encoded_graph_stream,
            encoded_relational_stream,
            manifest,
            self.store.commit_epoch(),
            projection_freshness,
            checkpoint,
            document_identities,
        );
        let (relational_table_count, relational_row_count) =
            relational_state_counts(self.store.relational_state());
        Ok(SkeinLightningInitialImportApplyReport {
            applied: true,
            ready_for_cutover: updated_plan.ready_for_cutover,
            database_commit_epoch: self.store.commit_epoch(),
            node_count: self.store.basic_statistics().node_count as usize,
            relationship_count: self.store.basic_statistics().relationship_count as usize,
            relational_table_count,
            relational_row_count,
            plan: updated_plan,
            blocker_codes: Vec::new(),
        })
    }

    pub fn skein_lightning_bootstrap_export_background_work_plan(
        &self,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let estimated_operations = self.skein_lightning_bootstrap_export_estimated_operations();
        if estimated_operations == 0 {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Import,
            estimated_operations,
            hint,
        ))
    }

    pub fn prepare_background_skein_lightning_bootstrap_export(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
    ) -> Result<SkeinLightningBootstrapExport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations = self.skein_lightning_bootstrap_export_estimated_operations();
        if estimated_operations == 0 {
            return self.prepare_skein_lightning_bootstrap_export();
        }
        let request = WorkRequest::background(WorkClass::Import, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.prepare_skein_lightning_bootstrap_export(),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background Skein Lightning bootstrap export deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background Skein Lightning bootstrap export rejected: {reason}"
            ))),
        }
    }

    pub fn prepare_scheduled_background_skein_lightning_bootstrap_export(
        &mut self,
        scheduler: &mut LocalQosScheduler,
    ) -> Result<SkeinLightningBootstrapExport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations = self.skein_lightning_bootstrap_export_estimated_operations();
        if estimated_operations == 0 {
            return self.prepare_skein_lightning_bootstrap_export();
        }
        self.configure_qos_scheduler_telemetry(scheduler);
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Import,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background Skein Lightning bootstrap export deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background Skein Lightning bootstrap export rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.prepare_skein_lightning_bootstrap_export();
        scheduler.finish_with_outcome(permit, result.is_ok());
        result
    }

    fn skein_lightning_bootstrap_export_estimated_operations(&self) -> usize {
        let statistics = self.store.basic_statistics();
        let graph_total = statistics
            .node_count
            .saturating_add(statistics.relationship_count);
        let relational_total = self
            .store
            .relational_state()
            .table_schemas()
            .map(|schema| {
                1usize.saturating_add(self.store.relational_state().row_count(&schema.name))
            })
            .fold(0usize, usize::saturating_add)
            .saturating_add(self.store.relational_state().overflow_segment_count());
        usize::try_from(graph_total)
            .unwrap_or(usize::MAX)
            .saturating_add(relational_total)
    }

    pub fn storage_version(&self) -> &'static str {
        self.store.storage_version()
    }

    #[cfg(test)]
    pub(crate) fn statistics(&self) -> GraphStatistics {
        self.store.statistics(&self.catalog)
    }

    pub fn refresh_optimizer_statistics_external(
        &mut self,
        options: &crate::store::OptimizerStatisticsRefreshOptions,
    ) -> Result<crate::store::OptimizerStatisticsRefreshReport> {
        self.ensure_writable()?;
        let previous_statistics = self.store.checkpoint_statistics_snapshot();
        let previous_dirty_state = self.store.advanced_statistics_dirty_snapshot();
        let mut report = self
            .store
            .refresh_optimizer_statistics_external(&self.catalog, options)?;
        if let Err(error) = self.checkpoint() {
            self.store
                .restore_checkpoint_statistics(previous_statistics, previous_dirty_state);
            return Err(error);
        }
        report.checkpoint_persisted = true;
        *self.plan_cache.borrow_mut() = PlanCache::new(self.config.max_plan_cache_entries);
        self.optimizer_planning_cache.borrow_mut().invalidate();
        Ok(report)
    }

    fn optimizer_statistics_refresh_work(&self) -> Option<OptimizerStatisticsRefreshWork> {
        if self.config.read_only {
            return None;
        }
        self.store.optimizer_statistics_refresh_work(&self.catalog)
    }

    pub fn optimizer_statistics_refresh_background_work_plan(
        &self,
        mut hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let work = self.optimizer_statistics_refresh_work()?;
        hint.recent_delta_operations = hint
            .recent_delta_operations
            .max(work.recent_delta_operations);
        hint.source_graph_commit_lag = hint.source_graph_commit_lag.max(work.source_commit_lag);
        Some(BackgroundWorkPlan::background(
            WorkClass::Projection,
            work.estimated_operations,
            hint,
        ))
    }

    pub fn refresh_background_optimizer_statistics(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: &crate::store::OptimizerStatisticsRefreshOptions,
        hint: BackgroundWorkHint,
    ) -> Result<Option<crate::store::OptimizerStatisticsRefreshReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(plan) = self.optimizer_statistics_refresh_background_work_plan(hint) else {
            return Ok(None);
        };
        match policy.admit(state, &plan.request) {
            QosAdmission::Admit => self
                .refresh_optimizer_statistics_external(options)
                .map(Some),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background optimizer statistics refresh deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background optimizer statistics refresh rejected: {reason}"
            ))),
        }
    }

    pub fn refresh_scheduled_background_optimizer_statistics(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        options: &crate::store::OptimizerStatisticsRefreshOptions,
        hint: BackgroundWorkHint,
    ) -> Result<Option<crate::store::OptimizerStatisticsRefreshReport>> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(plan) = self.optimizer_statistics_refresh_background_work_plan(hint) else {
            return Ok(None);
        };
        self.configure_qos_scheduler_telemetry(scheduler);
        let permit = match scheduler.try_start(plan.request) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background optimizer statistics refresh deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background optimizer statistics refresh rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self
            .refresh_optimizer_statistics_external(options)
            .map(Some);
        scheduler.finish_with_outcome(permit, result.is_ok());
        result
    }

    #[cfg(test)]
    pub(crate) fn basic_statistics(&self) -> crate::schema::BasicGraphStatistics {
        self.store.basic_statistics()
    }

    pub fn basic_statistics_consistency_report(&self) -> BasicStatisticsConsistencyReport {
        self.store.basic_statistics_consistency_report()
    }

    pub fn adjacency_consistency_report(&self) -> AdjacencyConsistencyReport {
        self.store.adjacency_consistency_report()
    }

    pub fn adjacency_consolidation_plan(&self) -> AdjacencyConsolidationPlan {
        self.store.adjacency_consolidation_plan()
    }

    pub fn storage_checkpoint_background_work_plan(
        &self,
        mut hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        if self.config.read_only {
            return None;
        }
        let pressure = self.storage_pressure_snapshot();
        if !pressure.recommends_checkpoint() {
            return None;
        }
        hint.recent_delta_operations = hint
            .recent_delta_operations
            .max(self.store.checkpoint_estimated_operations());
        hint.source_graph_commit_lag = hint.source_graph_commit_lag.max(
            pressure
                .current_commit_epoch
                .saturating_sub(pressure.checkpoint_commit_epoch),
        );
        hint.staleness_millis = hint.staleness_millis.max(pressure.wal_age_millis);
        Some(BackgroundWorkPlan::background(
            WorkClass::Mutation,
            self.store.checkpoint_estimated_operations(),
            hint,
        ))
    }

    pub fn checkpoint_background(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        hint: BackgroundWorkHint,
    ) -> Result<()> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(plan) = self.storage_checkpoint_background_work_plan(hint) else {
            return Ok(());
        };
        match policy.admit(state, &plan.request) {
            QosAdmission::Admit => self.checkpoint(),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background storage checkpoint deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background storage checkpoint rejected: {reason}"
            ))),
        }
    }

    pub fn checkpoint_scheduled_background(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        hint: BackgroundWorkHint,
    ) -> Result<()> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let Some(plan) = self.storage_checkpoint_background_work_plan(hint) else {
            return Ok(());
        };
        self.configure_qos_scheduler_telemetry(scheduler);
        let permit = match scheduler.try_start(plan.request) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background storage checkpoint deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background storage checkpoint rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };
        let result = self.checkpoint();
        scheduler.finish_with_outcome(permit, result.is_ok());
        result
    }

    pub fn adjacency_consolidation_background_work_plan(
        &self,
        max_estimated_entries: usize,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let estimated_entries = self
            .store
            .bounded_adjacency_consolidation_estimated_entries(max_estimated_entries);
        (estimated_entries > 0)
            .then(|| BackgroundWorkPlan::background(WorkClass::Mutation, estimated_entries, hint))
    }

    pub fn consolidate_bounded_adjacency_deltas(
        &mut self,
        max_estimated_entries: usize,
    ) -> AdjacencyConsolidationReport {
        self.store
            .consolidate_bounded_adjacency_deltas(max_estimated_entries)
    }

    pub fn consolidate_bounded_background_adjacency_deltas(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        max_estimated_entries: usize,
    ) -> Result<AdjacencyConsolidationReport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_entries = self
            .store
            .bounded_adjacency_consolidation_estimated_entries(max_estimated_entries);
        if estimated_entries == 0 {
            return Ok(self.consolidate_bounded_adjacency_deltas(max_estimated_entries));
        }
        match policy.admit(
            state,
            &WorkRequest::background(WorkClass::Mutation, estimated_entries),
        ) {
            QosAdmission::Admit => {
                Ok(self.consolidate_bounded_adjacency_deltas(max_estimated_entries))
            }
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background adjacency consolidation deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background adjacency consolidation rejected: {reason}"
            ))),
        }
    }

    pub fn consolidate_bounded_scheduled_background_adjacency_deltas(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        max_estimated_entries: usize,
    ) -> Result<AdjacencyConsolidationReport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_entries = self
            .store
            .bounded_adjacency_consolidation_estimated_entries(max_estimated_entries);
        if estimated_entries == 0 {
            return Ok(self.consolidate_bounded_adjacency_deltas(max_estimated_entries));
        }
        self.configure_qos_scheduler_telemetry(scheduler);
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Mutation,
            estimated_entries,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background adjacency consolidation deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background adjacency consolidation rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = Ok(self.consolidate_bounded_adjacency_deltas(max_estimated_entries));
        scheduler.finish_with_outcome(permit, result.is_ok());
        result
    }

    pub fn degree_statistics_consistency_report(&self) -> DegreeStatisticsConsistencyReport {
        self.store.degree_statistics_consistency_report()
    }

    pub fn distinct_value_statistics_consistency_report(
        &self,
    ) -> DistinctValueStatisticsConsistencyReport {
        self.store
            .distinct_value_statistics_consistency_report(&self.catalog)
    }

    pub fn property_index_consistency_report(&self) -> PropertyIndexConsistencyReport {
        self.store.property_index_consistency_report(&self.catalog)
    }

    #[cfg(test)]
    pub(crate) fn property_indexes(&self) -> Vec<IndexDescriptor> {
        self.catalog.property_indexes().cloned().collect()
    }

    #[cfg(test)]
    pub(crate) fn composite_property_indexes(&self) -> Vec<CompositeIndexDescriptor> {
        self.catalog.composite_property_indexes().cloned().collect()
    }

    pub fn rebuild_bounded_property_index_projections(
        &mut self,
        max_estimated_operations: usize,
    ) -> QueryOutput {
        property_index_projection_rebuild_output(
            self.store.rebuild_bounded_property_index_projections(
                &self.catalog,
                max_estimated_operations,
            ),
        )
    }

    pub fn property_index_projection_background_work_plan(
        &self,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let estimated_operations = self
            .store
            .property_index_projection_estimated_operations(&self.catalog);
        if estimated_operations == 0 {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Projection,
            estimated_operations,
            hint,
        ))
    }

    pub fn rebuild_bounded_background_property_index_projections(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations = self
            .store
            .bounded_property_index_projection_estimated_operations(
                &self.catalog,
                max_estimated_operations,
            );
        if estimated_operations == 0 {
            return Ok(self.rebuild_bounded_property_index_projections(max_estimated_operations));
        }
        let request = WorkRequest::background(WorkClass::Projection, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => {
                Ok(self.rebuild_bounded_property_index_projections(max_estimated_operations))
            }
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background property index projection rebuild deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background property index projection rebuild rejected: {reason}"
            ))),
        }
    }

    pub fn rebuild_bounded_scheduled_background_property_index_projections(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations = self
            .store
            .bounded_property_index_projection_estimated_operations(
                &self.catalog,
                max_estimated_operations,
            );
        if estimated_operations == 0 {
            return Ok(self.rebuild_bounded_property_index_projections(max_estimated_operations));
        }
        self.configure_qos_scheduler_telemetry(scheduler);
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Projection,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background property index projection rebuild deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background property index projection rebuild rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = Ok(self.rebuild_bounded_property_index_projections(max_estimated_operations));
        scheduler.finish_with_outcome(permit, result.is_ok());
        result
    }

    #[cfg(test)]
    pub(crate) fn unique_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog.unique_constraints().cloned().collect()
    }

    #[cfg(test)]
    pub(crate) fn node_property_exists_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog
            .node_property_exists_constraints()
            .cloned()
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn relationship_property_exists_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog
            .relationship_property_exists_constraints()
            .cloned()
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn relationship_unique_constraints(&self) -> Vec<ConstraintDescriptor> {
        self.catalog
            .relationship_unique_constraints()
            .cloned()
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn table_descriptors(&self) -> Vec<TableDescriptor> {
        self.catalog.table_descriptors().cloned().collect()
    }

    #[cfg(test)]
    pub(crate) fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        self.catalog.property_descriptors().cloned().collect()
    }

    pub fn plan_schema_maintenance(&self) -> QueryOutput {
        let rows: Vec<Row> = self
            .store
            .plan_schema_maintenance(&self.catalog)
            .into_iter()
            .map(|item| {
                BTreeMap::from([
                    ("object_type".to_string(), Value::String(item.object_type)),
                    ("object".to_string(), Value::String(item.object)),
                    (
                        "from_state".to_string(),
                        schema_state_value(item.from_state),
                    ),
                    (
                        "to_state".to_string(),
                        item.to_state.map(schema_state_value).unwrap_or(Value::Null),
                    ),
                    ("action".to_string(), Value::String(item.action)),
                    (
                        "estimated_operations".to_string(),
                        Value::Int(i64::try_from(item.estimated_operations).unwrap_or(i64::MAX)),
                    ),
                ])
            })
            .collect();
        QueryOutput { rows: rows.into() }
    }

    pub fn schema_maintenance_background_work_plan(
        &self,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let estimated_operations = self.schema_maintenance_estimated_operations();
        if estimated_operations == 0 {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Mutation,
            estimated_operations,
            hint,
        ))
    }

    pub fn run_schema_maintenance(&mut self) -> Result<QueryOutput> {
        self.ensure_writable()?;
        let actions = self.store.run_schema_maintenance(&mut self.catalog)?;
        Ok(schema_maintenance_actions_output(actions))
    }

    pub fn run_bounded_schema_maintenance(
        &mut self,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_writable()?;
        let actions = self
            .store
            .run_bounded_schema_maintenance(&mut self.catalog, max_estimated_operations)?;
        Ok(schema_maintenance_actions_output(actions))
    }

    pub fn run_background_schema_maintenance(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let request = WorkRequest::background(WorkClass::Mutation, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.run_schema_maintenance(),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background schema maintenance deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background schema maintenance rejected: {reason}"
            ))),
        }
    }

    pub fn run_bounded_background_schema_maintenance(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations =
            self.bounded_schema_maintenance_estimated_operations(max_estimated_operations);
        if estimated_operations == 0 {
            return self.run_bounded_schema_maintenance(max_estimated_operations);
        }
        let request = WorkRequest::background(WorkClass::Mutation, estimated_operations);
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.run_bounded_schema_maintenance(max_estimated_operations),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background schema maintenance deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background schema maintenance rejected: {reason}"
            ))),
        }
    }

    pub fn run_planned_background_schema_maintenance(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
    ) -> Result<QueryOutput> {
        self.run_background_schema_maintenance(
            policy,
            state,
            self.schema_maintenance_estimated_operations(),
        )
    }

    pub fn run_scheduled_background_schema_maintenance(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        self.configure_qos_scheduler_telemetry(scheduler);
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Mutation,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background schema maintenance deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background schema maintenance rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.run_schema_maintenance();
        scheduler.finish_with_outcome(permit, result.is_ok());
        result
    }

    pub fn run_bounded_scheduled_background_schema_maintenance(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        max_estimated_operations: usize,
    ) -> Result<QueryOutput> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        let estimated_operations =
            self.bounded_schema_maintenance_estimated_operations(max_estimated_operations);
        if estimated_operations == 0 {
            return self.run_bounded_schema_maintenance(max_estimated_operations);
        }
        self.configure_qos_scheduler_telemetry(scheduler);
        let permit = match scheduler.try_start(WorkRequest::background(
            WorkClass::Mutation,
            estimated_operations,
        )) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background schema maintenance deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background schema maintenance rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.run_bounded_schema_maintenance(max_estimated_operations);
        scheduler.finish_with_outcome(permit, result.is_ok());
        result
    }

    pub fn run_planned_scheduled_background_schema_maintenance(
        &mut self,
        scheduler: &mut LocalQosScheduler,
    ) -> Result<QueryOutput> {
        self.run_scheduled_background_schema_maintenance(
            scheduler,
            self.schema_maintenance_estimated_operations(),
        )
    }

    fn schema_maintenance_estimated_operations(&self) -> usize {
        self.store
            .plan_schema_maintenance(&self.catalog)
            .into_iter()
            .map(|item| item.estimated_operations)
            .fold(0usize, usize::saturating_add)
    }

    fn bounded_schema_maintenance_estimated_operations(
        &self,
        max_estimated_operations: usize,
    ) -> usize {
        let mut used_estimated_operations = 0usize;
        for item in self.store.plan_schema_maintenance(&self.catalog) {
            let Some(next) = used_estimated_operations.checked_add(item.estimated_operations)
            else {
                continue;
            };
            if next <= max_estimated_operations {
                used_estimated_operations = next;
            }
        }
        used_estimated_operations
    }

    #[cfg(test)]
    pub(crate) fn projected_graph_statuses(&self) -> Vec<crate::store::ProjectedGraphStatus> {
        self.store.projected_graph_statuses()
    }

    pub fn rebuild_projected_graph_artifacts(&mut self) -> Result<()> {
        self.ensure_writable()?;
        self.store.rebuild_projected_graph_artifacts(&self.catalog)
    }

    pub fn rebuild_search_projection(
        &self,
        search_index: &mut SearchIndex,
        options: SearchRebuildOptions,
    ) -> Result<SearchRebuildSummary> {
        search_index.rebuild_from_graph(&self.catalog, &self.store, options)
    }

    pub fn search_projection_rebuild_background_work_plan(
        &self,
        search_index: &SearchIndex,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        search_index.rebuild_background_work_plan(&self.store, hint)
    }

    pub fn rebuild_background_search_projection(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: SearchRebuildOptions,
    ) -> Result<SearchDerivedArtifactReport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        search_index.rebuild_background_derived_artifacts(
            policy,
            state,
            &self.catalog,
            &self.store,
            options,
        )
    }

    pub fn rebuild_scheduled_background_search_projection(
        &self,
        search_index: &mut SearchIndex,
        scheduler: &mut LocalQosScheduler,
        options: SearchRebuildOptions,
    ) -> Result<SearchDerivedArtifactReport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        search_index.rebuild_scheduled_background_derived_artifacts(
            scheduler,
            &self.catalog,
            &self.store,
            options,
        )
    }

    pub fn repair_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        options: MetadataRepairOptions,
    ) -> Result<MetadataRepairSummary> {
        search_index.repair_metadata_from_graph(&self.catalog, &self.store, options)
    }

    pub fn search_projection_metadata_repair_background_work_plan(
        &self,
        search_index: &SearchIndex,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        search_index.metadata_repair_background_work_plan(&self.store, hint)
    }

    pub fn repair_background_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: MetadataRepairOptions,
        estimated_operations: usize,
    ) -> Result<MetadataRepairSummary> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        search_index.repair_background_metadata_from_graph(
            policy,
            state,
            &self.catalog,
            &self.store,
            options,
            estimated_operations,
        )
    }

    pub fn repair_scheduled_background_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        scheduler: &mut LocalQosScheduler,
        options: MetadataRepairOptions,
        estimated_operations: usize,
    ) -> Result<MetadataRepairSummary> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        search_index.repair_scheduled_background_metadata_from_graph(
            scheduler,
            &self.catalog,
            &self.store,
            options,
            estimated_operations,
        )
    }

    pub fn search_projection_delta_background_work_plan(
        &self,
        delta: &SearchProjectionDelta,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        delta.background_work_plan(hint)
    }

    pub fn search_projection_graph_delta_background_work_plan(
        &self,
        request: &SearchProjectionGraphDeltaRequest,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        request.background_work_plan(hint)
    }

    pub fn search_projection_graph_delta_freshness_background_work_plan(
        &self,
        search_index: &SearchIndex,
        request: &SearchProjectionGraphDeltaRequest,
        mut hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        if hint.recent_delta_operations == 0 {
            hint.recent_delta_operations = request.operation_count();
        }
        if hint.source_graph_commit_lag == 0 {
            hint.source_graph_commit_lag = search_projection_commit_lag(
                search_index,
                self.store
                    .search_projection_changefeed_status()
                    .required_projection_commit_epoch(),
            );
        }
        if request.operation_count() == 0
            && hint.source_graph_commit_lag > 0
            && request.complete_through_graph_commit_epoch.is_some()
        {
            if hint.recent_delta_operations == 0 {
                hint.recent_delta_operations = 1;
            }
            return Some(BackgroundWorkPlan::background(
                WorkClass::Projection,
                1,
                hint,
            ));
        }
        request.background_work_plan(hint)
    }

    pub fn search_projection_freshness_lag_background_work_plan(
        &self,
        search_index: &SearchIndex,
        mut hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let source_graph_commit_lag = search_projection_commit_lag(
            search_index,
            self.store
                .search_projection_changefeed_status()
                .required_projection_commit_epoch(),
        );
        if source_graph_commit_lag == 0 {
            return None;
        }
        let operation_count = usize::try_from(source_graph_commit_lag).unwrap_or(usize::MAX);
        if hint.recent_delta_operations == 0 {
            hint.recent_delta_operations = operation_count;
        }
        if hint.source_graph_commit_lag == 0 {
            hint.source_graph_commit_lag = source_graph_commit_lag;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Projection,
            operation_count,
            hint,
        ))
    }

    pub fn build_search_projection_graph_delta_request_after(
        &self,
        source_graph_commit_epoch: u64,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionGraphDeltaRequest>> {
        let Some(batch) = self.build_search_projection_change_batch_after(
            source_graph_commit_epoch,
            max_operations,
        )?
        else {
            return Ok(None);
        };
        if batch.has_relational_changes() {
            return Err(SkeinError::Storage(format!(
                "search projection commits through epoch {} contain relational primary-key changes; use the unified search projection changefeed and publish one combined projection delta",
                batch.complete_through_commit_epoch().unwrap_or(source_graph_commit_epoch)
            )));
        }
        Ok(Some(batch.graph_delta))
    }

    pub fn build_search_projection_change_batch_after(
        &self,
        source_commit_epoch: u64,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionChangeBatch>> {
        let current_epoch = self.store.commit_epoch();
        if source_commit_epoch >= current_epoch {
            return Ok(None);
        }
        let change_log_start_epoch = self.store.search_projection_change_log_start_epoch();
        if source_commit_epoch < change_log_start_epoch {
            return Err(SkeinError::Storage(format!(
                "search projection change log starts at commit epoch {change_log_start_epoch}; requested source commit epoch {source_commit_epoch}; full search projection rebuild required"
            )));
        }

        let mut upsert_node_ids = BTreeSet::new();
        let mut delete_document_ids = BTreeSet::new();
        let mut relational_primary_keys =
            BTreeMap::<String, BTreeSet<skein_storage::RelationalKey>>::new();
        let mut complete_through_commit_epoch = source_commit_epoch;
        let mut truncated_by_budget = false;
        for change in self
            .store
            .search_projection_changes_after(source_commit_epoch)
        {
            let tables = match &change.relational_primary_key_changes {
                skein_storage::RelationalPrimaryKeyChangeCapture::Captured { tables, .. } => tables,
                skein_storage::RelationalPrimaryKeyChangeCapture::RequiresRebuild { reason } => {
                    return Err(SkeinError::Storage(format!(
                        "search projection relational change at commit epoch {} requires a full rebuild: {reason:?}",
                        change.commit_epoch
                    )));
                }
            };
            let additional_operation_count = change
                .upsert_node_ids
                .iter()
                .filter(|node_id| !upsert_node_ids.contains(*node_id))
                .count()
                .saturating_add(
                    change
                        .delete_document_ids
                        .iter()
                        .filter(|document_id| !delete_document_ids.contains(*document_id))
                        .count(),
                )
                .saturating_add(
                    tables
                        .iter()
                        .map(|table| {
                            let existing = relational_primary_keys.get(&table.table);
                            table
                                .primary_keys
                                .iter()
                                .filter(|key| existing.is_none_or(|keys| !keys.contains(*key)))
                                .count()
                        })
                        .sum::<usize>(),
                );
            let next_operation_count = upsert_node_ids
                .len()
                .saturating_add(delete_document_ids.len())
                .saturating_add(
                    relational_primary_keys
                        .values()
                        .map(BTreeSet::len)
                        .sum::<usize>(),
                )
                .saturating_add(additional_operation_count);
            if let Some(limit) = max_operations
                && next_operation_count > limit
            {
                if complete_through_commit_epoch == source_commit_epoch {
                    return Err(SkeinError::Storage(format!(
                        "search projection change at commit epoch {} requires {next_operation_count} operations, exceeding configured per-batch limit {limit}",
                        change.commit_epoch
                    )));
                }
                truncated_by_budget = true;
                break;
            }
            upsert_node_ids.extend(change.upsert_node_ids);
            delete_document_ids.extend(change.delete_document_ids);
            for table in tables {
                relational_primary_keys
                    .entry(table.table.clone())
                    .or_default()
                    .extend(table.primary_keys.iter().cloned());
            }
            complete_through_commit_epoch = change.commit_epoch;
        }
        if !truncated_by_budget {
            complete_through_commit_epoch = current_epoch;
        }

        Ok(Some(SearchProjectionChangeBatch {
            graph_delta: SearchProjectionGraphDeltaRequest {
                upsert_node_ids: upsert_node_ids.into_iter().collect(),
                delete_document_ids: delete_document_ids.into_iter().collect(),
                max_operations,
                complete_through_graph_commit_epoch: Some(complete_through_commit_epoch),
            },
            relational_primary_key_changes: relational_primary_keys
                .into_iter()
                .map(
                    |(table, primary_keys)| skein_storage::RelationalTablePrimaryKeyChanges {
                        table,
                        primary_keys: primary_keys.into_iter().collect(),
                    },
                )
                .collect(),
        }))
    }

    pub fn build_search_projection_graph_delta_request_from_freshness(
        &self,
        search_index: &SearchIndex,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionGraphDeltaRequest>> {
        self.build_search_projection_graph_delta_request_after(
            search_index
                .projection_freshness()
                .source_graph_commit_epoch
                .unwrap_or(0),
            max_operations,
        )
    }

    pub fn build_search_projection_change_batch_from_freshness(
        &self,
        search_index: &SearchIndex,
        max_operations: Option<usize>,
    ) -> Result<Option<SearchProjectionChangeBatch>> {
        self.build_search_projection_change_batch_after(
            search_index
                .projection_freshness()
                .source_graph_commit_epoch
                .unwrap_or(0),
            max_operations,
        )
    }

    pub fn background_maintenance_candidates(
        &self,
        search_index: Option<&SearchIndex>,
        options: BackgroundMaintenanceOptions,
    ) -> Vec<BackgroundMaintenanceCandidate> {
        let mut candidates = Vec::new();

        if options.include_storage_checkpoint
            && let Some(plan) = self.storage_checkpoint_background_work_plan(options.hint.clone())
        {
            candidates.push(BackgroundMaintenanceCandidate::new(
                BackgroundMaintenanceKind::StorageCheckpoint,
                plan,
            ));
        }

        if options.include_schema_maintenance
            && let Some(plan) = self.schema_maintenance_background_work_plan(options.hint.clone())
        {
            candidates.push(BackgroundMaintenanceCandidate::new(
                BackgroundMaintenanceKind::SchemaMaintenance,
                plan,
            ));
        }

        if options.include_property_index_projection
            && let Some(plan) =
                self.property_index_projection_background_work_plan(options.hint.clone())
        {
            candidates.push(BackgroundMaintenanceCandidate::new(
                BackgroundMaintenanceKind::PropertyIndexProjection,
                plan,
            ));
        }

        if options.include_optimizer_statistics_refresh
            && let Some(plan) =
                self.optimizer_statistics_refresh_background_work_plan(options.hint.clone())
        {
            candidates.push(BackgroundMaintenanceCandidate::new(
                BackgroundMaintenanceKind::OptimizerStatisticsRefresh,
                plan,
            ));
        }

        if let Some(delta_request) = &options.search_projection_graph_delta {
            let plan = match search_index {
                Some(search_index) => self
                    .search_projection_graph_delta_freshness_background_work_plan(
                        search_index,
                        delta_request,
                        options.hint.clone(),
                    ),
                None => self.search_projection_graph_delta_background_work_plan(
                    delta_request,
                    options.hint.clone(),
                ),
            };
            if let Some(plan) = plan {
                candidates.push(
                    BackgroundMaintenanceCandidate::new(
                        BackgroundMaintenanceKind::SearchProjectionGraphDelta,
                        plan,
                    )
                    .with_search_projection_graph_delta(delta_request.clone()),
                );
            }
        } else if options.include_search_projection_graph_delta_freshness
            && let Some(search_index) = search_index
        {
            let executable_request = self
                .build_search_projection_graph_delta_request_from_freshness(search_index, None)
                .ok()
                .flatten();
            if let Some(request) = executable_request {
                if let Some(plan) = self
                    .search_projection_graph_delta_freshness_background_work_plan(
                        search_index,
                        &request,
                        options.hint.clone(),
                    )
                {
                    candidates.push(
                        BackgroundMaintenanceCandidate::new(
                            BackgroundMaintenanceKind::SearchProjectionGraphDelta,
                            plan,
                        )
                        .with_search_projection_graph_delta(request),
                    );
                }
            } else if let Some(plan) = self.search_projection_freshness_lag_background_work_plan(
                search_index,
                options.hint.clone(),
            ) {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::SearchProjectionGraphDelta,
                    plan,
                ));
            }
        }

        if let Some(search_index) = search_index {
            if options.include_search_projection_rebuild {
                let mut hint = options.hint.clone();
                if hint.source_graph_commit_lag == 0 {
                    hint.source_graph_commit_lag = search_projection_commit_lag(
                        search_index,
                        self.store
                            .search_projection_changefeed_status()
                            .required_projection_commit_epoch(),
                    );
                }
                if let Some(plan) =
                    self.search_projection_rebuild_background_work_plan(search_index, hint)
                {
                    candidates.push(BackgroundMaintenanceCandidate::new(
                        BackgroundMaintenanceKind::SearchProjectionRebuild,
                        plan,
                    ));
                }
            }

            if options.include_search_projection_metadata_repair
                && let Some(plan) = self.search_projection_metadata_repair_background_work_plan(
                    search_index,
                    options.hint.clone(),
                )
            {
                candidates.push(BackgroundMaintenanceCandidate::new(
                    BackgroundMaintenanceKind::SearchProjectionMetadataRepair,
                    plan,
                ));
            }
        }

        if options.include_skein_lightning_bootstrap_export
            && let Some(plan) =
                self.skein_lightning_bootstrap_export_background_work_plan(options.hint.clone())
        {
            candidates.push(BackgroundMaintenanceCandidate::new(
                BackgroundMaintenanceKind::SkeinLightningBootstrapExport,
                plan,
            ));
        }

        if options.include_external_content_artifact_jobs
            && let Some(plan) = self.external_content_artifact_job_background_work_plan(
                options.hint,
                options.external_content_artifact_estimated_operations,
            )
        {
            candidates.push(BackgroundMaintenanceCandidate::new(
                BackgroundMaintenanceKind::ExternalContentArtifactJob,
                plan,
            ));
        }

        candidates
    }

    pub fn rank_background_maintenance(
        &self,
        search_index: Option<&SearchIndex>,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> Vec<RankedBackgroundMaintenance> {
        let candidates = self.background_maintenance_candidates(search_index, options);
        let plans = candidates
            .iter()
            .map(|candidate| candidate.plan.clone())
            .collect::<Vec<_>>();
        policy
            .rank_background_work(state, &plans)
            .into_iter()
            .map(|ranked| {
                let candidate = &candidates[ranked.index];
                RankedBackgroundMaintenance {
                    kind: candidate.kind,
                    name: candidate.name.clone(),
                    plan: candidate.plan.clone(),
                    decision: ranked.decision,
                    search_projection_graph_delta: candidate.search_projection_graph_delta.clone(),
                }
            })
            .collect()
    }

    pub fn background_maintenance_summary(
        &self,
        search_index: Option<&SearchIndex>,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        options: BackgroundMaintenanceOptions,
    ) -> BackgroundMaintenanceSummary {
        let ranked = self.rank_background_maintenance(search_index, policy, state, options);
        BackgroundMaintenanceSummary::from_ranked(ranked, policy, state)
    }

    pub fn build_search_projection_graph_delta(
        &self,
        request: &SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDelta> {
        search_projection_graph_delta_for(&self.catalog, &self.store, request)
    }

    pub fn apply_search_projection_delta(
        &self,
        search_index: &mut SearchIndex,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        search_index.apply_projection_delta(delta)
    }

    pub fn apply_search_projection_graph_delta(
        &self,
        search_index: &mut SearchIndex,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        let delta = self.build_search_projection_graph_delta(&request)?;
        search_index.apply_projection_delta(delta)
    }

    pub fn apply_search_projection_change_batch(
        &self,
        search_index: &mut SearchIndex,
        batch: SearchProjectionChangeBatch,
        relational: SearchProjectionRelationalDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        let expected_primary_key_count = batch
            .relational_primary_key_changes
            .iter()
            .map(|table| table.primary_keys.len())
            .fold(0usize, usize::saturating_add);
        if relational.processed_primary_key_count != expected_primary_key_count {
            return Err(SkeinError::Storage(format!(
                "search projection relational delta processed {} primary keys, expected {expected_primary_key_count}; projection watermark was not published",
                relational.processed_primary_key_count
            )));
        }
        if relational.delta.source_graph_commit_epoch.is_some() {
            return Err(SkeinError::Storage(
                "search projection relational delta must not publish its own source epoch"
                    .to_string(),
            ));
        }
        let complete_through_commit_epoch = batch.complete_through_commit_epoch();
        let max_operations = batch.graph_delta.max_operations;
        let mut graph = self.build_search_projection_graph_delta(&batch.graph_delta)?;
        let SearchProjectionDelta {
            upserts,
            deletes,
            max_operations: _,
            source_graph_commit_epoch: _,
        } = relational.delta;
        graph.upserts.extend(upserts);
        graph.deletes.extend(deletes);
        graph.max_operations = max_operations;
        graph.source_graph_commit_epoch = complete_through_commit_epoch;
        search_index.apply_projection_delta(graph)
    }

    pub fn apply_background_search_projection_delta(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        search_index.apply_background_projection_delta(policy, state, delta)
    }

    pub fn apply_background_search_projection_graph_delta(
        &self,
        search_index: &mut SearchIndex,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        match policy.admit(state, &request.background_work_request()) {
            QosAdmission::Admit => self.apply_search_projection_graph_delta(search_index, request),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background search projection graph delta deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background search projection graph delta rejected: {reason}"
            ))),
        }
    }

    pub fn apply_scheduled_background_search_projection_delta(
        &self,
        search_index: &mut SearchIndex,
        scheduler: &mut LocalQosScheduler,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        search_index.apply_scheduled_background_projection_delta(scheduler, delta)
    }

    pub fn apply_scheduled_background_search_projection_graph_delta(
        &self,
        search_index: &mut SearchIndex,
        scheduler: &mut LocalQosScheduler,
        request: SearchProjectionGraphDeltaRequest,
    ) -> Result<SearchProjectionDeltaReport> {
        self.ensure_runtime_capability(skein_core::RuntimeCapability::BackgroundMaintenance)?;
        self.configure_qos_scheduler_telemetry(scheduler);
        let permit = match scheduler.try_start(request.background_work_request()) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background search projection graph delta deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background search projection graph delta rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.apply_search_projection_graph_delta(search_index, request);
        scheduler.finish_with_outcome(permit, result.is_ok());
        result
    }

    pub fn retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> KnowledgeRetrievalOutput {
        KnowledgeRetrievalGraphContext {
            catalog: &self.catalog,
            store: &self.store,
            compressed_vector_search_mode: self.config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.config.adaptive_vector_backend_policy,
            query_memory_budget: self.config.execution_memory.query_memory_bytes,
            result_payload_budget: self
                .config
                .max_read_result_payload_bytes
                .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
        }
        .retrieve_knowledge(search_index, request)
    }

    pub fn try_retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        KnowledgeRetrievalGraphContext {
            catalog: &self.catalog,
            store: &self.store,
            compressed_vector_search_mode: self.config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.config.adaptive_vector_backend_policy,
            query_memory_budget: self.config.execution_memory.query_memory_bytes,
            result_payload_budget: self
                .config
                .max_read_result_payload_bytes
                .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
        }
        .try_retrieve_knowledge(search_index, request)
    }

    pub(crate) fn try_retrieve_knowledge_from_search(
        &self,
        search: SearchResultSet,
        projection_freshness: SearchProjectionFreshness,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        KnowledgeRetrievalGraphContext {
            catalog: &self.catalog,
            store: &self.store,
            compressed_vector_search_mode: self.config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.config.adaptive_vector_backend_policy,
            query_memory_budget: self.config.execution_memory.query_memory_bytes,
            result_payload_budget: self
                .config
                .max_read_result_payload_bytes
                .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
        }
        .retrieve_knowledge_from_search(search, projection_freshness, request)
    }
}

// Keep the old typed mutation adapters only as regression-test fixtures. The
// production embedded surface is query-first: callers execute parameterized
// Cypher through Database or DatabaseTransaction.
#[cfg(test)]
impl Database {
    pub fn merge_knowledge_crystal_source(
        &mut self,
        request: &KnowledgeCrystalSourceMergeRequest,
    ) -> Result<KnowledgeCrystalSourceMergeOutput> {
        merge_knowledge_crystal_source_for(self, request)
    }

    pub fn create_knowledge_entity(
        &mut self,
        request: &KnowledgeEntityCreateRequest,
    ) -> Result<KnowledgeEntityCreateOutput> {
        create_knowledge_entity_for(self, request)
    }

    pub fn create_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeEntityCreateBatchRequest,
    ) -> Result<KnowledgeEntityCreateBatchOutput> {
        create_knowledge_entity_batch_for(self, request)
    }

    pub fn upsert_knowledge_entity(
        &mut self,
        request: &KnowledgeEntityUpsertRequest,
    ) -> Result<KnowledgeEntityUpsertOutput> {
        upsert_knowledge_entity_for(self, request)
    }

    pub fn upsert_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeEntityUpsertBatchRequest,
    ) -> Result<KnowledgeEntityUpsertBatchOutput> {
        upsert_knowledge_entity_batch_for(self, request)
    }

    pub fn update_knowledge_properties(
        &mut self,
        request: &KnowledgePropertyUpdateRequest,
    ) -> Result<KnowledgePropertyUpdateOutput> {
        update_knowledge_properties_for(self, request)
    }

    pub fn update_scoped_knowledge_properties(
        &mut self,
        request: &KnowledgeScopedPropertyUpdateRequest,
    ) -> Result<KnowledgePropertyUpdateOutput> {
        update_scoped_knowledge_properties_for(self, request)
    }

    pub fn update_knowledge_properties_batch(
        &mut self,
        request: &KnowledgePropertyUpdateBatchRequest,
    ) -> Result<KnowledgePropertyUpdateBatchOutput> {
        update_knowledge_properties_batch_for(self, request)
    }

    pub fn update_scoped_knowledge_properties_batch(
        &mut self,
        request: &KnowledgeScopedPropertyUpdateBatchRequest,
    ) -> Result<KnowledgePropertyUpdateBatchOutput> {
        update_scoped_knowledge_properties_batch_for(self, request)
    }

    pub fn move_knowledge_normalized_space_batch(
        &mut self,
        request: &KnowledgeNormalizedSpaceMoveBatchRequest,
    ) -> Result<KnowledgeNormalizedSpaceMoveBatchOutput> {
        move_knowledge_normalized_space_batch_for(self, request)
    }

    pub fn touch_knowledge_memory_access_batch(
        &mut self,
        request: &KnowledgeMemoryAccessBatchRequest,
    ) -> Result<KnowledgeMemoryAccessBatchOutput> {
        touch_knowledge_memory_access_batch_for(self, request)
    }

    pub fn update_knowledge_memory_content_batch(
        &mut self,
        request: &KnowledgeMemoryContentBatchRequest,
    ) -> Result<KnowledgeMemoryContentBatchOutput> {
        update_knowledge_memory_content_batch_for(self, request)
    }

    pub fn update_knowledge_memory_metadata_batch(
        &mut self,
        request: &KnowledgeMemoryMetadataBatchRequest,
    ) -> Result<KnowledgeMemoryMetadataBatchOutput> {
        update_knowledge_memory_metadata_batch_for(self, request)
    }

    pub fn update_knowledge_memory_dedup_reviewed_batch(
        &mut self,
        request: &KnowledgeMemoryDedupReviewedBatchRequest,
    ) -> Result<KnowledgeMemoryDedupReviewedBatchOutput> {
        update_knowledge_memory_dedup_reviewed_batch_for(self, request)
    }

    pub fn update_knowledge_memory_decay_refresh_batch(
        &mut self,
        request: &KnowledgeMemoryDecayRefreshBatchRequest,
    ) -> Result<KnowledgeMemoryDecayRefreshBatchOutput> {
        update_knowledge_memory_decay_refresh_batch_for(self, request)
    }

    pub fn adjust_knowledge_source_memory_count_batch(
        &mut self,
        request: &KnowledgeSourceMemoryCountBatchRequest,
    ) -> Result<KnowledgeSourceMemoryCountBatchOutput> {
        adjust_knowledge_source_memory_count_batch_for(self, request)
    }

    pub fn update_knowledge_source_lifecycle_batch(
        &mut self,
        request: &KnowledgeSourceLifecycleBatchRequest,
    ) -> Result<KnowledgeSourceLifecycleBatchOutput> {
        update_knowledge_source_lifecycle_batch_for(self, request)
    }

    pub fn update_knowledge_source_metadata_batch(
        &mut self,
        request: &KnowledgeSourceMetadataBatchRequest,
    ) -> Result<KnowledgeSourceMetadataBatchOutput> {
        update_knowledge_source_metadata_batch_for(self, request)
    }

    pub fn update_knowledge_source_parsed_metadata_batch(
        &mut self,
        request: &KnowledgeSourceParsedMetadataBatchRequest,
    ) -> Result<KnowledgeSourceParsedMetadataBatchOutput> {
        update_knowledge_source_parsed_metadata_batch_for(self, request)
    }

    pub fn create_knowledge_source_parsed_batch(
        &mut self,
        request: &KnowledgeSourceParsedCreateBatchRequest,
    ) -> Result<KnowledgeSourceParsedCreateBatchOutput> {
        create_knowledge_source_parsed_batch_for(self, request)
    }

    pub fn create_knowledge_source_revision_batch(
        &mut self,
        request: &KnowledgeSourceRevisionCreateBatchRequest,
    ) -> Result<KnowledgeSourceRevisionCreateBatchOutput> {
        create_knowledge_source_revision_batch_for(self, request)
    }

    pub fn delete_knowledge_sources(
        &mut self,
        request: &KnowledgeSourceDeleteBatchRequest,
    ) -> Result<KnowledgeSourceDeleteBatchOutput> {
        delete_knowledge_sources_for(self, request)
    }

    pub fn assign_knowledge_source_labels_batch(
        &mut self,
        request: &KnowledgeSourceLabelAssignmentBatchRequest,
    ) -> Result<KnowledgeSourceLabelAssignmentBatchOutput> {
        assign_knowledge_source_labels_batch_for(self, request)
    }

    pub fn delete_knowledge_source_labels_batch(
        &mut self,
        request: &KnowledgeSourceLabelDeleteBatchRequest,
    ) -> Result<KnowledgeSourceLabelDeleteBatchOutput> {
        delete_knowledge_source_labels_batch_for(self, request)
    }

    pub fn update_knowledge_memory_lifecycle_batch(
        &mut self,
        request: &KnowledgeMemoryLifecycleBatchRequest,
    ) -> Result<KnowledgeMemoryLifecycleBatchOutput> {
        update_knowledge_memory_lifecycle_batch_for(self, request)
    }

    pub fn update_knowledge_memory_latest_batch(
        &mut self,
        request: &KnowledgeMemoryLatestBatchRequest,
    ) -> Result<KnowledgeMemoryLatestBatchOutput> {
        update_knowledge_memory_latest_batch_for(self, request)
    }

    pub fn create_knowledge_memory_evolves_batch(
        &mut self,
        request: &KnowledgeMemoryEvolvesCreateBatchRequest,
    ) -> Result<KnowledgeMemoryEvolvesCreateBatchOutput> {
        create_knowledge_memory_evolves_batch_for(self, request)
    }

    pub fn update_knowledge_skill_usage_stats_batch(
        &mut self,
        request: &KnowledgeSkillUsageStatsBatchRequest,
    ) -> Result<KnowledgeSkillUsageStatsBatchOutput> {
        update_knowledge_skill_usage_stats_batch_for(self, request)
    }

    pub fn update_knowledge_skill_metadata_batch(
        &mut self,
        request: &KnowledgeSkillMetadataBatchRequest,
    ) -> Result<KnowledgeSkillMetadataBatchOutput> {
        update_knowledge_skill_metadata_batch_for(self, request)
    }

    pub fn merge_knowledge_skill_source(
        &mut self,
        request: &KnowledgeSkillSourceMergeRequest,
    ) -> Result<KnowledgeSkillSourceMergeOutput> {
        merge_knowledge_skill_source_for(self, request)
    }

    pub fn update_knowledge_skill_lifecycle_batch(
        &mut self,
        request: &KnowledgeSkillLifecycleBatchRequest,
    ) -> Result<KnowledgeSkillLifecycleBatchOutput> {
        update_knowledge_skill_lifecycle_batch_for(self, request)
    }

    pub fn delete_knowledge_skills(
        &mut self,
        request: &KnowledgeSkillDeleteBatchRequest,
    ) -> Result<KnowledgeSkillDeleteBatchOutput> {
        delete_knowledge_skills_for(self, request)
    }

    pub fn update_knowledge_thread_metadata_batch(
        &mut self,
        request: &KnowledgeThreadMetadataBatchRequest,
    ) -> Result<KnowledgeThreadMetadataBatchOutput> {
        update_knowledge_thread_metadata_batch_for(self, request)
    }

    pub fn update_knowledge_thread_message_count_batch(
        &mut self,
        request: &KnowledgeThreadMessageCountBatchRequest,
    ) -> Result<KnowledgeThreadMessageCountBatchOutput> {
        update_knowledge_thread_message_count_batch_for(self, request)
    }

    pub fn delete_knowledge_threads(
        &mut self,
        request: &KnowledgeThreadDeleteBatchRequest,
    ) -> Result<KnowledgeThreadDeleteBatchOutput> {
        delete_knowledge_threads_for(self, request)
    }

    pub fn delete_knowledge_thread_identities(
        &mut self,
        request: &KnowledgeThreadIdentityDeleteRequest,
    ) -> Result<KnowledgeThreadIdentityDeleteOutput> {
        delete_knowledge_thread_identities_for(self, request)
    }

    pub fn create_knowledge_thread_compaction_link(
        &mut self,
        request: &KnowledgeThreadCompactionLinkRequest,
    ) -> Result<KnowledgeThreadCompactionLinkOutput> {
        create_knowledge_thread_compaction_link_for(self, request)
    }

    pub fn delete_knowledge_thread_messages(
        &mut self,
        request: &KnowledgeThreadMessageDeleteRequest,
    ) -> Result<KnowledgeThreadMessageDeleteOutput> {
        delete_knowledge_thread_messages_for(self, request)
    }

    pub fn update_knowledge_label_lifecycle_batch(
        &mut self,
        request: &KnowledgeLabelLifecycleBatchRequest,
    ) -> Result<KnowledgeLabelLifecycleBatchOutput> {
        update_knowledge_label_lifecycle_batch_for(self, request)
    }

    pub fn delete_knowledge_memory_labels(
        &mut self,
        request: &KnowledgeMemoryLabelDeleteRequest,
    ) -> Result<KnowledgeMemoryLabelDeleteOutput> {
        delete_knowledge_memory_labels_for(self, request)
    }

    pub fn transfer_knowledge_label_memory_edges(
        &mut self,
        request: &KnowledgeLabelMemoryTransferRequest,
    ) -> Result<KnowledgeLabelMemoryTransferOutput> {
        transfer_knowledge_label_memory_edges_for(self, request)
    }

    pub fn transfer_knowledge_memory_label_edges(
        &mut self,
        request: &KnowledgeMemoryLabelTransferRequest,
    ) -> Result<KnowledgeMemoryLabelTransferOutput> {
        transfer_knowledge_memory_label_edges_for(self, request)
    }

    pub fn update_knowledge_pagerank_scores_batch(
        &mut self,
        request: &KnowledgePageRankScoreBatchRequest,
    ) -> Result<KnowledgePageRankScoreBatchOutput> {
        update_knowledge_pagerank_scores_batch_for(self, request)
    }

    pub fn clear_knowledge_pagerank_scores(
        &mut self,
        request: &KnowledgePageRankClearRequest,
    ) -> Result<KnowledgePageRankClearOutput> {
        clear_knowledge_pagerank_scores_for(self, request)
    }

    pub fn clear_knowledge_community_assignments(
        &mut self,
        request: &KnowledgeCommunityAssignmentClearRequest,
    ) -> Result<KnowledgeCommunityAssignmentClearOutput> {
        clear_knowledge_community_assignments_for(self, request)
    }

    pub fn create_knowledge_community_memberships_batch(
        &mut self,
        request: &KnowledgeCommunityMembershipCreateBatchRequest,
    ) -> Result<KnowledgeCommunityMembershipCreateBatchOutput> {
        create_knowledge_community_memberships_batch_for(self, request)
    }

    pub fn update_knowledge_communities_batch(
        &mut self,
        request: &KnowledgeCommunityLifecycleBatchRequest,
    ) -> Result<KnowledgeCommunityLifecycleBatchOutput> {
        update_knowledge_communities_batch_for(self, request)
    }

    pub fn delete_knowledge_communities(
        &mut self,
        request: &KnowledgeCommunityCleanupRequest,
    ) -> Result<KnowledgeCommunityCleanupOutput> {
        delete_knowledge_communities_for(self, request)
    }

    pub fn stamp_knowledge_graph_meta_batch(
        &mut self,
        request: &KnowledgeGraphMetaStampBatchRequest,
    ) -> Result<KnowledgeGraphMetaStampBatchOutput> {
        stamp_knowledge_graph_meta_batch_for(self, request)
    }

    pub fn delete_knowledge_graph_meta(
        &mut self,
        request: &KnowledgeGraphMetaRequest,
    ) -> Result<KnowledgeGraphMetaDeleteOutput> {
        delete_knowledge_graph_meta_for(self, request)
    }

    pub fn apply_knowledge_schema_migrations_batch(
        &mut self,
        request: &KnowledgeSchemaMigrationApplyBatchRequest,
    ) -> Result<KnowledgeSchemaMigrationApplyBatchOutput> {
        apply_knowledge_schema_migrations_batch_for(self, request)
    }

    pub fn update_knowledge_augmentation_jobs_batch(
        &mut self,
        request: &KnowledgeAugmentationJobLifecycleBatchRequest,
    ) -> Result<KnowledgeAugmentationJobLifecycleBatchOutput> {
        update_knowledge_augmentation_jobs_batch_for(self, request)
    }

    pub fn interrupt_knowledge_augmentation_jobs(
        &mut self,
        request: &KnowledgeAugmentationJobInterruptRequest,
    ) -> Result<KnowledgeAugmentationJobInterruptOutput> {
        interrupt_knowledge_augmentation_jobs_for(self, request)
    }

    pub fn delete_knowledge_entity(
        &mut self,
        request: &KnowledgeEntityDeleteRequest,
    ) -> Result<KnowledgeEntityDeleteOutput> {
        delete_knowledge_entity_for(self, request)
    }

    pub fn delete_scoped_knowledge_entity(
        &mut self,
        request: &KnowledgeScopedEntityDeleteRequest,
    ) -> Result<KnowledgeEntityDeleteOutput> {
        delete_scoped_knowledge_entity_for(self, request)
    }

    pub fn delete_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeEntityDeleteBatchRequest,
    ) -> Result<KnowledgeEntityDeleteBatchOutput> {
        delete_knowledge_entity_batch_for(self, request)
    }

    pub fn delete_scoped_knowledge_entity_batch(
        &mut self,
        request: &KnowledgeScopedEntityDeleteBatchRequest,
    ) -> Result<KnowledgeEntityDeleteBatchOutput> {
        delete_scoped_knowledge_entity_batch_for(self, request)
    }

    pub fn create_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipCreateRequest,
    ) -> Result<KnowledgeRelationshipCreateOutput> {
        create_knowledge_relationship_for(self, request)
    }

    pub fn create_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipCreateRequest,
    ) -> Result<KnowledgeRelationshipCreateOutput> {
        create_scoped_knowledge_relationship_for(self, request)
    }

    pub fn create_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipCreateBatchRequest,
    ) -> Result<KnowledgeRelationshipCreateBatchOutput> {
        create_knowledge_relationship_batch_for(self, request)
    }

    pub fn create_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipCreateBatchRequest,
    ) -> Result<KnowledgeRelationshipCreateBatchOutput> {
        create_scoped_knowledge_relationship_batch_for(self, request)
    }

    pub fn upsert_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipUpsertRequest,
    ) -> Result<KnowledgeRelationshipUpsertOutput> {
        upsert_knowledge_relationship_for(self, request)
    }

    pub fn upsert_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipUpsertRequest,
    ) -> Result<KnowledgeRelationshipUpsertOutput> {
        upsert_scoped_knowledge_relationship_for(self, request)
    }

    pub fn upsert_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipUpsertBatchRequest,
    ) -> Result<KnowledgeRelationshipUpsertBatchOutput> {
        upsert_knowledge_relationship_batch_for(self, request)
    }

    pub fn upsert_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipUpsertBatchRequest,
    ) -> Result<KnowledgeRelationshipUpsertBatchOutput> {
        upsert_scoped_knowledge_relationship_batch_for(self, request)
    }

    pub fn delete_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipDeleteRequest,
    ) -> Result<KnowledgeRelationshipDeleteOutput> {
        delete_knowledge_relationship_for(self, request)
    }

    pub fn delete_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipDeleteRequest,
    ) -> Result<KnowledgeRelationshipDeleteOutput> {
        delete_scoped_knowledge_relationship_for(self, request)
    }

    pub fn update_knowledge_relationship(
        &mut self,
        request: &KnowledgeRelationshipUpdateRequest,
    ) -> Result<KnowledgeRelationshipUpdateOutput> {
        update_knowledge_relationship_for(self, request)
    }

    pub fn update_scoped_knowledge_relationship(
        &mut self,
        request: &KnowledgeScopedRelationshipUpdateRequest,
    ) -> Result<KnowledgeRelationshipUpdateOutput> {
        update_scoped_knowledge_relationship_for(self, request)
    }

    pub fn update_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipUpdateBatchRequest,
    ) -> Result<KnowledgeRelationshipUpdateBatchOutput> {
        update_knowledge_relationship_batch_for(self, request)
    }

    pub fn update_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipUpdateBatchRequest,
    ) -> Result<KnowledgeRelationshipUpdateBatchOutput> {
        update_scoped_knowledge_relationship_batch_for(self, request)
    }

    pub fn delete_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeRelationshipDeleteBatchRequest,
    ) -> Result<KnowledgeRelationshipDeleteBatchOutput> {
        delete_knowledge_relationship_batch_for(self, request)
    }

    pub fn delete_scoped_knowledge_relationship_batch(
        &mut self,
        request: &KnowledgeScopedRelationshipDeleteBatchRequest,
    ) -> Result<KnowledgeRelationshipDeleteBatchOutput> {
        delete_scoped_knowledge_relationship_batch_for(self, request)
    }

    pub fn delete_knowledge_source_reference_relationships(
        &mut self,
        request: &KnowledgeSourceReferenceRelationshipCleanupRequest,
    ) -> Result<KnowledgeSourceReferenceRelationshipCleanupOutput> {
        delete_knowledge_source_reference_relationships_for(self, request)
    }
}

impl Database {
    fn ensure_writable(&self) -> Result<()> {
        self.store.ensure_usable()?;
        if self.config.read_only {
            return Err(SkeinError::Execution(
                "database is opened in read-only mode".to_string(),
            ));
        }
        Ok(())
    }

    pub fn project_graph(&self, rel_type: Option<&str>) -> ProjectedGraph {
        match rel_type {
            Some(name) => self
                .catalog
                .rel_type_id(name)
                .map(|rel_type_id| ProjectedGraph::from_store(&self.store, Some(rel_type_id)))
                .unwrap_or_else(|| ProjectedGraph::from_store_without_edges(&self.store)),
            None => ProjectedGraph::from_store(&self.store, None),
        }
    }
}

fn effective_database_config(mut config: DatabaseConfig) -> DatabaseConfig {
    config.runtime_capabilities =
        crate::compiled_capabilities::effective_runtime_capabilities(config.runtime_capabilities);
    config
}

struct KnowledgeRetrievalGraphContext<'a> {
    catalog: &'a Catalog,
    store: &'a GraphStore,
    compressed_vector_search_mode: CompressedVectorSearchMode,
    adaptive_vector_backend_policy: skein_optimizer::AdaptiveVectorBackendPolicy,
    query_memory_budget: NonZeroUsize,
    result_payload_budget: usize,
}

impl KnowledgeRetrievalGraphContext<'_> {
    fn retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> KnowledgeRetrievalOutput {
        self.retrieve_knowledge_internal(search_index, request, false)
            .expect("in-memory retrieval path does not perform fallible range I/O")
    }

    fn try_retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        self.retrieve_knowledge_internal(search_index, request, true)
    }

    fn retrieve_knowledge_internal(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
        use_physical_range_reads: bool,
    ) -> Result<KnowledgeRetrievalOutput> {
        let search_options = SearchQueryOptions {
            limit: request.limit,
            offset: request.offset,
            rank_window: request.rank_window,
            fusion_weights: request.search_fusion_weights,
            metadata_filters: request.metadata_filters.clone(),
            policy_epoch: None,
        };
        let search = if use_physical_range_reads {
            search_index.try_search_with_options_adaptive_vector_projection(
                &request.query_text,
                request.query_embedding.as_deref(),
                request.mode,
                search_options,
                AdaptiveVectorSearchOptions::new(self.compressed_vector_search_mode)
                    .with_backend_policy(self.adaptive_vector_backend_policy),
            )?
        } else {
            search_index.search_with_options_adaptive_vector_projection(
                &request.query_text,
                request.query_embedding.as_deref(),
                request.mode,
                search_options,
                AdaptiveVectorSearchOptions::new(self.compressed_vector_search_mode)
                    .with_backend_policy(self.adaptive_vector_backend_policy),
            )
        };
        self.retrieve_knowledge_from_search(search, search_index.projection_freshness(), request)
    }

    fn retrieve_knowledge_from_search(
        &self,
        mut search: SearchResultSet,
        projection_freshness: SearchProjectionFreshness,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        let graph_commit_epoch = self.store.commit_epoch();
        let mut pipeline = retrieval_pipeline::KnowledgeRetrievalPipelineBudget::new(
            self.query_memory_budget,
            self.result_payload_budget,
        )?;
        pipeline.enter(KnowledgeRetrievalStage::SearchCandidate)?;
        pipeline.enter(KnowledgeRetrievalStage::MetadataFilter)?;
        let canonical_search_nodes =
            self.canonical_search_nodes(&search, &request.metadata_filters)?;
        let search_hit_count = search.hits.len();
        search
            .hits
            .retain(|hit| canonical_search_nodes.contains_key(&hit.id));
        let canonical_identity_filtered_out_count =
            search_hit_count.saturating_sub(search.hits.len());
        let graph_seed_search = self.search_knowledge_graph_seeds(
            &request.query_text,
            request.graph_seed_limit,
            &request.metadata_filters,
        )?;
        pipeline.retain_working(knowledge_search_node_map_memory_bytes(
            &canonical_search_nodes,
        ))?;
        pipeline.retain_working(knowledge_graph_seed_candidates_memory_bytes(
            &graph_seed_search.seeds,
        ))?;
        pipeline.enter(KnowledgeRetrievalStage::AuthorizedGraphExpand)?;
        let graph_context_search = self.expand_knowledge_context(
            &canonical_search_nodes,
            &graph_seed_search.seeds,
            &request.metadata_filters,
            request.graph_context_limit,
            request.graph_context_max_hops,
        )?;
        pipeline.retain_working(knowledge_graph_context_candidates_memory_bytes(
            &graph_context_search.paths,
        ))?;
        let evidence = self.knowledge_evidence_for_search(
            &search,
            &canonical_search_nodes,
            &graph_context_search.paths,
        );
        pipeline.enter(KnowledgeRetrievalStage::Rerank)?;
        let mut candidates = self.knowledge_candidates(
            &search,
            &evidence,
            &graph_seed_search.seeds,
            &graph_context_search.paths,
            request.candidate_scoring,
        );
        pipeline.retain_working(knowledge_candidates_memory_bytes(&candidates))?;
        let candidate_total_count = candidates.len();
        pipeline.enter(KnowledgeRetrievalStage::TopK)?;
        let mut candidate_fanout_details = Vec::new();
        if let Some(limit) = request.candidate_limit {
            candidates.truncate(limit);
            if candidate_total_count > limit {
                candidate_fanout_details.push(KnowledgeFanoutReasonDetail::candidate_limit(
                    limit,
                    candidate_total_count,
                ));
            }
        }
        pipeline.enter(KnowledgeRetrievalStage::CanonicalHydration)?;
        let (graph_seeds, graph_context_paths, canonical_hydrated_node_count) = self
            .hydrate_knowledge_output(
                &mut candidates,
                &graph_seed_search.seeds,
                &graph_context_search.paths,
            )?;
        let required_projection_commit_epoch = self
            .store
            .search_projection_changefeed_status()
            .required_projection_commit_epoch();
        let retrievers = knowledge_retriever_reports(
            &search,
            &evidence,
            &graph_seeds,
            &graph_context_paths,
            &projection_freshness,
            KnowledgeGraphSeedRetrieverInput {
                limit: request.graph_seed_limit,
                input_candidate_count: graph_seed_search.input_candidate_count,
                input_filtered_out_count: graph_seed_search.input_filtered_out_count,
                metadata_filters: request.metadata_filters.clone(),
                candidate_count: graph_seed_search.candidate_count,
                graph_commit_epoch,
            },
        );
        let mut fanout_reason_details = graph_context_search.fanout_reason_details.clone();
        fanout_reason_details.extend(graph_seed_search.fanout_reason_details.clone());
        fanout_reason_details.extend(candidate_fanout_details);
        let fanout_reason_codes = knowledge_fanout_reason_codes(&fanout_reason_details);
        let fanout_reasons = knowledge_fanout_reason_messages(&fanout_reason_details);
        let (result_memory_bytes, result_payload_bytes) =
            knowledge_retrieval_result_resource_bytes(KnowledgeRetrievalResultResources {
                search: &search,
                retrievers: &retrievers,
                candidates: &candidates,
                evidence: &evidence,
                graph_seeds: &graph_seeds,
                graph_context_paths: &graph_context_paths,
                fanout_reason_details: &fanout_reason_details,
                fanout_reasons: &fanout_reasons,
            });
        pipeline.retain_result(result_memory_bytes, result_payload_bytes)?;
        let pipeline_report = pipeline.finish(
            graph_commit_epoch,
            canonical_identity_filtered_out_count,
            canonical_hydrated_node_count,
            candidates
                .iter()
                .filter(|candidate| candidate.entity.is_some())
                .count(),
            true,
        )?;
        let diagnostics = knowledge_retrieval_diagnostics(
            &search,
            request,
            &projection_freshness,
            graph_commit_epoch,
            required_projection_commit_epoch,
            KnowledgeRetrievalDiagnosticsInput {
                graph_seed_input_candidate_set: knowledge_graph_seed_input_candidate_set_report(
                    graph_seed_search.input_candidate_count,
                    graph_commit_epoch,
                    graph_seed_search.input_filtered_out_count,
                    request.metadata_filters.clone(),
                ),
                graph_seed_candidate_set: knowledge_graph_seed_candidate_set_report(
                    graph_seed_search.seeds.len(),
                    graph_commit_epoch,
                ),
                graph_seed_candidate_count: graph_seed_search.candidate_count,
                graph_seed_returned_count: graph_seeds.len(),
                graph_context_input_candidate_set:
                    knowledge_graph_context_input_candidate_set_report(
                        graph_context_search.input_seed_count,
                        graph_commit_epoch,
                    ),
                graph_context_candidate_set: knowledge_graph_context_candidate_set_report(
                    graph_context_search.expanded_relationship_count,
                    graph_commit_epoch,
                ),
                graph_context_path_count: graph_context_paths.len(),
                graph_context_node_count: knowledge_context_path_node_count(&graph_context_paths),
                graph_context_relationship_count: graph_context_paths.len(),
                graph_context_truncation_reasons: graph_context_search.truncation_reasons.clone(),
                fanout_reason_details: fanout_reason_details.clone(),
                candidate_count: candidates.len(),
                candidate_total_count,
                pipeline: pipeline_report,
            },
        );
        Ok(KnowledgeRetrievalOutput {
            graph_commit_epoch,
            projection_freshness,
            search,
            retrievers,
            diagnostics,
            candidates,
            evidence,
            graph_seeds,
            graph_context_paths,
            fanout_reason_codes,
            fanout_reason_details,
            fanout_reasons,
        })
    }

    fn expand_knowledge_context(
        &self,
        search_nodes: &BTreeMap<String, NodeId>,
        graph_seeds: &[KnowledgeGraphSeedCandidate],
        metadata_filters: &BTreeMap<String, String>,
        graph_context_limit: usize,
        graph_context_max_hops: usize,
    ) -> Result<KnowledgeGraphContextSearchOutput> {
        let mut paths = Vec::new();
        let mut fanout_reasons = Vec::new();
        let mut truncation_reasons = Vec::new();
        let mut seen_relationships = BTreeSet::new();
        let mut seen_frontier_nodes = BTreeSet::new();
        let mut reported_dense_groups = BTreeSet::new();
        let mut frontier = VecDeque::new();

        for (hit_id, seed_node) in search_nodes {
            if seen_frontier_nodes.insert((hit_id.clone(), seed_node.0)) {
                frontier.push_back((hit_id.clone(), *seed_node, 0usize));
            }
        }
        for seed in graph_seeds {
            let seed_id = graph_seed_candidate_id_internal(seed);
            let seed_node = seed.node_id;
            if seen_frontier_nodes.insert((seed_id.clone(), seed_node.0)) {
                frontier.push_back((seed_id, seed_node, 0usize));
            }
        }
        let input_seed_count = seen_frontier_nodes.len();

        while let Some((seed_hit_id, current_node, depth)) = frontier.pop_front() {
            if depth >= graph_context_max_hops {
                continue;
            }
            record_dense_adjacency_diagnostics(
                DenseAdjacencyDiagnosticContext {
                    catalog: self.catalog,
                    store: self.store,
                    operation: "graph_context",
                    relationship_type: None,
                    requested_direction: KnowledgeNeighborDirection::Both,
                },
                current_node,
                &mut reported_dense_groups,
                &mut fanout_reasons,
            );
            for edge in knowledge_expansion_edges_for_node(
                self.store,
                current_node,
                None,
                KnowledgeNeighborDirection::Both,
                graph_context_limit
                    .saturating_sub(paths.len())
                    .saturating_add(1),
            )? {
                if !self.graph_expansion_node_is_authorized(edge.next_node, metadata_filters)? {
                    continue;
                }
                if !seen_relationships.insert((seed_hit_id.clone(), edge.relationship.id.0)) {
                    continue;
                }
                if paths.len() >= graph_context_limit {
                    let detail = KnowledgeFanoutReasonDetail::graph_context_limit(
                        graph_context_limit,
                        &seed_hit_id,
                    );
                    truncation_reasons.push(detail.message.clone());
                    fanout_reasons.push(detail);
                    let expanded_relationship_count = paths.len();
                    return Ok(KnowledgeGraphContextSearchOutput {
                        paths,
                        input_seed_count,
                        expanded_relationship_count,
                        fanout_reason_details: fanout_reasons,
                        truncation_reasons,
                    });
                }
                paths.push(KnowledgeGraphContextCandidate {
                    seed_hit_id: seed_hit_id.clone(),
                    hop: depth + 1,
                    direction: edge.direction,
                    relationship_id: edge.relationship.id,
                    source_node_id: edge.relationship.source,
                    target_node_id: edge.relationship.target,
                });
                if seen_frontier_nodes.insert((seed_hit_id.clone(), edge.next_node.0)) {
                    frontier.push_back((seed_hit_id.clone(), edge.next_node, depth + 1));
                }
            }
        }

        let expanded_relationship_count = paths.len();
        Ok(KnowledgeGraphContextSearchOutput {
            paths,
            input_seed_count,
            expanded_relationship_count,
            fanout_reason_details: fanout_reasons,
            truncation_reasons,
        })
    }

    fn graph_expansion_node_is_authorized(
        &self,
        node_id: NodeId,
        metadata_filters: &BTreeMap<String, String>,
    ) -> Result<bool> {
        let scope_filters = knowledge_graph_expansion_scope_filters(metadata_filters);
        if scope_filters.is_empty() {
            return Ok(true);
        }
        let Some(node) = self.store.node_owned(node_id)? else {
            return Ok(false);
        };
        try_knowledge_graph_seed_matches_filters(self.catalog, self.store, &node, &scope_filters)
    }

    fn seed_node_for_hit(
        &self,
        kind: Option<&str>,
        external_id: Option<&str>,
        metadata_filters: &BTreeMap<String, String>,
    ) -> Result<Option<NodeRecord>> {
        let Some(external_id) = external_id else {
            return Ok(None);
        };
        let Some(label) = kind.and_then(search_kind_to_label) else {
            return Ok(None);
        };
        let Some(node) =
            try_seed_node_by_label_and_external_id(self.catalog, self.store, label, external_id)?
        else {
            return Ok(None);
        };
        if !try_knowledge_graph_seed_matches_filters(
            self.catalog,
            self.store,
            &node,
            metadata_filters,
        )? {
            return Ok(None);
        }
        Ok(Some(node))
    }

    fn canonical_search_nodes(
        &self,
        search: &SearchResultSet,
        metadata_filters: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, NodeId>> {
        let scope_filters = knowledge_graph_expansion_scope_filters(metadata_filters);
        let mut nodes = BTreeMap::new();
        for hit in &search.hits {
            if let Some(node) = self.seed_node_for_hit(
                hit.kind.as_deref(),
                hit.external_id.as_deref(),
                &scope_filters,
            )? {
                nodes.insert(hit.id.clone(), node.id);
            }
        }
        Ok(nodes)
    }

    fn knowledge_entity_from_node(&self, node: &NodeRecord) -> KnowledgeEntity {
        knowledge_entity_from_node(self.catalog, node)
    }

    fn knowledge_evidence_for_search(
        &self,
        search: &SearchResultSet,
        canonical_search_nodes: &BTreeMap<String, NodeId>,
        graph_context_paths: &[KnowledgeGraphContextCandidate],
    ) -> Vec<KnowledgeEvidence> {
        let mut evidence = Vec::with_capacity(search.hits.len());
        for hit in &search.hits {
            let canonical_node_id = canonical_search_nodes.get(&hit.id).map(|node_id| node_id.0);
            let graph_context_path_count = graph_context_paths
                .iter()
                .filter(|path| path.seed_hit_id == hit.id)
                .count();
            evidence.push(KnowledgeEvidence {
                hit_id: hit.id.clone(),
                kind: hit.kind.clone(),
                external_id: hit.external_id.clone(),
                source_id: hit.source_id.clone(),
                canonical_node_id,
                graph_context_path_count,
                matched_terms: hit.matched_terms.clone(),
                matched_spans: hit.matched_spans.clone(),
                score: hit.score,
                rrf_score: hit.rrf_score,
                vector_rrf_score: hit.vector_rrf_score,
                text_rrf_score: hit.text_rrf_score,
                vector_score: hit.vector_score,
                text_score: hit.text_score,
                vector_rank: hit.vector_rank,
                text_rank: hit.text_rank,
            });
        }
        evidence
    }

    fn knowledge_candidates(
        &self,
        search: &SearchResultSet,
        evidence: &[KnowledgeEvidence],
        graph_seeds: &[KnowledgeGraphSeedCandidate],
        graph_context_paths: &[KnowledgeGraphContextCandidate],
        scoring: KnowledgeCandidateScoringPolicy,
    ) -> Vec<KnowledgeCandidate> {
        let mut candidates = Vec::with_capacity(search.hits.len());
        for (index, (hit, evidence)) in search.hits.iter().zip(evidence.iter()).enumerate() {
            let score_breakdown =
                knowledge_candidate_score_breakdown(Some(hit.score), None, scoring);
            candidates.push(KnowledgeCandidate {
                id: hit.id.clone(),
                canonical_node_id: evidence.canonical_node_id,
                source: KnowledgeCandidateSource::SearchHit,
                source_rank: index + 1,
                merged_sources: vec![KnowledgeCandidateSource::SearchHit],
                score: score_breakdown.combined_score,
                score_breakdown,
                entity: None,
                evidence: Some(evidence.clone()),
                matched_properties: Vec::new(),
                graph_context_path_count: evidence.graph_context_path_count,
            });
        }

        for (index, seed) in graph_seeds.iter().enumerate() {
            let seed_candidate_id = graph_seed_candidate_id_internal(seed);
            let seed_graph_context_path_count = graph_context_paths
                .iter()
                .filter(|path| path.seed_hit_id == seed_candidate_id)
                .count();
            if let Some(candidate) = candidates
                .iter_mut()
                .find(|candidate| candidate.canonical_node_id == Some(seed.node_id.0))
            {
                candidate.score_breakdown = knowledge_candidate_score_breakdown(
                    candidate.score_breakdown.search_score,
                    Some(seed.score),
                    scoring,
                );
                candidate.score = candidate.score_breakdown.combined_score;
                if !candidate
                    .merged_sources
                    .contains(&KnowledgeCandidateSource::GraphSeed)
                {
                    candidate
                        .merged_sources
                        .push(KnowledgeCandidateSource::GraphSeed);
                }
                for property in &seed.matched_properties {
                    if !candidate.matched_properties.contains(property) {
                        candidate.matched_properties.push(property.clone());
                    }
                }
                candidate.graph_context_path_count += seed_graph_context_path_count;
                continue;
            }
            let score_breakdown =
                knowledge_candidate_score_breakdown(None, Some(seed.score), scoring);
            candidates.push(KnowledgeCandidate {
                id: seed_candidate_id,
                canonical_node_id: Some(seed.node_id.0),
                source: KnowledgeCandidateSource::GraphSeed,
                source_rank: index + 1,
                merged_sources: vec![KnowledgeCandidateSource::GraphSeed],
                score: score_breakdown.combined_score,
                score_breakdown,
                entity: None,
                evidence: None,
                matched_properties: seed.matched_properties.clone(),
                graph_context_path_count: seed_graph_context_path_count,
            });
        }

        candidates.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.source_rank.cmp(&right.source_rank))
                .then_with(|| left.id.cmp(&right.id))
        });
        candidates
    }

    fn hydrate_knowledge_output(
        &self,
        candidates: &mut [KnowledgeCandidate],
        graph_seeds: &[KnowledgeGraphSeedCandidate],
        graph_context_paths: &[KnowledgeGraphContextCandidate],
    ) -> Result<(
        Vec<KnowledgeGraphSeed>,
        Vec<KnowledgeGraphContextPath>,
        usize,
    )> {
        let node_ids = candidates
            .iter()
            .filter_map(|candidate| candidate.canonical_node_id.map(NodeId))
            .chain(graph_seeds.iter().map(|seed| seed.node_id))
            .chain(
                graph_context_paths
                    .iter()
                    .flat_map(|path| [path.source_node_id, path.target_node_id]),
            )
            .collect::<BTreeSet<_>>();
        let entities = self.hydrate_knowledge_entities(&node_ids)?;

        for candidate in candidates {
            candidate.entity = candidate
                .canonical_node_id
                .and_then(|node_id| entities.get(&NodeId(node_id)).cloned());
        }
        let hydrated_seeds = graph_seeds
            .iter()
            .map(|seed| {
                let entity = entities.get(&seed.node_id).cloned().ok_or_else(|| {
                    SkeinError::StorageIntegrity(format!(
                        "knowledge retrieval graph seed references missing canonical node {}",
                        seed.node_id.0
                    ))
                })?;
                Ok(KnowledgeGraphSeed {
                    entity,
                    score: seed.score,
                    matched_properties: seed.matched_properties.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let hydrated_paths = graph_context_paths
            .iter()
            .map(|path| self.hydrate_graph_context_path(path, &entities))
            .collect::<Result<Vec<_>>>()?;
        Ok((hydrated_seeds, hydrated_paths, entities.len()))
    }

    fn hydrate_knowledge_entities(
        &self,
        node_ids: &BTreeSet<NodeId>,
    ) -> Result<BTreeMap<NodeId, KnowledgeEntity>> {
        let mut entities = BTreeMap::new();
        for node_id in node_ids {
            let Some(node) = self.store.node_owned(*node_id)? else {
                return Err(SkeinError::StorageIntegrity(format!(
                    "knowledge retrieval canonical hydration references missing node {}",
                    node_id.0
                )));
            };
            entities.insert(*node_id, self.knowledge_entity_from_node(&node));
        }
        Ok(entities)
    }

    fn hydrate_graph_context_path(
        &self,
        path: &KnowledgeGraphContextCandidate,
        entities: &BTreeMap<NodeId, KnowledgeEntity>,
    ) -> Result<KnowledgeGraphContextPath> {
        let relationship = self
            .store
            .relationship_owned(path.relationship_id)?
            .ok_or_else(|| {
                SkeinError::StorageIntegrity(format!(
                    "knowledge retrieval graph context references missing relationship {}",
                    path.relationship_id.0
                ))
            })?;
        let source = entities.get(&path.source_node_id).ok_or_else(|| {
            SkeinError::StorageIntegrity(format!(
                "knowledge retrieval graph context references missing source node {}",
                path.source_node_id.0
            ))
        })?;
        let target = entities.get(&path.target_node_id).ok_or_else(|| {
            SkeinError::StorageIntegrity(format!(
                "knowledge retrieval graph context references missing target node {}",
                path.target_node_id.0
            ))
        })?;
        Ok(KnowledgeGraphContextPath {
            seed_hit_id: path.seed_hit_id.clone(),
            hop: path.hop,
            direction: path.direction,
            relationship_id: path.relationship_id.0,
            relationship_type: self
                .catalog
                .rel_type_name(relationship.rel_type)
                .unwrap_or("<unknown>")
                .to_string(),
            relationship_properties: relationship.properties,
            source_node_id: path.source_node_id.0,
            source_labels: source.labels.clone(),
            source_external_id: source.external_id.clone(),
            target_node_id: path.target_node_id.0,
            target_labels: target.labels.clone(),
            target_external_id: target.external_id.clone(),
        })
    }

    fn search_knowledge_graph_seeds(
        &self,
        query_text: &str,
        limit: usize,
        metadata_filters: &BTreeMap<String, String>,
    ) -> Result<KnowledgeGraphSeedSearchOutput> {
        if limit == 0 {
            return Ok(KnowledgeGraphSeedSearchOutput::default());
        }
        let query_terms = knowledge_query_terms(query_text);
        if query_terms.is_empty() {
            return Ok(KnowledgeGraphSeedSearchOutput::default());
        }
        let normalized_query = query_text.trim().to_ascii_lowercase();
        let mut input_candidate_count = 0usize;
        let mut input_filtered_out_count = 0usize;
        let mut candidate_count = 0usize;
        let mut scored = Vec::new();
        let mut scan_error = None;
        self.store.visit_nodes_owned(None, |node| {
            let matches = match try_knowledge_graph_seed_matches_filters(
                self.catalog,
                self.store,
                &node,
                metadata_filters,
            ) {
                Ok(matches) => matches,
                Err(error) => {
                    scan_error = Some(error);
                    return crate::store::GraphScanControl::Stop;
                }
            };
            if !matches {
                input_filtered_out_count += 1;
                return crate::store::GraphScanControl::Continue;
            }
            input_candidate_count += 1;
            if let Some(seed) = {
                let (score, matched_properties) =
                    graph_seed_score(&node, &query_terms, &normalized_query);
                (score > 0.0).then(|| KnowledgeGraphSeedCandidate {
                    id: graph_seed_candidate_id_from_node(self.catalog, &node),
                    node_id: node.id,
                    score,
                    matched_properties,
                })
            } {
                candidate_count = candidate_count.saturating_add(1);
                scored.push(seed);
                scored.sort_by(|left, right| {
                    right
                        .score
                        .partial_cmp(&left.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| left.node_id.cmp(&right.node_id))
                });
                scored.truncate(limit);
            }
            crate::store::GraphScanControl::Continue
        })?;
        if let Some(error) = scan_error {
            return Err(error);
        }

        let fanout_reasons = if candidate_count > limit {
            vec![KnowledgeFanoutReasonDetail::graph_seed_limit(
                limit,
                candidate_count,
            )]
        } else {
            Vec::new()
        };
        Ok(KnowledgeGraphSeedSearchOutput {
            seeds: scored,
            input_candidate_count,
            input_filtered_out_count,
            candidate_count,
            fanout_reason_details: fanout_reasons,
        })
    }
}

#[derive(Debug, Clone, Default)]
struct KnowledgeGraphContextSearchOutput {
    paths: Vec<KnowledgeGraphContextCandidate>,
    input_seed_count: usize,
    expanded_relationship_count: usize,
    fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    truncation_reasons: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct KnowledgeGraphSeedSearchOutput {
    seeds: Vec<KnowledgeGraphSeedCandidate>,
    input_candidate_count: usize,
    input_filtered_out_count: usize,
    candidate_count: usize,
    fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
}

#[derive(Debug, Clone)]
struct KnowledgeGraphSeedCandidate {
    id: String,
    node_id: NodeId,
    score: f64,
    matched_properties: Vec<String>,
}

#[derive(Debug, Clone)]
struct KnowledgeGraphContextCandidate {
    seed_hit_id: String,
    hop: usize,
    direction: KnowledgeGraphPathDirection,
    relationship_id: RelId,
    source_node_id: NodeId,
    target_node_id: NodeId,
}

fn knowledge_search_node_map_memory_bytes(nodes: &BTreeMap<String, NodeId>) -> usize {
    std::mem::size_of::<BTreeMap<String, NodeId>>().saturating_add(nodes.iter().fold(
        0usize,
        |bytes, (hit_id, _)| {
            bytes
                .saturating_add(std::mem::size_of::<(String, NodeId)>() * 3)
                .saturating_add(hit_id.len())
        },
    ))
}

fn knowledge_graph_seed_candidates_memory_bytes(seeds: &[KnowledgeGraphSeedCandidate]) -> usize {
    std::mem::size_of_val(seeds).saturating_add(seeds.iter().fold(0usize, |bytes, seed| {
        bytes
            .saturating_add(std::mem::size_of::<KnowledgeGraphSeedCandidate>())
            .saturating_add(seed.id.len())
            .saturating_add(string_slice_bytes(&seed.matched_properties))
    }))
}

fn knowledge_graph_context_candidates_memory_bytes(
    paths: &[KnowledgeGraphContextCandidate],
) -> usize {
    std::mem::size_of_val(paths).saturating_add(paths.iter().fold(0usize, |bytes, path| {
        bytes
            .saturating_add(std::mem::size_of::<KnowledgeGraphContextCandidate>())
            .saturating_add(path.seed_hit_id.len())
    }))
}

fn knowledge_candidates_memory_bytes(candidates: &[KnowledgeCandidate]) -> usize {
    std::mem::size_of_val(candidates).saturating_add(
        candidates
            .iter()
            .map(knowledge_candidate_resource_bytes)
            .fold(0usize, usize::saturating_add),
    )
}

struct KnowledgeRetrievalResultResources<'a> {
    search: &'a SearchResultSet,
    retrievers: &'a [KnowledgeRetrieverReport],
    candidates: &'a [KnowledgeCandidate],
    evidence: &'a [KnowledgeEvidence],
    graph_seeds: &'a [KnowledgeGraphSeed],
    graph_context_paths: &'a [KnowledgeGraphContextPath],
    fanout_reason_details: &'a [KnowledgeFanoutReasonDetail],
    fanout_reasons: &'a [String],
}

fn knowledge_retrieval_result_resource_bytes(
    resources: KnowledgeRetrievalResultResources<'_>,
) -> (usize, usize) {
    let payload_bytes = resources
        .search
        .hits
        .iter()
        .map(search_hit_payload_bytes)
        .chain(
            resources
                .retrievers
                .iter()
                .map(knowledge_retriever_payload_bytes),
        )
        .chain(
            resources
                .candidates
                .iter()
                .map(knowledge_candidate_payload_bytes),
        )
        .chain(
            resources
                .evidence
                .iter()
                .map(knowledge_evidence_payload_bytes),
        )
        .chain(
            resources
                .graph_seeds
                .iter()
                .map(knowledge_graph_seed_payload_bytes),
        )
        .chain(
            resources
                .graph_context_paths
                .iter()
                .map(knowledge_graph_context_path_payload_bytes),
        )
        .chain(
            resources
                .fanout_reason_details
                .iter()
                .map(knowledge_fanout_detail_payload_bytes),
        )
        .fold(
            string_slice_bytes(resources.fanout_reasons),
            usize::saturating_add,
        );
    let item_count = resources
        .search
        .hits
        .len()
        .saturating_add(resources.retrievers.len())
        .saturating_add(resources.candidates.len())
        .saturating_add(resources.evidence.len())
        .saturating_add(resources.graph_seeds.len())
        .saturating_add(resources.graph_context_paths.len())
        .saturating_add(resources.fanout_reason_details.len())
        .saturating_add(resources.fanout_reasons.len());
    let memory_bytes = payload_bytes
        .saturating_add(std::mem::size_of::<KnowledgeRetrievalOutput>())
        .saturating_add(item_count.saturating_mul(std::mem::size_of::<usize>() * 4));
    (memory_bytes, payload_bytes)
}

fn string_slice_bytes(values: &[String]) -> usize {
    values.iter().fold(0usize, |bytes, value| {
        bytes
            .saturating_add(std::mem::size_of::<String>())
            .saturating_add(value.len())
    })
}

fn option_string_bytes(value: &Option<String>) -> usize {
    value.as_ref().map_or(0, |value| value.len())
}

fn matched_span_payload_bytes(span: &SearchMatchedSpan) -> usize {
    span.field
        .len()
        .saturating_add(span.text.len())
        .saturating_add(span.term.len())
}

fn search_hit_payload_bytes(hit: &crate::search::SearchHit) -> usize {
    hit.id
        .len()
        .saturating_add(option_string_bytes(&hit.kind))
        .saturating_add(option_string_bytes(&hit.external_id))
        .saturating_add(option_string_bytes(&hit.source_id))
        .saturating_add(string_slice_bytes(&hit.matched_terms))
        .saturating_add(
            hit.matched_spans
                .iter()
                .map(matched_span_payload_bytes)
                .fold(0usize, usize::saturating_add),
        )
        .saturating_add(string_slice_bytes(&hit.fallback_reasons))
}

fn knowledge_entity_payload_bytes(entity: &KnowledgeEntity) -> usize {
    string_slice_bytes(&entity.labels)
        .saturating_add(option_string_bytes(&entity.external_id))
        .saturating_add(skein_executor::binding::map_payload_bytes(
            &entity.properties,
        ))
}

fn knowledge_evidence_payload_bytes(evidence: &KnowledgeEvidence) -> usize {
    evidence
        .hit_id
        .len()
        .saturating_add(option_string_bytes(&evidence.kind))
        .saturating_add(option_string_bytes(&evidence.external_id))
        .saturating_add(option_string_bytes(&evidence.source_id))
        .saturating_add(string_slice_bytes(&evidence.matched_terms))
        .saturating_add(
            evidence
                .matched_spans
                .iter()
                .map(matched_span_payload_bytes)
                .fold(0usize, usize::saturating_add),
        )
}

fn knowledge_candidate_payload_bytes(candidate: &KnowledgeCandidate) -> usize {
    candidate
        .id
        .len()
        .saturating_add(string_slice_bytes(&candidate.matched_properties))
        .saturating_add(
            candidate
                .entity
                .as_ref()
                .map_or(0, knowledge_entity_payload_bytes),
        )
        .saturating_add(
            candidate
                .evidence
                .as_ref()
                .map_or(0, knowledge_evidence_payload_bytes),
        )
}

fn knowledge_candidate_resource_bytes(candidate: &KnowledgeCandidate) -> usize {
    std::mem::size_of::<KnowledgeCandidate>()
        .saturating_add(knowledge_candidate_payload_bytes(candidate))
        .saturating_add(
            candidate
                .merged_sources
                .len()
                .saturating_mul(std::mem::size_of::<KnowledgeCandidateSource>()),
        )
}

fn knowledge_graph_seed_payload_bytes(seed: &KnowledgeGraphSeed) -> usize {
    knowledge_entity_payload_bytes(&seed.entity)
        .saturating_add(string_slice_bytes(&seed.matched_properties))
}

fn knowledge_graph_context_path_payload_bytes(path: &KnowledgeGraphContextPath) -> usize {
    path.seed_hit_id
        .len()
        .saturating_add(path.relationship_type.len())
        .saturating_add(skein_executor::binding::map_payload_bytes(
            &path.relationship_properties,
        ))
        .saturating_add(string_slice_bytes(&path.source_labels))
        .saturating_add(option_string_bytes(&path.source_external_id))
        .saturating_add(string_slice_bytes(&path.target_labels))
        .saturating_add(option_string_bytes(&path.target_external_id))
}

fn knowledge_retriever_payload_bytes(report: &KnowledgeRetrieverReport) -> usize {
    report
        .name
        .len()
        .saturating_add(report.backend.len())
        .saturating_add(string_slice_bytes(&report.fallback_reasons))
        .saturating_add(string_slice_bytes(&report.truncation_reasons))
        .saturating_add(
            report
                .top_candidates
                .iter()
                .fold(0usize, |bytes, candidate| {
                    bytes
                        .saturating_add(candidate.id.len())
                        .saturating_add(option_string_bytes(&candidate.kind))
                        .saturating_add(option_string_bytes(&candidate.external_id))
                        .saturating_add(option_string_bytes(&candidate.source_id))
                        .saturating_add(
                            candidate
                                .matched_spans
                                .iter()
                                .map(matched_span_payload_bytes)
                                .fold(0usize, usize::saturating_add),
                        )
                }),
        )
}

fn knowledge_fanout_detail_payload_bytes(detail: &KnowledgeFanoutReasonDetail) -> usize {
    detail
        .message
        .len()
        .saturating_add(option_string_bytes(&detail.operation))
        .saturating_add(option_string_bytes(&detail.seed_hit_id))
        .saturating_add(option_string_bytes(&detail.relationship_type))
        .saturating_add(option_string_bytes(&detail.direction))
}

fn knowledge_retriever_reports(
    search: &SearchResultSet,
    evidence: &[KnowledgeEvidence],
    graph_seeds: &[KnowledgeGraphSeed],
    graph_context_paths: &[KnowledgeGraphContextPath],
    projection_freshness: &SearchProjectionFreshness,
    graph_seed_input: KnowledgeGraphSeedRetrieverInput,
) -> Vec<KnowledgeRetrieverReport> {
    let evidence_by_hit = evidence
        .iter()
        .map(|evidence| (evidence.hit_id.as_str(), evidence))
        .collect::<BTreeMap<_, _>>();
    let mut reports = search
        .retrievers
        .iter()
        .map(|report| KnowledgeRetrieverReport {
            name: report.name.clone(),
            backend: report.backend.clone(),
            available: report.available,
            input_candidate_set: report.input_candidate_set.clone(),
            candidate_count: report.candidate_count,
            candidate_set: report.candidate_set.clone(),
            limit: Some(search.limit),
            rank_window: search.rank_window,
            fusion_weight: knowledge_search_retriever_fusion_weight(
                report.name.as_str(),
                search.fusion_weights,
            ),
            fallback_reason_codes: report.fallback_reason_codes.clone(),
            knowledge_fallback_reason_codes: Vec::new(),
            fallback_reasons: report.fallback_reasons.clone(),
            truncated: report.candidate_count > report.top_candidates.len(),
            truncation_reason_codes: knowledge_search_retriever_truncation_reason_codes(
                report.candidate_count,
                report.top_candidates.len(),
                search.limit,
                search.rank_window,
            ),
            truncation_reasons: knowledge_search_retriever_truncation_reasons(
                report.name.as_str(),
                report.candidate_count,
                report.top_candidates.len(),
                search.limit,
                search.rank_window,
            ),
            top_candidates: report
                .top_candidates
                .iter()
                .map(|candidate| {
                    let evidence = evidence_by_hit.get(candidate.id.as_str()).copied();
                    KnowledgeRetrieverCandidate {
                        id: candidate.id.clone(),
                        kind: evidence.and_then(|evidence| evidence.kind.clone()),
                        external_id: evidence.and_then(|evidence| evidence.external_id.clone()),
                        source_id: evidence.and_then(|evidence| evidence.source_id.clone()),
                        canonical_node_id: evidence.and_then(|evidence| evidence.canonical_node_id),
                        rank: candidate.rank,
                        score: candidate.score,
                        matched_spans: evidence
                            .map(|evidence| evidence.matched_spans.clone())
                            .unwrap_or_default(),
                        graph_context_path_count: evidence
                            .map(|evidence| evidence.graph_context_path_count)
                            .unwrap_or_default(),
                        projection_freshness: Some(projection_freshness.clone()),
                    }
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    reports.push(KnowledgeRetrieverReport {
        name: "graph_seed".to_string(),
        backend: "graph_seed_expand".to_string(),
        available: graph_seed_input.limit > 0,
        input_candidate_set: knowledge_graph_seed_input_candidate_set_report(
            graph_seed_input.input_candidate_count,
            graph_seed_input.graph_commit_epoch,
            graph_seed_input.input_filtered_out_count,
            graph_seed_input.metadata_filters,
        ),
        candidate_count: graph_seed_input.candidate_count,
        candidate_set: knowledge_graph_seed_candidate_set_report(
            graph_seeds.len(),
            graph_seed_input.graph_commit_epoch,
        ),
        limit: Some(graph_seed_input.limit),
        rank_window: None,
        fusion_weight: None,
        fallback_reason_codes: Vec::new(),
        knowledge_fallback_reason_codes: knowledge_graph_seed_fallback_reason_codes(
            graph_seed_input.limit,
        ),
        fallback_reasons: knowledge_graph_seed_fallback_reasons(graph_seed_input.limit),
        truncated: graph_seed_input.candidate_count > graph_seeds.len(),
        truncation_reason_codes: knowledge_graph_seed_truncation_reason_codes(
            graph_seed_input.candidate_count,
            graph_seeds.len(),
        ),
        truncation_reasons: knowledge_graph_seed_truncation_reasons(
            graph_seed_input.candidate_count,
            graph_seeds.len(),
            graph_seed_input.limit,
        ),
        top_candidates: graph_seeds
            .iter()
            .enumerate()
            .map(|(index, seed)| {
                let id = graph_seed_candidate_id(seed);
                let graph_context_path_count = graph_context_paths
                    .iter()
                    .filter(|path| path.seed_hit_id == id)
                    .count();
                KnowledgeRetrieverCandidate {
                    id,
                    kind: seed.entity.labels.first().cloned(),
                    external_id: seed.entity.external_id.clone(),
                    source_id: None,
                    canonical_node_id: Some(seed.entity.node_id),
                    rank: index + 1,
                    score: seed.score,
                    matched_spans: Vec::new(),
                    graph_context_path_count,
                    projection_freshness: None,
                }
            })
            .collect(),
    });
    reports
}

#[derive(Debug, Clone)]
struct KnowledgeGraphSeedRetrieverInput {
    limit: usize,
    input_candidate_count: usize,
    input_filtered_out_count: usize,
    metadata_filters: BTreeMap<String, String>,
    candidate_count: usize,
    graph_commit_epoch: u64,
}

fn knowledge_search_retriever_truncation_reasons(
    name: &str,
    candidate_count: usize,
    returned_count: usize,
    search_limit: usize,
    rank_window: Option<usize>,
) -> Vec<String> {
    if candidate_count <= returned_count {
        return Vec::new();
    }
    let mut reasons = Vec::new();
    if let Some(rank_window) = rank_window
        && candidate_count > rank_window
        && returned_count <= rank_window
    {
        reasons.push(format!(
            "{name} rank_window {rank_window} returned from {candidate_count} candidates"
        ));
    }
    if returned_count >= search_limit && candidate_count > search_limit {
        reasons.push(format!(
            "{name} search_limit {search_limit} returned from {candidate_count} candidates"
        ));
    }
    if reasons.is_empty() {
        reasons.push(format!(
            "{name} returned {returned_count} of {candidate_count} candidates"
        ));
    }
    reasons
}

fn knowledge_search_retriever_truncation_reason_codes(
    candidate_count: usize,
    returned_count: usize,
    search_limit: usize,
    rank_window: Option<usize>,
) -> Vec<KnowledgeTruncationReasonCode> {
    if candidate_count <= returned_count {
        return Vec::new();
    }
    let mut codes = Vec::new();
    if let Some(rank_window) = rank_window
        && candidate_count > rank_window
        && returned_count <= rank_window
    {
        codes.push(KnowledgeTruncationReasonCode::RankWindowExceeded);
    }
    if returned_count >= search_limit && candidate_count > search_limit {
        codes.push(KnowledgeTruncationReasonCode::SearchLimitExceeded);
    }
    if codes.is_empty() {
        codes.push(KnowledgeTruncationReasonCode::PartialCandidateReturn);
    }
    codes
}

fn knowledge_search_retriever_fusion_weight(
    name: &str,
    weights: SearchFusionWeights,
) -> Option<f64> {
    match name {
        "vector" => Some(weights.vector_weight),
        "text" => Some(weights.text_weight),
        _ => None,
    }
}

fn knowledge_graph_seed_truncation_reasons(
    candidate_count: usize,
    returned_count: usize,
    graph_seed_limit: usize,
) -> Vec<String> {
    if candidate_count > returned_count {
        vec![format!(
            "graph_seed limit {graph_seed_limit} returned from {candidate_count} candidates"
        )]
    } else {
        Vec::new()
    }
}

fn knowledge_graph_seed_truncation_reason_codes(
    candidate_count: usize,
    returned_count: usize,
) -> Vec<KnowledgeTruncationReasonCode> {
    if candidate_count > returned_count {
        vec![KnowledgeTruncationReasonCode::GraphSeedLimitExceeded]
    } else {
        Vec::new()
    }
}

fn knowledge_graph_seed_fallback_reasons(graph_seed_limit: usize) -> Vec<String> {
    if graph_seed_limit == 0 {
        vec!["graph seed retriever disabled by limit 0".to_string()]
    } else {
        Vec::new()
    }
}

fn knowledge_graph_seed_fallback_reason_codes(
    graph_seed_limit: usize,
) -> Vec<KnowledgeFallbackReasonCode> {
    if graph_seed_limit == 0 {
        vec![KnowledgeFallbackReasonCode::GraphSeedLimitZero]
    } else {
        Vec::new()
    }
}

fn knowledge_graph_seed_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
) -> SearchRetrieverCandidateSetReport {
    SearchRetrieverCandidateSetReport {
        id_space: "canonical_graph_node_id".to_string(),
        representation: "ranked_node_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
    }
}

fn knowledge_graph_seed_input_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
    filtered_out_count: usize,
    metadata_filters: BTreeMap<String, String>,
) -> SearchCandidateSetReport {
    let metadata_predicate_pushdown = search_metadata_predicate_pushdown(&metadata_filters).report;
    SearchCandidateSetReport {
        id_space: "canonical_graph_node_id".to_string(),
        representation: "filtered_node_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
        filtered_out_count,
        metadata_filters,
        metadata_predicate_pushdown,
    }
}

fn knowledge_graph_context_input_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
) -> SearchCandidateSetReport {
    SearchCandidateSetReport {
        id_space: "canonical_graph_node_id".to_string(),
        representation: "context_seed_node_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
        filtered_out_count: 0,
        metadata_filters: BTreeMap::new(),
        metadata_predicate_pushdown: SearchPredicatePushdownReport::default(),
    }
}

fn knowledge_graph_context_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
) -> SearchRetrieverCandidateSetReport {
    SearchRetrieverCandidateSetReport {
        id_space: "canonical_graph_relationship_id".to_string(),
        representation: "expanded_relationship_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
    }
}

fn knowledge_graph_context_fallback_reasons(request: &KnowledgeRetrievalRequest) -> Vec<String> {
    let mut reasons = Vec::new();
    if request.graph_context_limit == 0 {
        reasons.push("graph context expansion disabled by limit 0".to_string());
    }
    if request.graph_context_max_hops == 0 {
        reasons.push("graph context expansion disabled by max_hops 0".to_string());
    }
    reasons
}

fn knowledge_graph_context_fallback_reason_codes(
    request: &KnowledgeRetrievalRequest,
) -> Vec<KnowledgeFallbackReasonCode> {
    let mut codes = Vec::new();
    if request.graph_context_limit == 0 {
        codes.push(KnowledgeFallbackReasonCode::GraphContextLimitZero);
    }
    if request.graph_context_max_hops == 0 {
        codes.push(KnowledgeFallbackReasonCode::GraphContextMaxHopsZero);
    }
    codes
}

#[derive(Debug, Clone)]
struct KnowledgeRetrievalDiagnosticsInput {
    graph_seed_input_candidate_set: SearchCandidateSetReport,
    graph_seed_candidate_set: SearchRetrieverCandidateSetReport,
    graph_seed_candidate_count: usize,
    graph_seed_returned_count: usize,
    graph_context_input_candidate_set: SearchCandidateSetReport,
    graph_context_candidate_set: SearchRetrieverCandidateSetReport,
    graph_context_path_count: usize,
    graph_context_node_count: usize,
    graph_context_relationship_count: usize,
    graph_context_truncation_reasons: Vec<String>,
    fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    candidate_count: usize,
    candidate_total_count: usize,
    pipeline: KnowledgeRetrievalPipelineReport,
}

fn knowledge_retrieval_diagnostics(
    search: &SearchResultSet,
    request: &KnowledgeRetrievalRequest,
    projection_freshness: &SearchProjectionFreshness,
    graph_commit_epoch: u64,
    required_projection_commit_epoch: u64,
    input: KnowledgeRetrievalDiagnosticsInput,
) -> KnowledgeRetrievalDiagnostics {
    let mut empty_reasons = Vec::new();
    let mut empty_reason_codes = Vec::new();
    let candidate_truncation_reasons = knowledge_candidate_truncation_reasons(
        input.candidate_total_count,
        input.candidate_count,
        request.candidate_limit,
    );
    let candidate_truncation_reason_codes = knowledge_candidate_truncation_reason_codes(
        input.candidate_total_count,
        input.candidate_count,
    );
    if input.candidate_count == 0 {
        empty_reason_codes.extend(
            search
                .empty_reason_codes
                .iter()
                .map(knowledge_empty_reason_code_from_search),
        );
        empty_reasons.extend(search.empty_reasons.iter().cloned());
        if input.candidate_total_count == 0 {
            if request.graph_seed_limit == 0 {
                empty_reason_codes.push(KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero);
                empty_reasons.push("graph seed retriever disabled by limit 0".to_string());
            } else if input.graph_seed_candidate_count == 0 {
                empty_reason_codes.push(KnowledgeRetrievalEmptyReasonCode::GraphSeedNoCandidates);
                empty_reasons.push("graph seed retriever returned no candidates".to_string());
            }
        }
        if !candidate_truncation_reasons.is_empty() {
            empty_reason_codes
                .push(KnowledgeRetrievalEmptyReasonCode::CandidateLimitExcludedAllCandidates);
        }
        empty_reasons.extend(candidate_truncation_reasons.iter().cloned());
        empty_reason_codes.push(KnowledgeRetrievalEmptyReasonCode::NoCandidates);
        empty_reasons.push("retrieval produced no candidates".to_string());
    }
    let graph_seed_truncation_reasons = knowledge_graph_seed_truncation_reasons(
        input.graph_seed_candidate_count,
        input.graph_seed_returned_count,
        request.graph_seed_limit,
    );
    let graph_seed_truncation_reason_codes = knowledge_graph_seed_truncation_reason_codes(
        input.graph_seed_candidate_count,
        input.graph_seed_returned_count,
    );
    let graph_context_truncation_reason_codes =
        knowledge_graph_context_truncation_reason_codes(&input.graph_context_truncation_reasons);
    KnowledgeRetrievalDiagnostics {
        graph_commit_epoch,
        projection_source_graph_commit_epoch: projection_freshness.source_graph_commit_epoch,
        projection_commit_lag: search_projection_freshness_commit_lag(
            projection_freshness,
            required_projection_commit_epoch,
        ),
        projection_stale: search_projection_is_stale(
            projection_freshness,
            required_projection_commit_epoch,
        ),
        projection_full_reindex_needed: projection_freshness.full_reindex_needed,
        projection_full_reindex_reasons: projection_freshness.full_reindex_reasons.clone(),
        projection_metadata_repair_needed: projection_freshness.metadata_repair_needed,
        projection_metadata_repair_reasons: projection_freshness.metadata_repair_reasons.clone(),
        search_document_count: search.document_count,
        search_filtered_document_count: search.filtered_document_count,
        search_total_hits: search.total_hits,
        search_candidate_set: search.candidate_set.clone(),
        search_candidate_filtered_out_count: search.candidate_set.filtered_out_count,
        search_limit: search.limit,
        search_truncated: search.truncated,
        search_truncation_reason_codes: search.truncation_reason_codes.clone(),
        search_truncation_reasons: search.truncation_reasons.clone(),
        search_fallback_reason_codes: search.fallback_reason_codes.clone(),
        search_fallback_reasons: search.fallback_reasons.clone(),
        rank_window: search.rank_window,
        search_fusion_weights: search.fusion_weights,
        graph_seed_input_candidate_set: input.graph_seed_input_candidate_set,
        graph_seed_candidate_set: input.graph_seed_candidate_set,
        graph_seed_candidate_count: input.graph_seed_candidate_count,
        graph_seed_returned_count: input.graph_seed_returned_count,
        graph_seed_limit: request.graph_seed_limit,
        graph_seed_truncated: !graph_seed_truncation_reasons.is_empty(),
        graph_seed_truncation_reason_codes,
        graph_seed_truncation_reasons,
        graph_context_input_candidate_set: input.graph_context_input_candidate_set,
        graph_context_candidate_set: input.graph_context_candidate_set,
        graph_context_path_count: input.graph_context_path_count,
        graph_context_node_count: input.graph_context_node_count,
        graph_context_relationship_count: input.graph_context_relationship_count,
        graph_context_limit: request.graph_context_limit,
        graph_context_max_hops: request.graph_context_max_hops,
        graph_context_truncated: !input.graph_context_truncation_reasons.is_empty(),
        graph_context_truncation_reason_codes,
        graph_context_truncation_reasons: input.graph_context_truncation_reasons,
        graph_context_fallback_reason_codes: knowledge_graph_context_fallback_reason_codes(request),
        graph_context_fallback_reasons: knowledge_graph_context_fallback_reasons(request),
        fanout_reason_count: input.fanout_reason_details.len(),
        fanout_reason_codes: knowledge_fanout_reason_codes(&input.fanout_reason_details),
        fanout_reasons: knowledge_fanout_reason_messages(&input.fanout_reason_details),
        fanout_reason_details: input.fanout_reason_details,
        candidate_count: input.candidate_count,
        candidate_total_count: input.candidate_total_count,
        candidate_limit: request.candidate_limit,
        candidate_truncated: !candidate_truncation_reasons.is_empty(),
        candidate_truncation_reason_codes,
        candidate_truncation_reasons,
        warnings: knowledge_retrieval_warnings(
            projection_freshness,
            required_projection_commit_epoch,
        ),
        empty_reason_codes,
        empty_reasons,
        pipeline: input.pipeline,
    }
}

fn knowledge_fanout_reason_codes(
    details: &[KnowledgeFanoutReasonDetail],
) -> Vec<KnowledgeFanoutReasonCode> {
    details.iter().map(|detail| detail.code).collect()
}

fn knowledge_fanout_reason_messages(details: &[KnowledgeFanoutReasonDetail]) -> Vec<String> {
    details
        .iter()
        .map(|detail| detail.message.clone())
        .collect()
}

fn knowledge_empty_reason_code_from_search(
    code: &SearchEmptyReasonCode,
) -> KnowledgeRetrievalEmptyReasonCode {
    match code {
        SearchEmptyReasonCode::ProjectionEmpty => {
            KnowledgeRetrievalEmptyReasonCode::SearchProjectionEmpty
        }
        SearchEmptyReasonCode::MetadataFilterEmpty => {
            KnowledgeRetrievalEmptyReasonCode::SearchMetadataFilterEmpty
        }
        SearchEmptyReasonCode::RetrieverNoHits => {
            KnowledgeRetrievalEmptyReasonCode::SearchRetrieverNoHits
        }
        SearchEmptyReasonCode::LimitExcludedAllHits => {
            KnowledgeRetrievalEmptyReasonCode::SearchLimitExcludedAllHits
        }
    }
}

fn knowledge_candidate_truncation_reasons(
    candidate_total_count: usize,
    candidate_count: usize,
    candidate_limit: Option<usize>,
) -> Vec<String> {
    match candidate_limit {
        Some(limit) if candidate_total_count > candidate_count => vec![format!(
            "knowledge_candidate_limit {limit} returned from {candidate_total_count} merged candidates"
        )],
        _ => Vec::new(),
    }
}

fn knowledge_candidate_truncation_reason_codes(
    candidate_total_count: usize,
    candidate_count: usize,
) -> Vec<KnowledgeTruncationReasonCode> {
    if candidate_total_count > candidate_count {
        vec![KnowledgeTruncationReasonCode::CandidateLimitExceeded]
    } else {
        Vec::new()
    }
}

fn knowledge_graph_context_truncation_reason_codes(
    truncation_reasons: &[String],
) -> Vec<KnowledgeTruncationReasonCode> {
    if truncation_reasons.is_empty() {
        Vec::new()
    } else {
        vec![KnowledgeTruncationReasonCode::GraphContextLimitExceeded]
    }
}

fn knowledge_retrieval_warnings(
    projection_freshness: &SearchProjectionFreshness,
    required_projection_commit_epoch: u64,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if search_projection_is_stale(projection_freshness, required_projection_commit_epoch) {
        warnings.push("search projection is older than graph snapshot".to_string());
    }
    if projection_freshness.full_reindex_needed {
        warnings.push("search projection requires full reindex".to_string());
        warnings.extend(
            projection_freshness
                .full_reindex_reasons
                .iter()
                .map(|reason| format!("search projection full reindex reason: {reason}")),
        );
    }
    if projection_freshness.metadata_repair_needed {
        warnings.push("search projection metadata repair is needed".to_string());
        warnings.extend(
            projection_freshness
                .metadata_repair_reasons
                .iter()
                .map(|reason| format!("search projection metadata repair reason: {reason}")),
        );
    }
    warnings
}

fn search_projection_is_stale(
    projection_freshness: &SearchProjectionFreshness,
    required_projection_commit_epoch: u64,
) -> bool {
    projection_freshness
        .source_graph_commit_epoch
        .map(|projection_epoch| projection_epoch < required_projection_commit_epoch)
        .unwrap_or(false)
}

fn search_projection_freshness_commit_lag(
    projection_freshness: &SearchProjectionFreshness,
    required_projection_commit_epoch: u64,
) -> u64 {
    required_projection_commit_epoch
        .saturating_sub(projection_freshness.source_graph_commit_epoch.unwrap_or(0))
}

fn graph_seed_candidate_id(seed: &KnowledgeGraphSeed) -> String {
    let label = seed
        .entity
        .labels
        .first()
        .map(String::as_str)
        .unwrap_or("node");
    match seed.entity.external_id.as_deref() {
        Some(external_id) if !external_id.is_empty() => format!("{label}:{external_id}"),
        _ => format!("node:{}", seed.entity.node_id),
    }
}

fn graph_seed_candidate_id_internal(seed: &KnowledgeGraphSeedCandidate) -> String {
    seed.id.clone()
}

fn graph_seed_candidate_id_from_node(catalog: &Catalog, node: &NodeRecord) -> String {
    let label = node
        .labels
        .iter()
        .find_map(|label_id| catalog.label_name(*label_id))
        .unwrap_or("node");
    let external_id = projected_node_external_id(node);
    if external_id.is_empty() {
        format!("node:{}", node.id.0)
    } else {
        format!("{label}:{external_id}")
    }
}

fn knowledge_graph_expansion_scope_filters(
    metadata_filters: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    const SCOPE_FIELDS: &[&str] = &["space_id", "tenant_id", "workspace_id", "visibility"];
    metadata_filters
        .iter()
        .filter(|(field, _)| SCOPE_FIELDS.contains(&field.as_str()))
        .map(|(field, value)| (field.clone(), value.clone()))
        .collect()
}

fn knowledge_candidate_score_breakdown(
    search_score: Option<f64>,
    graph_seed_score: Option<f64>,
    scoring: KnowledgeCandidateScoringPolicy,
) -> KnowledgeCandidateScoreBreakdown {
    let combined_score = match scoring {
        KnowledgeCandidateScoringPolicy::Max => search_score
            .into_iter()
            .chain(graph_seed_score)
            .fold(0.0, f64::max),
        KnowledgeCandidateScoringPolicy::WeightedSum {
            search_weight,
            graph_seed_weight,
        } => {
            search_score.unwrap_or(0.0) * search_weight
                + graph_seed_score.unwrap_or(0.0) * graph_seed_weight
        }
    };
    KnowledgeCandidateScoreBreakdown {
        search_score,
        graph_seed_score,
        combined_score,
    }
}

fn graph_seed_score(
    node: &NodeRecord,
    query_terms: &BTreeSet<String>,
    normalized_query: &str,
) -> (f64, Vec<String>) {
    const GRAPH_SEED_PROPERTIES: &[&str] =
        &["id", "title", "name", "summary", "content", "body", "text"];
    let mut score = 0.0;
    let mut matched_properties = Vec::new();
    for property in GRAPH_SEED_PROPERTIES {
        let Some(value) = node.properties.get(*property) else {
            continue;
        };
        let text = value_to_external_id(value);
        let normalized_text = text.to_ascii_lowercase();
        let property_terms = knowledge_query_terms(&text);
        let matched_term_count = query_terms
            .iter()
            .filter(|term| property_terms.contains(*term))
            .count();
        let exact_match = !normalized_query.is_empty() && normalized_text == normalized_query;
        let contains_query =
            !normalized_query.is_empty() && normalized_text.contains(normalized_query);
        if matched_term_count > 0 || exact_match || contains_query {
            matched_properties.push((*property).to_string());
            score += matched_term_count as f64;
            if contains_query {
                score += 2.0;
            }
            if *property == "id" && exact_match {
                score += 8.0;
            }
        }
    }
    (score, matched_properties)
}

#[cfg(test)]
fn knowledge_graph_seed_matches_filters(
    catalog: &Catalog,
    store: &GraphStore,
    node: &NodeRecord,
    metadata_filters: &BTreeMap<String, String>,
) -> bool {
    try_knowledge_graph_seed_matches_filters(catalog, store, node, metadata_filters)
        .unwrap_or(false)
}

fn try_knowledge_graph_seed_matches_filters(
    catalog: &Catalog,
    store: &GraphStore,
    node: &NodeRecord,
    metadata_filters: &BTreeMap<String, String>,
) -> Result<bool> {
    let pushdown = search_metadata_predicate_pushdown(metadata_filters);
    knowledge_graph_seed_matches_predicates(catalog, store, node, &pushdown.predicates)
}

fn knowledge_graph_seed_matches_predicates(
    catalog: &Catalog,
    store: &GraphStore,
    node: &NodeRecord,
    predicates: &SearchPredicateSet,
) -> Result<bool> {
    if predicates.is_unsatisfiable() {
        return Ok(false);
    }
    for predicate in predicates.predicates() {
        if !knowledge_graph_seed_matches_predicate(catalog, store, node, predicate)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn knowledge_graph_seed_matches_predicate(
    catalog: &Catalog,
    store: &GraphStore,
    node: &NodeRecord,
    predicate: &SearchPredicate,
) -> Result<bool> {
    Ok(match predicate.op() {
        SearchPredicateOp::Eq(expected) => {
            knowledge_graph_seed_filter_values(catalog, store, node, predicate.field().name())?
                .iter()
                .any(|actual| {
                    knowledge_graph_seed_filter_value_matches(
                        predicate.field().name(),
                        actual,
                        expected.as_str(),
                    )
                })
        }
        SearchPredicateOp::In(expected_values) => {
            let actual_values =
                knowledge_graph_seed_filter_values(catalog, store, node, predicate.field().name())?;
            actual_values.iter().any(|actual| {
                expected_values.iter().any(|expected| {
                    knowledge_graph_seed_filter_value_matches(
                        predicate.field().name(),
                        actual,
                        expected.as_str(),
                    )
                })
            })
        }
        SearchPredicateOp::NotIn(excluded_values) => {
            let actual_values =
                knowledge_graph_seed_filter_values(catalog, store, node, predicate.field().name())?;
            actual_values.iter().all(|actual| {
                excluded_values.iter().all(|excluded| {
                    !knowledge_graph_seed_filter_value_matches(
                        predicate.field().name(),
                        actual,
                        excluded.as_str(),
                    )
                })
            })
        }
        SearchPredicateOp::Gt(expected) => knowledge_graph_seed_matches_numeric_filter(
            node,
            predicate.field().name(),
            expected.as_str(),
            |actual, expected| actual > expected,
        ),
        SearchPredicateOp::Gte(expected) => knowledge_graph_seed_matches_numeric_filter(
            node,
            predicate.field().name(),
            expected.as_str(),
            |actual, expected| actual >= expected,
        ),
        SearchPredicateOp::Lt(expected) => knowledge_graph_seed_matches_numeric_filter(
            node,
            predicate.field().name(),
            expected.as_str(),
            |actual, expected| actual < expected,
        ),
        SearchPredicateOp::Lte(expected) => knowledge_graph_seed_matches_numeric_filter(
            node,
            predicate.field().name(),
            expected.as_str(),
            |actual, expected| actual <= expected,
        ),
        SearchPredicateOp::Exists => {
            !knowledge_graph_seed_filter_values(catalog, store, node, predicate.field().name())?
                .is_empty()
        }
        SearchPredicateOp::IsMissing => {
            knowledge_graph_seed_filter_values(catalog, store, node, predicate.field().name())?
                .is_empty()
        }
    })
}

fn knowledge_graph_seed_matches_numeric_filter(
    node: &NodeRecord,
    key: &str,
    expected: &str,
    matches: impl FnOnce(f64, f64) -> bool,
) -> bool {
    let Some(actual) = knowledge_graph_seed_filter_numeric_value(node, key) else {
        return false;
    };
    let Some(expected) = parse_metadata_filter_number(expected) else {
        return false;
    };
    matches(actual, expected)
}

fn knowledge_graph_seed_filter_numeric_value(node: &NodeRecord, key: &str) -> Option<f64> {
    match key {
        "kind" => None,
        "external_id" => parse_metadata_filter_number(&projected_node_external_id(node)),
        "source_id" => node_projection_source_id(node)
            .as_deref()
            .and_then(parse_metadata_filter_number),
        "space_id" => parse_metadata_filter_number(&normalized_node_space_id(node)),
        _ => node
            .properties
            .get(key)
            .map(value_to_external_id)
            .as_deref()
            .and_then(parse_metadata_filter_number),
    }
}

fn parse_metadata_filter_number(value: &str) -> Option<f64> {
    let number = value.parse::<f64>().ok()?;
    number.is_finite().then_some(number)
}

fn knowledge_graph_seed_filter_values(
    catalog: &Catalog,
    store: &GraphStore,
    node: &NodeRecord,
    key: &str,
) -> Result<Vec<String>> {
    Ok(match key {
        "kind" => {
            let label = node
                .labels
                .iter()
                .find_map(|label_id| catalog.label_name(*label_id).and_then(search_label_to_kind));
            label.map(str::to_string).into_iter().collect()
        }
        "external_id" => vec![projected_node_external_id(node)],
        "source_id" => node_projection_source_id(node).into_iter().collect(),
        "space_id" => vec![normalized_node_space_id(node)],
        "labels" => knowledge_graph_seed_business_labels(catalog, store, node)?,
        _ => node
            .properties
            .get(key)
            .map(value_to_external_id)
            .into_iter()
            .collect(),
    })
}

fn knowledge_graph_seed_business_labels(
    catalog: &Catalog,
    store: &GraphStore,
    node: &NodeRecord,
) -> Result<Vec<String>> {
    let Some(has_label_type_id) = catalog.rel_type_id("HAS_LABEL") else {
        return Ok(Vec::new());
    };
    let Some(label_label_id) = catalog.label_id("Label") else {
        return Ok(Vec::new());
    };
    let mut labels = BTreeSet::new();
    let mut scan_error = None;
    for direction in [AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming] {
        store.visit_adjacent_relationships_owned(
            node.id,
            Some(has_label_type_id),
            direction,
            |relationship| {
                let label_node_id = match direction {
                    AdjacencyDirection::Outgoing => relationship.target,
                    AdjacencyDirection::Incoming => relationship.source,
                };
                let label = match store.node_owned(label_node_id) {
                    Ok(Some(label)) => label,
                    Ok(None) => return crate::store::GraphScanControl::Continue,
                    Err(error) => {
                        scan_error = Some(error);
                        return crate::store::GraphScanControl::Stop;
                    }
                };
                if label.labels.contains(&label_label_id)
                    && let Some(value) =
                        first_non_empty_node_value(&label, &["canonical_name", "name", "id"])
                {
                    labels.insert(value);
                }
                crate::store::GraphScanControl::Continue
            },
        )?;
        if scan_error.is_some() {
            break;
        }
    }
    if let Some(error) = scan_error {
        return Err(error);
    }
    Ok(labels.into_iter().collect())
}

fn first_non_empty_node_value(node: &NodeRecord, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| node.properties.get(*key).map(value_to_external_id))
        .find(|value| !value.is_empty())
}

fn knowledge_graph_seed_filter_value_matches(key: &str, actual: &str, expected: &str) -> bool {
    if key == "kind" {
        let Some(expected_label) = search_kind_to_label(expected) else {
            return false;
        };
        return search_label_to_kind(expected_label)
            .is_some_and(|expected_kind| actual == expected_kind);
    }
    if key == "space_id" {
        return normalize_graph_seed_string_filter_value(actual)
            == normalize_graph_seed_string_filter_value(expected);
    }
    if search_field_is_enum_like(key) {
        return normalize_search_enum_value(actual) == normalize_search_enum_value(expected);
    }
    normalize_graph_seed_string_filter_value(actual)
        == normalize_graph_seed_string_filter_value(expected)
}

fn normalize_graph_seed_string_filter_value(value: &str) -> String {
    value.trim().to_lowercase()
}

fn normalized_node_space_id(node: &NodeRecord) -> String {
    node.properties
        .get("space_id")
        .map(value_to_external_id)
        .filter(|space_id| !space_id.is_empty())
        .unwrap_or_else(|| "default".to_string())
}

fn node_projection_source_id(node: &NodeRecord) -> Option<String> {
    ["source_id", "thread_id", "source"]
        .into_iter()
        .filter_map(|key| node.properties.get(key).map(value_to_external_id))
        .find(|source_id| !source_id.is_empty())
}

fn knowledge_query_terms(text: &str) -> BTreeSet<String> {
    text.split(|ch: char| !ch.is_alphanumeric() && ch != '_')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

#[rustfmt::skip]
#[cfg(test)]
mod legacy_business_api {
use super::*;

pub(super) fn knowledge_community_entity_visibility_via_query_runtime(
    db: &Database,
    request: &KnowledgeCommunityEntityVisibilityRequest,
) -> Result<KnowledgeCommunityEntityVisibilityOutput> {
    validate_knowledge_community_entity_visibility_request(request)?;
    let graph_commit_epoch = db.store.commit_epoch();
    if db.catalog.label_id("Entity").is_none() || db.catalog.label_id("Memory").is_none() {
        return Ok(empty_community_entity_visibility_output(graph_commit_epoch));
    }

    let parameters = BTreeMap::from([(
        "community_ids".to_string(),
        Value::List(request.community_ids.clone()),
    )]);
    let entity_output = db.query_read_only_with_params_bounded(
        "MATCH (e:Entity) \
         WHERE e.community_id IN $community_ids \
         RETURN e.community_id AS community_id, e.id AS entity_id, \
         id(e) AS entity_node_id, e.name AS entity_name, e.entity_type AS entity_type",
        &parameters,
        None,
    )?;
    let mut entities = entity_output
        .rows
        .iter()
        .map(community_entity_visibility_entity_row_from_query)
        .collect::<Result<Vec<_>>>()?;
    let matched_entity_count = entities.len();
    if entities.is_empty() {
        return Ok(KnowledgeCommunityEntityVisibilityOutput {
            graph_commit_epoch,
            rows: Vec::new(),
            matched_entity_count: 0,
            matched_row_count: 0,
            returned_count: 0,
        });
    }

    let entity_node_ids = entities
        .iter()
        .map(|entity| {
            i64::try_from(entity.entity_node_id)
                .map(Value::Int)
                .map_err(|_| {
                    SkeinError::Execution(format!(
                        "entity node id {} exceeds query parameter range",
                        entity.entity_node_id
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let memory_parameters =
        BTreeMap::from([("entity_node_ids".to_string(), Value::List(entity_node_ids))]);
    let memory_output = db.query_read_only_with_params_bounded(
        "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) \
         WHERE id(e) IN $entity_node_ids \
         RETURN id(e) AS entity_node_id, m.id AS memory_id, id(m) AS memory_node_id, \
         m.metadata AS memory_metadata, m.is_latest AS memory_is_latest, \
         m.lifecycle_state AS memory_lifecycle_state",
        &memory_parameters,
        None,
    )?;
    let mut memory_rows_by_entity = BTreeMap::<u64, Vec<CommunityEntityVisibilityMemoryRow>>::new();
    for row in &memory_output.rows {
        let memory_row = community_entity_visibility_memory_row_from_query(row)?;
        memory_rows_by_entity
            .entry(memory_row.entity_node_id)
            .or_default()
            .push(memory_row);
    }

    let mut rows = Vec::new();
    entities.sort_by_key(|entity| entity.entity_node_id);
    for entity in entities {
        if let Some(memory_rows) = memory_rows_by_entity.remove(&entity.entity_node_id) {
            rows.extend(
                memory_rows
                    .into_iter()
                    .map(|memory| entity.clone().with_memory(Some(memory))),
            );
        } else {
            rows.push(entity.with_memory(None));
        }
    }

    sort_community_entity_visibility_rows(&mut rows);
    let matched_row_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();

    Ok(KnowledgeCommunityEntityVisibilityOutput {
        graph_commit_epoch,
        rows,
        matched_entity_count,
        matched_row_count,
        returned_count,
    })
}

#[derive(Debug, Clone)]
#[cfg(test)]
pub(super) struct CommunityEntityVisibilityEntityRow {
    community_id: Value,
    entity_id: Option<String>,
    entity_node_id: u64,
    entity_name: Option<String>,
    entity_type: Option<String>,
}

#[cfg(test)]
impl CommunityEntityVisibilityEntityRow {
    fn with_memory(
        self,
        memory: Option<CommunityEntityVisibilityMemoryRow>,
    ) -> KnowledgeCommunityEntityVisibilityRow {
        KnowledgeCommunityEntityVisibilityRow {
            community_id: self.community_id,
            entity_id: self.entity_id,
            entity_node_id: self.entity_node_id,
            entity_name: self.entity_name,
            entity_type: self.entity_type,
            memory_id: memory.as_ref().and_then(|memory| memory.memory_id.clone()),
            memory_node_id: memory.as_ref().map(|memory| memory.memory_node_id),
            memory_metadata: memory
                .as_ref()
                .and_then(|memory| memory.memory_metadata.clone()),
            memory_is_latest: memory
                .as_ref()
                .and_then(|memory| memory.memory_is_latest)
                .unwrap_or(true),
            memory_lifecycle_state: memory.and_then(|memory| memory.memory_lifecycle_state),
        }
    }
}

#[derive(Debug, Clone)]
#[cfg(test)]
pub(super) struct CommunityEntityVisibilityMemoryRow {
    entity_node_id: u64,
    memory_id: Option<String>,
    memory_node_id: u64,
    memory_metadata: Option<Value>,
    memory_is_latest: Option<bool>,
    memory_lifecycle_state: Option<String>,
}

#[cfg(test)]
pub(super) fn community_entity_visibility_entity_row_from_query(
    row: impl QueryRowLookup,
) -> Result<CommunityEntityVisibilityEntityRow> {
    let community_id = row.get("community_id").cloned().ok_or_else(|| {
        SkeinError::Execution(
            "knowledge community entity visibility row is missing community_id".to_string(),
        )
    })?;
    let entity_node_id = row
        .get("entity_node_id")
        .and_then(value_to_non_negative_u64)
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge community entity visibility row is missing entity_node_id".to_string(),
            )
        })?;
    Ok(CommunityEntityVisibilityEntityRow {
        community_id,
        entity_id: optional_string_cell(row, "entity_id"),
        entity_node_id,
        entity_name: optional_string_cell(row, "entity_name"),
        entity_type: optional_string_cell(row, "entity_type"),
    })
}

#[cfg(test)]
pub(super) fn community_entity_visibility_memory_row_from_query(
    row: impl QueryRowLookup,
) -> Result<CommunityEntityVisibilityMemoryRow> {
    let entity_node_id = row
        .get("entity_node_id")
        .and_then(value_to_non_negative_u64)
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge community entity visibility memory row is missing entity_node_id"
                    .to_string(),
            )
        })?;
    let memory_node_id = row
        .get("memory_node_id")
        .and_then(value_to_non_negative_u64)
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge community entity visibility memory row is missing memory_node_id"
                    .to_string(),
            )
        })?;
    let memory_is_latest = match row.get("memory_is_latest") {
        Some(Value::Bool(value)) => Some(*value),
        Some(Value::Null) | None => None,
        Some(value) => {
            return Err(SkeinError::Execution(format!(
                "knowledge community entity visibility memory row has non-boolean memory_is_latest: {value:?}"
            )));
        }
    };
    Ok(CommunityEntityVisibilityMemoryRow {
        entity_node_id,
        memory_id: optional_string_cell(row, "memory_id"),
        memory_node_id,
        memory_metadata: optional_value_cell(row, "memory_metadata"),
        memory_is_latest,
        memory_lifecycle_state: optional_string_cell(row, "memory_lifecycle_state"),
    })
}

#[cfg(test)]
pub(super) fn empty_community_entity_visibility_output(
    graph_commit_epoch: u64,
) -> KnowledgeCommunityEntityVisibilityOutput {
    KnowledgeCommunityEntityVisibilityOutput {
        graph_commit_epoch,
        rows: Vec::new(),
        matched_entity_count: 0,
        matched_row_count: 0,
        returned_count: 0,
    }
}

#[cfg(test)]
pub(super) fn validate_knowledge_community_entity_visibility_request(
    request: &KnowledgeCommunityEntityVisibilityRequest,
) -> Result<()> {
    if request.community_ids.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community entity visibility requires non-empty community ids".to_string(),
        ));
    }
    if request
        .community_ids
        .iter()
        .any(|community_id| community_id == &Value::Null)
    {
        return Err(SkeinError::Semantic(
            "knowledge community entity visibility requires non-null community ids".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn sort_community_entity_visibility_rows(rows: &mut [KnowledgeCommunityEntityVisibilityRow]) {
    rows.sort_by(|left, right| {
        left.community_id
            .cmp(&right.community_id)
            .then_with(|| left.entity_name.cmp(&right.entity_name))
            .then_with(|| left.entity_id.cmp(&right.entity_id))
            .then_with(|| left.memory_id.cmp(&right.memory_id))
            .then_with(|| left.entity_node_id.cmp(&right.entity_node_id))
            .then_with(|| left.memory_node_id.cmp(&right.memory_node_id))
    });
}

#[cfg(test)]
pub(super) fn knowledge_community_memories_via_query_runtime(
    db: &Database,
    request: &KnowledgeCommunityMemoryListRequest,
) -> Result<KnowledgeCommunityMemoryListOutput> {
    validate_knowledge_community_memory_list_request(request)?;
    let graph_commit_epoch = db.store.commit_epoch();
    let mut rows = Vec::new();

    if matches!(
        request.source,
        KnowledgeCommunityMemorySource::MentionedEntities | KnowledgeCommunityMemorySource::Both
    ) {
        rows.extend(mentioned_community_memory_rows_via_query_runtime(
            db, request,
        )?);
    }
    if matches!(
        request.source,
        KnowledgeCommunityMemorySource::DirectMemoryCommunity
            | KnowledgeCommunityMemorySource::Both
    ) {
        rows.extend(direct_community_memory_rows_via_query_runtime(db, request)?);
    }

    sort_community_memory_rows(&mut rows, request.order);
    let matched_row_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();

    Ok(KnowledgeCommunityMemoryListOutput {
        graph_commit_epoch,
        rows,
        matched_row_count,
        returned_count,
    })
}

#[cfg(test)]
pub(super) fn validate_knowledge_community_memory_list_request(
    request: &KnowledgeCommunityMemoryListRequest,
) -> Result<()> {
    if request.community_ids.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community memory list requires non-empty community ids".to_string(),
        ));
    }
    if request
        .community_ids
        .iter()
        .any(|community_id| community_id == &Value::Null)
    {
        return Err(SkeinError::Semantic(
            "knowledge community memory list requires non-null community ids".to_string(),
        ));
    }
    if request.unit_types.iter().any(String::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge community memory list requires non-empty unit types".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn mentioned_community_memory_rows_via_query_runtime(
    db: &Database,
    request: &KnowledgeCommunityMemoryListRequest,
) -> Result<Vec<KnowledgeCommunityMemoryRow>> {
    let mut parameters = knowledge_community_memory_query_parameters(request);
    let predicate = knowledge_community_memory_query_predicate("m", request, &mut parameters);
    let query = format!(
        "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) \
         WHERE e.community_id IN $community_ids AND {predicate} \
         WITH e.community_id AS community_id, m AS memory, count(*) AS mention_count, \
         COLLECT(DISTINCT e.id) AS entity_ids \
         RETURN community_id, memory, mention_count, entity_ids"
    );
    let output = db.query_read_only_with_params_bounded(&query, &parameters, None)?;
    output
        .rows
        .iter()
        .map(|row| {
            knowledge_community_memory_row_from_query(
                row,
                KnowledgeCommunityMemoryRowSource::MentionedEntities,
            )
        })
        .collect()
}

#[cfg(test)]
pub(super) fn direct_community_memory_rows_via_query_runtime(
    db: &Database,
    request: &KnowledgeCommunityMemoryListRequest,
) -> Result<Vec<KnowledgeCommunityMemoryRow>> {
    let mut parameters = knowledge_community_memory_query_parameters(request);
    let predicate = knowledge_community_memory_query_predicate("m", request, &mut parameters);
    let query = format!(
        "MATCH (m:Memory) \
         WHERE m.community_id IN $community_ids AND {predicate} \
         RETURN m.community_id AS community_id, m AS memory"
    );
    let output = db.query_read_only_with_params_bounded(&query, &parameters, None)?;
    output
        .rows
        .iter()
        .map(|row| {
            knowledge_community_memory_row_from_query(
                row,
                KnowledgeCommunityMemoryRowSource::DirectMemoryCommunity,
            )
        })
        .collect()
}

#[cfg(test)]
pub(super) fn knowledge_community_memory_query_parameters(
    request: &KnowledgeCommunityMemoryListRequest,
) -> BTreeMap<String, Value> {
    let mut parameters = BTreeMap::from([(
        "community_ids".to_string(),
        Value::List(request.community_ids.clone()),
    )]);
    if !request.unit_types.is_empty() {
        parameters.insert(
            "unit_types".to_string(),
            Value::List(
                request
                    .unit_types
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    parameters
}

#[cfg(test)]
pub(super) fn knowledge_community_memory_query_predicate(
    alias: &str,
    request: &KnowledgeCommunityMemoryListRequest,
    parameters: &mut BTreeMap<String, Value>,
) -> String {
    let mut predicates = Vec::new();
    match request.crystal_filter {
        KnowledgeCommunityMemoryCrystalFilter::Any => {}
        KnowledgeCommunityMemoryCrystalFilter::FalseOnly => {
            parameters.insert("is_crystal".to_string(), Value::Bool(false));
            predicates.push(format!("{alias}.is_crystal = $is_crystal"));
        }
        KnowledgeCommunityMemoryCrystalFilter::NullOrFalse => {
            predicates.push(format!(
                "({alias}.is_crystal IS NULL OR {alias}.is_crystal = false)"
            ));
        }
    }
    if !request.unit_types.is_empty() {
        predicates.push(format!("{alias}.unit_type IN $unit_types"));
    }
    if predicates.is_empty() {
        "true".to_string()
    } else {
        predicates.join(" AND ")
    }
}

#[cfg(test)]
pub(super) fn knowledge_community_memory_row_from_query(
    row: impl QueryRowLookup,
    source: KnowledgeCommunityMemoryRowSource,
) -> Result<KnowledgeCommunityMemoryRow> {
    let community_id = row.get("community_id").cloned().ok_or_else(|| {
        SkeinError::Execution("knowledge community memory row is missing community_id".to_string())
    })?;
    let memory = row
        .get("memory")
        .and_then(knowledge_entity_from_value)
        .ok_or_else(|| {
            SkeinError::Execution("knowledge community memory row is missing memory".to_string())
        })?;
    let entity_ids = row
        .get("entity_ids")
        .and_then(value_to_string_list)
        .unwrap_or_default()
        .into_iter()
        .filter(|entity_id| !entity_id.is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mention_count = row
        .get("mention_count")
        .and_then(value_to_non_negative_usize)
        .unwrap_or(0);
    let mention_breadth = if entity_ids.is_empty() {
        mention_count
    } else {
        entity_ids.len()
    };
    let title = string_property_value(&memory.properties, "title");
    let content = string_property_value(&memory.properties, "content");

    Ok(KnowledgeCommunityMemoryRow {
        community_id,
        source,
        memory_id: memory.external_id,
        memory_node_id: memory.node_id,
        title_or_empty: title.clone().unwrap_or_default(),
        title,
        content_or_empty: content.clone().unwrap_or_default(),
        content,
        unit_type: string_property_value(&memory.properties, "unit_type"),
        metadata: memory.properties.get("metadata").cloned(),
        is_latest: boolean_property_value(&memory.properties, "is_latest").unwrap_or(true),
        lifecycle_state: string_property_value(&memory.properties, "lifecycle_state"),
        importance: memory.properties.get("importance").cloned(),
        created_at: memory.properties.get("created_at").cloned(),
        is_crystal: boolean_property_value(&memory.properties, "is_crystal"),
        pagerank_score: memory.properties.get("pagerank_score").cloned(),
        mention_breadth,
        entity_ids,
    })
}

#[cfg(test)]
pub(super) fn sort_community_memory_rows(
    rows: &mut [KnowledgeCommunityMemoryRow],
    order: KnowledgeCommunityMemoryListOrder,
) {
    rows.sort_by(|left, right| match order {
        KnowledgeCommunityMemoryListOrder::CommunityBreadthImportanceCreatedAt => left
            .community_id
            .cmp(&right.community_id)
            .then_with(|| right.mention_breadth.cmp(&left.mention_breadth))
            .then_with(|| compare_community_memory_importance_desc(left, right))
            .then_with(|| {
                compare_knowledge_created_at(
                    &left.created_at,
                    &right.created_at,
                    KnowledgeCreatedAtOrder::Descending,
                )
            })
            .then_with(|| compare_community_memory_ids(left, right)),
        KnowledgeCommunityMemoryListOrder::EntityCountImportancePagerank => right
            .mention_breadth
            .cmp(&left.mention_breadth)
            .then_with(|| compare_community_memory_importance_desc(left, right))
            .then_with(|| {
                compare_optional_values_desc(
                    left.pagerank_score.as_ref(),
                    right.pagerank_score.as_ref(),
                )
            })
            .then_with(|| compare_community_memory_ids(left, right)),
        KnowledgeCommunityMemoryListOrder::CommunityImportanceCreatedAt => left
            .community_id
            .cmp(&right.community_id)
            .then_with(|| compare_community_memory_importance_desc(left, right))
            .then_with(|| {
                compare_knowledge_created_at(
                    &left.created_at,
                    &right.created_at,
                    KnowledgeCreatedAtOrder::Descending,
                )
            })
            .then_with(|| compare_community_memory_ids(left, right)),
    });
}

#[cfg(test)]
pub(super) fn compare_community_memory_importance_desc(
    left: &KnowledgeCommunityMemoryRow,
    right: &KnowledgeCommunityMemoryRow,
) -> std::cmp::Ordering {
    let left_importance = left.importance.clone().unwrap_or(Value::Float(0.5));
    let right_importance = right.importance.clone().unwrap_or(Value::Float(0.5));
    compare_knowledge_values(&right_importance, &left_importance)
}

#[cfg(test)]
pub(super) fn compare_community_memory_ids(
    left: &KnowledgeCommunityMemoryRow,
    right: &KnowledgeCommunityMemoryRow,
) -> std::cmp::Ordering {
    left.memory_id
        .cmp(&right.memory_id)
        .then_with(|| left.source.cmp(&right.source))
        .then_with(|| left.memory_node_id.cmp(&right.memory_node_id))
}

#[cfg(test)]
pub(super) fn projected_properties(
    properties: &BTreeMap<String, Value>,
    property_names: &[String],
) -> BTreeMap<String, Value> {
    let mut projected = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for property_name in property_names {
        if seen.insert(property_name)
            && let Some(value) = properties.get(property_name)
        {
            projected.insert(property_name.clone(), value.clone());
        }
    }
    projected
}

#[cfg(test)]
pub(super) fn boolean_property_value(properties: &BTreeMap<String, Value>, property: &str) -> Option<bool> {
    match properties.get(property) {
        Some(Value::Bool(value)) => Some(*value),
        _ => None,
    }
}

#[cfg(test)]
pub(super) fn integer_property_value(properties: &BTreeMap<String, Value>, property: &str) -> Option<i64> {
    match properties.get(property) {
        Some(Value::Int(value)) => Some(*value),
        _ => None,
    }
}

#[cfg(test)]
pub(super) fn knowledge_crystals_via_query_runtime(
    db: &Database,
    request: &KnowledgeCrystalListRequest,
) -> Result<KnowledgeCrystalListOutput> {
    validate_knowledge_crystal_list_request(request)?;
    let graph_commit_epoch = db.store.commit_epoch();
    let mut parameters = BTreeMap::new();
    let predicate = knowledge_crystal_list_query_predicate(request, &mut parameters);
    let query = format!("MATCH (m:Memory){predicate} RETURN m AS memory");
    let output = db.query_read_only_with_params_bounded(&query, &parameters, None)?;
    let mut rows = output
        .rows
        .iter()
        .filter_map(|row| {
            row.get("memory")
                .and_then(knowledge_entity_from_value)
                .map(|memory| knowledge_crystal_row_from_entity(&memory))
        })
        .collect::<Vec<_>>();
    sort_crystal_rows(&mut rows, request);
    let matched_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();

    Ok(KnowledgeCrystalListOutput {
        graph_commit_epoch,
        rows,
        matched_count,
        returned_count,
    })
}

#[cfg(test)]
pub(super) fn validate_knowledge_crystal_list_request(request: &KnowledgeCrystalListRequest) -> Result<()> {
    if request.key_match.as_ref().is_some_and(String::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge crystal list requires a non-empty key match".to_string(),
        ));
    }
    if request.after_id.as_ref().is_some_and(String::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge crystal list requires a non-empty after id".to_string(),
        ));
    }
    if request.key_match.is_some() && request.after_id.is_some() {
        return Err(SkeinError::Semantic(
            "knowledge crystal list accepts key_match or after_id, not both".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn knowledge_crystal_list_query_predicate(
    request: &KnowledgeCrystalListRequest,
    parameters: &mut BTreeMap<String, Value>,
) -> String {
    let mut predicates = vec!["m.is_crystal = true"];
    if let Some(key_match) = &request.key_match {
        parameters.insert("key_match".to_string(), Value::String(key_match.clone()));
        predicates
            .push("(m.id = $key_match OR m.id STARTS WITH $key_match OR m.id CONTAINS $key_match)");
    }
    if let Some(after_id) = &request.after_id {
        parameters.insert("after_id".to_string(), Value::String(after_id.clone()));
        predicates.push("m.id > $after_id");
    }
    format!(" WHERE {}", predicates.join(" AND "))
}

#[cfg(test)]
pub(super) fn knowledge_crystal_row_from_entity(memory: &KnowledgeEntity) -> KnowledgeCrystalRow {
    let crystal_title = string_property_value(&memory.properties, "crystal_title");
    let title = string_property_value(&memory.properties, "title");
    let display_title = crystal_title
        .clone()
        .or_else(|| title.clone())
        .unwrap_or_default();
    KnowledgeCrystalRow {
        memory_id: memory.external_id.clone(),
        node_id: memory.node_id,
        crystal_title,
        title,
        display_title,
        content: string_property_value(&memory.properties, "content"),
        importance: memory.properties.get("importance").cloned(),
        unit_type: string_property_value(&memory.properties, "unit_type"),
        created_at: memory.properties.get("created_at").cloned(),
        updated_at: memory.properties.get("updated_at").cloned(),
        metadata: memory.properties.get("metadata").cloned(),
        is_latest: boolean_property_value(&memory.properties, "is_latest"),
        is_crystal: boolean_property_value(&memory.properties, "is_crystal"),
    }
}

#[cfg(test)]
pub(super) fn sort_crystal_rows(rows: &mut [KnowledgeCrystalRow], request: &KnowledgeCrystalListRequest) {
    rows.sort_by(|left, right| {
        crystal_key_match_rank(left, request)
            .cmp(&crystal_key_match_rank(right, request))
            .then_with(|| match request.order {
                KnowledgeCrystalListOrder::ExternalIdAsc => compare_crystal_ids(left, right),
                KnowledgeCrystalListOrder::ImportanceDescCreatedAtDesc => {
                    compare_crystal_importance_created_at(left, right)
                        .then_with(|| compare_crystal_ids(left, right))
                }
            })
    });
}

pub(super) fn merge_knowledge_crystal_source_for(
    db: &mut Database,
    request: &KnowledgeCrystalSourceMergeRequest,
) -> Result<KnowledgeCrystalSourceMergeOutput> {
    if request.crystal_memory_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge crystal source merge requires a non-empty crystal memory id".to_string(),
        ));
    }
    if request.source_memory_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge crystal source merge requires a non-empty source memory id".to_string(),
        ));
    }
    validate_knowledge_crystal_source_weight(&request.weight)?;

    let upsert = KnowledgeRelationshipUpsertRequest {
        source: KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: request.crystal_memory_id.clone(),
        },
        target: KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: request.source_memory_id.clone(),
        },
        relationship_type: "SYNTHESIZED_FROM".to_string(),
        create_properties: BTreeMap::from([
            ("weight".to_string(), request.weight.clone()),
            ("occasion_key".to_string(), Value::String(String::new())),
            ("created_at".to_string(), request.created_at.clone()),
        ]),
    };
    let output = upsert_knowledge_relationship_for(db, &upsert)?;
    let missing_endpoint = !output.matched
        && !output.non_writable
        && !output.source_filtered_out
        && !output.target_filtered_out;

    Ok(KnowledgeCrystalSourceMergeOutput {
        graph_commit_epoch_before: output.graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        crystal_memory_id: request.crystal_memory_id.clone(),
        source_memory_id: request.source_memory_id.clone(),
        crystal_node_id: output.source_node_id,
        source_node_id: output.target_node_id,
        relationship_id: output.relationship_id,
        matched: output.matched,
        created: output.created,
        already_exists: output.already_exists,
        missing_endpoint,
        non_writable: output.non_writable,
        created_relationship_count: output.created_relationship_count,
    })
}

pub(super) fn validate_knowledge_crystal_source_weight(weight: &Value) -> Result<()> {
    let valid = match weight {
        Value::Float(value) => value.is_finite(),
        Value::Int(_) => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(SkeinError::Semantic(
            "knowledge crystal source merge requires a numeric finite weight".to_string(),
        ))
    }
}

#[cfg(test)]
pub(super) fn crystal_key_match_rank(row: &KnowledgeCrystalRow, request: &KnowledgeCrystalListRequest) -> u8 {
    let Some(key) = &request.key_match else {
        return 0;
    };
    let Some(memory_id) = &row.memory_id else {
        return 3;
    };
    if memory_id == key {
        0
    } else if memory_id.starts_with(key) {
        1
    } else {
        2
    }
}

#[cfg(test)]
pub(super) fn compare_crystal_ids(
    left: &KnowledgeCrystalRow,
    right: &KnowledgeCrystalRow,
) -> std::cmp::Ordering {
    left.memory_id
        .cmp(&right.memory_id)
        .then_with(|| left.node_id.cmp(&right.node_id))
}

#[cfg(test)]
pub(super) fn compare_crystal_importance_created_at(
    left: &KnowledgeCrystalRow,
    right: &KnowledgeCrystalRow,
) -> std::cmp::Ordering {
    compare_optional_values_desc(left.importance.as_ref(), right.importance.as_ref()).then_with(
        || {
            compare_knowledge_created_at(
                &left.created_at,
                &right.created_at,
                KnowledgeCreatedAtOrder::Descending,
            )
        },
    )
}

#[cfg(test)]
pub(super) fn knowledge_crystal_communities_via_query_runtime(
    db: &Database,
    request: &KnowledgeCrystalCommunityListRequest,
) -> Result<KnowledgeCrystalCommunityListOutput> {
    validate_knowledge_crystal_community_list_request(request)?;
    let graph_commit_epoch = db.store.commit_epoch();
    let mut parameters = BTreeMap::new();
    let predicate = knowledge_crystal_community_query_predicate(request, &mut parameters);
    let query = format!(
        "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory)-[:MENTIONS]->(e:Entity) \
         WHERE c.is_crystal = true AND {predicate} \
         WITH c.id AS crystal_memory_id, id(c) AS crystal_node_id, \
         c.crystal_title AS crystal_title, c.title AS title, c.content AS content, \
         c.importance AS importance, c.metadata AS metadata, c.is_latest AS is_latest, \
         c.lifecycle_state AS lifecycle_state, e.community_id AS community_id, \
         count(*) AS hit_count, \
         count(DISTINCT s) AS source_memory_count \
         RETURN crystal_memory_id, crystal_node_id, community_id, hit_count, \
         source_memory_count, crystal_title, title, content, importance, metadata, \
         is_latest, lifecycle_state"
    );
    let output = db.query_read_only_with_params_bounded(&query, &parameters, None)?;
    let mut rows = output
        .rows
        .iter()
        .map(knowledge_crystal_community_row_from_query)
        .collect::<Result<Vec<_>>>()?;
    sort_crystal_community_rows(&mut rows, request.order);
    let matched_path_count = rows.iter().map(|row| row.hit_count).sum();
    let matched_pair_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();

    Ok(KnowledgeCrystalCommunityListOutput {
        graph_commit_epoch,
        rows,
        matched_path_count,
        matched_pair_count,
        returned_count,
    })
}

#[cfg(test)]
pub(super) fn validate_knowledge_crystal_community_list_request(
    request: &KnowledgeCrystalCommunityListRequest,
) -> Result<()> {
    match &request.scope {
        KnowledgeCrystalCommunityScope::CommunityIds(community_ids) => {
            if community_ids.is_empty() {
                return Err(SkeinError::Semantic(
                    "knowledge crystal community list requires non-empty community ids".to_string(),
                ));
            }
            if community_ids
                .iter()
                .any(|community_id| community_id == &Value::Null)
            {
                return Err(SkeinError::Semantic(
                    "knowledge crystal community list requires non-null community ids".to_string(),
                ));
            }
        }
        KnowledgeCrystalCommunityScope::NonNullCommunity => {}
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn knowledge_crystal_community_query_predicate(
    request: &KnowledgeCrystalCommunityListRequest,
    parameters: &mut BTreeMap<String, Value>,
) -> String {
    match &request.scope {
        KnowledgeCrystalCommunityScope::CommunityIds(community_ids) => {
            parameters.insert(
                "community_ids".to_string(),
                Value::List(community_ids.clone()),
            );
            "e.community_id IN $community_ids".to_string()
        }
        KnowledgeCrystalCommunityScope::NonNullCommunity => {
            "e.community_id IS NOT NULL".to_string()
        }
    }
}

#[cfg(test)]
pub(super) fn knowledge_crystal_community_row_from_query(
    row: impl QueryRowLookup,
) -> Result<KnowledgeCrystalCommunityRow> {
    let crystal_memory_id = row
        .get("crystal_memory_id")
        .map(value_to_external_id)
        .filter(|memory_id| !memory_id.is_empty());
    let crystal_node_id = row
        .get("crystal_node_id")
        .and_then(value_to_non_negative_u64)
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge crystal community row is missing crystal_node_id".to_string(),
            )
        })?;
    let community_id = row.get("community_id").cloned().ok_or_else(|| {
        SkeinError::Execution("knowledge crystal community row is missing community_id".to_string())
    })?;
    let hit_count = row
        .get("hit_count")
        .and_then(value_to_non_negative_usize)
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge crystal community row is missing hit_count".to_string(),
            )
        })?;
    let source_memory_count = row
        .get("source_memory_count")
        .and_then(value_to_non_negative_usize)
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge crystal community row is missing source_memory_count".to_string(),
            )
        })?;
    let crystal_title = row
        .get("crystal_title")
        .and_then(optional_external_id_value);
    let title = row.get("title").and_then(optional_external_id_value);
    let display_title = crystal_title
        .clone()
        .or_else(|| title.clone())
        .unwrap_or_default();

    Ok(KnowledgeCrystalCommunityRow {
        crystal_memory_id,
        crystal_node_id,
        community_id,
        hit_count,
        source_memory_count,
        crystal_title,
        title,
        display_title,
        content: row.get("content").and_then(optional_external_id_value),
        importance: row.get("importance").and_then(optional_non_null_value),
        metadata: row.get("metadata").and_then(optional_non_null_value),
        is_latest: row.get("is_latest").and_then(value_to_bool),
        lifecycle_state: row
            .get("lifecycle_state")
            .and_then(optional_external_id_value),
    })
}

#[cfg(test)]
pub(super) fn sort_crystal_community_rows(
    rows: &mut [KnowledgeCrystalCommunityRow],
    order: KnowledgeCrystalCommunityListOrder,
) {
    rows.sort_by(|left, right| match order {
        KnowledgeCrystalCommunityListOrder::CommunityIdAscCrystalIdAsc => {
            compare_crystal_community_ids(left, right)
        }
        KnowledgeCrystalCommunityListOrder::HitsDescImportanceDesc => right
            .hit_count
            .cmp(&left.hit_count)
            .then_with(|| {
                compare_optional_values_desc(left.importance.as_ref(), right.importance.as_ref())
            })
            .then_with(|| compare_crystal_community_ids(left, right)),
    });
}

#[cfg(test)]
pub(super) fn compare_crystal_community_ids(
    left: &KnowledgeCrystalCommunityRow,
    right: &KnowledgeCrystalCommunityRow,
) -> std::cmp::Ordering {
    left.community_id
        .cmp(&right.community_id)
        .then_with(|| left.crystal_memory_id.cmp(&right.crystal_memory_id))
        .then_with(|| left.crystal_node_id.cmp(&right.crystal_node_id))
}

#[cfg(test)]
pub(super) fn knowledge_crystal_source_visibility_via_query_runtime(
    db: &Database,
    request: &KnowledgeCrystalSourceVisibilityRequest,
) -> Result<KnowledgeCrystalSourceVisibilityOutput> {
    validate_knowledge_crystal_source_visibility_request(request)?;
    let graph_commit_epoch = db.store.commit_epoch();
    let parameters = BTreeMap::from([(
        "community_ids".to_string(),
        Value::List(request.community_ids.clone()),
    )]);
    let output = db.query_read_only_with_params_bounded(
        "MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory)-[:MENTIONS]->(e:Entity) \
         WHERE c.is_crystal = true AND e.community_id IN $community_ids \
         RETURN c AS crystal, s AS source_memory, e AS entity",
        &parameters,
        None,
    )?;
    let mut rows = output
        .rows
        .iter()
        .map(knowledge_crystal_source_visibility_row_from_query)
        .collect::<Result<Vec<_>>>()?;

    sort_crystal_source_visibility_rows(&mut rows);
    let matched_path_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();

    Ok(KnowledgeCrystalSourceVisibilityOutput {
        graph_commit_epoch,
        rows,
        matched_path_count,
        returned_count,
    })
}

#[cfg(test)]
pub(super) fn validate_knowledge_crystal_source_visibility_request(
    request: &KnowledgeCrystalSourceVisibilityRequest,
) -> Result<()> {
    if request.community_ids.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge crystal source visibility requires non-empty community ids".to_string(),
        ));
    }
    if request
        .community_ids
        .iter()
        .any(|community_id| community_id == &Value::Null)
    {
        return Err(SkeinError::Semantic(
            "knowledge crystal source visibility requires non-null community ids".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn knowledge_crystal_source_visibility_row_from_query(
    row: impl QueryRowLookup,
) -> Result<KnowledgeCrystalSourceVisibilityRow> {
    let crystal = row
        .get("crystal")
        .and_then(knowledge_entity_from_value)
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge crystal source visibility row is missing crystal".to_string(),
            )
        })?;
    let source_memory = row
        .get("source_memory")
        .and_then(knowledge_entity_from_value)
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge crystal source visibility row is missing source_memory".to_string(),
            )
        })?;
    let entity = row
        .get("entity")
        .and_then(knowledge_entity_from_value)
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge crystal source visibility row is missing entity".to_string(),
            )
        })?;
    let community_id = entity
        .properties
        .get("community_id")
        .cloned()
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge crystal source visibility row is missing community_id".to_string(),
            )
        })?;
    let crystal_title = string_property_value(&crystal.properties, "crystal_title");
    let title = string_property_value(&crystal.properties, "title");
    let display_title = crystal_title
        .clone()
        .or_else(|| title.clone())
        .unwrap_or_default();

    Ok(KnowledgeCrystalSourceVisibilityRow {
        crystal_memory_id: crystal.external_id,
        crystal_node_id: crystal.node_id,
        source_memory_id: source_memory.external_id,
        source_node_id: source_memory.node_id,
        entity_id: entity.external_id,
        entity_node_id: entity.node_id,
        community_id,
        crystal_title,
        title,
        display_title,
        content: string_property_value(&crystal.properties, "content"),
        importance: crystal.properties.get("importance").cloned(),
        crystal_metadata: crystal.properties.get("metadata").cloned(),
        crystal_is_latest: boolean_property_value(&crystal.properties, "is_latest").unwrap_or(true),
        crystal_lifecycle_state: string_property_value(&crystal.properties, "lifecycle_state"),
        source_metadata: source_memory.properties.get("metadata").cloned(),
        source_is_latest: boolean_property_value(&source_memory.properties, "is_latest")
            .unwrap_or(true),
        source_lifecycle_state: string_property_value(&source_memory.properties, "lifecycle_state"),
    })
}

#[cfg(test)]
pub(super) fn sort_crystal_source_visibility_rows(rows: &mut [KnowledgeCrystalSourceVisibilityRow]) {
    rows.sort_by(|left, right| {
        left.community_id
            .cmp(&right.community_id)
            .then_with(|| left.crystal_memory_id.cmp(&right.crystal_memory_id))
            .then_with(|| left.source_memory_id.cmp(&right.source_memory_id))
            .then_with(|| left.entity_id.cmp(&right.entity_id))
            .then_with(|| left.crystal_node_id.cmp(&right.crystal_node_id))
            .then_with(|| left.source_node_id.cmp(&right.source_node_id))
            .then_with(|| left.entity_node_id.cmp(&right.entity_node_id))
    });
}

pub(super) fn create_knowledge_entity_for(
    db: &mut Database,
    request: &KnowledgeEntityCreateRequest,
) -> Result<KnowledgeEntityCreateOutput> {
    db.ensure_writable()?;
    validate_knowledge_entity_create(request)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    if let Some(existing) = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.label.as_str(),
        request.external_id.as_str(),
    )? {
        return Ok(KnowledgeEntityCreateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: Some(existing.id.0),
            created: false,
            already_exists: true,
            created_node_count: 0,
        });
    }

    let (cypher, parameters) = knowledge_entity_create_statement(request);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let created_node_count = output.rows.len();
    let created = created_node_count > 0;
    let node_id = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.label.as_str(),
        request.external_id.as_str(),
    )?
    .map(|node| node.id.0);
    Ok(KnowledgeEntityCreateOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        node_id,
        created,
        already_exists: false,
        created_node_count,
    })
}

pub(super) fn create_knowledge_entity_batch_for(
    db: &mut Database,
    request: &KnowledgeEntityCreateBatchRequest,
) -> Result<KnowledgeEntityCreateBatchOutput> {
    db.ensure_writable()?;
    for create in &request.creates {
        validate_knowledge_entity_create(create)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.creates.len());
    let mut created_count = 0;
    let mut already_exists_count = 0;
    let mut eligible_creates = Vec::new();
    let mut pending_identities = BTreeSet::new();

    for create in &request.creates {
        if let Some(existing) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            create.label.as_str(),
            create.external_id.as_str(),
        )? {
            already_exists_count += 1;
            rows.push(KnowledgeEntityCreateBatchRow {
                label: create.label.clone(),
                external_id: create.external_id.clone(),
                node_id: Some(existing.id.0),
                created: false,
                already_exists: true,
            });
            continue;
        }
        let identity = (create.label.clone(), create.external_id.clone());
        if !pending_identities.insert(identity) {
            already_exists_count += 1;
            rows.push(KnowledgeEntityCreateBatchRow {
                label: create.label.clone(),
                external_id: create.external_id.clone(),
                node_id: None,
                created: false,
                already_exists: true,
            });
            continue;
        }

        created_count += 1;
        eligible_creates.push(create.clone());
        rows.push(KnowledgeEntityCreateBatchRow {
            label: create.label.clone(),
            external_id: create.external_id.clone(),
            node_id: None,
            created: true,
            already_exists: false,
        });
    }

    if eligible_creates.is_empty() {
        return Ok(KnowledgeEntityCreateBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            created_count,
            already_exists_count,
            created_node_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_entity_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    let output = tx.commit()?;
    for row in &mut rows {
        if row.created {
            row.node_id = try_seed_node_by_label_and_external_id(
                &db.catalog,
                &db.store,
                row.label.as_str(),
                row.external_id.as_str(),
            )?
            .map(|node| node.id.0);
        }
    }
    Ok(KnowledgeEntityCreateBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        created_count,
        already_exists_count,
        created_node_count: output.rows.len(),
    })
}

pub(super) fn validate_knowledge_entity_create(request: &KnowledgeEntityCreateRequest) -> Result<()> {
    validate_cypher_identifier(&request.label, "label")?;
    if request.external_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge entity create requires a non-empty external id".to_string(),
        ));
    }
    for property in request.properties.keys() {
        validate_cypher_identifier(property, "property")?;
    }
    if let Some(id) = request.properties.get("id") {
        let property_external_id = value_to_external_id(id);
        if property_external_id != request.external_id {
            return Err(SkeinError::Semantic(format!(
                "knowledge entity create id property {property_external_id:?} does not match external id {:?}",
                request.external_id
            )));
        }
    }
    Ok(())
}

pub(super) fn knowledge_entity_create_statement(
    request: &KnowledgeEntityCreateRequest,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = format!("CREATE (:{} {{id: $external_id", request.label);
    let mut parameters = BTreeMap::from([(
        "external_id".to_string(),
        Value::String(request.external_id.clone()),
    )]);
    for (index, (property, value)) in request
        .properties
        .iter()
        .filter(|(property, _)| property.as_str() != "id")
        .enumerate()
    {
        let parameter_name = format!("property_value_{index}");
        cypher.push_str(&format!(", {property}: ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    cypher.push_str("})");
    (cypher, parameters)
}

pub(super) fn upsert_knowledge_entity_for(
    db: &mut Database,
    request: &KnowledgeEntityUpsertRequest,
) -> Result<KnowledgeEntityUpsertOutput> {
    db.ensure_writable()?;
    validate_knowledge_entity_upsert(request)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let update_properties = knowledge_entity_upsert_update_properties(request);
    if let Some(existing) = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.label.as_str(),
        request.external_id.as_str(),
    )? {
        let node_id = existing.id.0;
        if !node_has_external_id_property(&existing, request.external_id.as_str()) {
            return Ok(KnowledgeEntityUpsertOutput {
                graph_commit_epoch_before,
                graph_commit_epoch_after: graph_commit_epoch_before,
                node_id: Some(node_id),
                created: false,
                updated: false,
                already_exists: true,
                non_writable: true,
                created_node_count: 0,
                updated_property_count: 0,
            });
        }
        if update_properties.is_empty() {
            return Ok(KnowledgeEntityUpsertOutput {
                graph_commit_epoch_before,
                graph_commit_epoch_after: graph_commit_epoch_before,
                node_id: Some(node_id),
                created: false,
                updated: false,
                already_exists: true,
                non_writable: false,
                created_node_count: 0,
                updated_property_count: 0,
            });
        }
        let (cypher, parameters) = knowledge_property_update_statement(
            request.label.as_str(),
            node_id,
            &update_properties,
        );
        db.query_with_params(cypher.as_str(), &parameters)?;
        return Ok(KnowledgeEntityUpsertOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: db.store.commit_epoch(),
            node_id: Some(node_id),
            created: false,
            updated: true,
            already_exists: true,
            non_writable: false,
            created_node_count: 0,
            updated_property_count: update_properties.len(),
        });
    }

    let create = knowledge_entity_upsert_create_request(request);
    let (cypher, parameters) = knowledge_entity_create_statement(&create);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let node_id = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.label.as_str(),
        request.external_id.as_str(),
    )?
    .map(|node| node.id.0);
    Ok(KnowledgeEntityUpsertOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        node_id,
        created: true,
        updated: false,
        already_exists: false,
        non_writable: false,
        created_node_count: output.rows.len(),
        updated_property_count: 0,
    })
}

pub(super) fn upsert_knowledge_entity_batch_for(
    db: &mut Database,
    request: &KnowledgeEntityUpsertBatchRequest,
) -> Result<KnowledgeEntityUpsertBatchOutput> {
    db.ensure_writable()?;
    for upsert in &request.upserts {
        validate_knowledge_entity_upsert(upsert)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.upserts.len());
    let mut created_count = 0;
    let mut updated_count = 0;
    let mut already_exists_count = 0;
    let mut non_writable_count = 0;
    let mut updated_property_count = 0;
    let mut eligible_creates = Vec::new();
    let mut eligible_updates = Vec::new();
    let mut pending_identities = BTreeSet::new();

    for upsert in &request.upserts {
        if let Some(existing) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            upsert.label.as_str(),
            upsert.external_id.as_str(),
        )? {
            let node_id = existing.id.0;
            already_exists_count += 1;
            if !node_has_external_id_property(&existing, upsert.external_id.as_str()) {
                non_writable_count += 1;
                rows.push(KnowledgeEntityUpsertBatchRow {
                    label: upsert.label.clone(),
                    external_id: upsert.external_id.clone(),
                    node_id: Some(node_id),
                    created: false,
                    updated: false,
                    already_exists: true,
                    non_writable: true,
                    updated_property_count: 0,
                });
                continue;
            }
            let update_properties = knowledge_entity_upsert_update_properties(upsert);
            if update_properties.is_empty() {
                rows.push(KnowledgeEntityUpsertBatchRow {
                    label: upsert.label.clone(),
                    external_id: upsert.external_id.clone(),
                    node_id: Some(node_id),
                    created: false,
                    updated: false,
                    already_exists: true,
                    non_writable: false,
                    updated_property_count: 0,
                });
                continue;
            }
            let row_updated_property_count = update_properties.len();
            updated_count += 1;
            updated_property_count += row_updated_property_count;
            eligible_updates.push((upsert.label.clone(), node_id, update_properties));
            rows.push(KnowledgeEntityUpsertBatchRow {
                label: upsert.label.clone(),
                external_id: upsert.external_id.clone(),
                node_id: Some(node_id),
                created: false,
                updated: true,
                already_exists: true,
                non_writable: false,
                updated_property_count: row_updated_property_count,
            });
            continue;
        }

        let identity = (upsert.label.clone(), upsert.external_id.clone());
        if !pending_identities.insert(identity) {
            already_exists_count += 1;
            rows.push(KnowledgeEntityUpsertBatchRow {
                label: upsert.label.clone(),
                external_id: upsert.external_id.clone(),
                node_id: None,
                created: false,
                updated: false,
                already_exists: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        created_count += 1;
        eligible_creates.push(knowledge_entity_upsert_create_request(upsert));
        rows.push(KnowledgeEntityUpsertBatchRow {
            label: upsert.label.clone(),
            external_id: upsert.external_id.clone(),
            node_id: None,
            created: true,
            updated: false,
            already_exists: false,
            non_writable: false,
            updated_property_count: 0,
        });
    }

    if eligible_creates.is_empty() && eligible_updates.is_empty() {
        return Ok(KnowledgeEntityUpsertBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            created_count,
            updated_count,
            already_exists_count,
            non_writable_count,
            created_node_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_entity_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    for (label, node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement(label.as_str(), *node_id, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    for row in &mut rows {
        if row.created {
            row.node_id = try_seed_node_by_label_and_external_id(
                &db.catalog,
                &db.store,
                row.label.as_str(),
                row.external_id.as_str(),
            )?
            .map(|node| node.id.0);
        }
    }
    Ok(KnowledgeEntityUpsertBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        created_count,
        updated_count,
        already_exists_count,
        non_writable_count,
        created_node_count: eligible_creates.len(),
        updated_property_count,
    })
}

pub(super) fn validate_knowledge_entity_upsert(request: &KnowledgeEntityUpsertRequest) -> Result<()> {
    validate_cypher_identifier(&request.label, "label")?;
    if request.external_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge entity upsert requires a non-empty external id".to_string(),
        ));
    }
    validate_knowledge_entity_upsert_properties(
        request.external_id.as_str(),
        "create",
        &request.create_properties,
    )?;
    validate_knowledge_entity_upsert_properties(
        request.external_id.as_str(),
        "update",
        &request.update_properties,
    )
}

pub(super) fn validate_knowledge_entity_upsert_properties(
    external_id: &str,
    phase: &str,
    properties: &BTreeMap<String, Value>,
) -> Result<()> {
    for property in properties.keys() {
        validate_cypher_identifier(property, "property")?;
    }
    if let Some(id) = properties.get("id") {
        let property_external_id = value_to_external_id(id);
        if property_external_id != external_id {
            return Err(SkeinError::Semantic(format!(
                "knowledge entity upsert {phase} id property {property_external_id:?} does not match external id {external_id:?}"
            )));
        }
    }
    Ok(())
}

pub(super) fn knowledge_entity_upsert_create_request(
    request: &KnowledgeEntityUpsertRequest,
) -> KnowledgeEntityCreateRequest {
    KnowledgeEntityCreateRequest {
        label: request.label.clone(),
        external_id: request.external_id.clone(),
        properties: request.create_properties.clone(),
    }
}

pub(super) fn knowledge_entity_upsert_update_properties(
    request: &KnowledgeEntityUpsertRequest,
) -> BTreeMap<String, Value> {
    request
        .update_properties
        .iter()
        .filter(|(property, _)| property.as_str() != "id")
        .map(|(property, value)| (property.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
pub(super) fn dedup_property_names(property_names: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    property_names
        .iter()
        .filter(|name| seen.insert((*name).clone()))
        .cloned()
        .collect()
}

#[cfg(test)]
pub(super) fn empty_property_projection(property_names: &[String]) -> BTreeMap<String, Option<Value>> {
    property_names
        .iter()
        .cloned()
        .map(|name| (name, None))
        .collect()
}

#[cfg(test)]
pub(super) fn project_knowledge_entity_properties(
    entity: &KnowledgeEntity,
    property_names: &[String],
) -> BTreeMap<String, Option<Value>> {
    property_names
        .iter()
        .cloned()
        .map(|name| {
            let value = entity.properties.get(&name).cloned();
            (name, value)
        })
        .collect()
}

pub(super) fn update_knowledge_properties_for(
    db: &mut Database,
    request: &KnowledgePropertyUpdateRequest,
) -> Result<KnowledgePropertyUpdateOutput> {
    update_scoped_knowledge_properties_for(
        db,
        &KnowledgeScopedPropertyUpdateRequest {
            update: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn update_scoped_knowledge_properties_for(
    db: &mut Database,
    request: &KnowledgeScopedPropertyUpdateRequest,
) -> Result<KnowledgePropertyUpdateOutput> {
    db.ensure_writable()?;
    if request.update.assignments.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge property update requires at least one assignment".to_string(),
        ));
    }
    validate_cypher_identifier(&request.update.entity.label, "label")?;
    for property in request.update.assignments.keys() {
        validate_cypher_identifier(property, "property")?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(seed) = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.update.entity.label.as_str(),
        request.update.entity.external_id.as_str(),
    )?
    else {
        return Ok(KnowledgePropertyUpdateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: None,
            matched: false,
            filtered_out: false,
            updated_property_count: 0,
        });
    };
    if !request.metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            &db.store,
            &seed,
            &request.metadata_filters,
        )
    {
        let node_id = seed.id.0;
        return Ok(KnowledgePropertyUpdateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: Some(node_id),
            matched: false,
            filtered_out: true,
            updated_property_count: 0,
        });
    }
    let node_id = seed.id.0;

    let (cypher, parameters) = knowledge_property_update_statement(
        request.update.entity.label.as_str(),
        node_id,
        &request.update.assignments,
    );
    db.query_with_params(cypher.as_str(), &parameters)?;
    Ok(KnowledgePropertyUpdateOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        node_id: Some(node_id),
        matched: true,
        filtered_out: false,
        updated_property_count: request.update.assignments.len(),
    })
}

pub(super) fn knowledge_property_update_statement(
    label: &str,
    node_id: u64,
    assignments: &BTreeMap<String, Value>,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = format!("MATCH (n:{label}) WHERE id(n) = $node_id SET ");
    let mut parameters = BTreeMap::from([("node_id".to_string(), Value::Int(node_id as i64))]);
    for (index, (property, value)) in assignments.iter().enumerate() {
        if index > 0 {
            cypher.push_str(", ");
        }
        let parameter_name = format!("value_{index}");
        cypher.push_str(&format!("n.{property} = ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    (cypher, parameters)
}

pub(super) fn update_knowledge_properties_batch_for(
    db: &mut Database,
    request: &KnowledgePropertyUpdateBatchRequest,
) -> Result<KnowledgePropertyUpdateBatchOutput> {
    update_scoped_knowledge_properties_batch_for(
        db,
        &KnowledgeScopedPropertyUpdateBatchRequest {
            updates: request.updates.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn update_scoped_knowledge_properties_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedPropertyUpdateBatchRequest,
) -> Result<KnowledgePropertyUpdateBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.assignments.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge property batch update requires every row to have at least one assignment"
                    .to_string(),
            ));
        }
        validate_cypher_identifier(&update.entity.label, "label")?;
        for property in update.assignments.keys() {
            validate_cypher_identifier(property, "property")?;
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut updated_property_count = 0;
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            update.entity.label.as_str(),
            update.entity.external_id.as_str(),
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgePropertyUpdateBatchRow {
                entity: update.entity.clone(),
                node_id: None,
                matched: false,
                filtered_out: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id.0;
        if !node_has_external_id_property(&seed, update.entity.external_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgePropertyUpdateBatchRow {
                entity: update.entity.clone(),
                node_id: Some(node_id),
                matched: false,
                filtered_out: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !request.metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                &db.store,
                &seed,
                &request.metadata_filters,
            )
        {
            filtered_out_count += 1;
            rows.push(KnowledgePropertyUpdateBatchRow {
                entity: update.entity.clone(),
                node_id: Some(node_id),
                matched: false,
                filtered_out: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let row_updated_property_count = update.assignments.len();
        matched_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((update.clone(), node_id));
        rows.push(KnowledgePropertyUpdateBatchRow {
            entity: update.entity.clone(),
            node_id: Some(node_id),
            matched: true,
            filtered_out: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgePropertyUpdateBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            filtered_out_count,
            non_writable_count,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (update, node_id) in &eligible_updates {
        let (cypher, parameters) = knowledge_property_update_statement(
            update.entity.label.as_str(),
            *node_id,
            &update.assignments,
        );
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    Ok(KnowledgePropertyUpdateBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        filtered_out_count,
        non_writable_count,
        updated_property_count,
    })
}

pub(super) fn move_knowledge_normalized_space_batch_for(
    db: &mut Database,
    request: &KnowledgeNormalizedSpaceMoveBatchRequest,
) -> Result<KnowledgeNormalizedSpaceMoveBatchOutput> {
    db.ensure_writable()?;
    validate_cypher_identifier(&request.label, "label")?;
    validate_cypher_identifier(&request.identity_property, "identity property")?;
    if request.target_space_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge normalized space move requires a non-empty target space id".to_string(),
        ));
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.external_ids.len());
    let mut moved_external_ids = Vec::new();
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut source_mismatch_count = 0;
    let mut already_in_target_count = 0;
    let mut duplicate_count = 0;
    let mut eligible_updates = Vec::new();
    let mut pending_node_ids = BTreeSet::new();

    for external_id in &request.external_ids {
        let Some(node) = try_node_by_label_property_external_id(
            &db.catalog,
            &db.store,
            request.label.as_str(),
            request.identity_property.as_str(),
            external_id.as_str(),
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeNormalizedSpaceMoveBatchRow {
                external_id: external_id.clone(),
                node_id: None,
                matched: false,
                moved: false,
                source_mismatch: false,
                already_in_target: false,
                duplicate: false,
            });
            continue;
        };
        matched_count += 1;
        let node_id = node.id.0;
        let normalized_space_id = normalized_node_space_id(&node);
        if request
            .source_space_id
            .as_deref()
            .is_some_and(|source_space_id| normalized_space_id != source_space_id)
        {
            source_mismatch_count += 1;
            rows.push(KnowledgeNormalizedSpaceMoveBatchRow {
                external_id: external_id.clone(),
                node_id: Some(node_id),
                matched: true,
                moved: false,
                source_mismatch: true,
                already_in_target: false,
                duplicate: false,
            });
            continue;
        }
        if normalized_space_id == request.target_space_id {
            already_in_target_count += 1;
            rows.push(KnowledgeNormalizedSpaceMoveBatchRow {
                external_id: external_id.clone(),
                node_id: Some(node_id),
                matched: true,
                moved: false,
                source_mismatch: false,
                already_in_target: true,
                duplicate: false,
            });
            continue;
        }
        if !pending_node_ids.insert(node.id) {
            duplicate_count += 1;
            rows.push(KnowledgeNormalizedSpaceMoveBatchRow {
                external_id: external_id.clone(),
                node_id: Some(node_id),
                matched: true,
                moved: false,
                source_mismatch: false,
                already_in_target: false,
                duplicate: true,
            });
            continue;
        }

        let mut assignments = BTreeMap::from([(
            "space_id".to_string(),
            Value::String(request.target_space_id.clone()),
        )]);
        if let Some(updated_at) = &request.updated_at {
            assignments.insert("updated_at".to_string(), updated_at.clone());
        }
        eligible_updates.push((node.id, assignments));
        moved_external_ids.push(external_id.clone());
        rows.push(KnowledgeNormalizedSpaceMoveBatchRow {
            external_id: external_id.clone(),
            node_id: Some(node_id),
            matched: true,
            moved: true,
            source_mismatch: false,
            already_in_target: false,
            duplicate: false,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeNormalizedSpaceMoveBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            moved_external_ids,
            matched_count,
            missing_count,
            source_mismatch_count,
            already_in_target_count,
            duplicate_count,
            moved_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement(request.label.as_str(), node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    Ok(KnowledgeNormalizedSpaceMoveBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        moved_external_ids,
        matched_count,
        missing_count,
        source_mismatch_count,
        already_in_target_count,
        duplicate_count,
        moved_count: eligible_updates.len(),
    })
}

pub(super) fn touch_knowledge_memory_access_batch_for(
    db: &mut Database,
    request: &KnowledgeMemoryAccessBatchRequest,
) -> Result<KnowledgeMemoryAccessBatchOutput> {
    db.ensure_writable()?;
    for touch in &request.touches {
        if touch.memory_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge memory access touch requires a non-empty memory id".to_string(),
            ));
        }
        if touch
            .click_dwell_time_ms
            .is_some_and(|dwell_time_ms| dwell_time_ms < 0)
        {
            return Err(SkeinError::Semantic(
                "knowledge memory access touch requires non-negative dwell time".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.touches.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut non_writable_count = 0;
    let mut touched_count = 0;
    let mut click_touch_count = 0;
    let mut eligible_touches = BTreeMap::new();

    for touch in &request.touches {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Memory",
            &touch.memory_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeMemoryAccessBatchRow {
                memory_id: touch.memory_id.clone(),
                node_id: None,
                matched: false,
                touched: false,
                clicked: false,
                non_writable: false,
            });
            continue;
        };
        let node_id = seed.id.0;
        if !node_has_external_id_property(&seed, touch.memory_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeMemoryAccessBatchRow {
                memory_id: touch.memory_id.clone(),
                node_id: Some(node_id),
                matched: false,
                touched: false,
                clicked: false,
                non_writable: true,
            });
            continue;
        }

        matched_count += 1;
        touched_count += 1;
        let clicked = touch.click_dwell_time_ms.is_some();
        if clicked {
            click_touch_count += 1;
        }
        eligible_touches
            .entry(seed.id)
            .and_modify(|aggregated: &mut AggregatedMemoryAccessTouch| {
                aggregated.access_count_increment += 1;
                aggregated.last_accessed_at = touch.accessed_at.clone();
                if let Some(dwell_time_ms) = touch.click_dwell_time_ms {
                    aggregated.click_increment += 1;
                    aggregated.dwell_time_ms_increment += dwell_time_ms;
                    aggregated.last_clicked_at = Some(touch.accessed_at.clone());
                }
            })
            .or_insert_with(|| AggregatedMemoryAccessTouch {
                access_count_increment: 1,
                last_accessed_at: touch.accessed_at.clone(),
                click_increment: usize::from(clicked),
                dwell_time_ms_increment: touch.click_dwell_time_ms.unwrap_or(0),
                last_clicked_at: clicked.then(|| touch.accessed_at.clone()),
            });
        rows.push(KnowledgeMemoryAccessBatchRow {
            memory_id: touch.memory_id.clone(),
            node_id: Some(node_id),
            matched: true,
            touched: true,
            clicked,
            non_writable: false,
        });
    }

    if eligible_touches.is_empty() {
        return Ok(KnowledgeMemoryAccessBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            non_writable_count,
            touched_count: 0,
            click_touch_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, touch) in &eligible_touches {
        let (cypher, parameters) = knowledge_memory_access_touch_statement(*node_id, touch);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeMemoryAccessBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        non_writable_count,
        touched_count,
        click_touch_count,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AggregatedMemoryAccessTouch {
    access_count_increment: usize,
    last_accessed_at: Value,
    click_increment: usize,
    dwell_time_ms_increment: i64,
    last_clicked_at: Option<Value>,
}

pub(super) fn knowledge_memory_access_touch_statement(
    node_id: NodeId,
    touch: &AggregatedMemoryAccessTouch,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = "MATCH (m:Memory) WHERE id(m) = $node_id SET m.access_count = COALESCE(m.access_count, 0) + $access_count_increment, m.last_accessed_at = $last_accessed_at".to_string();
    let mut parameters = BTreeMap::from([
        ("node_id".to_string(), Value::Int(node_id.0 as i64)),
        (
            "access_count_increment".to_string(),
            Value::Int(touch.access_count_increment as i64),
        ),
        (
            "last_accessed_at".to_string(),
            touch.last_accessed_at.clone(),
        ),
    ]);
    if touch.click_increment > 0 {
        cypher.push_str(", m.clicks = COALESCE(m.clicks, 0) + $click_increment, m.last_clicked_at = $last_clicked_at, m.total_dwell_time_ms = COALESCE(m.total_dwell_time_ms, 0) + $dwell_time_ms_increment");
        parameters.insert(
            "click_increment".to_string(),
            Value::Int(touch.click_increment as i64),
        );
        parameters.insert(
            "last_clicked_at".to_string(),
            touch
                .last_clicked_at
                .clone()
                .expect("click aggregate should carry last_clicked_at"),
        );
        parameters.insert(
            "dwell_time_ms_increment".to_string(),
            Value::Int(touch.dwell_time_ms_increment),
        );
    }
    (cypher, parameters)
}

pub(super) fn update_knowledge_memory_content_batch_for(
    db: &mut Database,
    request: &KnowledgeMemoryContentBatchRequest,
) -> Result<KnowledgeMemoryContentBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        validate_knowledge_memory_content_update(update)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Memory",
            &update.memory_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeMemoryContentBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.memory_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeMemoryContentBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeMemoryContentBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = memory_content_assignments(update);
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeMemoryContentBatchRow {
            memory_id: update.memory_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeMemoryContentBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Memory", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeMemoryContentBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn validate_knowledge_memory_content_update(update: &KnowledgeMemoryContentUpdate) -> Result<()> {
    if update.memory_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge memory content update requires a non-empty memory id".to_string(),
        ));
    }
    if update.unit_type.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge memory content update requires a non-empty unit type".to_string(),
        ));
    }
    if update.extraction_method.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge memory content update requires a non-empty extraction method".to_string(),
        ));
    }
    validate_finite_numeric_value(
        &update.importance,
        "knowledge memory content update requires numeric finite importance",
    )?;
    validate_finite_numeric_value(
        &update.confidence,
        "knowledge memory content update requires numeric finite confidence",
    )?;
    Ok(())
}

pub(super) fn validate_finite_numeric_value(value: &Value, message: &str) -> Result<()> {
    let valid = match value {
        Value::Float(value) => value.is_finite(),
        Value::Int(_) => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(SkeinError::Semantic(message.to_string()))
    }
}

pub(super) fn memory_content_assignments(update: &KnowledgeMemoryContentUpdate) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("content".to_string(), Value::String(update.content.clone())),
        ("title".to_string(), Value::String(update.title.clone())),
        (
            "semantic_field".to_string(),
            Value::String(update.semantic_field.clone()),
        ),
        ("importance".to_string(), update.importance.clone()),
        ("confidence".to_string(), update.confidence.clone()),
        (
            "unit_type".to_string(),
            Value::String(update.unit_type.clone()),
        ),
        ("source".to_string(), Value::String(update.source.clone())),
        ("source_range".to_string(), update.source_range.clone()),
        (
            "space_id".to_string(),
            Value::String(update.space_id.clone()),
        ),
        ("updated_at".to_string(), update.updated_at.clone()),
        (
            "reindex_needed".to_string(),
            Value::Bool(update.reindex_needed),
        ),
        (
            "review_status".to_string(),
            Value::String(update.review_status.clone()),
        ),
        (
            "extraction_method".to_string(),
            Value::String(update.extraction_method.clone()),
        ),
    ])
}

pub(super) fn update_knowledge_memory_metadata_batch_for(
    db: &mut Database,
    request: &KnowledgeMemoryMetadataBatchRequest,
) -> Result<KnowledgeMemoryMetadataBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.memory_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge memory metadata update requires a non-empty memory id".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Memory",
            &update.memory_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeMemoryMetadataBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.memory_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeMemoryMetadataBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeMemoryMetadataBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let mut assignments = BTreeMap::from([("metadata".to_string(), update.metadata.clone())]);
        if let Some(updated_at) = &update.updated_at {
            assignments.insert("updated_at".to_string(), updated_at.clone());
        }
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeMemoryMetadataBatchRow {
            memory_id: update.memory_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeMemoryMetadataBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Memory", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeMemoryMetadataBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn update_knowledge_memory_dedup_reviewed_batch_for(
    db: &mut Database,
    request: &KnowledgeMemoryDedupReviewedBatchRequest,
) -> Result<KnowledgeMemoryDedupReviewedBatchOutput> {
    db.ensure_writable()?;
    if request.memory_ids.iter().any(String::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge memory dedup reviewed update requires non-empty memory ids".to_string(),
        ));
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.memory_ids.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for memory_id in &request.memory_ids {
        let Some(seed) =
            try_seed_node_by_label_and_external_id(&db.catalog, &db.store, "Memory", memory_id)?
        else {
            missing_count += 1;
            rows.push(KnowledgeMemoryDedupReviewedBatchRow {
                memory_id: memory_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, memory_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeMemoryDedupReviewedBatchRow {
                memory_id: memory_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeMemoryDedupReviewedBatchRow {
                memory_id: memory_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
            });
            continue;
        }

        matched_count += 1;
        eligible_updates.push((
            node_id,
            BTreeMap::from([("dedup_reviewed_at".to_string(), request.reviewed_at.clone())]),
        ));
        rows.push(KnowledgeMemoryDedupReviewedBatchRow {
            memory_id: memory_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeMemoryDedupReviewedBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Memory", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeMemoryDedupReviewedBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count: eligible_updates.len(),
    })
}

pub(super) fn update_knowledge_memory_decay_refresh_batch_for(
    db: &mut Database,
    request: &KnowledgeMemoryDecayRefreshBatchRequest,
) -> Result<KnowledgeMemoryDecayRefreshBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        validate_knowledge_memory_decay_refresh_update(update)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Memory",
            &update.memory_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeMemoryDecayRefreshBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.memory_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeMemoryDecayRefreshBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeMemoryDecayRefreshBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = memory_decay_refresh_assignments(update);
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeMemoryDecayRefreshBatchRow {
            memory_id: update.memory_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeMemoryDecayRefreshBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Memory", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeMemoryDecayRefreshBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn validate_knowledge_memory_decay_refresh_update(
    update: &KnowledgeMemoryDecayRefreshUpdate,
) -> Result<()> {
    if update.memory_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge memory decay refresh update requires a non-empty memory id".to_string(),
        ));
    }
    validate_finite_numeric_value(
        &update.decay_score_cached,
        "knowledge memory decay refresh update requires numeric finite decay score",
    )?;
    if let Some(confidence) = &update.confidence {
        validate_finite_numeric_value(
            confidence,
            "knowledge memory decay refresh update requires numeric finite confidence",
        )?;
    }
    Ok(())
}

pub(super) fn memory_decay_refresh_assignments(
    update: &KnowledgeMemoryDecayRefreshUpdate,
) -> BTreeMap<String, Value> {
    let mut assignments = BTreeMap::from([(
        "decay_score_cached".to_string(),
        update.decay_score_cached.clone(),
    )]);
    if let Some(confidence) = &update.confidence {
        assignments.insert("confidence".to_string(), confidence.clone());
    }
    assignments
}

pub(super) fn adjust_knowledge_source_memory_count_batch_for(
    db: &mut Database,
    request: &KnowledgeSourceMemoryCountBatchRequest,
) -> Result<KnowledgeSourceMemoryCountBatchOutput> {
    db.ensure_writable()?;
    for adjustment in &request.adjustments {
        if adjustment.source_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source memory count adjustment requires a non-empty source id"
                    .to_string(),
            ));
        }
        if adjustment.delta == 0 {
            return Err(SkeinError::Semantic(
                "knowledge source memory count adjustment requires a non-zero delta".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.adjustments.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut non_writable_count = 0;
    let mut invalid_current_count_count = 0;
    let mut adjusted_count = 0;
    let mut aggregates: BTreeMap<NodeId, AggregatedSourceMemoryCountAdjustment> = BTreeMap::new();

    for adjustment in &request.adjustments {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Source",
            &adjustment.source_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeSourceMemoryCountBatchRow {
                source_id: adjustment.source_id.clone(),
                node_id: None,
                matched: false,
                adjusted: false,
                non_writable: false,
                invalid_current_count: false,
                old_count: None,
                new_count: None,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, adjustment.source_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeSourceMemoryCountBatchRow {
                source_id: adjustment.source_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                adjusted: false,
                non_writable: true,
                invalid_current_count: false,
                old_count: None,
                new_count: None,
            });
            continue;
        }
        let Some(current_count) = source_memory_count(&seed) else {
            invalid_current_count_count += 1;
            rows.push(KnowledgeSourceMemoryCountBatchRow {
                source_id: adjustment.source_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                adjusted: false,
                non_writable: false,
                invalid_current_count: true,
                old_count: None,
                new_count: None,
            });
            continue;
        };

        let aggregate =
            aggregates
                .entry(node_id)
                .or_insert_with(|| AggregatedSourceMemoryCountAdjustment {
                    old_count: current_count,
                    new_count: current_count,
                });
        let old_count = aggregate.new_count;
        aggregate.new_count = apply_source_memory_count_delta(old_count, adjustment.delta);
        matched_count += 1;
        adjusted_count += 1;
        rows.push(KnowledgeSourceMemoryCountBatchRow {
            source_id: adjustment.source_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            adjusted: true,
            non_writable: false,
            invalid_current_count: false,
            old_count: Some(old_count),
            new_count: Some(aggregate.new_count),
        });
    }

    if aggregates.is_empty() {
        return Ok(KnowledgeSourceMemoryCountBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            non_writable_count,
            invalid_current_count_count,
            adjusted_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, adjustment) in &aggregates {
        let (cypher, parameters) =
            knowledge_source_memory_count_set_statement(*node_id, adjustment.new_count);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeSourceMemoryCountBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        non_writable_count,
        invalid_current_count_count,
        adjusted_count,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AggregatedSourceMemoryCountAdjustment {
    old_count: i64,
    new_count: i64,
}

pub(super) fn source_memory_count(node: &NodeRecord) -> Option<i64> {
    match node.properties.get("memory_count") {
        None | Some(Value::Null) => Some(0),
        Some(Value::Int(value)) => Some((*value).max(0)),
        Some(_) => None,
    }
}

pub(super) fn apply_source_memory_count_delta(current_count: i64, delta: i64) -> i64 {
    current_count.saturating_add(delta).max(0)
}

pub(super) fn knowledge_source_memory_count_set_statement(
    node_id: NodeId,
    memory_count: i64,
) -> (String, BTreeMap<String, Value>) {
    (
        "MATCH (s:Source) WHERE id(s) = $node_id SET s.memory_count = $memory_count".to_string(),
        BTreeMap::from([
            ("node_id".to_string(), Value::Int(node_id.0 as i64)),
            ("memory_count".to_string(), Value::Int(memory_count)),
        ]),
    )
}

pub(super) fn update_knowledge_source_lifecycle_batch_for(
    db: &mut Database,
    request: &KnowledgeSourceLifecycleBatchRequest,
) -> Result<KnowledgeSourceLifecycleBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.source_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source lifecycle update requires a non-empty source id".to_string(),
            ));
        }
        if update.lifecycle_state.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source lifecycle update requires a non-empty lifecycle state"
                    .to_string(),
            ));
        }
        if update
            .current_lifecycle_state
            .as_deref()
            .is_some_and(str::is_empty)
        {
            return Err(SkeinError::Semantic(
                "knowledge source lifecycle update requires a non-empty current lifecycle state"
                    .to_string(),
            ));
        }
        if update
            .chunk_count
            .is_some_and(|chunk_count| chunk_count < 0)
        {
            return Err(SkeinError::Semantic(
                "knowledge source lifecycle update requires non-negative chunk count".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Source",
            &update.source_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeSourceLifecycleBatchRow {
                source_id: update.source_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                filtered_out: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.source_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeSourceLifecycleBatchRow {
                source_id: update.source_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                filtered_out: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if update
            .current_lifecycle_state
            .as_deref()
            .is_some_and(|state| {
                seed.properties
                    .get("lifecycle_state")
                    .map(value_to_external_id)
                    .as_deref()
                    != Some(state)
            })
        {
            filtered_out_count += 1;
            rows.push(KnowledgeSourceLifecycleBatchRow {
                source_id: update.source_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                filtered_out: true,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeSourceLifecycleBatchRow {
                source_id: update.source_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                filtered_out: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let mut assignments = BTreeMap::from([
            (
                "lifecycle_state".to_string(),
                Value::String(update.lifecycle_state.clone()),
            ),
            ("updated_at".to_string(), update.updated_at.clone()),
        ]);
        if let Some(chunk_count) = update.chunk_count {
            assignments.insert("chunk_count".to_string(), Value::Int(chunk_count));
        }
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeSourceLifecycleBatchRow {
            source_id: update.source_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            filtered_out: false,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeSourceLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            filtered_out_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Source", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeSourceLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        filtered_out_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn update_knowledge_source_metadata_batch_for(
    db: &mut Database,
    request: &KnowledgeSourceMetadataBatchRequest,
) -> Result<KnowledgeSourceMetadataBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.source_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source metadata update requires a non-empty source id".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Source",
            &update.source_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeSourceMetadataBatchRow {
                source_id: update.source_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.source_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeSourceMetadataBatchRow {
                source_id: update.source_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeSourceMetadataBatchRow {
                source_id: update.source_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = BTreeMap::from([
            ("metadata".to_string(), update.metadata.clone()),
            ("updated_at".to_string(), update.updated_at.clone()),
        ]);
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeSourceMetadataBatchRow {
            source_id: update.source_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeSourceMetadataBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Source", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeSourceMetadataBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn update_knowledge_source_parsed_metadata_batch_for(
    db: &mut Database,
    request: &KnowledgeSourceParsedMetadataBatchRequest,
) -> Result<KnowledgeSourceParsedMetadataBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.source_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source parsed metadata update requires a non-empty source id"
                    .to_string(),
            ));
        }
        if update.sha256.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source parsed metadata update requires a non-empty sha256".to_string(),
            ));
        }
        if update.size_bytes < 0 {
            return Err(SkeinError::Semantic(
                "knowledge source parsed metadata update requires non-negative size bytes"
                    .to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Source",
            &update.source_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeSourceParsedMetadataBatchRow {
                source_id: update.source_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.source_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeSourceParsedMetadataBatchRow {
                source_id: update.source_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeSourceParsedMetadataBatchRow {
                source_id: update.source_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = source_parsed_metadata_assignments(update);
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeSourceParsedMetadataBatchRow {
            source_id: update.source_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeSourceParsedMetadataBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Source", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeSourceParsedMetadataBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn source_parsed_metadata_assignments(
    update: &KnowledgeSourceParsedMetadataUpdate,
) -> BTreeMap<String, Value> {
    let mut assignments = BTreeMap::from([
        (
            "lifecycle_state".to_string(),
            Value::String("parsed".to_string()),
        ),
        ("summary".to_string(), Value::String(update.summary.clone())),
        ("sha256".to_string(), Value::String(update.sha256.clone())),
        ("size_bytes".to_string(), Value::Int(update.size_bytes)),
        ("updated_at".to_string(), update.updated_at.clone()),
    ]);
    if let Some(parsed_path) = update.parsed_path.as_ref() {
        assignments.insert(
            "parsed_path".to_string(),
            Value::String(parsed_path.clone()),
        );
    }
    if let Some(file_path) = update.file_path.as_ref() {
        assignments.insert("file_path".to_string(), Value::String(file_path.clone()));
    }
    if let Some(original_name) = update.original_name.as_ref() {
        assignments.insert(
            "original_name".to_string(),
            Value::String(original_name.clone()),
        );
    }
    if let Some(mime_type) = update.mime_type.as_ref() {
        assignments.insert("mime_type".to_string(), Value::String(mime_type.clone()));
    }
    if let Some(source_url) = update.source_url.as_ref() {
        assignments.insert("source_url".to_string(), Value::String(source_url.clone()));
    }
    if let Some(metadata) = update.metadata.as_ref() {
        assignments.insert("metadata".to_string(), metadata.clone());
    }
    assignments
}

pub(super) fn create_knowledge_source_parsed_batch_for(
    db: &mut Database,
    request: &KnowledgeSourceParsedCreateBatchRequest,
) -> Result<KnowledgeSourceParsedCreateBatchOutput> {
    for create in &request.creates {
        validate_knowledge_source_parsed_create(create)?;
    }

    let entity_creates = request
        .creates
        .iter()
        .map(knowledge_source_parsed_entity_create)
        .collect::<Vec<_>>();
    let output = create_knowledge_entity_batch_for(
        db,
        &KnowledgeEntityCreateBatchRequest {
            creates: entity_creates,
        },
    )?;

    Ok(KnowledgeSourceParsedCreateBatchOutput {
        graph_commit_epoch_before: output.graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        rows: output
            .rows
            .into_iter()
            .map(|row| KnowledgeSourceParsedCreateBatchRow {
                source_id: row.external_id,
                node_id: row.node_id,
                created: row.created,
                already_exists: row.already_exists,
            })
            .collect(),
        created_count: output.created_count,
        already_exists_count: output.already_exists_count,
        created_node_count: output.created_node_count,
    })
}

pub(super) fn validate_knowledge_source_parsed_create(create: &KnowledgeSourceParsedCreate) -> Result<()> {
    if create.source_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source parsed create requires a non-empty source id".to_string(),
        ));
    }
    if create.source_type.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source parsed create requires a non-empty source type".to_string(),
        ));
    }
    if create.original_name.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source parsed create requires a non-empty original name".to_string(),
        ));
    }
    if create.mime_type.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source parsed create requires a non-empty mime type".to_string(),
        ));
    }
    if create.parsed_path.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source parsed create requires a non-empty parsed path".to_string(),
        ));
    }
    if create.sha256.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source parsed create requires a non-empty sha256".to_string(),
        ));
    }
    if create.size_bytes < 0 {
        return Err(SkeinError::Semantic(
            "knowledge source parsed create requires non-negative size bytes".to_string(),
        ));
    }
    if create.version < 1 {
        return Err(SkeinError::Semantic(
            "knowledge source parsed create requires a positive version".to_string(),
        ));
    }
    if create.space_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source parsed create requires a non-empty space id".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn knowledge_source_parsed_entity_create(
    create: &KnowledgeSourceParsedCreate,
) -> KnowledgeEntityCreateRequest {
    KnowledgeEntityCreateRequest {
        label: "Source".to_string(),
        external_id: create.source_id.clone(),
        properties: BTreeMap::from([
            ("id".to_string(), Value::String(create.source_id.clone())),
            (
                "source_type".to_string(),
                Value::String(create.source_type.clone()),
            ),
            (
                "original_name".to_string(),
                Value::String(create.original_name.clone()),
            ),
            (
                "mime_type".to_string(),
                Value::String(create.mime_type.clone()),
            ),
            (
                "file_path".to_string(),
                Value::String(create.file_path.clone()),
            ),
            (
                "parsed_path".to_string(),
                Value::String(create.parsed_path.clone()),
            ),
            (
                "source_url".to_string(),
                Value::String(create.source_url.clone()),
            ),
            ("sha256".to_string(), Value::String(create.sha256.clone())),
            ("size_bytes".to_string(), Value::Int(create.size_bytes)),
            ("version".to_string(), Value::Int(create.version)),
            (
                "space_id".to_string(),
                Value::String(create.space_id.clone()),
            ),
            (
                "lifecycle_state".to_string(),
                Value::String("parsed".to_string()),
            ),
            ("chunk_count".to_string(), Value::Int(0)),
            ("memory_count".to_string(), Value::Int(0)),
            (
                "section_tree".to_string(),
                Value::String(create.section_tree.clone()),
            ),
            ("summary".to_string(), Value::String(create.summary.clone())),
            ("error_message".to_string(), Value::String(String::new())),
            ("created_at".to_string(), create.created_at.clone()),
            ("updated_at".to_string(), create.updated_at.clone()),
            ("metadata".to_string(), create.metadata.clone()),
        ]),
    }
}

pub(super) fn create_knowledge_source_revision_batch_for(
    db: &mut Database,
    request: &KnowledgeSourceRevisionCreateBatchRequest,
) -> Result<KnowledgeSourceRevisionCreateBatchOutput> {
    for create in &request.creates {
        if create.newer_source_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source revision create requires a non-empty newer source id".to_string(),
            ));
        }
        if create.older_source_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source revision create requires a non-empty older source id".to_string(),
            ));
        }
    }

    let creates = request
        .creates
        .iter()
        .map(knowledge_source_revision_relationship_create)
        .collect::<Vec<_>>();
    let output = create_knowledge_relationship_batch_for(
        db,
        &KnowledgeRelationshipCreateBatchRequest { creates },
    )?;

    Ok(KnowledgeSourceRevisionCreateBatchOutput {
        graph_commit_epoch_before: output.graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        rows: output
            .rows
            .into_iter()
            .map(|row| KnowledgeSourceRevisionCreateBatchRow {
                newer_source_id: row.source.external_id,
                older_source_id: row.target.external_id,
                newer_node_id: row.source_node_id,
                older_node_id: row.target_node_id,
                matched: row.matched,
                non_writable: row.non_writable,
            })
            .collect(),
        matched_count: output.matched_count,
        missing_endpoint_count: output.missing_endpoint_count,
        non_writable_count: output.non_writable_count,
        created_relationship_count: output.created_relationship_count,
    })
}

pub(super) fn knowledge_source_revision_relationship_create(
    create: &KnowledgeSourceRevisionCreate,
) -> KnowledgeRelationshipCreateRequest {
    KnowledgeRelationshipCreateRequest {
        source: KnowledgeEntityRequest {
            label: "Source".to_string(),
            external_id: create.newer_source_id.clone(),
        },
        target: KnowledgeEntityRequest {
            label: "Source".to_string(),
            external_id: create.older_source_id.clone(),
        },
        relationship_type: "REVISED_AS".to_string(),
        properties: BTreeMap::from([
            ("diff_summary".to_string(), Value::String(String::new())),
            (
                "sections_changed".to_string(),
                Value::String("[]".to_string()),
            ),
            (
                "revision_type".to_string(),
                Value::String("update".to_string()),
            ),
            (
                "detected_by".to_string(),
                Value::String("filename_match".to_string()),
            ),
            ("created_at".to_string(), create.created_at.clone()),
        ]),
    }
}

pub(super) fn delete_knowledge_sources_for(
    db: &mut Database,
    request: &KnowledgeSourceDeleteBatchRequest,
) -> Result<KnowledgeSourceDeleteBatchOutput> {
    for source_id in &request.source_ids {
        if source_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge source delete requires a non-empty source id".to_string(),
            ));
        }
    }

    let output = delete_knowledge_entity_batch_for(
        db,
        &KnowledgeEntityDeleteBatchRequest {
            label: "Source".to_string(),
            external_ids: request.source_ids.clone(),
        },
    )?;

    Ok(KnowledgeSourceDeleteBatchOutput {
        graph_commit_epoch_before: output.graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        rows: output
            .rows
            .into_iter()
            .map(|row| KnowledgeSourceDeleteBatchRow {
                source_id: row.external_id,
                node_id: row.node_id,
                matched: row.matched,
                non_writable: row.non_writable,
            })
            .collect(),
        matched_count: output.matched_count,
        missing_count: output.missing_count,
        non_writable_count: output.non_writable_count,
        deleted_node_count: output.deleted_node_count,
    })
}

pub(super) fn delete_knowledge_skills_for(
    db: &mut Database,
    request: &KnowledgeSkillDeleteBatchRequest,
) -> Result<KnowledgeSkillDeleteBatchOutput> {
    for skill_id in &request.skill_ids {
        if skill_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge skill delete requires a non-empty skill id".to_string(),
            ));
        }
    }

    let output = delete_knowledge_entity_batch_for(
        db,
        &KnowledgeEntityDeleteBatchRequest {
            label: "Skill".to_string(),
            external_ids: request.skill_ids.clone(),
        },
    )?;

    Ok(KnowledgeSkillDeleteBatchOutput {
        graph_commit_epoch_before: output.graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        rows: output
            .rows
            .into_iter()
            .map(|row| KnowledgeSkillDeleteBatchRow {
                skill_id: row.external_id,
                node_id: row.node_id,
                matched: row.matched,
                non_writable: row.non_writable,
            })
            .collect(),
        matched_count: output.matched_count,
        missing_count: output.missing_count,
        non_writable_count: output.non_writable_count,
        deleted_node_count: output.deleted_node_count,
    })
}

pub(super) fn assign_knowledge_source_labels_batch_for(
    db: &mut Database,
    request: &KnowledgeSourceLabelAssignmentBatchRequest,
) -> Result<KnowledgeSourceLabelAssignmentBatchOutput> {
    for assignment in &request.assignments {
        validate_knowledge_source_label_assignment(assignment)?;
    }

    let upserts = request
        .assignments
        .iter()
        .map(|assignment| KnowledgeRelationshipUpsertRequest {
            source: KnowledgeEntityRequest {
                label: "Source".to_string(),
                external_id: assignment.source_id.clone(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: assignment.label_id.clone(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            create_properties: BTreeMap::from([
                (
                    "assigned_by".to_string(),
                    Value::String(assignment.assigned_by.clone()),
                ),
                ("created_at".to_string(), assignment.created_at.clone()),
                ("properties".to_string(), assignment.properties.clone()),
            ]),
        })
        .collect::<Vec<_>>();
    let output = upsert_knowledge_relationship_batch_for(
        db,
        &KnowledgeRelationshipUpsertBatchRequest { upserts },
    )?;

    Ok(KnowledgeSourceLabelAssignmentBatchOutput {
        graph_commit_epoch_before: output.graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        rows: output
            .rows
            .into_iter()
            .map(|row| KnowledgeSourceLabelAssignmentRow {
                source_id: row.source.external_id,
                label_id: row.target.external_id,
                source_node_id: row.source_node_id,
                label_node_id: row.target_node_id,
                relationship_id: row.relationship_id,
                matched: row.matched,
                created: row.created,
                already_exists: row.already_exists,
                non_writable: row.non_writable,
            })
            .collect(),
        matched_count: output.matched_count,
        missing_endpoint_count: output.missing_endpoint_count,
        non_writable_count: output.non_writable_count,
        created_count: output.created_count,
        already_exists_count: output.already_exists_count,
        created_relationship_count: output.created_relationship_count,
    })
}

pub(super) fn validate_knowledge_source_label_assignment(
    assignment: &KnowledgeSourceLabelAssignment,
) -> Result<()> {
    if assignment.source_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source label assignment requires a non-empty source id".to_string(),
        ));
    }
    if assignment.label_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source label assignment requires a non-empty label id".to_string(),
        ));
    }
    if assignment.assigned_by.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source label assignment requires a non-empty assigned_by".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn delete_knowledge_source_labels_batch_for(
    db: &mut Database,
    request: &KnowledgeSourceLabelDeleteBatchRequest,
) -> Result<KnowledgeSourceLabelDeleteBatchOutput> {
    for delete in &request.deletes {
        validate_knowledge_source_label_delete(delete)?;
    }

    let deletes = request
        .deletes
        .iter()
        .map(|delete| KnowledgeRelationshipDeleteRequest {
            source: KnowledgeEntityRequest {
                label: "Source".to_string(),
                external_id: delete.source_id.clone(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: delete.label_id.clone(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            relationship_properties: BTreeMap::new(),
        })
        .collect::<Vec<_>>();
    let output = delete_knowledge_relationship_batch_for(
        db,
        &KnowledgeRelationshipDeleteBatchRequest { deletes },
    )?;

    Ok(KnowledgeSourceLabelDeleteBatchOutput {
        graph_commit_epoch_before: output.graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        rows: output
            .rows
            .into_iter()
            .map(|row| KnowledgeSourceLabelDeleteRow {
                source_id: row.source.external_id,
                label_id: row.target.external_id,
                source_node_id: row.source_node_id,
                label_node_id: row.target_node_id,
                matched: row.matched,
                non_writable: row.non_writable,
            })
            .collect(),
        matched_count: output.matched_count,
        missing_endpoint_count: output.missing_endpoint_count,
        non_writable_count: output.non_writable_count,
        deleted_relationship_count: output.deleted_relationship_count,
    })
}

pub(super) fn validate_knowledge_source_label_delete(delete: &KnowledgeSourceLabelDelete) -> Result<()> {
    if delete.source_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source label delete requires a non-empty source id".to_string(),
        ));
    }
    if delete.label_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge source label delete requires a non-empty label id".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn value_to_non_negative_usize(value: &Value) -> Option<usize> {
    match value {
        Value::Int(value) if *value >= 0 => usize::try_from(*value).ok(),
        _ => None,
    }
}

#[cfg(test)]
pub(super) fn value_to_non_negative_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Int(value) if *value >= 0 => u64::try_from(*value).ok(),
        _ => None,
    }
}

#[cfg(test)]
pub(super) fn value_to_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        _ => None,
    }
}

#[cfg(test)]
pub(super) fn optional_external_id_value(value: &Value) -> Option<String> {
    if matches!(value, Value::Null) {
        return None;
    }
    let external_id = value_to_external_id(value);
    (!external_id.is_empty()).then_some(external_id)
}

#[cfg(test)]
pub(super) fn optional_non_null_value(value: &Value) -> Option<Value> {
    (!matches!(value, Value::Null)).then(|| value.clone())
}

#[cfg(test)]
pub(super) fn value_to_map(value: &Value) -> Option<&BTreeMap<String, Value>> {
    match value {
        Value::Map(values) => Some(values),
        _ => None,
    }
}

#[cfg(test)]
pub(super) fn value_to_string_list(value: &Value) -> Option<Vec<String>> {
    let Value::List(values) = value else {
        return None;
    };
    Some(
        values
            .iter()
            .filter(|value| !matches!(value, Value::Null))
            .map(value_to_external_id)
            .filter(|value| !value.is_empty())
            .collect(),
    )
}

#[cfg(test)]
pub(super) fn optional_string_cell(row: impl QueryRowLookup, column: &str) -> Option<String> {
    row.get(column)
        .filter(|value| !matches!(value, Value::Null))
        .map(value_to_external_id)
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
pub(super) fn optional_value_cell(row: impl QueryRowLookup, column: &str) -> Option<Value> {
    row.get(column)
        .filter(|value| !matches!(value, Value::Null))
        .cloned()
}

pub(super) fn string_property(node: &NodeRecord, property: &str) -> Option<String> {
    node.properties
        .get(property)
        .map(value_to_external_id)
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
pub(super) fn string_property_value(properties: &BTreeMap<String, Value>, property: &str) -> Option<String> {
    properties
        .get(property)
        .map(value_to_external_id)
        .filter(|value| !value.is_empty())
}

pub(super) fn update_knowledge_memory_lifecycle_batch_for(
    db: &mut Database,
    request: &KnowledgeMemoryLifecycleBatchRequest,
) -> Result<KnowledgeMemoryLifecycleBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.memory_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge memory lifecycle update requires a non-empty memory id".to_string(),
            ));
        }
        if update.lifecycle_state.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge memory lifecycle update requires a non-empty lifecycle state"
                    .to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Memory",
            &update.memory_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeMemoryLifecycleBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.memory_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeMemoryLifecycleBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeMemoryLifecycleBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = BTreeMap::from([
            ("metadata".to_string(), update.metadata.clone()),
            ("is_latest".to_string(), Value::Bool(update.is_latest)),
            (
                "lifecycle_state".to_string(),
                Value::String(update.lifecycle_state.clone()),
            ),
            ("updated_at".to_string(), update.updated_at.clone()),
        ]);
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeMemoryLifecycleBatchRow {
            memory_id: update.memory_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeMemoryLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Memory", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeMemoryLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn update_knowledge_memory_latest_batch_for(
    db: &mut Database,
    request: &KnowledgeMemoryLatestBatchRequest,
) -> Result<KnowledgeMemoryLatestBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.memory_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge memory latest update requires a non-empty memory id".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Memory",
            &update.memory_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeMemoryLatestBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                filtered_out: false,
                duplicate: false,
                non_writable: false,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.memory_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeMemoryLatestBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                filtered_out: false,
                duplicate: false,
                non_writable: true,
            });
            continue;
        }
        if !memory_latest_update_matches_space(&seed, update) {
            filtered_out_count += 1;
            rows.push(KnowledgeMemoryLatestBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                filtered_out: true,
                duplicate: false,
                non_writable: false,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeMemoryLatestBatchRow {
                memory_id: update.memory_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                filtered_out: false,
                duplicate: true,
                non_writable: false,
            });
            continue;
        }

        let assignments =
            BTreeMap::from([("is_latest".to_string(), Value::Bool(update.is_latest))]);
        matched_count += 1;
        updated_count += 1;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeMemoryLatestBatchRow {
            memory_id: update.memory_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            filtered_out: false,
            duplicate: false,
            non_writable: false,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeMemoryLatestBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            filtered_out_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Memory", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeMemoryLatestBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        filtered_out_count,
        duplicate_count,
        non_writable_count,
        updated_count,
    })
}

pub(super) fn memory_latest_update_matches_space(
    node: &NodeRecord,
    update: &KnowledgeMemoryLatestUpdate,
) -> bool {
    let Some(space_id_filter) = &update.space_id_filter else {
        return true;
    };
    node.properties
        .get("space_id")
        .is_some_and(|value| value_to_external_id(value) == *space_id_filter)
}

pub(super) fn create_knowledge_memory_evolves_batch_for(
    db: &mut Database,
    request: &KnowledgeMemoryEvolvesCreateBatchRequest,
) -> Result<KnowledgeMemoryEvolvesCreateBatchOutput> {
    for create in &request.creates {
        validate_knowledge_memory_evolves_create(create)?;
    }

    let creates = request
        .creates
        .iter()
        .map(|create| KnowledgeRelationshipCreateRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: create.older_memory_id.clone(),
            },
            target: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: create.newer_memory_id.clone(),
            },
            relationship_type: "EVOLVES".to_string(),
            properties: memory_evolves_create_properties(create),
        })
        .collect::<Vec<_>>();
    let output = create_knowledge_relationship_batch_for(
        db,
        &KnowledgeRelationshipCreateBatchRequest { creates },
    )?;

    Ok(KnowledgeMemoryEvolvesCreateBatchOutput {
        graph_commit_epoch_before: output.graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        rows: output
            .rows
            .into_iter()
            .map(|row| KnowledgeMemoryEvolvesCreateBatchRow {
                older_memory_id: row.source.external_id,
                newer_memory_id: row.target.external_id,
                older_node_id: row.source_node_id,
                newer_node_id: row.target_node_id,
                matched: row.matched,
                non_writable: row.non_writable,
            })
            .collect(),
        matched_count: output.matched_count,
        missing_endpoint_count: output.missing_endpoint_count,
        non_writable_count: output.non_writable_count,
        created_relationship_count: output.created_relationship_count,
    })
}

pub(super) fn validate_knowledge_memory_evolves_create(create: &KnowledgeMemoryEvolvesCreate) -> Result<()> {
    if create.older_memory_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge memory evolves create requires a non-empty older memory id".to_string(),
        ));
    }
    if create.newer_memory_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge memory evolves create requires a non-empty newer memory id".to_string(),
        ));
    }
    if create.content_relation.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge memory evolves create requires a non-empty content relation".to_string(),
        ));
    }
    if let Some(confidence) = &create.confidence {
        validate_finite_numeric_value(
            confidence,
            "knowledge memory evolves create requires numeric finite confidence",
        )?;
    }
    if create.detected_by.as_deref().is_some_and(str::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge memory evolves create requires a non-empty detected_by".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn memory_evolves_create_properties(
    create: &KnowledgeMemoryEvolvesCreate,
) -> BTreeMap<String, Value> {
    let mut properties = BTreeMap::from([
        (
            "content_relation".to_string(),
            Value::String(create.content_relation.clone()),
        ),
        ("created_at".to_string(), create.created_at.clone()),
    ]);
    if let Some(is_progression) = create.is_progression {
        properties.insert("is_progression".to_string(), Value::Bool(is_progression));
    }
    if let Some(confidence) = &create.confidence {
        properties.insert("confidence".to_string(), confidence.clone());
    }
    if let Some(detected_by) = &create.detected_by {
        properties.insert(
            "detected_by".to_string(),
            Value::String(detected_by.clone()),
        );
    }
    if let Some(reviewed) = create.reviewed {
        properties.insert("reviewed".to_string(), Value::Bool(reviewed));
    }
    if let Some(reason) = &create.reason {
        properties.insert("reason".to_string(), reason.clone());
    }
    properties
}

pub(super) fn update_knowledge_skill_usage_stats_batch_for(
    db: &mut Database,
    request: &KnowledgeSkillUsageStatsBatchRequest,
) -> Result<KnowledgeSkillUsageStatsBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.skill_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge skill usage stats update requires a non-empty skill id".to_string(),
            ));
        }
        if update.use_count < 0 {
            return Err(SkeinError::Semantic(
                "knowledge skill usage stats update requires non-negative use count".to_string(),
            ));
        }
        if let Some(success_rate) = &update.success_rate {
            validate_skill_success_rate(success_rate)?;
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Skill",
            &update.skill_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeSkillUsageStatsBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.skill_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeSkillUsageStatsBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeSkillUsageStatsBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let mut assignments = BTreeMap::from([
            ("use_count".to_string(), Value::Int(update.use_count)),
            (
                "last_activity_at".to_string(),
                update.last_activity_at.clone(),
            ),
            ("updated_at".to_string(), update.updated_at.clone()),
            ("metadata".to_string(), update.metadata.clone()),
        ]);
        if let Some(success_rate) = &update.success_rate {
            assignments.insert("success_rate".to_string(), success_rate.clone());
        }
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeSkillUsageStatsBatchRow {
            skill_id: update.skill_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeSkillUsageStatsBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Skill", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeSkillUsageStatsBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn update_knowledge_skill_metadata_batch_for(
    db: &mut Database,
    request: &KnowledgeSkillMetadataBatchRequest,
) -> Result<KnowledgeSkillMetadataBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.skill_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge skill metadata update requires a non-empty skill id".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Skill",
            &update.skill_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeSkillMetadataBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.skill_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeSkillMetadataBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeSkillMetadataBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = BTreeMap::from([
            ("metadata".to_string(), update.metadata.clone()),
            ("updated_at".to_string(), update.updated_at.clone()),
        ]);
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeSkillMetadataBatchRow {
            skill_id: update.skill_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeSkillMetadataBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Skill", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeSkillMetadataBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn validate_skill_success_rate(value: &Value) -> Result<()> {
    let valid = match value {
        Value::Float(rate) => rate.is_finite() && (0.0..=1.0).contains(rate),
        Value::Int(rate) => (0..=1).contains(rate),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(SkeinError::Semantic(
            "knowledge skill usage stats update requires success rate between 0 and 1".to_string(),
        ))
    }
}

pub(super) fn merge_knowledge_skill_source_for(
    db: &mut Database,
    request: &KnowledgeSkillSourceMergeRequest,
) -> Result<KnowledgeSkillSourceMergeOutput> {
    if request.skill_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge skill source merge requires a non-empty skill id".to_string(),
        ));
    }
    if request.memory_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge skill source merge requires a non-empty memory id".to_string(),
        ));
    }

    let upsert = KnowledgeRelationshipUpsertRequest {
        source: KnowledgeEntityRequest {
            label: "Skill".to_string(),
            external_id: request.skill_id.clone(),
        },
        target: KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: request.memory_id.clone(),
        },
        relationship_type: "SYNTHESIZED_FROM".to_string(),
        create_properties: BTreeMap::from([
            ("weight".to_string(), Value::Float(1.0)),
            (
                "occasion_key".to_string(),
                Value::String(request.occasion_key.clone()),
            ),
            ("created_at".to_string(), request.created_at.clone()),
        ]),
    };
    let output = upsert_knowledge_relationship_for(db, &upsert)?;
    let missing_endpoint = !output.matched
        && !output.non_writable
        && !output.source_filtered_out
        && !output.target_filtered_out;

    Ok(KnowledgeSkillSourceMergeOutput {
        graph_commit_epoch_before: output.graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        skill_id: request.skill_id.clone(),
        memory_id: request.memory_id.clone(),
        skill_node_id: output.source_node_id,
        memory_node_id: output.target_node_id,
        relationship_id: output.relationship_id,
        matched: output.matched,
        created: output.created,
        already_exists: output.already_exists,
        missing_endpoint,
        non_writable: output.non_writable,
        created_relationship_count: output.created_relationship_count,
    })
}

pub(super) fn update_knowledge_skill_lifecycle_batch_for(
    db: &mut Database,
    request: &KnowledgeSkillLifecycleBatchRequest,
) -> Result<KnowledgeSkillLifecycleBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.skill_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge skill lifecycle update requires a non-empty skill id".to_string(),
            ));
        }
        if update.stage.as_deref().is_some_and(str::is_empty) {
            return Err(SkeinError::Semantic(
                "knowledge skill lifecycle update requires a non-empty stage".to_string(),
            ));
        }
        if update.write_origin.as_deref().is_some_and(str::is_empty) {
            return Err(SkeinError::Semantic(
                "knowledge skill lifecycle update requires a non-empty write origin".to_string(),
            ));
        }
        if !skill_lifecycle_update_has_business_field(update) {
            return Err(SkeinError::Semantic(
                "knowledge skill lifecycle update requires at least one lifecycle field"
                    .to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Skill",
            &update.skill_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeSkillLifecycleBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.skill_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeSkillLifecycleBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeSkillLifecycleBatchRow {
                skill_id: update.skill_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = skill_lifecycle_assignments(update);
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeSkillLifecycleBatchRow {
            skill_id: update.skill_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeSkillLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Skill", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeSkillLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn skill_lifecycle_update_has_business_field(update: &KnowledgeSkillLifecycleUpdate) -> bool {
    update.stage.is_some()
        || update.rejected_at.is_some()
        || update.rationale.is_some()
        || update.version.is_some()
        || update.title.is_some()
        || update.name.is_some()
        || update.description.is_some()
        || update.triggers.is_some()
        || update.tools.is_some()
        || update.bundle_path.is_some()
        || update.content_hash.is_some()
        || update.write_origin.is_some()
        || update.metadata.is_some()
}

pub(super) fn skill_lifecycle_assignments(update: &KnowledgeSkillLifecycleUpdate) -> BTreeMap<String, Value> {
    let mut assignments = BTreeMap::new();
    if let Some(stage) = &update.stage {
        assignments.insert("stage".to_string(), Value::String(stage.clone()));
    }
    insert_optional_assignment(&mut assignments, "rejected_at", &update.rejected_at);
    insert_optional_assignment(&mut assignments, "rationale", &update.rationale);
    insert_optional_assignment(&mut assignments, "version", &update.version);
    insert_optional_assignment(&mut assignments, "title", &update.title);
    insert_optional_assignment(&mut assignments, "name", &update.name);
    insert_optional_assignment(&mut assignments, "description", &update.description);
    insert_optional_assignment(&mut assignments, "triggers", &update.triggers);
    insert_optional_assignment(&mut assignments, "tools", &update.tools);
    insert_optional_assignment(&mut assignments, "bundle_path", &update.bundle_path);
    insert_optional_assignment(&mut assignments, "content_hash", &update.content_hash);
    if let Some(write_origin) = &update.write_origin {
        assignments.insert(
            "write_origin".to_string(),
            Value::String(write_origin.clone()),
        );
    }
    insert_optional_assignment(&mut assignments, "metadata", &update.metadata);
    assignments.insert("updated_at".to_string(), update.updated_at.clone());
    assignments
}

pub(super) fn insert_optional_assignment(
    assignments: &mut BTreeMap<String, Value>,
    property_name: &str,
    value: &Option<Value>,
) {
    if let Some(value) = value {
        assignments.insert(property_name.to_string(), value.clone());
    }
}

pub(super) fn update_knowledge_thread_metadata_batch_for(
    db: &mut Database,
    request: &KnowledgeThreadMetadataBatchRequest,
) -> Result<KnowledgeThreadMetadataBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.thread_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge thread metadata update requires a non-empty thread id".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Thread",
            &update.thread_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeThreadMetadataBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.thread_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeThreadMetadataBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeThreadMetadataBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let mut assignments = BTreeMap::from([("metadata".to_string(), update.metadata.clone())]);
        if let Some(updated_at) = &update.updated_at {
            assignments.insert("updated_at".to_string(), updated_at.clone());
        }
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeThreadMetadataBatchRow {
            thread_id: update.thread_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeThreadMetadataBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Thread", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeThreadMetadataBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn delete_knowledge_threads_for(
    db: &mut Database,
    request: &KnowledgeThreadDeleteBatchRequest,
) -> Result<KnowledgeThreadDeleteBatchOutput> {
    for thread_id in &request.thread_ids {
        if thread_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge thread delete requires a non-empty thread id".to_string(),
            ));
        }
    }

    let output = delete_knowledge_entity_batch_for(
        db,
        &KnowledgeEntityDeleteBatchRequest {
            label: "Thread".to_string(),
            external_ids: request.thread_ids.clone(),
        },
    )?;

    Ok(KnowledgeThreadDeleteBatchOutput {
        graph_commit_epoch_before: output.graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        rows: output
            .rows
            .into_iter()
            .map(|row| KnowledgeThreadDeleteBatchRow {
                thread_id: row.external_id,
                node_id: row.node_id,
                matched: row.matched,
                non_writable: row.non_writable,
            })
            .collect(),
        matched_count: output.matched_count,
        missing_count: output.missing_count,
        non_writable_count: output.non_writable_count,
        deleted_node_count: output.deleted_node_count,
    })
}

pub(super) fn update_knowledge_thread_message_count_batch_for(
    db: &mut Database,
    request: &KnowledgeThreadMessageCountBatchRequest,
) -> Result<KnowledgeThreadMessageCountBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.thread_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge thread message-count update requires a non-empty thread id".to_string(),
            ));
        }
        if update.message_count < 0 {
            return Err(SkeinError::Semantic(
                "knowledge thread message-count update requires non-negative message count"
                    .to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_at_changed_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Thread",
            &update.thread_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeThreadMessageCountBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_at_changed: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.thread_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeThreadMessageCountBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_at_changed: false,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeThreadMessageCountBatchRow {
                thread_id: update.thread_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_at_changed: false,
                updated_property_count: 0,
            });
            continue;
        }

        let mut assignments = BTreeMap::from([(
            "message_count".to_string(),
            Value::Int(update.message_count),
        )]);
        let updated_at_changed = should_update_thread_updated_at(&seed, update);
        if updated_at_changed && let Some(updated_at) = &update.updated_at {
            assignments.insert("updated_at".to_string(), updated_at.clone());
        }
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        if updated_at_changed {
            updated_at_changed_count += 1;
        }
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeThreadMessageCountBatchRow {
            thread_id: update.thread_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_at_changed,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeThreadMessageCountBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_at_changed_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Thread", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeThreadMessageCountBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_at_changed_count,
        updated_property_count,
    })
}

pub(super) fn should_update_thread_updated_at(
    seed: &NodeRecord,
    update: &KnowledgeThreadMessageCountUpdate,
) -> bool {
    let Some(candidate) = &update.updated_at else {
        return false;
    };
    if !update.preserve_newer_existing_updated_at {
        return true;
    }
    match seed.properties.get("updated_at") {
        Some(current) if current != &Value::Null => !value_is_greater(current, candidate),
        _ => true,
    }
}

pub(super) fn value_is_greater(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => left > right,
        (Value::Float(left), Value::Float(right)) => left > right,
        (Value::Int(left), Value::Float(right)) => (*left as f64) > *right,
        (Value::Float(left), Value::Int(right)) => *left > (*right as f64),
        (Value::String(left), Value::String(right)) => left > right,
        _ => false,
    }
}

#[derive(Clone, Copy)]
#[cfg(test)]
pub(super) enum KnowledgeCreatedAtOrder {
    Descending,
}

#[cfg(test)]
pub(super) fn compare_knowledge_created_at(
    left: &Option<Value>,
    right: &Option<Value>,
    order: KnowledgeCreatedAtOrder,
) -> std::cmp::Ordering {
    let base = match (left, right) {
        (Some(left), Some(right)) => compare_knowledge_values(left, right),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    };
    match order {
        KnowledgeCreatedAtOrder::Descending => {
            if left.is_some() && right.is_some() {
                base.reverse()
            } else {
                base
            }
        }
    }
}

#[cfg(test)]
pub(super) fn compare_knowledge_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => left.cmp(right),
        (Value::Float(left), Value::Float(right)) => left.total_cmp(right),
        (Value::Int(left), Value::Float(right)) => (*left as f64).total_cmp(right),
        (Value::Float(left), Value::Int(right)) => left.total_cmp(&(*right as f64)),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        _ => value_to_external_id(left).cmp(&value_to_external_id(right)),
    }
}

pub(super) fn delete_knowledge_thread_identities_for(
    db: &mut Database,
    request: &KnowledgeThreadIdentityDeleteRequest,
) -> Result<KnowledgeThreadIdentityDeleteOutput> {
    db.ensure_writable()?;
    validate_knowledge_thread_identity_delete_request(request)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let deleted_node_ids = thread_identity_delete_candidates(&db.catalog, &db.store, request)?
        .into_iter()
        .map(|node| node.id.0)
        .collect::<Vec<_>>();
    let matched_identity_count = deleted_node_ids.len();

    if deleted_node_ids.is_empty() {
        return Ok(KnowledgeThreadIdentityDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            matched_identity_count: 0,
            deleted_identity_count: 0,
            deleted_node_ids,
        });
    }

    match (&request.identity_key, &request.cascade_keys) {
        (Some(identity_key), None) => {
            db.query_with_params(
                "MATCH (ti:ThreadIdentity {id: $identity_key}) DETACH DELETE ti",
                &BTreeMap::from([(
                    "identity_key".to_string(),
                    Value::String(identity_key.clone()),
                )]),
            )?;
        }
        (None, Some(keys)) => {
            db.query_with_params(
                "MATCH (ti:ThreadIdentity) WHERE ti.id = $public_thread_id OR ti.id = $input_thread_id OR ti.thread_node_id = $thread_uuid DETACH DELETE ti",
                &BTreeMap::from([
                    (
                        "public_thread_id".to_string(),
                        Value::String(keys.public_thread_id.clone()),
                    ),
                    (
                        "input_thread_id".to_string(),
                        Value::String(keys.input_thread_id.clone()),
                    ),
                    (
                        "thread_uuid".to_string(),
                        Value::String(keys.thread_uuid.clone()),
                    ),
                ]),
            )?;
        }
        _ => unreachable!("thread identity delete request was validated"),
    }

    Ok(KnowledgeThreadIdentityDeleteOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        matched_identity_count,
        deleted_identity_count: deleted_node_ids.len(),
        deleted_node_ids,
    })
}

pub(super) fn validate_knowledge_thread_identity_delete_request(
    request: &KnowledgeThreadIdentityDeleteRequest,
) -> Result<()> {
    match (&request.identity_key, &request.cascade_keys) {
        (Some(identity_key), None) if !identity_key.is_empty() => Ok(()),
        (Some(_), None) => Err(SkeinError::Semantic(
            "knowledge thread identity delete requires a non-empty identity key".to_string(),
        )),
        (None, Some(keys))
            if !keys.public_thread_id.is_empty()
                && !keys.input_thread_id.is_empty()
                && !keys.thread_uuid.is_empty() =>
        {
            Ok(())
        }
        (None, Some(_)) => Err(SkeinError::Semantic(
            "knowledge thread identity cascade delete requires non-empty cascade keys".to_string(),
        )),
        _ => Err(SkeinError::Semantic(
            "knowledge thread identity delete requires exactly one delete mode".to_string(),
        )),
    }
}

pub(super) fn thread_identity_delete_candidates(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeThreadIdentityDeleteRequest,
) -> Result<Vec<NodeRecord>> {
    match (&request.identity_key, &request.cascade_keys) {
        (Some(identity_key), None) => Ok(try_seed_node_by_label_and_external_id(
            catalog,
            store,
            "ThreadIdentity",
            identity_key,
        )?
        .into_iter()
        .collect()),
        (None, Some(keys)) => {
            let Some(label_id) = catalog.label_id("ThreadIdentity") else {
                return Ok(Vec::new());
            };
            let mut seen = BTreeSet::new();
            let mut matched = Vec::new();
            store.visit_nodes_owned(Some(label_id), |node| {
                let matches = node_external_id(&node).as_deref()
                    == Some(keys.public_thread_id.as_str())
                    || node_external_id(&node).as_deref() == Some(keys.input_thread_id.as_str())
                    || string_property(&node, "thread_node_id").as_deref()
                        == Some(keys.thread_uuid.as_str());
                if matches && seen.insert(node.id) {
                    matched.push(node);
                }
                crate::store::GraphScanControl::Continue
            })?;
            matched.sort_by_key(|node| node.id.0);
            Ok(matched)
        }
        _ => Ok(Vec::new()),
    }
}

pub(super) fn create_knowledge_thread_compaction_link_for(
    db: &mut Database,
    request: &KnowledgeThreadCompactionLinkRequest,
) -> Result<KnowledgeThreadCompactionLinkOutput> {
    db.ensure_writable()?;
    validate_knowledge_thread_compaction_link_request(request)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let thread = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        "Thread",
        request.thread_id.as_str(),
    )?;
    let memory = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        "Memory",
        request.memory_id.as_str(),
    )?;
    let thread_node_id = thread.as_ref().map(|node| node.id.0);
    let memory_node_id = memory.as_ref().map(|node| node.id.0);
    let (Some(thread), Some(memory)) = (thread, memory) else {
        return Ok(KnowledgeThreadCompactionLinkOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            thread_id: request.thread_id.clone(),
            memory_id: request.memory_id.clone(),
            thread_node_id,
            memory_node_id,
            matched: false,
            missing_endpoint: true,
            non_writable: false,
            created_relationship_count: 0,
        });
    };
    if !node_has_external_id_property(&thread, request.thread_id.as_str())
        || !node_has_external_id_property(&memory, request.memory_id.as_str())
    {
        return Ok(KnowledgeThreadCompactionLinkOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            thread_id: request.thread_id.clone(),
            memory_id: request.memory_id.clone(),
            thread_node_id,
            memory_node_id,
            matched: false,
            missing_endpoint: false,
            non_writable: true,
            created_relationship_count: 0,
        });
    }

    let create = KnowledgeRelationshipCreateRequest {
        source: KnowledgeEntityRequest {
            label: "Thread".to_string(),
            external_id: request.thread_id.clone(),
        },
        target: KnowledgeEntityRequest {
            label: "Memory".to_string(),
            external_id: request.memory_id.clone(),
        },
        relationship_type: "COMPACTS_TO".to_string(),
        properties: BTreeMap::from([
            (
                "compaction_method".to_string(),
                Value::String(request.compaction_method.clone()),
            ),
            ("created_at".to_string(), request.created_at.clone()),
            ("properties".to_string(), request.properties.clone()),
        ]),
    };
    let output = create_knowledge_relationship_for(db, &create)?;
    Ok(KnowledgeThreadCompactionLinkOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        thread_id: request.thread_id.clone(),
        memory_id: request.memory_id.clone(),
        thread_node_id,
        memory_node_id,
        matched: output.matched,
        missing_endpoint: false,
        non_writable: false,
        created_relationship_count: output.created_relationship_count,
    })
}

pub(super) fn validate_knowledge_thread_compaction_link_request(
    request: &KnowledgeThreadCompactionLinkRequest,
) -> Result<()> {
    if request.thread_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge thread compaction link create requires a non-empty thread id".to_string(),
        ));
    }
    if request.memory_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge thread compaction link create requires a non-empty memory id".to_string(),
        ));
    }
    if request.compaction_method.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge thread compaction link create requires a non-empty compaction method"
                .to_string(),
        ));
    }
    Ok(())
}

pub(super) fn delete_knowledge_thread_messages_for(
    db: &mut Database,
    request: &KnowledgeThreadMessageDeleteRequest,
) -> Result<KnowledgeThreadMessageDeleteOutput> {
    db.ensure_writable()?;
    if request.thread_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge thread message delete requires a non-empty thread id".to_string(),
        ));
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(thread) = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        "Thread",
        &request.thread_id,
    )?
    else {
        return Ok(KnowledgeThreadMessageDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            thread_id: request.thread_id.clone(),
            thread_node_id: None,
            found_thread: false,
            matched_relationship_count: 0,
            deleted_message_count: 0,
        });
    };

    let thread_node_id = thread.id;
    let message_node_ids = thread_message_node_ids(&db.catalog, &db.store, thread_node_id)?;
    let matched_relationship_count = message_node_ids.len();
    let deleted_message_count = message_node_ids.into_iter().collect::<BTreeSet<_>>().len();

    if deleted_message_count == 0 {
        return Ok(KnowledgeThreadMessageDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            thread_id: request.thread_id.clone(),
            thread_node_id: Some(thread_node_id.0),
            found_thread: true,
            matched_relationship_count,
            deleted_message_count: 0,
        });
    }

    db.query_with_params(
        "MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) DETACH DELETE m",
        &BTreeMap::from([(
            "thread_uuid".to_string(),
            Value::String(request.thread_id.clone()),
        )]),
    )?;

    Ok(KnowledgeThreadMessageDeleteOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        thread_id: request.thread_id.clone(),
        thread_node_id: Some(thread_node_id.0),
        found_thread: true,
        matched_relationship_count,
        deleted_message_count,
    })
}

pub(super) fn thread_message_node_ids(
    catalog: &Catalog,
    store: &GraphStore,
    thread_node_id: NodeId,
) -> Result<Vec<u64>> {
    let Some(rel_type_id) = catalog.rel_type_id("CONTAINS") else {
        return Ok(Vec::new());
    };
    let Some(message_label_id) = catalog.label_id("Message") else {
        return Ok(Vec::new());
    };
    let mut node_ids = Vec::new();
    store.try_visit_adjacent_relationships_owned(
        thread_node_id,
        Some(rel_type_id),
        AdjacencyDirection::Outgoing,
        |relationship| {
            if let Some(message) = store
                .node_owned(relationship.target)?
                .filter(|message| message.labels.contains(&message_label_id))
            {
                node_ids.push(message.id.0);
            }
            Ok(crate::store::GraphScanControl::Continue)
        },
    )?;
    Ok(node_ids)
}

pub(super) fn update_knowledge_label_lifecycle_batch_for(
    db: &mut Database,
    request: &KnowledgeLabelLifecycleBatchRequest,
) -> Result<KnowledgeLabelLifecycleBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        if update.label_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge label lifecycle update requires a non-empty label id".to_string(),
            ));
        }
        if update.name.as_deref().is_some_and(str::is_empty) {
            return Err(SkeinError::Semantic(
                "knowledge label lifecycle update requires a non-empty name".to_string(),
            ));
        }
        if update.canonical_name.as_deref().is_some_and(str::is_empty) {
            return Err(SkeinError::Semantic(
                "knowledge label lifecycle update requires a non-empty canonical name".to_string(),
            ));
        }
        if !label_lifecycle_update_has_business_field(update) {
            return Err(SkeinError::Semantic(
                "knowledge label lifecycle update requires at least one lifecycle field"
                    .to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut updated_property_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Label",
            &update.label_id,
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeLabelLifecycleBatchRow {
                label_id: update.label_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.label_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeLabelLifecycleBatchRow {
                label_id: update.label_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgeLabelLifecycleBatchRow {
                label_id: update.label_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = label_lifecycle_assignments(update);
        let row_updated_property_count = assignments.len();
        matched_count += 1;
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((node_id, assignments));
        rows.push(KnowledgeLabelLifecycleBatchRow {
            label_id: update.label_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeLabelLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement("Label", node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeLabelLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
        updated_property_count,
    })
}

pub(super) fn label_lifecycle_update_has_business_field(update: &KnowledgeLabelLifecycleUpdate) -> bool {
    update.name.is_some() || update.canonical_name.is_some() || update.metadata.is_some()
}

pub(super) fn label_lifecycle_assignments(update: &KnowledgeLabelLifecycleUpdate) -> BTreeMap<String, Value> {
    let mut assignments = BTreeMap::new();
    if let Some(name) = &update.name {
        assignments.insert("name".to_string(), Value::String(name.clone()));
    }
    if let Some(canonical_name) = &update.canonical_name {
        assignments.insert(
            "canonical_name".to_string(),
            Value::String(canonical_name.clone()),
        );
    }
    insert_optional_assignment(&mut assignments, "metadata", &update.metadata);
    insert_optional_assignment(&mut assignments, "updated_at", &update.updated_at);
    assignments
}

pub(super) fn delete_knowledge_memory_labels_for(
    db: &mut Database,
    request: &KnowledgeMemoryLabelDeleteRequest,
) -> Result<KnowledgeMemoryLabelDeleteOutput> {
    db.ensure_writable()?;
    validate_knowledge_memory_label_delete_request(request)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let memory = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        "Memory",
        request.memory_id.as_str(),
    )?;
    let memory_node_id = memory.as_ref().map(|node| node.id.0);
    let Some(memory) = memory else {
        return Ok(knowledge_memory_label_delete_empty_output(
            request,
            graph_commit_epoch_before,
            memory_node_id,
            None,
            false,
            request.label_id.is_none(),
            false,
        ));
    };
    if !node_has_external_id_property(&memory, request.memory_id.as_str()) {
        return Ok(knowledge_memory_label_delete_empty_output(
            request,
            graph_commit_epoch_before,
            memory_node_id,
            None,
            true,
            request.label_id.is_none(),
            true,
        ));
    }

    let (label_node_id, found_label, relationship_ids) = match request.label_id.as_ref() {
        Some(label_id) => {
            let label =
                try_seed_node_by_label_and_external_id(&db.catalog, &db.store, "Label", label_id)?;
            let label_node_id = label.as_ref().map(|node| node.id.0);
            let Some(label) = label else {
                return Ok(knowledge_memory_label_delete_empty_output(
                    request,
                    graph_commit_epoch_before,
                    memory_node_id,
                    label_node_id,
                    true,
                    false,
                    false,
                ));
            };
            if !node_has_external_id_property(&label, label_id.as_str()) {
                return Ok(knowledge_memory_label_delete_empty_output(
                    request,
                    graph_commit_epoch_before,
                    memory_node_id,
                    label_node_id,
                    true,
                    true,
                    true,
                ));
            }
            (
                label_node_id,
                true,
                memory_label_relationship_ids(&db.catalog, &db.store, memory.id, Some(label.id))?,
            )
        }
        None => (
            None,
            true,
            memory_label_relationship_ids(&db.catalog, &db.store, memory.id, None)?,
        ),
    };

    if relationship_ids.is_empty() {
        return Ok(knowledge_memory_label_delete_empty_output(
            request,
            graph_commit_epoch_before,
            memory_node_id,
            label_node_id,
            true,
            found_label,
            false,
        ));
    }

    let mut tx = db.begin_transaction();
    for relationship_id in &relationship_ids {
        tx.query_with_params(
            "MATCH (:Memory)-[r:HAS_LABEL]->(:Label) WHERE id(r) = $relationship_id DELETE r",
            &BTreeMap::from([(
                "relationship_id".to_string(),
                Value::Int(*relationship_id as i64),
            )]),
        )?;
    }
    tx.commit()?;

    Ok(KnowledgeMemoryLabelDeleteOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        memory_id: request.memory_id.clone(),
        label_id: request.label_id.clone(),
        memory_node_id,
        label_node_id,
        found_memory: true,
        found_label,
        non_writable: false,
        matched_relationship_count: relationship_ids.len(),
        deleted_relationship_count: relationship_ids.len(),
        deleted_relationship_ids: relationship_ids,
    })
}

pub(super) fn validate_knowledge_memory_label_delete_request(
    request: &KnowledgeMemoryLabelDeleteRequest,
) -> Result<()> {
    if request.memory_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge memory label delete requires a non-empty memory id".to_string(),
        ));
    }
    if request.label_id.as_deref().is_some_and(str::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge memory label delete requires a non-empty label id".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn knowledge_memory_label_delete_empty_output(
    request: &KnowledgeMemoryLabelDeleteRequest,
    graph_commit_epoch: u64,
    memory_node_id: Option<u64>,
    label_node_id: Option<u64>,
    found_memory: bool,
    found_label: bool,
    non_writable: bool,
) -> KnowledgeMemoryLabelDeleteOutput {
    KnowledgeMemoryLabelDeleteOutput {
        graph_commit_epoch_before: graph_commit_epoch,
        graph_commit_epoch_after: graph_commit_epoch,
        memory_id: request.memory_id.clone(),
        label_id: request.label_id.clone(),
        memory_node_id,
        label_node_id,
        found_memory,
        found_label,
        non_writable,
        matched_relationship_count: 0,
        deleted_relationship_count: 0,
        deleted_relationship_ids: Vec::new(),
    }
}

pub(super) fn memory_label_relationship_ids(
    catalog: &Catalog,
    store: &GraphStore,
    memory_node_id: NodeId,
    label_node_id: Option<NodeId>,
) -> Result<Vec<u64>> {
    let Some(has_label_type_id) = catalog.rel_type_id("HAS_LABEL") else {
        return Ok(Vec::new());
    };
    let Some(label_type_id) = catalog.label_id("Label") else {
        return Ok(Vec::new());
    };
    let mut relationship_ids = Vec::new();
    store.try_visit_adjacent_relationships_owned(
        memory_node_id,
        Some(has_label_type_id),
        AdjacencyDirection::Outgoing,
        |relationship| {
            if label_node_id.is_none_or(|label_node_id| relationship.target == label_node_id)
                && store
                    .node_owned(relationship.target)?
                    .is_some_and(|node| node.labels.contains(&label_type_id))
            {
                relationship_ids.push(relationship.id.0);
            }
            Ok(crate::store::GraphScanControl::Continue)
        },
    )?;
    relationship_ids.sort_unstable();
    Ok(relationship_ids)
}

pub(super) fn transfer_knowledge_label_memory_edges_for(
    db: &mut Database,
    request: &KnowledgeLabelMemoryTransferRequest,
) -> Result<KnowledgeLabelMemoryTransferOutput> {
    db.ensure_writable()?;
    validate_knowledge_label_memory_transfer_request(request)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let source_label = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        "Label",
        request.source_label_id.as_str(),
    )?;
    let target_label = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        "Label",
        request.target_label_id.as_str(),
    )?;
    let source_label_node_id = source_label.as_ref().map(|node| node.id.0);
    let target_label_node_id = target_label.as_ref().map(|node| node.id.0);
    let found_source_label = source_label.is_some();
    let found_target_label = target_label.is_some();
    let (Some(source_label), Some(target_label)) = (source_label, target_label) else {
        return Ok(knowledge_label_memory_transfer_empty_output(
            request,
            graph_commit_epoch_before,
            source_label_node_id,
            target_label_node_id,
            found_source_label,
            found_target_label,
        ));
    };
    if !node_has_external_id_property(&source_label, request.source_label_id.as_str())
        || !node_has_external_id_property(&target_label, request.target_label_id.as_str())
    {
        return Ok(KnowledgeLabelMemoryTransferOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_label_id: request.source_label_id.clone(),
            target_label_id: request.target_label_id.clone(),
            source_label_node_id,
            target_label_node_id,
            found_source_label: true,
            found_target_label: true,
            rows: Vec::new(),
            matched_memory_count: 0,
            created_count: 0,
            already_exists_count: 0,
            non_writable_count: 1,
        });
    }

    let memory_candidates =
        source_label_memory_transfer_candidates(&db.catalog, &db.store, source_label.id)?;
    if memory_candidates.is_empty() {
        return Ok(knowledge_label_memory_transfer_empty_output(
            request,
            graph_commit_epoch_before,
            source_label_node_id,
            target_label_node_id,
            true,
            true,
        ));
    }

    let mut rows = Vec::with_capacity(memory_candidates.len());
    let mut upserts = Vec::new();
    let mut upsert_row_indexes = Vec::new();
    let mut non_writable_count = 0;
    for memory in memory_candidates {
        let memory_id = node_external_id(&memory);
        if !memory_id
            .as_deref()
            .is_some_and(|memory_id| node_has_external_id_property(&memory, memory_id))
        {
            non_writable_count += 1;
            rows.push(KnowledgeLabelMemoryTransferRow {
                memory_id,
                memory_node_id: memory.id.0,
                relationship_id: None,
                created: false,
                already_exists: false,
                non_writable: true,
            });
            continue;
        }
        let row_index = rows.len();
        let memory_id = memory_id.expect("memory external id was validated");
        upsert_row_indexes.push(row_index);
        upserts.push(KnowledgeRelationshipUpsertRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: memory_id.clone(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: request.target_label_id.clone(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            create_properties: BTreeMap::from([
                (
                    "assigned_by".to_string(),
                    Value::String("label_merge".to_string()),
                ),
                ("created_at".to_string(), request.created_at.clone()),
                ("properties".to_string(), Value::String("{}".to_string())),
            ]),
        });
        rows.push(KnowledgeLabelMemoryTransferRow {
            memory_id: Some(memory_id),
            memory_node_id: memory.id.0,
            relationship_id: None,
            created: false,
            already_exists: false,
            non_writable: false,
        });
    }

    let mut created_count = 0;
    let mut already_exists_count = 0;
    if !upserts.is_empty() {
        let output = upsert_knowledge_relationship_batch_for(
            db,
            &KnowledgeRelationshipUpsertBatchRequest { upserts },
        )?;
        for (upsert_row, transfer_row_index) in output.rows.iter().zip(upsert_row_indexes) {
            let row = &mut rows[transfer_row_index];
            row.relationship_id = upsert_row.relationship_id;
            row.created = upsert_row.created;
            row.already_exists = upsert_row.already_exists;
            row.non_writable = upsert_row.non_writable;
            if upsert_row.created {
                created_count += 1;
            }
            if upsert_row.already_exists {
                already_exists_count += 1;
            }
            if upsert_row.non_writable {
                non_writable_count += 1;
            }
        }
    }

    Ok(KnowledgeLabelMemoryTransferOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        source_label_id: request.source_label_id.clone(),
        target_label_id: request.target_label_id.clone(),
        source_label_node_id,
        target_label_node_id,
        found_source_label: true,
        found_target_label: true,
        matched_memory_count: rows.len(),
        rows,
        created_count,
        already_exists_count,
        non_writable_count,
    })
}

pub(super) fn validate_knowledge_label_memory_transfer_request(
    request: &KnowledgeLabelMemoryTransferRequest,
) -> Result<()> {
    if request.source_label_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge label memory transfer requires a non-empty source label id".to_string(),
        ));
    }
    if request.target_label_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge label memory transfer requires a non-empty target label id".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn knowledge_label_memory_transfer_empty_output(
    request: &KnowledgeLabelMemoryTransferRequest,
    graph_commit_epoch: u64,
    source_label_node_id: Option<u64>,
    target_label_node_id: Option<u64>,
    found_source_label: bool,
    found_target_label: bool,
) -> KnowledgeLabelMemoryTransferOutput {
    KnowledgeLabelMemoryTransferOutput {
        graph_commit_epoch_before: graph_commit_epoch,
        graph_commit_epoch_after: graph_commit_epoch,
        source_label_id: request.source_label_id.clone(),
        target_label_id: request.target_label_id.clone(),
        source_label_node_id,
        target_label_node_id,
        found_source_label,
        found_target_label,
        rows: Vec::new(),
        matched_memory_count: 0,
        created_count: 0,
        already_exists_count: 0,
        non_writable_count: 0,
    }
}

pub(super) fn source_label_memory_transfer_candidates(
    catalog: &Catalog,
    store: &GraphStore,
    source_label_node_id: NodeId,
) -> Result<Vec<NodeRecord>> {
    let Some(has_label_type_id) = catalog.rel_type_id("HAS_LABEL") else {
        return Ok(Vec::new());
    };
    let Some(memory_label_id) = catalog.label_id("Memory") else {
        return Ok(Vec::new());
    };
    let mut seen_memory_ids = BTreeSet::new();
    let mut memories = Vec::new();
    store.try_visit_nodes_owned(Some(memory_label_id), |memory| {
        let mut found = false;
        store.visit_adjacent_relationships_owned(
            memory.id,
            Some(has_label_type_id),
            AdjacencyDirection::Outgoing,
            |relationship| {
                if relationship.target == source_label_node_id {
                    found = true;
                    crate::store::GraphScanControl::Stop
                } else {
                    crate::store::GraphScanControl::Continue
                }
            },
        )?;
        if found && seen_memory_ids.insert(memory.id) {
            memories.push(memory);
        }
        Ok(crate::store::GraphScanControl::Continue)
    })?;
    memories.sort_by_key(|memory| memory.id.0);
    Ok(memories)
}

pub(super) fn transfer_knowledge_memory_label_edges_for(
    db: &mut Database,
    request: &KnowledgeMemoryLabelTransferRequest,
) -> Result<KnowledgeMemoryLabelTransferOutput> {
    db.ensure_writable()?;
    validate_knowledge_memory_label_transfer_request(request)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let older_memory = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        "Memory",
        request.older_memory_id.as_str(),
    )?;
    let newer_memory = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        "Memory",
        request.newer_memory_id.as_str(),
    )?;
    let older_memory_node_id = older_memory.as_ref().map(|node| node.id.0);
    let newer_memory_node_id = newer_memory.as_ref().map(|node| node.id.0);
    let found_older_memory = older_memory.is_some();
    let found_newer_memory = newer_memory.is_some();
    let (Some(older_memory), Some(newer_memory)) = (older_memory, newer_memory) else {
        return Ok(knowledge_memory_label_transfer_empty_output(
            request,
            MemoryLabelTransferEmptyInput {
                graph_commit_epoch: graph_commit_epoch_before,
                older_memory_node_id,
                newer_memory_node_id,
                found_older_memory,
                found_newer_memory,
                older_space_matches: false,
                newer_space_matches: false,
                duplicate_source_edge_count: 0,
            },
        ));
    };

    let older_space_matches =
        node_property_equals_external_id(&older_memory, "space_id", request.space_id.as_str());
    let newer_space_matches =
        node_property_equals_external_id(&newer_memory, "space_id", request.space_id.as_str());
    if !node_has_external_id_property(&older_memory, request.older_memory_id.as_str())
        || !node_has_external_id_property(&newer_memory, request.newer_memory_id.as_str())
    {
        return Ok(KnowledgeMemoryLabelTransferOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            older_memory_id: request.older_memory_id.clone(),
            newer_memory_id: request.newer_memory_id.clone(),
            space_id: request.space_id.clone(),
            older_memory_node_id,
            newer_memory_node_id,
            found_older_memory: true,
            found_newer_memory: true,
            older_space_matches,
            newer_space_matches,
            rows: Vec::new(),
            matched_label_count: 0,
            created_count: 0,
            already_exists_count: 0,
            non_writable_count: 1,
            duplicate_source_edge_count: 0,
        });
    }
    if !older_space_matches || !newer_space_matches {
        return Ok(knowledge_memory_label_transfer_empty_output(
            request,
            MemoryLabelTransferEmptyInput {
                graph_commit_epoch: graph_commit_epoch_before,
                older_memory_node_id,
                newer_memory_node_id,
                found_older_memory: true,
                found_newer_memory: true,
                older_space_matches,
                newer_space_matches,
                duplicate_source_edge_count: 0,
            },
        ));
    }

    let (label_candidates, duplicate_source_edge_count) =
        memory_label_transfer_candidates(&db.catalog, &db.store, older_memory.id)?;
    if label_candidates.is_empty() {
        return Ok(knowledge_memory_label_transfer_empty_output(
            request,
            MemoryLabelTransferEmptyInput {
                graph_commit_epoch: graph_commit_epoch_before,
                older_memory_node_id,
                newer_memory_node_id,
                found_older_memory: true,
                found_newer_memory: true,
                older_space_matches: true,
                newer_space_matches: true,
                duplicate_source_edge_count,
            },
        ));
    }

    let mut rows = Vec::with_capacity(label_candidates.len());
    let mut upserts = Vec::new();
    let mut upsert_row_indexes = Vec::new();
    let mut non_writable_count = 0;
    for label in label_candidates {
        let label_id = node_external_id(&label);
        if !label_id
            .as_deref()
            .is_some_and(|label_id| node_has_external_id_property(&label, label_id))
        {
            non_writable_count += 1;
            rows.push(KnowledgeMemoryLabelTransferRow {
                label_id,
                label_node_id: label.id.0,
                relationship_id: None,
                created: false,
                already_exists: false,
                non_writable: true,
            });
            continue;
        }
        let row_index = rows.len();
        let label_id = label_id.expect("label external id was validated");
        upsert_row_indexes.push(row_index);
        upserts.push(KnowledgeRelationshipUpsertRequest {
            source: KnowledgeEntityRequest {
                label: "Memory".to_string(),
                external_id: request.newer_memory_id.clone(),
            },
            target: KnowledgeEntityRequest {
                label: "Label".to_string(),
                external_id: label_id.clone(),
            },
            relationship_type: "HAS_LABEL".to_string(),
            create_properties: BTreeMap::from([
                (
                    "assigned_by".to_string(),
                    Value::String("system".to_string()),
                ),
                ("created_at".to_string(), request.created_at.clone()),
                ("properties".to_string(), Value::String("{}".to_string())),
            ]),
        });
        rows.push(KnowledgeMemoryLabelTransferRow {
            label_id: Some(label_id),
            label_node_id: label.id.0,
            relationship_id: None,
            created: false,
            already_exists: false,
            non_writable: false,
        });
    }

    let mut created_count = 0;
    let mut already_exists_count = 0;
    if !upserts.is_empty() {
        let output = upsert_knowledge_relationship_batch_for(
            db,
            &KnowledgeRelationshipUpsertBatchRequest { upserts },
        )?;
        for (upsert_row, transfer_row_index) in output.rows.iter().zip(upsert_row_indexes) {
            let row = &mut rows[transfer_row_index];
            row.relationship_id = upsert_row.relationship_id;
            row.created = upsert_row.created;
            row.already_exists = upsert_row.already_exists;
            row.non_writable = upsert_row.non_writable;
            if upsert_row.created {
                created_count += 1;
            }
            if upsert_row.already_exists {
                already_exists_count += 1;
            }
            if upsert_row.non_writable {
                non_writable_count += 1;
            }
        }
    }

    Ok(KnowledgeMemoryLabelTransferOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        older_memory_id: request.older_memory_id.clone(),
        newer_memory_id: request.newer_memory_id.clone(),
        space_id: request.space_id.clone(),
        older_memory_node_id,
        newer_memory_node_id,
        found_older_memory: true,
        found_newer_memory: true,
        older_space_matches: true,
        newer_space_matches: true,
        matched_label_count: rows.len(),
        rows,
        created_count,
        already_exists_count,
        non_writable_count,
        duplicate_source_edge_count,
    })
}

pub(super) fn validate_knowledge_memory_label_transfer_request(
    request: &KnowledgeMemoryLabelTransferRequest,
) -> Result<()> {
    if request.older_memory_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge memory label transfer requires a non-empty older memory id".to_string(),
        ));
    }
    if request.newer_memory_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge memory label transfer requires a non-empty newer memory id".to_string(),
        ));
    }
    if request.space_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge memory label transfer requires a non-empty space id".to_string(),
        ));
    }
    Ok(())
}

pub(super) struct MemoryLabelTransferEmptyInput {
    graph_commit_epoch: u64,
    older_memory_node_id: Option<u64>,
    newer_memory_node_id: Option<u64>,
    found_older_memory: bool,
    found_newer_memory: bool,
    older_space_matches: bool,
    newer_space_matches: bool,
    duplicate_source_edge_count: usize,
}

pub(super) fn knowledge_memory_label_transfer_empty_output(
    request: &KnowledgeMemoryLabelTransferRequest,
    input: MemoryLabelTransferEmptyInput,
) -> KnowledgeMemoryLabelTransferOutput {
    KnowledgeMemoryLabelTransferOutput {
        graph_commit_epoch_before: input.graph_commit_epoch,
        graph_commit_epoch_after: input.graph_commit_epoch,
        older_memory_id: request.older_memory_id.clone(),
        newer_memory_id: request.newer_memory_id.clone(),
        space_id: request.space_id.clone(),
        older_memory_node_id: input.older_memory_node_id,
        newer_memory_node_id: input.newer_memory_node_id,
        found_older_memory: input.found_older_memory,
        found_newer_memory: input.found_newer_memory,
        older_space_matches: input.older_space_matches,
        newer_space_matches: input.newer_space_matches,
        rows: Vec::new(),
        matched_label_count: 0,
        created_count: 0,
        already_exists_count: 0,
        non_writable_count: 0,
        duplicate_source_edge_count: input.duplicate_source_edge_count,
    }
}

pub(super) fn memory_label_transfer_candidates(
    catalog: &Catalog,
    store: &GraphStore,
    older_memory_node_id: NodeId,
) -> Result<(Vec<NodeRecord>, usize)> {
    let Some(has_label_type_id) = catalog.rel_type_id("HAS_LABEL") else {
        return Ok((Vec::new(), 0));
    };
    let Some(label_type_id) = catalog.label_id("Label") else {
        return Ok((Vec::new(), 0));
    };
    let mut seen_label_ids = BTreeSet::new();
    let mut duplicate_source_edge_count = 0;
    let mut labels = Vec::new();
    store.try_visit_adjacent_relationships_owned(
        older_memory_node_id,
        Some(has_label_type_id),
        AdjacencyDirection::Outgoing,
        |relationship| {
            let Some(label) = store
                .node_owned(relationship.target)?
                .filter(|label| label.labels.contains(&label_type_id))
            else {
                return Ok(crate::store::GraphScanControl::Continue);
            };
            if seen_label_ids.insert(label.id) {
                labels.push(label);
            } else {
                duplicate_source_edge_count += 1;
            }
            Ok(crate::store::GraphScanControl::Continue)
        },
    )?;
    labels.sort_by(|left, right| {
        node_external_id(left)
            .cmp(&node_external_id(right))
            .then_with(|| left.id.0.cmp(&right.id.0))
    });
    Ok((labels, duplicate_source_edge_count))
}

pub(super) fn node_property_equals_external_id(node: &NodeRecord, key: &str, expected: &str) -> bool {
    node.properties
        .get(key)
        .is_some_and(|value| value_to_external_id(value) == expected)
}

#[cfg(test)]
pub(super) fn knowledge_entity_labels_via_query_runtime(
    db: &Database,
    request: &KnowledgeEntityLabelListRequest,
) -> Result<KnowledgeEntityLabelListOutput> {
    validate_knowledge_entity_label_list_request(request)?;
    let graph_commit_epoch = db.store.commit_epoch();
    let entities = knowledge_entity_label_entities_via_query_runtime(
        db,
        &request.entity_label,
        &request.external_ids,
    )?;
    let mut groups = Vec::with_capacity(request.external_ids.len());
    let mut found_entity_count = 0;
    let mut missing_entity_count = 0;
    let mut label_count = 0;

    for external_id in &request.external_ids {
        let Some(entity) = entities.get(external_id) else {
            missing_entity_count += 1;
            groups.push(KnowledgeEntityLabelGroup {
                external_id: external_id.clone(),
                node_id: None,
                found: false,
                labels: Vec::new(),
                returned_count: 0,
            });
            continue;
        };
        found_entity_count += 1;
        let labels =
            entity_label_rows_via_query_runtime(db, entity.node_id, request.limit_per_entity)?;
        label_count += labels.len();
        groups.push(KnowledgeEntityLabelGroup {
            external_id: external_id.clone(),
            node_id: Some(entity.node_id),
            found: true,
            returned_count: labels.len(),
            labels,
        });
    }

    Ok(KnowledgeEntityLabelListOutput {
        graph_commit_epoch,
        groups,
        found_entity_count,
        missing_entity_count,
        label_count,
    })
}

#[cfg(test)]
pub(super) fn knowledge_entity_label_projected_list_via_query_runtime(
    db: &Database,
    request: &KnowledgeEntityLabelProjectedListRequest,
) -> Result<KnowledgeEntityLabelProjectedListOutput> {
    validate_knowledge_entity_label_projected_list_request(request)?;
    let graph_commit_epoch = db.store.commit_epoch();
    let entities = knowledge_entity_label_entities_via_query_runtime(
        db,
        &request.list.entity_label,
        &request.list.external_ids,
    )?;
    let mut groups = Vec::with_capacity(request.list.external_ids.len());
    let mut found_entity_count = 0;
    let mut missing_entity_count = 0;
    let mut label_count = 0;

    for external_id in &request.list.external_ids {
        let Some(entity) = entities.get(external_id) else {
            missing_entity_count += 1;
            groups.push(KnowledgeEntityLabelProjectedGroup {
                external_id: external_id.clone(),
                node_id: None,
                found: false,
                labels: Vec::new(),
                returned_count: 0,
            });
            continue;
        };
        found_entity_count += 1;
        let labels = entity_label_projected_rows_via_query_runtime(
            db,
            entity.node_id,
            request.list.limit_per_entity,
            &request.label_property_names,
            &request.relationship_property_names,
        )?;
        label_count += labels.len();
        groups.push(KnowledgeEntityLabelProjectedGroup {
            external_id: external_id.clone(),
            node_id: Some(entity.node_id),
            found: true,
            returned_count: labels.len(),
            labels,
        });
    }

    Ok(KnowledgeEntityLabelProjectedListOutput {
        graph_commit_epoch,
        groups,
        found_entity_count,
        missing_entity_count,
        label_count,
    })
}

#[cfg(test)]
pub(super) fn validate_knowledge_entity_label_list_request(
    request: &KnowledgeEntityLabelListRequest,
) -> Result<()> {
    validate_cypher_identifier(&request.entity_label, "knowledge entity label")?;
    if request.external_ids.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge entity label read requires non-empty external ids".to_string(),
        ));
    }
    validate_non_empty_external_ids(
        &request.external_ids,
        "knowledge entity label read requires non-empty external ids",
    )?;
    Ok(())
}

#[cfg(test)]
pub(super) fn validate_knowledge_entity_label_projected_list_request(
    request: &KnowledgeEntityLabelProjectedListRequest,
) -> Result<()> {
    validate_knowledge_entity_label_list_request(&request.list)?;
    if request.label_property_names.iter().any(String::is_empty)
        || request
            .relationship_property_names
            .iter()
            .any(String::is_empty)
    {
        return Err(SkeinError::Semantic(
            "knowledge entity label projected read requires non-empty property names".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn knowledge_entity_label_entities_via_query_runtime(
    db: &Database,
    entity_label: &str,
    external_ids: &[String],
) -> Result<BTreeMap<String, KnowledgeEntity>> {
    validate_cypher_identifier(entity_label, "knowledge entity label")?;
    let requested_ids = external_ids.iter().cloned().collect::<BTreeSet<_>>();
    let parameters = BTreeMap::from([(
        "external_ids".to_string(),
        Value::List(
            requested_ids
                .iter()
                .cloned()
                .map(Value::String)
                .collect::<Vec<_>>(),
        ),
    )]);
    let query = format!(
        "MATCH (entity:{entity_label}) WHERE entity.id IN $external_ids \
         RETURN entity AS entity ORDER BY id(entity) ASC"
    );
    let output = db.query_read_only_with_params_bounded(&query, &parameters, None)?;
    let mut entities = BTreeMap::new();
    for entity in output
        .rows
        .iter()
        .filter_map(|row| row.get("entity").and_then(knowledge_entity_from_value))
    {
        let Some(external_id) = knowledge_entity_id_property(&entity) else {
            continue;
        };
        if requested_ids.contains(&external_id) {
            entities.entry(external_id).or_insert(entity);
        }
    }
    Ok(entities)
}

#[cfg(test)]
pub(super) fn entity_label_rows_via_query_runtime(
    db: &Database,
    entity_node_id: u64,
    limit: usize,
) -> Result<Vec<KnowledgeEntityLabelRow>> {
    let output = entity_label_query_output_via_query_runtime(db, entity_node_id)?;
    let mut rows = output
        .rows
        .iter()
        .map(entity_label_row_from_query_row)
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.label_id.cmp(&right.label_id))
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    if limit > 0 {
        rows.truncate(limit);
    }
    Ok(rows)
}

#[cfg(test)]
pub(super) fn entity_label_projected_rows_via_query_runtime(
    db: &Database,
    entity_node_id: u64,
    limit: usize,
    label_property_names: &[String],
    relationship_property_names: &[String],
) -> Result<Vec<KnowledgeEntityLabelProjectedRow>> {
    let output = entity_label_query_output_via_query_runtime(db, entity_node_id)?;
    let mut rows = output
        .rows
        .iter()
        .map(|row| {
            let label = row
                .get("label")
                .and_then(knowledge_entity_from_value)
                .ok_or_else(|| {
                    SkeinError::Execution("entity label row is missing label".to_string())
                })?;
            let relationship = row
                .get("relationship")
                .and_then(value_to_map)
                .ok_or_else(|| {
                    SkeinError::Execution("entity label row is missing relationship".to_string())
                })?;
            let relationship_id = row
                .get("relationship_id")
                .and_then(value_to_non_negative_u64)
                .or_else(|| relationship.get("_id").and_then(value_to_non_negative_u64))
                .ok_or_else(|| {
                    SkeinError::Execution("entity label row is missing relationship_id".to_string())
                })?;
            let mut relationship_properties = relationship.clone();
            relationship_properties.remove("_id");
            relationship_properties.remove("source_id");
            relationship_properties.remove("target_id");
            relationship_properties.remove("type");
            Ok((
                KnowledgeEntityLabelProjectedRow {
                    label_id: knowledge_entity_id_property(&label),
                    label_node_id: label.node_id,
                    relationship_id,
                    label_properties: projected_properties(&label.properties, label_property_names),
                    relationship_properties: projected_properties(
                        &relationship_properties,
                        relationship_property_names,
                    ),
                },
                knowledge_entity_string_property(&label, "name"),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then_with(|| left.0.label_id.cmp(&right.0.label_id))
            .then_with(|| left.0.relationship_id.cmp(&right.0.relationship_id))
            .then_with(|| left.0.label_node_id.cmp(&right.0.label_node_id))
    });
    if limit > 0 {
        rows.truncate(limit);
    }
    Ok(rows.into_iter().map(|(row, _name)| row).collect())
}

#[cfg(test)]
pub(super) fn entity_label_query_output_via_query_runtime(
    db: &Database,
    entity_node_id: u64,
) -> Result<QueryOutput> {
    let parameters =
        BTreeMap::from([(
            "entity_node_id".to_string(),
            Value::Int(i64::try_from(entity_node_id).map_err(|_| {
                SkeinError::Execution("entity label node id exceeds i64".to_string())
            })?),
        )]);
    let query = "MATCH (entity)-[relationship:HAS_LABEL]->(label:Label) \
         WHERE id(entity) = $entity_node_id \
         RETURN label AS label, relationship AS relationship, id(relationship) AS relationship_id";
    db.query_read_only_with_params_bounded(query, &parameters, None)
}

#[cfg(test)]
pub(super) fn entity_label_row_from_query_row(
    row: impl QueryRowLookup,
) -> Result<KnowledgeEntityLabelRow> {
    let label = row
        .get("label")
        .and_then(knowledge_entity_from_value)
        .ok_or_else(|| SkeinError::Execution("entity label row is missing label".to_string()))?;
    Ok(KnowledgeEntityLabelRow {
        label_id: knowledge_entity_id_property(&label),
        node_id: label.node_id,
        name: knowledge_entity_string_property(&label, "name"),
        canonical_name: knowledge_entity_string_property(&label, "canonical_name"),
        color: label.properties.get("color").cloned(),
        description: label.properties.get("description").cloned(),
    })
}

#[cfg(test)]
pub(super) fn knowledge_entity_string_property(entity: &KnowledgeEntity, key: &str) -> Option<String> {
    entity.properties.get(key).map(value_to_external_id)
}

pub(super) fn update_knowledge_pagerank_scores_batch_for(
    db: &mut Database,
    request: &KnowledgePageRankScoreBatchRequest,
) -> Result<KnowledgePageRankScoreBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        validate_pagerank_label(update.label.as_str())?;
        if update.external_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge pagerank score update requires a non-empty external id".to_string(),
            ));
        }
        if !update.score.is_finite() || update.score < 0.0 {
            return Err(SkeinError::Semantic(
                "knowledge pagerank score update requires a finite non-negative score".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut duplicate_count = 0;
    let mut non_writable_count = 0;
    let mut updated_count = 0;
    let mut pending_node_ids = BTreeSet::new();
    let mut eligible_updates = Vec::new();

    for update in &request.updates {
        let label = pagerank_label(update.label.as_str());
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            label,
            update.external_id.as_str(),
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgePageRankScoreBatchRow {
                label: label.to_string(),
                external_id: update.external_id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: false,
            });
            continue;
        };
        let node_id = seed.id;
        if !node_has_external_id_property(&seed, update.external_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgePageRankScoreBatchRow {
                label: label.to_string(),
                external_id: update.external_id.clone(),
                node_id: Some(node_id.0),
                matched: false,
                updated: false,
                duplicate: false,
                non_writable: true,
            });
            continue;
        }
        if !pending_node_ids.insert(node_id) {
            duplicate_count += 1;
            rows.push(KnowledgePageRankScoreBatchRow {
                label: label.to_string(),
                external_id: update.external_id.clone(),
                node_id: Some(node_id.0),
                matched: true,
                updated: false,
                duplicate: true,
                non_writable: false,
            });
            continue;
        }

        matched_count += 1;
        updated_count += 1;
        let assignments =
            BTreeMap::from([("pagerank_score".to_string(), Value::Float(update.score))]);
        eligible_updates.push((label.to_string(), node_id, assignments));
        rows.push(KnowledgePageRankScoreBatchRow {
            label: label.to_string(),
            external_id: update.external_id.clone(),
            node_id: Some(node_id.0),
            matched: true,
            updated: true,
            duplicate: false,
            non_writable: false,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgePageRankScoreBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            duplicate_count,
            non_writable_count,
            updated_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for (label, node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement(label.as_str(), node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgePageRankScoreBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        duplicate_count,
        non_writable_count,
        updated_count,
    })
}

pub(super) fn clear_knowledge_pagerank_scores_for(
    db: &mut Database,
    request: &KnowledgePageRankClearRequest,
) -> Result<KnowledgePageRankClearOutput> {
    db.ensure_writable()?;
    if request.labels.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge pagerank clear requires at least one label".to_string(),
        ));
    }
    for label in &request.labels {
        validate_pagerank_label(label.as_str())?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::new();
    let mut candidate_count = 0;
    let mut cleared_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_updates = Vec::new();
    let mut seen_node_ids = BTreeSet::new();

    for requested_label in &request.labels {
        let label = pagerank_label(requested_label.as_str());
        let Some(label_id) = db.catalog.label_id(label) else {
            continue;
        };
        db.store.visit_nodes_owned(Some(label_id), |node| {
            if !seen_node_ids.insert(node.id) {
                return crate::store::GraphScanControl::Continue;
            }
            if node
                .properties
                .get("pagerank_score")
                .is_none_or(|value| value == &Value::Null)
            {
                return crate::store::GraphScanControl::Continue;
            }
            candidate_count += 1;
            let external_id = node_external_id(&node);
            if external_id.is_none() {
                non_writable_count += 1;
                rows.push(KnowledgePageRankClearRow {
                    label: label.to_string(),
                    external_id,
                    node_id: node.id.0,
                    cleared: false,
                    non_writable: true,
                });
                return crate::store::GraphScanControl::Continue;
            }
            let assignments = BTreeMap::from([("pagerank_score".to_string(), Value::Null)]);
            eligible_updates.push((label.to_string(), node.id, assignments));
            cleared_count += 1;
            rows.push(KnowledgePageRankClearRow {
                label: label.to_string(),
                external_id,
                node_id: node.id.0,
                cleared: true,
                non_writable: false,
            });
            crate::store::GraphScanControl::Continue
        })?;
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgePageRankClearOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            candidate_count,
            cleared_count: 0,
            non_writable_count,
        });
    }

    let mut tx = db.begin_transaction();
    for (label, node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_property_update_statement(label.as_str(), node_id.0, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgePageRankClearOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        candidate_count,
        cleared_count,
        non_writable_count,
    })
}

#[cfg(test)]
pub(super) fn validate_non_empty_external_ids(external_ids: &[String], message: &str) -> Result<()> {
    if external_ids.iter().any(String::is_empty) {
        return Err(SkeinError::Semantic(message.to_string()));
    }
    Ok(())
}

pub(super) fn validate_pagerank_label(label: &str) -> Result<()> {
    match label {
        "Memory" | "memory" | "Entity" | "entity" => Ok(()),
        _ => Err(SkeinError::Semantic(
            "knowledge pagerank operations support only Memory and Entity labels".to_string(),
        )),
    }
}

pub(super) fn pagerank_label(label: &str) -> &'static str {
    match label {
        "Memory" | "memory" => "Memory",
        "Entity" | "entity" => "Entity",
        _ => unreachable!("pagerank label should be validated before canonicalization"),
    }
}

pub(super) fn clear_knowledge_community_assignments_for(
    db: &mut Database,
    request: &KnowledgeCommunityAssignmentClearRequest,
) -> Result<KnowledgeCommunityAssignmentClearOutput> {
    db.ensure_writable()?;
    for label in &request.labels {
        validate_cypher_identifier(label.as_str(), "node label")?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::new();
    let mut candidate_count = 0;
    let mut cleared_count = 0;
    let mut eligible_updates = Vec::new();
    let mut seen_node_ids = BTreeSet::new();

    if request.labels.is_empty() {
        collect_knowledge_community_assignment_clears(
            db,
            None,
            &mut seen_node_ids,
            &mut rows,
            &mut eligible_updates,
            &mut candidate_count,
            &mut cleared_count,
        )?;
    } else {
        for label in &request.labels {
            let Some(label_id) = db.catalog.label_id(label.as_str()) else {
                continue;
            };
            collect_knowledge_community_assignment_clears(
                db,
                Some(label_id),
                &mut seen_node_ids,
                &mut rows,
                &mut eligible_updates,
                &mut candidate_count,
                &mut cleared_count,
            )?;
        }
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeCommunityAssignmentClearOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            candidate_count,
            cleared_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for node_id in &eligible_updates {
        let (cypher, parameters) = community_assignment_clear_statement(*node_id);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    Ok(KnowledgeCommunityAssignmentClearOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        candidate_count,
        cleared_count,
    })
}

pub(super) fn collect_knowledge_community_assignment_clears(
    db: &Database,
    label_id: Option<LabelId>,
    seen_node_ids: &mut BTreeSet<NodeId>,
    rows: &mut Vec<KnowledgeCommunityAssignmentClearRow>,
    eligible_updates: &mut Vec<NodeId>,
    candidate_count: &mut usize,
    cleared_count: &mut usize,
) -> Result<()> {
    db.store.visit_nodes_owned(label_id, |node| {
        if !seen_node_ids.insert(node.id) {
            return crate::store::GraphScanControl::Continue;
        }
        if node
            .properties
            .get("community_id")
            .is_none_or(|value| value == &Value::Null)
        {
            return crate::store::GraphScanControl::Continue;
        }
        *candidate_count += 1;
        *cleared_count += 1;
        eligible_updates.push(node.id);
        rows.push(KnowledgeCommunityAssignmentClearRow {
            labels: node_label_names(&db.catalog, &node),
            external_id: node_external_id(&node),
            node_id: node.id.0,
            cleared: true,
        });
        crate::store::GraphScanControl::Continue
    })?;
    Ok(())
}

pub(super) fn community_assignment_clear_statement(node_id: NodeId) -> (String, BTreeMap<String, Value>) {
    (
        "MATCH (n) WHERE id(n) = $node_id SET n.community_id = $community_id".to_string(),
        BTreeMap::from([
            ("node_id".to_string(), Value::Int(node_id.0 as i64)),
            ("community_id".to_string(), Value::Null),
        ]),
    )
}

pub(super) fn create_knowledge_community_memberships_batch_for(
    db: &mut Database,
    request: &KnowledgeCommunityMembershipCreateBatchRequest,
) -> Result<KnowledgeCommunityMembershipCreateBatchOutput> {
    db.ensure_writable()?;
    for membership in &request.memberships {
        validate_knowledge_community_membership_create(membership)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let creates = request
        .memberships
        .iter()
        .map(knowledge_community_membership_relationship_create)
        .collect::<Vec<_>>();
    let output = create_knowledge_relationship_batch_for(
        db,
        &KnowledgeRelationshipCreateBatchRequest { creates },
    )?;
    let rows = output
        .rows
        .into_iter()
        .map(|row| KnowledgeCommunityMembershipCreateBatchRow {
            entity_id: row.source.external_id,
            community_id: row.target.external_id,
            entity_node_id: row.source_node_id,
            community_node_id: row.target_node_id,
            matched: row.matched,
            non_writable: row.non_writable,
            created: row.matched,
        })
        .collect::<Vec<_>>();

    Ok(KnowledgeCommunityMembershipCreateBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: output.graph_commit_epoch_after,
        rows,
        matched_count: output.matched_count,
        missing_endpoint_count: output.missing_endpoint_count,
        non_writable_count: output.non_writable_count,
        created_relationship_count: output.created_relationship_count,
    })
}

pub(super) fn validate_knowledge_community_membership_create(
    membership: &KnowledgeCommunityMembershipCreate,
) -> Result<()> {
    if membership.entity_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community membership create requires a non-empty entity_id".to_string(),
        ));
    }
    if membership.community_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community membership create requires a non-empty community_id".to_string(),
        ));
    }
    if !membership.strength.is_finite() {
        return Err(SkeinError::Semantic(
            "knowledge community membership strength must be finite".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn knowledge_community_membership_relationship_create(
    membership: &KnowledgeCommunityMembershipCreate,
) -> KnowledgeRelationshipCreateRequest {
    KnowledgeRelationshipCreateRequest {
        source: KnowledgeEntityRequest {
            label: "Entity".to_string(),
            external_id: membership.entity_id.clone(),
        },
        target: KnowledgeEntityRequest {
            label: "Community".to_string(),
            external_id: membership.community_id.clone(),
        },
        relationship_type: "BELONGS_TO".to_string(),
        properties: BTreeMap::from([
            ("strength".to_string(), Value::Float(membership.strength)),
            ("created_at".to_string(), membership.created_at.clone()),
            ("properties".to_string(), membership.properties.clone()),
        ]),
    }
}

#[cfg(test)]
pub(super) fn knowledge_communities_via_query_runtime(
    db: &Database,
    request: &KnowledgeCommunityListRequest,
) -> Result<KnowledgeCommunityListOutput> {
    let output = db.query_read_only_with_params_bounded(
        "MATCH (c:Community) RETURN c AS community",
        &BTreeMap::new(),
        None,
    )?;
    let mut rows = output
        .rows
        .iter()
        .filter_map(|row| row.get("community").and_then(knowledge_entity_from_value))
        .map(|community| knowledge_community_row_from_entity(&community))
        .filter(|row| !request.require_summary || row.has_summary)
        .filter(|row| {
            !request.require_non_negative_community_id
                || row
                    .community_id
                    .is_some_and(|community_id| community_id >= 0)
        })
        .collect::<Vec<_>>();
    let matched_count = rows.len();
    rows.sort_by(|left, right| compare_knowledge_community_rows(left, right, request.order));
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();

    Ok(KnowledgeCommunityListOutput {
        graph_commit_epoch: db.store.commit_epoch(),
        rows,
        matched_count,
        returned_count,
    })
}

#[cfg(test)]
pub(super) fn knowledge_community_via_query_runtime(
    db: &Database,
    request: &KnowledgeCommunityRequest,
) -> Result<KnowledgeCommunityOutput> {
    validate_knowledge_community_request(request)?;
    let (query, parameters) = match &request.key {
        KnowledgeCommunityLookupKey::Id(id) => (
            "MATCH (c:Community {id: $id}) \
             RETURN c AS community, id(c) AS node_id \
             ORDER BY node_id ASC \
             LIMIT 1",
            BTreeMap::from([("id".to_string(), Value::String(id.clone()))]),
        ),
        KnowledgeCommunityLookupKey::CommunityId(community_id) => (
            "MATCH (c:Community {community_id: $community_id}) \
             RETURN c AS community, id(c) AS node_id \
             ORDER BY node_id ASC \
             LIMIT 1",
            BTreeMap::from([("community_id".to_string(), Value::Int(*community_id))]),
        ),
    };
    let output = db.query_read_only_with_params_bounded(query, &parameters, Some(1))?;
    let row = output
        .rows
        .first()
        .and_then(|row| row.get("community"))
        .and_then(knowledge_entity_from_value)
        .map(|community| knowledge_community_row_from_entity(&community));
    let found = row.is_some();
    Ok(KnowledgeCommunityOutput {
        graph_commit_epoch: db.store.commit_epoch(),
        row,
        found,
    })
}

#[cfg(test)]
pub(super) fn validate_knowledge_community_request(request: &KnowledgeCommunityRequest) -> Result<()> {
    if let KnowledgeCommunityLookupKey::Id(id) = &request.key
        && id.is_empty()
    {
        return Err(SkeinError::Semantic(
            "knowledge community read requires a non-empty id".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn knowledge_community_row_from_entity(community: &KnowledgeEntity) -> KnowledgeCommunityRow {
    let ai_summary = community.properties.get("ai_summary").cloned();
    KnowledgeCommunityRow {
        id: string_property_value(&community.properties, "id")
            .or_else(|| community.external_id.clone()),
        node_id: community.node_id,
        community_id: integer_property_value(&community.properties, "community_id"),
        name: string_property_value(&community.properties, "name"),
        description: community.properties.get("description").cloned(),
        has_summary: ai_summary.as_ref().is_some_and(|value| {
            !matches!(value, Value::Null) && !value_to_external_id(value).is_empty()
        }),
        ai_summary,
        member_count: integer_property_value(&community.properties, "member_count"),
        updated_at: community.properties.get("updated_at").cloned(),
    }
}

#[cfg(test)]
pub(super) fn compare_knowledge_community_rows(
    left: &KnowledgeCommunityRow,
    right: &KnowledgeCommunityRow,
    order: KnowledgeCommunityListOrder,
) -> std::cmp::Ordering {
    match order {
        KnowledgeCommunityListOrder::MemberCountDesc => {
            compare_optional_i64_desc(left.member_count, right.member_count)
        }
        KnowledgeCommunityListOrder::SummaryPresenceThenMemberCountDesc => right
            .has_summary
            .cmp(&left.has_summary)
            .then_with(|| compare_optional_i64_desc(left.member_count, right.member_count)),
    }
    .then_with(|| left.name.cmp(&right.name))
    .then_with(|| left.community_id.cmp(&right.community_id))
    .then_with(|| left.id.cmp(&right.id))
    .then_with(|| left.node_id.cmp(&right.node_id))
}

#[cfg(test)]
pub(super) fn compare_optional_i64_desc(left: Option<i64>, right: Option<i64>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => right.cmp(&left),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

pub(super) fn update_knowledge_communities_batch_for(
    db: &mut Database,
    request: &KnowledgeCommunityLifecycleBatchRequest,
) -> Result<KnowledgeCommunityLifecycleBatchOutput> {
    db.ensure_writable()?;
    for create in &request.creates {
        validate_knowledge_community_create(create)?;
    }
    for update in &request.summary_updates {
        validate_knowledge_community_summary_update(update)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut create_rows = Vec::with_capacity(request.creates.len());
    let mut summary_update_rows = Vec::with_capacity(request.summary_updates.len());
    let mut created_count = 0;
    let mut already_exists_count = 0;
    let mut duplicate_count = 0;
    let mut updated_count = 0;
    let mut missing_count = 0;
    let mut non_writable_count = 0;
    let mut updated_property_count = 0;
    let mut pending_create_ids = BTreeSet::new();
    let mut pending_update_node_ids = BTreeSet::new();
    let mut eligible_creates = Vec::new();
    let mut eligible_updates = Vec::new();

    for create in &request.creates {
        if let Some(existing) =
            try_seed_node_by_label_and_external_id(&db.catalog, &db.store, "Community", &create.id)?
        {
            already_exists_count += 1;
            create_rows.push(KnowledgeCommunityCreateBatchRow {
                id: create.id.clone(),
                node_id: Some(existing.id.0),
                created: false,
                already_exists: true,
                duplicate: false,
            });
            continue;
        }
        if !pending_create_ids.insert(create.id.clone()) {
            duplicate_count += 1;
            create_rows.push(KnowledgeCommunityCreateBatchRow {
                id: create.id.clone(),
                node_id: None,
                created: false,
                already_exists: false,
                duplicate: true,
            });
            continue;
        }

        created_count += 1;
        eligible_creates.push(knowledge_community_create_entity_request(create));
        create_rows.push(KnowledgeCommunityCreateBatchRow {
            id: create.id.clone(),
            node_id: None,
            created: true,
            already_exists: false,
            duplicate: false,
        });
    }

    for update in &request.summary_updates {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "Community",
            &update.id,
        )?
        else {
            missing_count += 1;
            summary_update_rows.push(KnowledgeCommunitySummaryUpdateBatchRow {
                id: update.id.clone(),
                node_id: None,
                matched: false,
                updated: false,
                missing: true,
                duplicate: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        if !node_has_external_id_property(&seed, update.id.as_str()) {
            non_writable_count += 1;
            summary_update_rows.push(KnowledgeCommunitySummaryUpdateBatchRow {
                id: update.id.clone(),
                node_id: Some(seed.id.0),
                matched: false,
                updated: false,
                missing: false,
                duplicate: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }
        if !pending_update_node_ids.insert(seed.id) {
            duplicate_count += 1;
            summary_update_rows.push(KnowledgeCommunitySummaryUpdateBatchRow {
                id: update.id.clone(),
                node_id: Some(seed.id.0),
                matched: true,
                updated: false,
                missing: false,
                duplicate: true,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let assignments = knowledge_community_summary_assignments(update);
        let row_updated_property_count = assignments.len();
        updated_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push((seed.id, assignments));
        summary_update_rows.push(KnowledgeCommunitySummaryUpdateBatchRow {
            id: update.id.clone(),
            node_id: Some(seed.id.0),
            matched: true,
            updated: true,
            missing: false,
            duplicate: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_creates.is_empty() && eligible_updates.is_empty() {
        return Ok(KnowledgeCommunityLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            create_rows,
            summary_update_rows,
            created_count,
            already_exists_count,
            duplicate_count,
            updated_count,
            missing_count,
            non_writable_count,
            created_node_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_entity_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    for (node_id, assignments) in &eligible_updates {
        let (cypher, parameters) =
            knowledge_community_summary_update_statement(*node_id, assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    let output = tx.commit()?;
    for row in &mut create_rows {
        if row.created {
            row.node_id = try_seed_node_by_label_and_external_id(
                &db.catalog,
                &db.store,
                "Community",
                &row.id,
            )?
            .map(|node| node.id.0);
        }
    }

    Ok(KnowledgeCommunityLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        create_rows,
        summary_update_rows,
        created_count,
        already_exists_count,
        duplicate_count,
        updated_count,
        missing_count,
        non_writable_count,
        created_node_count: output.rows.len().saturating_sub(eligible_updates.len()),
        updated_property_count,
    })
}

pub(super) fn validate_knowledge_community_create(create: &KnowledgeCommunityCreate) -> Result<()> {
    if create.id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community create requires a non-empty id".to_string(),
        ));
    }
    if create.name.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community create requires a non-empty name".to_string(),
        ));
    }
    if create.community_id < 0 {
        return Err(SkeinError::Semantic(
            "knowledge community create requires non-negative community_id".to_string(),
        ));
    }
    if create.member_count < 0 {
        return Err(SkeinError::Semantic(
            "knowledge community create requires non-negative member_count".to_string(),
        ));
    }
    if !create.resolution.is_finite() {
        return Err(SkeinError::Semantic(
            "knowledge community create resolution must be finite".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn validate_knowledge_community_summary_update(
    update: &KnowledgeCommunitySummaryUpdate,
) -> Result<()> {
    if update.id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community summary update requires a non-empty id".to_string(),
        ));
    }
    if update.name.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge community summary update requires a non-empty name".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn knowledge_community_create_entity_request(
    create: &KnowledgeCommunityCreate,
) -> KnowledgeEntityCreateRequest {
    KnowledgeEntityCreateRequest {
        label: "Community".to_string(),
        external_id: create.id.clone(),
        properties: BTreeMap::from([
            ("community_id".to_string(), Value::Int(create.community_id)),
            ("name".to_string(), Value::String(create.name.clone())),
            ("description".to_string(), create.description.clone()),
            ("ai_summary".to_string(), create.ai_summary.clone()),
            ("member_count".to_string(), Value::Int(create.member_count)),
            (
                "algorithm".to_string(),
                Value::String("louvain".to_string()),
            ),
            ("resolution".to_string(), Value::Float(create.resolution)),
            ("created_at".to_string(), create.created_at.clone()),
            ("updated_at".to_string(), create.updated_at.clone()),
        ]),
    }
}

pub(super) fn knowledge_community_summary_assignments(
    update: &KnowledgeCommunitySummaryUpdate,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("name".to_string(), Value::String(update.name.clone())),
        ("description".to_string(), update.description.clone()),
        ("ai_summary".to_string(), update.ai_summary.clone()),
        ("updated_at".to_string(), update.updated_at.clone()),
    ])
}

pub(super) fn knowledge_community_summary_update_statement(
    node_id: NodeId,
    assignments: &BTreeMap<String, Value>,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = "MATCH (c:Community) WHERE id(c) = $node_id SET ".to_string();
    let mut parameters = BTreeMap::from([("node_id".to_string(), Value::Int(node_id.0 as i64))]);
    for (index, (property, value)) in assignments.iter().enumerate() {
        if index > 0 {
            cypher.push_str(", ");
        }
        let parameter_name = format!("property_value_{index}");
        cypher.push_str(&format!("c.{property} = ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    (cypher, parameters)
}

pub(super) fn delete_knowledge_communities_for(
    db: &mut Database,
    request: &KnowledgeCommunityCleanupRequest,
) -> Result<KnowledgeCommunityCleanupOutput> {
    db.ensure_writable()?;
    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(label_id) = db.catalog.label_id("Community") else {
        return Ok(KnowledgeCommunityCleanupOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows: Vec::new(),
            candidate_count: 0,
            deleted_count: 0,
        });
    };

    let mut rows = Vec::new();
    db.store.visit_nodes_owned(Some(label_id), |node| {
        rows.push(KnowledgeCommunityCleanupRow {
            id: node_external_id(&node),
            node_id: node.id.0,
            deleted: true,
        });
        crate::store::GraphScanControl::Continue
    })?;
    rows.sort_by_key(|row| row.node_id);

    if rows.is_empty() {
        return Ok(KnowledgeCommunityCleanupOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            candidate_count: 0,
            deleted_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for row in &rows {
        let (cypher, parameters) =
            knowledge_community_cleanup_statement(NodeId(row.node_id), request.detach)?;
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    let deleted_count = rows.len();

    Ok(KnowledgeCommunityCleanupOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        candidate_count: rows.len(),
        rows,
        deleted_count,
    })
}

pub(super) fn knowledge_community_cleanup_statement(
    node_id: NodeId,
    detach: bool,
) -> Result<(String, BTreeMap<String, Value>)> {
    let node_id = i64::try_from(node_id.0)
        .map_err(|_| SkeinError::Semantic("node id does not fit Cypher integer".to_string()))?;
    let verb = if detach { "DETACH DELETE" } else { "DELETE" };
    Ok((
        format!("MATCH (c:Community) WHERE id(c) = $node_id {verb} c"),
        BTreeMap::from([("node_id".to_string(), Value::Int(node_id))]),
    ))
}

pub(super) fn delete_knowledge_graph_meta_for(
    db: &mut Database,
    request: &KnowledgeGraphMetaRequest,
) -> Result<KnowledgeGraphMetaDeleteOutput> {
    db.ensure_writable()?;
    validate_graph_meta_request(request)?;
    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(node_id) = try_node_by_label_property_external_id(
        &db.catalog,
        &db.store,
        "GraphMeta",
        "meta_id",
        &request.meta_id,
    )?
    .map(|node| node.id) else {
        return Ok(KnowledgeGraphMetaDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: None,
            matched: false,
            deleted: false,
        });
    };

    let (cypher, parameters) = knowledge_graph_meta_delete_statement(node_id)?;
    let mut tx = db.begin_transaction();
    tx.query_with_params(cypher.as_str(), &parameters)?;
    tx.commit()?;

    Ok(KnowledgeGraphMetaDeleteOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        node_id: Some(node_id.0),
        matched: true,
        deleted: true,
    })
}

pub(super) fn validate_graph_meta_request(request: &KnowledgeGraphMetaRequest) -> Result<()> {
    if request.meta_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge graph meta request requires a non-empty meta id".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn knowledge_graph_meta_delete_statement(
    node_id: NodeId,
) -> Result<(String, BTreeMap<String, Value>)> {
    let node_id = i64::try_from(node_id.0)
        .map_err(|_| SkeinError::Semantic("node id does not fit Cypher integer".to_string()))?;
    Ok((
        "MATCH (m:GraphMeta) WHERE id(m) = $node_id DELETE m".to_string(),
        BTreeMap::from([("node_id".to_string(), Value::Int(node_id))]),
    ))
}

pub(super) fn stamp_knowledge_graph_meta_batch_for(
    db: &mut Database,
    request: &KnowledgeGraphMetaStampBatchRequest,
) -> Result<KnowledgeGraphMetaStampBatchOutput> {
    db.ensure_writable()?;
    for stamp in &request.stamps {
        if stamp.meta_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge graph meta stamp requires a non-empty meta id".to_string(),
            ));
        }
        if stamp.assignments.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge graph meta stamp requires at least one assignment".to_string(),
            ));
        }
        for property in stamp.assignments.keys() {
            validate_cypher_identifier(property, "property")?;
            if property == "meta_id" {
                return Err(SkeinError::Semantic(
                    "knowledge graph meta stamp cannot update meta_id".to_string(),
                ));
            }
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.stamps.len());
    let mut created_count = 0;
    let mut updated_count = 0;
    let mut duplicate_count = 0;
    let mut updated_property_count = 0;
    let mut pending_meta_ids = BTreeSet::new();
    let mut eligible_stamps = Vec::new();

    for stamp in &request.stamps {
        if !pending_meta_ids.insert(stamp.meta_id.clone()) {
            duplicate_count += 1;
            rows.push(KnowledgeGraphMetaStampBatchRow {
                meta_id: stamp.meta_id.clone(),
                node_id: None,
                created: false,
                updated: false,
                duplicate: true,
                updated_property_count: 0,
            });
            continue;
        }

        let existing = try_node_by_label_property_external_id(
            &db.catalog,
            &db.store,
            "GraphMeta",
            "meta_id",
            stamp.meta_id.as_str(),
        )?;
        let updated_properties = stamp.assignments.len();
        updated_property_count += updated_properties;
        match existing {
            Some(node) => {
                updated_count += 1;
                eligible_stamps.push(GraphMetaStampOperation::Update {
                    node_id: node.id,
                    assignments: stamp.assignments.clone(),
                });
                rows.push(KnowledgeGraphMetaStampBatchRow {
                    meta_id: stamp.meta_id.clone(),
                    node_id: Some(node.id.0),
                    created: false,
                    updated: true,
                    duplicate: false,
                    updated_property_count: updated_properties,
                });
            }
            None => {
                created_count += 1;
                eligible_stamps.push(GraphMetaStampOperation::Create {
                    meta_id: stamp.meta_id.clone(),
                    assignments: stamp.assignments.clone(),
                });
                rows.push(KnowledgeGraphMetaStampBatchRow {
                    meta_id: stamp.meta_id.clone(),
                    node_id: None,
                    created: true,
                    updated: false,
                    duplicate: false,
                    updated_property_count: updated_properties,
                });
            }
        }
    }

    if eligible_stamps.is_empty() {
        return Ok(KnowledgeGraphMetaStampBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            created_count: 0,
            updated_count: 0,
            duplicate_count,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for operation in &eligible_stamps {
        let (cypher, parameters) = graph_meta_stamp_statement(operation);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    for row in &mut rows {
        if row.created {
            row.node_id = try_node_by_label_property_external_id(
                &db.catalog,
                &db.store,
                "GraphMeta",
                "meta_id",
                row.meta_id.as_str(),
            )?
            .map(|node| node.id.0);
        }
    }

    Ok(KnowledgeGraphMetaStampBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        created_count,
        updated_count,
        duplicate_count,
        updated_property_count,
    })
}

pub(super) enum GraphMetaStampOperation {
    Create {
        meta_id: String,
        assignments: BTreeMap<String, Value>,
    },
    Update {
        node_id: NodeId,
        assignments: BTreeMap<String, Value>,
    },
}

pub(super) fn graph_meta_stamp_statement(
    operation: &GraphMetaStampOperation,
) -> (String, BTreeMap<String, Value>) {
    match operation {
        GraphMetaStampOperation::Create {
            meta_id,
            assignments,
        } => graph_meta_create_statement(meta_id, assignments),
        GraphMetaStampOperation::Update {
            node_id,
            assignments,
        } => knowledge_property_update_statement("GraphMeta", node_id.0, assignments),
    }
}

pub(super) fn graph_meta_create_statement(
    meta_id: &str,
    assignments: &BTreeMap<String, Value>,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = "CREATE (:GraphMeta {meta_id: $meta_id".to_string();
    let mut parameters =
        BTreeMap::from([("meta_id".to_string(), Value::String(meta_id.to_string()))]);
    for (index, (property, value)) in assignments.iter().enumerate() {
        let parameter_name = format!("property_value_{index}");
        cypher.push_str(&format!(", {property}: ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    cypher.push_str("})");
    (cypher, parameters)
}

pub(super) fn apply_knowledge_schema_migrations_batch_for(
    db: &mut Database,
    request: &KnowledgeSchemaMigrationApplyBatchRequest,
) -> Result<KnowledgeSchemaMigrationApplyBatchOutput> {
    db.ensure_writable()?;
    for migration in &request.migrations {
        if migration.migration_id.is_empty() {
            return Err(SkeinError::Semantic(
                "knowledge schema migration apply requires a non-empty migration id".to_string(),
            ));
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.migrations.len());
    let mut created_count = 0;
    let mut already_applied_count = 0;
    let mut duplicate_count = 0;
    let mut pending_migration_ids = BTreeSet::new();
    let mut eligible_creates = Vec::new();

    for migration in &request.migrations {
        let existing = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            "SchemaMigrationLog",
            migration.migration_id.as_str(),
        )?;
        if let Some(node) = existing {
            already_applied_count += 1;
            rows.push(KnowledgeSchemaMigrationApplyBatchRow {
                migration_id: migration.migration_id.clone(),
                node_id: Some(node.id.0),
                created: false,
                already_applied: true,
                duplicate: false,
            });
            continue;
        }
        if !pending_migration_ids.insert(migration.migration_id.clone()) {
            duplicate_count += 1;
            rows.push(KnowledgeSchemaMigrationApplyBatchRow {
                migration_id: migration.migration_id.clone(),
                node_id: None,
                created: false,
                already_applied: false,
                duplicate: true,
            });
            continue;
        }

        created_count += 1;
        let create = KnowledgeEntityCreateRequest {
            label: "SchemaMigrationLog".to_string(),
            external_id: migration.migration_id.clone(),
            properties: BTreeMap::from([("applied_at".to_string(), migration.applied_at.clone())]),
        };
        eligible_creates.push(create);
        rows.push(KnowledgeSchemaMigrationApplyBatchRow {
            migration_id: migration.migration_id.clone(),
            node_id: None,
            created: true,
            already_applied: false,
            duplicate: false,
        });
    }

    if eligible_creates.is_empty() {
        return Ok(KnowledgeSchemaMigrationApplyBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            created_count: 0,
            already_applied_count,
            duplicate_count,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_entity_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    for row in &mut rows {
        if row.created {
            row.node_id = try_seed_node_by_label_and_external_id(
                &db.catalog,
                &db.store,
                "SchemaMigrationLog",
                row.migration_id.as_str(),
            )?
            .map(|node| node.id.0);
        }
    }

    Ok(KnowledgeSchemaMigrationApplyBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        created_count,
        already_applied_count,
        duplicate_count,
    })
}

pub(super) fn update_knowledge_augmentation_jobs_batch_for(
    db: &mut Database,
    request: &KnowledgeAugmentationJobLifecycleBatchRequest,
) -> Result<KnowledgeAugmentationJobLifecycleBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        validate_augmentation_job_lifecycle_update(update)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut created_count = 0;
    let mut updated_count = 0;
    let mut missing_count = 0;
    let mut already_exists_count = 0;
    let mut status_mismatch_count = 0;
    let mut duplicate_count = 0;
    let mut updated_property_count = 0;
    let mut pending_job_ids = BTreeSet::new();
    let mut eligible_operations = Vec::new();

    for update in &request.updates {
        if !pending_job_ids.insert(update.job_id.clone()) {
            duplicate_count += 1;
            rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                job_id: update.job_id.clone(),
                node_id: None,
                created: false,
                updated: false,
                missing: false,
                already_exists: false,
                status_mismatch: false,
                duplicate: true,
                updated_property_count: 0,
            });
            continue;
        }

        match &update.transition {
            KnowledgeAugmentationJobLifecycleTransition::Create { .. } => {
                if let Some(existing) = try_node_by_label_property_external_id(
                    &db.catalog,
                    &db.store,
                    "AugmentationJob",
                    "job_id",
                    update.job_id.as_str(),
                )? {
                    already_exists_count += 1;
                    rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                        job_id: update.job_id.clone(),
                        node_id: Some(existing.id.0),
                        created: false,
                        updated: false,
                        missing: false,
                        already_exists: true,
                        status_mismatch: false,
                        duplicate: false,
                        updated_property_count: 0,
                    });
                    continue;
                }
                let assignments = augmentation_job_create_assignments(update);
                let row_updated_property_count = assignments.len();
                updated_property_count += row_updated_property_count;
                created_count += 1;
                eligible_operations.push(AugmentationJobLifecycleOperation::Create {
                    job_id: update.job_id.clone(),
                    assignments,
                });
                rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                    job_id: update.job_id.clone(),
                    node_id: None,
                    created: true,
                    updated: false,
                    missing: false,
                    already_exists: false,
                    status_mismatch: false,
                    duplicate: false,
                    updated_property_count: row_updated_property_count,
                });
            }
            _ => {
                let Some(existing) = try_node_by_label_property_external_id(
                    &db.catalog,
                    &db.store,
                    "AugmentationJob",
                    "job_id",
                    update.job_id.as_str(),
                )?
                else {
                    missing_count += 1;
                    rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                        job_id: update.job_id.clone(),
                        node_id: None,
                        created: false,
                        updated: false,
                        missing: true,
                        already_exists: false,
                        status_mismatch: false,
                        duplicate: false,
                        updated_property_count: 0,
                    });
                    continue;
                };
                if !augmentation_job_transition_allows_status(
                    &update.transition,
                    augmentation_job_status(&existing).as_deref(),
                ) {
                    status_mismatch_count += 1;
                    rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                        job_id: update.job_id.clone(),
                        node_id: Some(existing.id.0),
                        created: false,
                        updated: false,
                        missing: false,
                        already_exists: false,
                        status_mismatch: true,
                        duplicate: false,
                        updated_property_count: 0,
                    });
                    continue;
                }
                let assignments = augmentation_job_transition_assignments(&update.transition);
                let row_updated_property_count = assignments.len();
                updated_property_count += row_updated_property_count;
                updated_count += 1;
                eligible_operations.push(AugmentationJobLifecycleOperation::Update {
                    node_id: existing.id,
                    assignments,
                });
                rows.push(KnowledgeAugmentationJobLifecycleBatchRow {
                    job_id: update.job_id.clone(),
                    node_id: Some(existing.id.0),
                    created: false,
                    updated: true,
                    missing: false,
                    already_exists: false,
                    status_mismatch: false,
                    duplicate: false,
                    updated_property_count: row_updated_property_count,
                });
            }
        }
    }

    if eligible_operations.is_empty() {
        return Ok(KnowledgeAugmentationJobLifecycleBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            created_count: 0,
            updated_count: 0,
            missing_count,
            already_exists_count,
            status_mismatch_count,
            duplicate_count,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for operation in &eligible_operations {
        let (cypher, parameters) = augmentation_job_lifecycle_statement(operation);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;

    for row in &mut rows {
        if row.created {
            row.node_id = try_node_by_label_property_external_id(
                &db.catalog,
                &db.store,
                "AugmentationJob",
                "job_id",
                row.job_id.as_str(),
            )?
            .map(|node| node.id.0);
        }
    }

    Ok(KnowledgeAugmentationJobLifecycleBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        created_count,
        updated_count,
        missing_count,
        already_exists_count,
        status_mismatch_count,
        duplicate_count,
        updated_property_count,
    })
}

pub(super) enum AugmentationJobLifecycleOperation {
    Create {
        job_id: String,
        assignments: BTreeMap<String, Value>,
    },
    Update {
        node_id: NodeId,
        assignments: BTreeMap<String, Value>,
    },
}

pub(super) fn validate_augmentation_job_lifecycle_update(
    update: &KnowledgeAugmentationJobLifecycleUpdate,
) -> Result<()> {
    if update.job_id.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge augmentation job update requires a non-empty job id".to_string(),
        ));
    }
    match &update.transition {
        KnowledgeAugmentationJobLifecycleTransition::Create { job_type, .. } => {
            if job_type.is_empty() {
                return Err(SkeinError::Semantic(
                    "knowledge augmentation job create requires a non-empty job type".to_string(),
                ));
            }
        }
        KnowledgeAugmentationJobLifecycleTransition::UpdateProgress { progress, message } => {
            if !progress.is_finite() || *progress < 0.0 || *progress > 100.0 {
                return Err(SkeinError::Semantic(
                    "knowledge augmentation job progress requires a finite percentage".to_string(),
                ));
            }
            if message.is_empty() {
                return Err(SkeinError::Semantic(
                    "knowledge augmentation job progress requires a non-empty message".to_string(),
                ));
            }
        }
        KnowledgeAugmentationJobLifecycleTransition::MarkFailed { error_message, .. }
            if error_message.is_empty() =>
        {
            return Err(SkeinError::Semantic(
                "knowledge augmentation job failure requires a non-empty error message".to_string(),
            ));
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn augmentation_job_create_assignments(
    update: &KnowledgeAugmentationJobLifecycleUpdate,
) -> BTreeMap<String, Value> {
    let KnowledgeAugmentationJobLifecycleTransition::Create {
        job_type,
        parameters,
        created_at,
    } = &update.transition
    else {
        unreachable!("augmentation job create assignments require create transition");
    };
    BTreeMap::from([
        ("job_type".to_string(), Value::String(job_type.clone())),
        ("status".to_string(), Value::String("pending".to_string())),
        ("progress".to_string(), Value::Float(0.0)),
        (
            "message".to_string(),
            Value::String("Job created".to_string()),
        ),
        ("parameters".to_string(), parameters.clone()),
        ("result".to_string(), Value::String("{}".to_string())),
        ("error_message".to_string(), Value::String(String::new())),
        ("started_at".to_string(), Value::Null),
        ("completed_at".to_string(), Value::Null),
        ("created_at".to_string(), created_at.clone()),
    ])
}

pub(super) fn augmentation_job_transition_assignments(
    transition: &KnowledgeAugmentationJobLifecycleTransition,
) -> BTreeMap<String, Value> {
    match transition {
        KnowledgeAugmentationJobLifecycleTransition::MarkRunning { started_at } => {
            BTreeMap::from([
                ("status".to_string(), Value::String("running".to_string())),
                ("started_at".to_string(), started_at.clone()),
                (
                    "message".to_string(),
                    Value::String("Job started".to_string()),
                ),
            ])
        }
        KnowledgeAugmentationJobLifecycleTransition::UpdateProgress { progress, message } => {
            BTreeMap::from([
                ("progress".to_string(), Value::Float(*progress)),
                ("message".to_string(), Value::String(message.clone())),
            ])
        }
        KnowledgeAugmentationJobLifecycleTransition::MarkCompleted {
            result,
            completed_at,
        } => BTreeMap::from([
            ("status".to_string(), Value::String("completed".to_string())),
            ("progress".to_string(), Value::Float(100.0)),
            (
                "message".to_string(),
                Value::String("Job completed successfully".to_string()),
            ),
            ("result".to_string(), result.clone()),
            ("completed_at".to_string(), completed_at.clone()),
        ]),
        KnowledgeAugmentationJobLifecycleTransition::MarkFailed {
            error_message,
            completed_at,
        } => BTreeMap::from([
            ("status".to_string(), Value::String("failed".to_string())),
            (
                "message".to_string(),
                Value::String("Job failed".to_string()),
            ),
            (
                "error_message".to_string(),
                Value::String(error_message.clone()),
            ),
            ("completed_at".to_string(), completed_at.clone()),
        ]),
        KnowledgeAugmentationJobLifecycleTransition::Create { .. } => {
            unreachable!("augmentation job create is handled separately")
        }
    }
}

pub(super) fn augmentation_job_transition_allows_status(
    transition: &KnowledgeAugmentationJobLifecycleTransition,
    status: Option<&str>,
) -> bool {
    match transition {
        KnowledgeAugmentationJobLifecycleTransition::MarkRunning { .. } => {
            status == Some("pending")
        }
        KnowledgeAugmentationJobLifecycleTransition::UpdateProgress { .. }
        | KnowledgeAugmentationJobLifecycleTransition::MarkCompleted { .. } => {
            status == Some("running")
        }
        KnowledgeAugmentationJobLifecycleTransition::MarkFailed { .. } => {
            matches!(status, Some("pending" | "running"))
        }
        KnowledgeAugmentationJobLifecycleTransition::Create { .. } => false,
    }
}

pub(super) fn augmentation_job_status(node: &NodeRecord) -> Option<String> {
    match node.properties.get("status") {
        Some(Value::String(status)) => Some(status.clone()),
        _ => None,
    }
}

pub(super) fn augmentation_job_lifecycle_statement(
    operation: &AugmentationJobLifecycleOperation,
) -> (String, BTreeMap<String, Value>) {
    match operation {
        AugmentationJobLifecycleOperation::Create {
            job_id,
            assignments,
        } => augmentation_job_create_statement(job_id, assignments),
        AugmentationJobLifecycleOperation::Update {
            node_id,
            assignments,
        } => knowledge_property_update_statement("AugmentationJob", node_id.0, assignments),
    }
}

pub(super) fn augmentation_job_create_statement(
    job_id: &str,
    assignments: &BTreeMap<String, Value>,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = "CREATE (:AugmentationJob {job_id: $job_id".to_string();
    let mut parameters =
        BTreeMap::from([("job_id".to_string(), Value::String(job_id.to_string()))]);
    for (index, (property, value)) in assignments.iter().enumerate() {
        let parameter_name = format!("property_value_{index}");
        cypher.push_str(&format!(", {property}: ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    cypher.push_str("})");
    (cypher, parameters)
}

#[cfg(test)]
pub(super) fn compare_optional_values_desc(left: Option<&Value>, right: Option<&Value>) -> std::cmp::Ordering {
    match (
        left.and_then(value_sort_key),
        right.and_then(value_sort_key),
    ) {
        (Some(left), Some(right)) => right
            .partial_cmp(&left)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

#[cfg(test)]
pub(super) fn value_sort_key(value: &Value) -> Option<f64> {
    match value {
        Value::Int(value) => Some(*value as f64),
        Value::Float(value) if value.is_finite() => Some(*value),
        _ => None,
    }
}

pub(super) fn interrupt_knowledge_augmentation_jobs_for(
    db: &mut Database,
    request: &KnowledgeAugmentationJobInterruptRequest,
) -> Result<KnowledgeAugmentationJobInterruptOutput> {
    db.ensure_writable()?;
    if request.error_message.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge augmentation job interrupt requires a non-empty error message".to_string(),
        ));
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(label_id) = db.catalog.label_id("AugmentationJob") else {
        return Ok(KnowledgeAugmentationJobInterruptOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows: Vec::new(),
            candidate_count: 0,
            interrupted_count: 0,
            updated_property_count: 0,
        });
    };

    let mut rows = Vec::new();
    db.store.visit_nodes_owned(Some(label_id), |node| {
        if let Some(previous_status) = augmentation_job_status(&node)
            && matches!(previous_status.as_str(), "pending" | "running")
        {
            rows.push(KnowledgeAugmentationJobInterruptRow {
                job_id: node
                    .properties
                    .get("job_id")
                    .map(value_to_external_id)
                    .filter(|job_id| !job_id.is_empty()),
                node_id: node.id.0,
                previous_status,
                interrupted: true,
                updated_property_count: 4,
            });
        }
        crate::store::GraphScanControl::Continue
    })?;
    rows.sort_by_key(|row| row.node_id);

    if rows.is_empty() {
        return Ok(KnowledgeAugmentationJobInterruptOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            candidate_count: 0,
            interrupted_count: 0,
            updated_property_count: 0,
        });
    }

    let assignments = BTreeMap::from([
        ("status".to_string(), Value::String("failed".to_string())),
        (
            "message".to_string(),
            Value::String("Interrupted before completion".to_string()),
        ),
        (
            "error_message".to_string(),
            Value::String(request.error_message.clone()),
        ),
        ("completed_at".to_string(), request.completed_at.clone()),
    ]);
    let mut tx = db.begin_transaction();
    for row in &rows {
        let (cypher, parameters) =
            knowledge_property_update_statement("AugmentationJob", row.node_id, &assignments);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    let interrupted_count = rows.len();
    let updated_property_count = rows
        .iter()
        .map(|row| row.updated_property_count)
        .sum::<usize>();

    Ok(KnowledgeAugmentationJobInterruptOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        candidate_count: rows.len(),
        rows,
        interrupted_count,
        updated_property_count,
    })
}

pub(super) fn delete_knowledge_entity_for(
    db: &mut Database,
    request: &KnowledgeEntityDeleteRequest,
) -> Result<KnowledgeEntityDeleteOutput> {
    delete_scoped_knowledge_entity_for(
        db,
        &KnowledgeScopedEntityDeleteRequest {
            delete: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn delete_scoped_knowledge_entity_for(
    db: &mut Database,
    request: &KnowledgeScopedEntityDeleteRequest,
) -> Result<KnowledgeEntityDeleteOutput> {
    db.ensure_writable()?;
    validate_cypher_identifier(&request.delete.entity.label, "label")?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(seed) = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.delete.entity.label.as_str(),
        request.delete.entity.external_id.as_str(),
    )?
    else {
        return Ok(KnowledgeEntityDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: None,
            matched: false,
            filtered_out: false,
            deleted_node_count: 0,
        });
    };
    let node_id = seed.id.0;
    if !node_has_external_id_property(&seed, request.delete.entity.external_id.as_str()) {
        return Ok(KnowledgeEntityDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: Some(node_id),
            matched: false,
            filtered_out: false,
            deleted_node_count: 0,
        });
    }
    if !request.metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            &db.store,
            &seed,
            &request.metadata_filters,
        )
    {
        return Ok(KnowledgeEntityDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            node_id: Some(node_id),
            matched: false,
            filtered_out: true,
            deleted_node_count: 0,
        });
    }

    let cypher = format!(
        "MATCH (n:{} {{id: $external_id}}) DETACH DELETE n",
        request.delete.entity.label
    );
    let parameters = BTreeMap::from([(
        "external_id".to_string(),
        Value::String(request.delete.entity.external_id.clone()),
    )]);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let deleted_node_count = output.rows.len();
    Ok(KnowledgeEntityDeleteOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        node_id: Some(node_id),
        matched: deleted_node_count > 0,
        filtered_out: false,
        deleted_node_count,
    })
}

pub(super) fn delete_knowledge_entity_batch_for(
    db: &mut Database,
    request: &KnowledgeEntityDeleteBatchRequest,
) -> Result<KnowledgeEntityDeleteBatchOutput> {
    delete_scoped_knowledge_entity_batch_for(
        db,
        &KnowledgeScopedEntityDeleteBatchRequest {
            delete: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn delete_scoped_knowledge_entity_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedEntityDeleteBatchRequest,
) -> Result<KnowledgeEntityDeleteBatchOutput> {
    db.ensure_writable()?;
    validate_cypher_identifier(&request.delete.label, "label")?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.delete.external_ids.len());
    let mut matched_count = 0;
    let mut missing_count = 0;
    let mut filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_external_ids = Vec::new();
    let mut seen_eligible_external_ids = BTreeSet::new();

    for external_id in &request.delete.external_ids {
        let Some(seed) = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            request.delete.label.as_str(),
            external_id.as_str(),
        )?
        else {
            missing_count += 1;
            rows.push(KnowledgeEntityDeleteBatchRow {
                external_id: external_id.clone(),
                node_id: None,
                matched: false,
                filtered_out: false,
                non_writable: false,
            });
            continue;
        };
        let node_id = seed.id.0;
        if !node_has_external_id_property(&seed, external_id.as_str()) {
            non_writable_count += 1;
            rows.push(KnowledgeEntityDeleteBatchRow {
                external_id: external_id.clone(),
                node_id: Some(node_id),
                matched: false,
                filtered_out: false,
                non_writable: true,
            });
            continue;
        }
        if !request.metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                &db.store,
                &seed,
                &request.metadata_filters,
            )
        {
            filtered_out_count += 1;
            rows.push(KnowledgeEntityDeleteBatchRow {
                external_id: external_id.clone(),
                node_id: Some(node_id),
                matched: false,
                filtered_out: true,
                non_writable: false,
            });
            continue;
        }

        matched_count += 1;
        if seen_eligible_external_ids.insert(external_id.clone()) {
            eligible_external_ids.push(external_id.clone());
        }
        rows.push(KnowledgeEntityDeleteBatchRow {
            external_id: external_id.clone(),
            node_id: Some(node_id),
            matched: true,
            filtered_out: false,
            non_writable: false,
        });
    }

    if eligible_external_ids.is_empty() {
        return Ok(KnowledgeEntityDeleteBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_count,
            filtered_out_count,
            non_writable_count,
            deleted_node_count: 0,
        });
    }

    let cypher = format!(
        "MATCH (n:{}) WHERE n.id IN $external_ids DETACH DELETE n",
        request.delete.label
    );
    let parameters = BTreeMap::from([(
        "external_ids".to_string(),
        Value::List(
            eligible_external_ids
                .into_iter()
                .map(Value::String)
                .collect(),
        ),
    )]);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let deleted_node_count = output.rows.len();
    Ok(KnowledgeEntityDeleteBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_count,
        filtered_out_count,
        non_writable_count,
        deleted_node_count,
    })
}

pub(super) fn create_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeRelationshipCreateRequest,
) -> Result<KnowledgeRelationshipCreateOutput> {
    create_scoped_knowledge_relationship_for(
        db,
        &KnowledgeScopedRelationshipCreateRequest {
            create: request.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn create_scoped_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipCreateRequest,
) -> Result<KnowledgeRelationshipCreateOutput> {
    db.ensure_writable()?;
    validate_cypher_identifier(&request.create.source.label, "source label")?;
    validate_cypher_identifier(&request.create.target.label, "target label")?;
    validate_cypher_identifier(&request.create.relationship_type, "relationship type")?;
    for property in request.create.properties.keys() {
        validate_cypher_identifier(property, "relationship property")?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let source = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.create.source.label.as_str(),
        request.create.source.external_id.as_str(),
    )?;
    let target = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.create.target.label.as_str(),
        request.create.target.external_id.as_str(),
    )?;
    let source_node_id = source.as_ref().map(|node| node.id.0);
    let target_node_id = target.as_ref().map(|node| node.id.0);
    let (Some(source), Some(target)) = (source, target) else {
        return Ok(KnowledgeRelationshipCreateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            created_relationship_count: 0,
        });
    };
    if !node_has_external_id_property(&source, request.create.source.external_id.as_str())
        || !node_has_external_id_property(&target, request.create.target.external_id.as_str())
    {
        return Ok(KnowledgeRelationshipCreateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            created_relationship_count: 0,
        });
    }

    let source_filtered_out = !request.source_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            &db.store,
            &source,
            &request.source_metadata_filters,
        );
    let target_filtered_out = !request.target_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            &db.store,
            &target,
            &request.target_metadata_filters,
        );
    if source_filtered_out || target_filtered_out {
        return Ok(KnowledgeRelationshipCreateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out,
            target_filtered_out,
            created_relationship_count: 0,
        });
    }

    let (cypher, parameters) = knowledge_relationship_create_statement(&request.create);
    db.query_with_params(cypher.as_str(), &parameters)?;
    Ok(KnowledgeRelationshipCreateOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        source_node_id,
        target_node_id,
        matched: true,
        source_filtered_out: false,
        target_filtered_out: false,
        created_relationship_count: 1,
    })
}

pub(super) fn knowledge_relationship_create_statement(
    request: &KnowledgeRelationshipCreateRequest,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = format!(
        "MATCH (source:{} {{id: $source_external_id}}), (target:{} {{id: $target_external_id}}) CREATE (source)-[:{}",
        request.source.label, request.target.label, request.relationship_type
    );
    let mut parameters = BTreeMap::from([
        (
            "source_external_id".to_string(),
            Value::String(request.source.external_id.clone()),
        ),
        (
            "target_external_id".to_string(),
            Value::String(request.target.external_id.clone()),
        ),
    ]);
    if !request.properties.is_empty() {
        cypher.push_str(" {");
        for (index, (property, value)) in request.properties.iter().enumerate() {
            if index > 0 {
                cypher.push_str(", ");
            }
            let parameter_name = format!("relationship_value_{index}");
            cypher.push_str(&format!("{property}: ${parameter_name}"));
            parameters.insert(parameter_name, value.clone());
        }
        cypher.push('}');
    }
    cypher.push_str("]->(target)");
    (cypher, parameters)
}

pub(super) fn create_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeRelationshipCreateBatchRequest,
) -> Result<KnowledgeRelationshipCreateBatchOutput> {
    create_scoped_knowledge_relationship_batch_for(
        db,
        &KnowledgeScopedRelationshipCreateBatchRequest {
            creates: request.creates.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn create_scoped_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipCreateBatchRequest,
) -> Result<KnowledgeRelationshipCreateBatchOutput> {
    db.ensure_writable()?;
    for create in &request.creates {
        validate_cypher_identifier(&create.source.label, "source label")?;
        validate_cypher_identifier(&create.target.label, "target label")?;
        validate_cypher_identifier(&create.relationship_type, "relationship type")?;
        for property in create.properties.keys() {
            validate_cypher_identifier(property, "relationship property")?;
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.creates.len());
    let mut matched_count = 0;
    let mut missing_endpoint_count = 0;
    let mut source_filtered_out_count = 0;
    let mut target_filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_creates = Vec::new();

    for create in &request.creates {
        let source = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            create.source.label.as_str(),
            create.source.external_id.as_str(),
        )?;
        let target = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            create.target.label.as_str(),
            create.target.external_id.as_str(),
        )?;
        let source_node_id = source.as_ref().map(|node| node.id.0);
        let target_node_id = target.as_ref().map(|node| node.id.0);
        let (Some(source), Some(target)) = (source, target) else {
            missing_endpoint_count += 1;
            rows.push(KnowledgeRelationshipCreateBatchRow {
                source: create.source.clone(),
                target: create.target.clone(),
                relationship_type: create.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
            });
            continue;
        };
        if !node_has_external_id_property(&source, create.source.external_id.as_str())
            || !node_has_external_id_property(&target, create.target.external_id.as_str())
        {
            non_writable_count += 1;
            rows.push(KnowledgeRelationshipCreateBatchRow {
                source: create.source.clone(),
                target: create.target.clone(),
                relationship_type: create.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: true,
            });
            continue;
        }

        let source_filtered_out = !request.source_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                &db.store,
                &source,
                &request.source_metadata_filters,
            );
        let target_filtered_out = !request.target_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                &db.store,
                &target,
                &request.target_metadata_filters,
            );
        if source_filtered_out || target_filtered_out {
            if source_filtered_out {
                source_filtered_out_count += 1;
            }
            if target_filtered_out {
                target_filtered_out_count += 1;
            }
            rows.push(KnowledgeRelationshipCreateBatchRow {
                source: create.source.clone(),
                target: create.target.clone(),
                relationship_type: create.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out,
                target_filtered_out,
                non_writable: false,
            });
            continue;
        }

        matched_count += 1;
        eligible_creates.push(create.clone());
        rows.push(KnowledgeRelationshipCreateBatchRow {
            source: create.source.clone(),
            target: create.target.clone(),
            relationship_type: create.relationship_type.clone(),
            source_node_id,
            target_node_id,
            matched: true,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
        });
    }

    if eligible_creates.is_empty() {
        return Ok(KnowledgeRelationshipCreateBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_endpoint_count,
            source_filtered_out_count,
            target_filtered_out_count,
            non_writable_count,
            created_relationship_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_relationship_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    let output = tx.commit()?;
    let created_relationship_count = output.rows.len();
    Ok(KnowledgeRelationshipCreateBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_endpoint_count,
        source_filtered_out_count,
        target_filtered_out_count,
        non_writable_count,
        created_relationship_count,
    })
}

pub(super) fn upsert_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeRelationshipUpsertRequest,
) -> Result<KnowledgeRelationshipUpsertOutput> {
    upsert_scoped_knowledge_relationship_for(
        db,
        &KnowledgeScopedRelationshipUpsertRequest {
            upsert: request.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn upsert_scoped_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipUpsertRequest,
) -> Result<KnowledgeRelationshipUpsertOutput> {
    db.ensure_writable()?;
    validate_knowledge_relationship_upsert(&request.upsert)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let source = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.upsert.source.label.as_str(),
        request.upsert.source.external_id.as_str(),
    )?;
    let target = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.upsert.target.label.as_str(),
        request.upsert.target.external_id.as_str(),
    )?;
    let source_node_id = source.as_ref().map(|node| node.id.0);
    let target_node_id = target.as_ref().map(|node| node.id.0);
    let (Some(source), Some(target)) = (source, target) else {
        return Ok(KnowledgeRelationshipUpsertOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            relationship_id: None,
            matched: false,
            created: false,
            already_exists: false,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
            created_relationship_count: 0,
        });
    };
    if !node_has_external_id_property(&source, request.upsert.source.external_id.as_str())
        || !node_has_external_id_property(&target, request.upsert.target.external_id.as_str())
    {
        return Ok(KnowledgeRelationshipUpsertOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            relationship_id: None,
            matched: false,
            created: false,
            already_exists: false,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: true,
            created_relationship_count: 0,
        });
    }

    let source_filtered_out = !request.source_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            &db.store,
            &source,
            &request.source_metadata_filters,
        );
    let target_filtered_out = !request.target_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            &db.store,
            &target,
            &request.target_metadata_filters,
        );
    if source_filtered_out || target_filtered_out {
        return Ok(KnowledgeRelationshipUpsertOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            relationship_id: None,
            matched: false,
            created: false,
            already_exists: false,
            source_filtered_out,
            target_filtered_out,
            non_writable: false,
            created_relationship_count: 0,
        });
    }

    let source_id = source.id;
    let target_id = target.id;
    if let Some(relationship_id) = existing_knowledge_relationship_id(
        &db.catalog,
        &db.store,
        source_id,
        target_id,
        request.upsert.relationship_type.as_str(),
    )? {
        return Ok(KnowledgeRelationshipUpsertOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            relationship_id: Some(relationship_id),
            matched: true,
            created: false,
            already_exists: true,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
            created_relationship_count: 0,
        });
    }

    let create = knowledge_relationship_upsert_create_request(&request.upsert);
    let (cypher, parameters) = knowledge_relationship_create_statement(&create);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let relationship_id = existing_knowledge_relationship_id(
        &db.catalog,
        &db.store,
        source_id,
        target_id,
        request.upsert.relationship_type.as_str(),
    )?;
    Ok(KnowledgeRelationshipUpsertOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        source_node_id,
        target_node_id,
        relationship_id,
        matched: true,
        created: true,
        already_exists: false,
        source_filtered_out: false,
        target_filtered_out: false,
        non_writable: false,
        created_relationship_count: output.rows.len(),
    })
}

pub(super) fn upsert_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeRelationshipUpsertBatchRequest,
) -> Result<KnowledgeRelationshipUpsertBatchOutput> {
    upsert_scoped_knowledge_relationship_batch_for(
        db,
        &KnowledgeScopedRelationshipUpsertBatchRequest {
            upserts: request.upserts.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn upsert_scoped_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipUpsertBatchRequest,
) -> Result<KnowledgeRelationshipUpsertBatchOutput> {
    db.ensure_writable()?;
    for upsert in &request.upserts {
        validate_knowledge_relationship_upsert(upsert)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.upserts.len());
    let mut matched_count = 0;
    let mut created_count = 0;
    let mut already_exists_count = 0;
    let mut missing_endpoint_count = 0;
    let mut source_filtered_out_count = 0;
    let mut target_filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_creates = Vec::new();
    let mut pending_relationships = BTreeSet::new();

    for upsert in &request.upserts {
        let source = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            upsert.source.label.as_str(),
            upsert.source.external_id.as_str(),
        )?;
        let target = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            upsert.target.label.as_str(),
            upsert.target.external_id.as_str(),
        )?;
        let source_node_id = source.as_ref().map(|node| node.id.0);
        let target_node_id = target.as_ref().map(|node| node.id.0);
        let (Some(source), Some(target)) = (source, target) else {
            missing_endpoint_count += 1;
            rows.push(KnowledgeRelationshipUpsertBatchRow {
                source: upsert.source.clone(),
                target: upsert.target.clone(),
                relationship_type: upsert.relationship_type.clone(),
                source_node_id,
                target_node_id,
                relationship_id: None,
                matched: false,
                created: false,
                already_exists: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
            });
            continue;
        };
        if !node_has_external_id_property(&source, upsert.source.external_id.as_str())
            || !node_has_external_id_property(&target, upsert.target.external_id.as_str())
        {
            non_writable_count += 1;
            rows.push(KnowledgeRelationshipUpsertBatchRow {
                source: upsert.source.clone(),
                target: upsert.target.clone(),
                relationship_type: upsert.relationship_type.clone(),
                source_node_id,
                target_node_id,
                relationship_id: None,
                matched: false,
                created: false,
                already_exists: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: true,
            });
            continue;
        }

        let source_filtered_out = !request.source_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                &db.store,
                &source,
                &request.source_metadata_filters,
            );
        let target_filtered_out = !request.target_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                &db.store,
                &target,
                &request.target_metadata_filters,
            );
        if source_filtered_out || target_filtered_out {
            if source_filtered_out {
                source_filtered_out_count += 1;
            }
            if target_filtered_out {
                target_filtered_out_count += 1;
            }
            rows.push(KnowledgeRelationshipUpsertBatchRow {
                source: upsert.source.clone(),
                target: upsert.target.clone(),
                relationship_type: upsert.relationship_type.clone(),
                source_node_id,
                target_node_id,
                relationship_id: None,
                matched: false,
                created: false,
                already_exists: false,
                source_filtered_out,
                target_filtered_out,
                non_writable: false,
            });
            continue;
        }

        matched_count += 1;
        if let Some(relationship_id) = existing_knowledge_relationship_id(
            &db.catalog,
            &db.store,
            source.id,
            target.id,
            upsert.relationship_type.as_str(),
        )? {
            already_exists_count += 1;
            rows.push(KnowledgeRelationshipUpsertBatchRow {
                source: upsert.source.clone(),
                target: upsert.target.clone(),
                relationship_type: upsert.relationship_type.clone(),
                source_node_id,
                target_node_id,
                relationship_id: Some(relationship_id),
                matched: true,
                created: false,
                already_exists: true,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
            });
            continue;
        }

        let identity = (source.id.0, target.id.0, upsert.relationship_type.clone());
        if !pending_relationships.insert(identity) {
            already_exists_count += 1;
            rows.push(KnowledgeRelationshipUpsertBatchRow {
                source: upsert.source.clone(),
                target: upsert.target.clone(),
                relationship_type: upsert.relationship_type.clone(),
                source_node_id,
                target_node_id,
                relationship_id: None,
                matched: true,
                created: false,
                already_exists: true,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
            });
            continue;
        }

        created_count += 1;
        eligible_creates.push(knowledge_relationship_upsert_create_request(upsert));
        rows.push(KnowledgeRelationshipUpsertBatchRow {
            source: upsert.source.clone(),
            target: upsert.target.clone(),
            relationship_type: upsert.relationship_type.clone(),
            source_node_id,
            target_node_id,
            relationship_id: None,
            matched: true,
            created: true,
            already_exists: false,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
        });
    }

    if eligible_creates.is_empty() {
        return Ok(KnowledgeRelationshipUpsertBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            created_count,
            already_exists_count,
            missing_endpoint_count,
            source_filtered_out_count,
            target_filtered_out_count,
            non_writable_count,
            created_relationship_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for create in &eligible_creates {
        let (cypher, parameters) = knowledge_relationship_create_statement(create);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    for row in &mut rows {
        if row.created
            && let (Some(source_node_id), Some(target_node_id)) =
                (row.source_node_id, row.target_node_id)
        {
            row.relationship_id = existing_knowledge_relationship_id(
                &db.catalog,
                &db.store,
                NodeId(source_node_id),
                NodeId(target_node_id),
                row.relationship_type.as_str(),
            )?;
        }
    }
    Ok(KnowledgeRelationshipUpsertBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        created_count,
        already_exists_count,
        missing_endpoint_count,
        source_filtered_out_count,
        target_filtered_out_count,
        non_writable_count,
        created_relationship_count: eligible_creates.len(),
    })
}

pub(super) fn validate_knowledge_relationship_upsert(
    request: &KnowledgeRelationshipUpsertRequest,
) -> Result<()> {
    validate_cypher_identifier(&request.source.label, "source label")?;
    validate_cypher_identifier(&request.target.label, "target label")?;
    validate_cypher_identifier(&request.relationship_type, "relationship type")?;
    for property in request.create_properties.keys() {
        validate_cypher_identifier(property, "relationship property")?;
    }
    Ok(())
}

pub(super) fn knowledge_relationship_upsert_create_request(
    request: &KnowledgeRelationshipUpsertRequest,
) -> KnowledgeRelationshipCreateRequest {
    KnowledgeRelationshipCreateRequest {
        source: request.source.clone(),
        target: request.target.clone(),
        relationship_type: request.relationship_type.clone(),
        properties: request.create_properties.clone(),
    }
}

pub(super) fn existing_knowledge_relationship_id(
    catalog: &Catalog,
    store: &GraphStore,
    source: NodeId,
    target: NodeId,
    relationship_type: &str,
) -> Result<Option<u64>> {
    let Some(rel_type_id) = catalog.rel_type_id(relationship_type) else {
        return Ok(None);
    };
    let mut relationship_id = None;
    store.visit_adjacent_relationships_owned(
        source,
        Some(rel_type_id),
        AdjacencyDirection::Outgoing,
        |relationship| {
            if relationship.target == target {
                relationship_id = Some(relationship.id.0);
                crate::store::GraphScanControl::Stop
            } else {
                crate::store::GraphScanControl::Continue
            }
        },
    )?;
    Ok(relationship_id)
}

pub(super) fn delete_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeRelationshipDeleteRequest,
) -> Result<KnowledgeRelationshipDeleteOutput> {
    delete_scoped_knowledge_relationship_for(
        db,
        &KnowledgeScopedRelationshipDeleteRequest {
            delete: request.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn delete_scoped_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipDeleteRequest,
) -> Result<KnowledgeRelationshipDeleteOutput> {
    db.ensure_writable()?;
    validate_cypher_identifier(&request.delete.source.label, "source label")?;
    validate_cypher_identifier(&request.delete.target.label, "target label")?;
    validate_cypher_identifier(&request.delete.relationship_type, "relationship type")?;
    for property in request.delete.relationship_properties.keys() {
        validate_cypher_identifier(property, "relationship property")?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let source = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.delete.source.label.as_str(),
        request.delete.source.external_id.as_str(),
    )?;
    let target = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.delete.target.label.as_str(),
        request.delete.target.external_id.as_str(),
    )?;
    let source_node_id = source.as_ref().map(|node| node.id.0);
    let target_node_id = target.as_ref().map(|node| node.id.0);
    let (Some(source), Some(target)) = (source, target) else {
        return Ok(KnowledgeRelationshipDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            deleted_relationship_count: 0,
        });
    };
    if !node_has_external_id_property(&source, request.delete.source.external_id.as_str())
        || !node_has_external_id_property(&target, request.delete.target.external_id.as_str())
    {
        return Ok(KnowledgeRelationshipDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            deleted_relationship_count: 0,
        });
    }

    let source_filtered_out = !request.source_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            &db.store,
            &source,
            &request.source_metadata_filters,
        );
    let target_filtered_out = !request.target_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            &db.store,
            &target,
            &request.target_metadata_filters,
        );
    if source_filtered_out || target_filtered_out {
        return Ok(KnowledgeRelationshipDeleteOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out,
            target_filtered_out,
            deleted_relationship_count: 0,
        });
    }

    let (cypher, parameters) = knowledge_relationship_delete_statement(&request.delete);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let deleted_relationship_count = output.rows.len();
    Ok(KnowledgeRelationshipDeleteOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        source_node_id,
        target_node_id,
        matched: deleted_relationship_count > 0,
        source_filtered_out: false,
        target_filtered_out: false,
        deleted_relationship_count,
    })
}

pub(super) fn knowledge_relationship_delete_statement(
    request: &KnowledgeRelationshipDeleteRequest,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = format!(
        "MATCH (source:{} {{id: $source_external_id}})-[r:{}",
        request.source.label, request.relationship_type
    );
    let mut parameters = BTreeMap::from([
        (
            "source_external_id".to_string(),
            Value::String(request.source.external_id.clone()),
        ),
        (
            "target_external_id".to_string(),
            Value::String(request.target.external_id.clone()),
        ),
    ]);
    if !request.relationship_properties.is_empty() {
        cypher.push_str(" {");
        for (index, (property, value)) in request.relationship_properties.iter().enumerate() {
            if index > 0 {
                cypher.push_str(", ");
            }
            let parameter_name = format!("relationship_value_{index}");
            cypher.push_str(&format!("{property}: ${parameter_name}"));
            parameters.insert(parameter_name, value.clone());
        }
        cypher.push('}');
    }
    cypher.push_str(&format!(
        "]->(target:{} {{id: $target_external_id}}) DELETE r",
        request.target.label
    ));
    (cypher, parameters)
}

pub(super) fn update_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeRelationshipUpdateRequest,
) -> Result<KnowledgeRelationshipUpdateOutput> {
    update_scoped_knowledge_relationship_for(
        db,
        &KnowledgeScopedRelationshipUpdateRequest {
            update: request.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn update_scoped_knowledge_relationship_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipUpdateRequest,
) -> Result<KnowledgeRelationshipUpdateOutput> {
    db.ensure_writable()?;
    validate_knowledge_relationship_update(&request.update)?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let source = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.update.source.label.as_str(),
        request.update.source.external_id.as_str(),
    )?;
    let target = try_seed_node_by_label_and_external_id(
        &db.catalog,
        &db.store,
        request.update.target.label.as_str(),
        request.update.target.external_id.as_str(),
    )?;
    let source_node_id = source.as_ref().map(|node| node.id.0);
    let target_node_id = target.as_ref().map(|node| node.id.0);
    let (Some(source), Some(target)) = (source, target) else {
        return Ok(KnowledgeRelationshipUpdateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            updated_relationship_count: 0,
            updated_property_count: 0,
        });
    };
    if !node_has_external_id_property(&source, request.update.source.external_id.as_str())
        || !node_has_external_id_property(&target, request.update.target.external_id.as_str())
    {
        return Ok(KnowledgeRelationshipUpdateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out: false,
            target_filtered_out: false,
            updated_relationship_count: 0,
            updated_property_count: 0,
        });
    }

    let source_filtered_out = !request.source_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            &db.store,
            &source,
            &request.source_metadata_filters,
        );
    let target_filtered_out = !request.target_metadata_filters.is_empty()
        && !knowledge_graph_seed_matches_filters(
            &db.catalog,
            &db.store,
            &target,
            &request.target_metadata_filters,
        );
    if source_filtered_out || target_filtered_out {
        return Ok(KnowledgeRelationshipUpdateOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            source_node_id,
            target_node_id,
            matched: false,
            source_filtered_out,
            target_filtered_out,
            updated_relationship_count: 0,
            updated_property_count: 0,
        });
    }

    let (cypher, parameters) = knowledge_relationship_update_statement(&request.update);
    let output = db.query_with_params(cypher.as_str(), &parameters)?;
    let updated_relationship_count = output.rows.len();
    Ok(KnowledgeRelationshipUpdateOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        source_node_id,
        target_node_id,
        matched: updated_relationship_count > 0,
        source_filtered_out: false,
        target_filtered_out: false,
        updated_relationship_count,
        updated_property_count: updated_relationship_count * request.update.assignments.len(),
    })
}

pub(super) fn update_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeRelationshipUpdateBatchRequest,
) -> Result<KnowledgeRelationshipUpdateBatchOutput> {
    update_scoped_knowledge_relationship_batch_for(
        db,
        &KnowledgeScopedRelationshipUpdateBatchRequest {
            updates: request.updates.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn update_scoped_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipUpdateBatchRequest,
) -> Result<KnowledgeRelationshipUpdateBatchOutput> {
    db.ensure_writable()?;
    for update in &request.updates {
        validate_knowledge_relationship_update(update)?;
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.updates.len());
    let mut matched_count = 0;
    let mut missing_endpoint_count = 0;
    let mut source_filtered_out_count = 0;
    let mut target_filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_updates = Vec::new();
    let mut updated_property_count = 0;

    for update in &request.updates {
        let source = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            update.source.label.as_str(),
            update.source.external_id.as_str(),
        )?;
        let target = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            update.target.label.as_str(),
            update.target.external_id.as_str(),
        )?;
        let source_node_id = source.as_ref().map(|node| node.id.0);
        let target_node_id = target.as_ref().map(|node| node.id.0);
        let (Some(source), Some(target)) = (source, target) else {
            missing_endpoint_count += 1;
            rows.push(KnowledgeRelationshipUpdateBatchRow {
                source: update.source.clone(),
                target: update.target.clone(),
                relationship_type: update.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        };
        if !node_has_external_id_property(&source, update.source.external_id.as_str())
            || !node_has_external_id_property(&target, update.target.external_id.as_str())
        {
            non_writable_count += 1;
            rows.push(KnowledgeRelationshipUpdateBatchRow {
                source: update.source.clone(),
                target: update.target.clone(),
                relationship_type: update.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: true,
                updated_property_count: 0,
            });
            continue;
        }

        let source_filtered_out = !request.source_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                &db.store,
                &source,
                &request.source_metadata_filters,
            );
        let target_filtered_out = !request.target_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                &db.store,
                &target,
                &request.target_metadata_filters,
            );
        if source_filtered_out || target_filtered_out {
            if source_filtered_out {
                source_filtered_out_count += 1;
            }
            if target_filtered_out {
                target_filtered_out_count += 1;
            }
            rows.push(KnowledgeRelationshipUpdateBatchRow {
                source: update.source.clone(),
                target: update.target.clone(),
                relationship_type: update.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out,
                target_filtered_out,
                non_writable: false,
                updated_property_count: 0,
            });
            continue;
        }

        let row_updated_property_count = update.assignments.len();
        matched_count += 1;
        updated_property_count += row_updated_property_count;
        eligible_updates.push(update.clone());
        rows.push(KnowledgeRelationshipUpdateBatchRow {
            source: update.source.clone(),
            target: update.target.clone(),
            relationship_type: update.relationship_type.clone(),
            source_node_id,
            target_node_id,
            matched: true,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
            updated_property_count: row_updated_property_count,
        });
    }

    if eligible_updates.is_empty() {
        return Ok(KnowledgeRelationshipUpdateBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_endpoint_count,
            source_filtered_out_count,
            target_filtered_out_count,
            non_writable_count,
            updated_relationship_count: 0,
            updated_property_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for update in &eligible_updates {
        let (cypher, parameters) = knowledge_relationship_update_statement(update);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    let output = tx.commit()?;
    let updated_relationship_count = output.rows.len();
    Ok(KnowledgeRelationshipUpdateBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_endpoint_count,
        source_filtered_out_count,
        target_filtered_out_count,
        non_writable_count,
        updated_relationship_count,
        updated_property_count,
    })
}

pub(super) fn validate_knowledge_relationship_update(
    request: &KnowledgeRelationshipUpdateRequest,
) -> Result<()> {
    if request.assignments.is_empty() {
        return Err(SkeinError::Semantic(
            "knowledge relationship update requires at least one assignment".to_string(),
        ));
    }
    validate_cypher_identifier(&request.source.label, "source label")?;
    validate_cypher_identifier(&request.target.label, "target label")?;
    validate_cypher_identifier(&request.relationship_type, "relationship type")?;
    for property in request.relationship_properties.keys() {
        validate_cypher_identifier(property, "relationship property")?;
    }
    for property in request.assignments.keys() {
        validate_cypher_identifier(property, "relationship property")?;
    }
    Ok(())
}

pub(super) fn knowledge_relationship_update_statement(
    request: &KnowledgeRelationshipUpdateRequest,
) -> (String, BTreeMap<String, Value>) {
    let mut cypher = format!(
        "MATCH (source:{} {{id: $source_external_id}})-[r:{}",
        request.source.label, request.relationship_type
    );
    let mut parameters = BTreeMap::from([
        (
            "source_external_id".to_string(),
            Value::String(request.source.external_id.clone()),
        ),
        (
            "target_external_id".to_string(),
            Value::String(request.target.external_id.clone()),
        ),
    ]);
    if !request.relationship_properties.is_empty() {
        cypher.push_str(" {");
        for (index, (property, value)) in request.relationship_properties.iter().enumerate() {
            if index > 0 {
                cypher.push_str(", ");
            }
            let parameter_name = format!("relationship_filter_value_{index}");
            cypher.push_str(&format!("{property}: ${parameter_name}"));
            parameters.insert(parameter_name, value.clone());
        }
        cypher.push('}');
    }
    cypher.push_str(&format!(
        "]->(target:{} {{id: $target_external_id}}) SET ",
        request.target.label
    ));
    for (index, (property, value)) in request.assignments.iter().enumerate() {
        if index > 0 {
            cypher.push_str(", ");
        }
        let parameter_name = format!("relationship_update_value_{index}");
        cypher.push_str(&format!("r.{property} = ${parameter_name}"));
        parameters.insert(parameter_name, value.clone());
    }
    (cypher, parameters)
}

pub(super) fn delete_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeRelationshipDeleteBatchRequest,
) -> Result<KnowledgeRelationshipDeleteBatchOutput> {
    delete_scoped_knowledge_relationship_batch_for(
        db,
        &KnowledgeScopedRelationshipDeleteBatchRequest {
            deletes: request.deletes.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

pub(super) fn delete_scoped_knowledge_relationship_batch_for(
    db: &mut Database,
    request: &KnowledgeScopedRelationshipDeleteBatchRequest,
) -> Result<KnowledgeRelationshipDeleteBatchOutput> {
    db.ensure_writable()?;
    for delete in &request.deletes {
        validate_cypher_identifier(&delete.source.label, "source label")?;
        validate_cypher_identifier(&delete.target.label, "target label")?;
        validate_cypher_identifier(&delete.relationship_type, "relationship type")?;
        for property in delete.relationship_properties.keys() {
            validate_cypher_identifier(property, "relationship property")?;
        }
    }

    let graph_commit_epoch_before = db.store.commit_epoch();
    let mut rows = Vec::with_capacity(request.deletes.len());
    let mut matched_count = 0;
    let mut missing_endpoint_count = 0;
    let mut source_filtered_out_count = 0;
    let mut target_filtered_out_count = 0;
    let mut non_writable_count = 0;
    let mut eligible_deletes = Vec::new();

    for delete in &request.deletes {
        let source = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            delete.source.label.as_str(),
            delete.source.external_id.as_str(),
        )?;
        let target = try_seed_node_by_label_and_external_id(
            &db.catalog,
            &db.store,
            delete.target.label.as_str(),
            delete.target.external_id.as_str(),
        )?;
        let source_node_id = source.as_ref().map(|node| node.id.0);
        let target_node_id = target.as_ref().map(|node| node.id.0);
        let (Some(source), Some(target)) = (source, target) else {
            missing_endpoint_count += 1;
            rows.push(KnowledgeRelationshipDeleteBatchRow {
                source: delete.source.clone(),
                target: delete.target.clone(),
                relationship_type: delete.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: false,
            });
            continue;
        };
        if !node_has_external_id_property(&source, delete.source.external_id.as_str())
            || !node_has_external_id_property(&target, delete.target.external_id.as_str())
        {
            non_writable_count += 1;
            rows.push(KnowledgeRelationshipDeleteBatchRow {
                source: delete.source.clone(),
                target: delete.target.clone(),
                relationship_type: delete.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out: false,
                target_filtered_out: false,
                non_writable: true,
            });
            continue;
        }

        let source_filtered_out = !request.source_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                &db.store,
                &source,
                &request.source_metadata_filters,
            );
        let target_filtered_out = !request.target_metadata_filters.is_empty()
            && !knowledge_graph_seed_matches_filters(
                &db.catalog,
                &db.store,
                &target,
                &request.target_metadata_filters,
            );
        if source_filtered_out || target_filtered_out {
            if source_filtered_out {
                source_filtered_out_count += 1;
            }
            if target_filtered_out {
                target_filtered_out_count += 1;
            }
            rows.push(KnowledgeRelationshipDeleteBatchRow {
                source: delete.source.clone(),
                target: delete.target.clone(),
                relationship_type: delete.relationship_type.clone(),
                source_node_id,
                target_node_id,
                matched: false,
                source_filtered_out,
                target_filtered_out,
                non_writable: false,
            });
            continue;
        }

        matched_count += 1;
        eligible_deletes.push(delete.clone());
        rows.push(KnowledgeRelationshipDeleteBatchRow {
            source: delete.source.clone(),
            target: delete.target.clone(),
            relationship_type: delete.relationship_type.clone(),
            source_node_id,
            target_node_id,
            matched: true,
            source_filtered_out: false,
            target_filtered_out: false,
            non_writable: false,
        });
    }

    if eligible_deletes.is_empty() {
        return Ok(KnowledgeRelationshipDeleteBatchOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows,
            matched_count,
            missing_endpoint_count,
            source_filtered_out_count,
            target_filtered_out_count,
            non_writable_count,
            deleted_relationship_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for delete in &eligible_deletes {
        let (cypher, parameters) = knowledge_relationship_delete_statement(delete);
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    let output = tx.commit()?;
    let deleted_relationship_count = output.rows.len();
    Ok(KnowledgeRelationshipDeleteBatchOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        rows,
        matched_count,
        missing_endpoint_count,
        source_filtered_out_count,
        target_filtered_out_count,
        non_writable_count,
        deleted_relationship_count,
    })
}

pub(super) struct KnowledgeSourceReferenceRelationshipDeleteCandidate {
    relationship_id: u64,
    source_node_id: u64,
    target_node_id: u64,
    source_external_id: Option<String>,
    target_external_id: Option<String>,
}

pub(super) fn validate_source_reference(source_reference: &str, operation: &str) -> Result<()> {
    if source_reference.trim().is_empty() {
        return Err(SkeinError::Semantic(format!(
            "knowledge {operation} requires a non-empty source_reference"
        )));
    }
    Ok(())
}

pub(super) fn delete_knowledge_source_reference_relationships_for(
    db: &mut Database,
    request: &KnowledgeSourceReferenceRelationshipCleanupRequest,
) -> Result<KnowledgeSourceReferenceRelationshipCleanupOutput> {
    db.ensure_writable()?;
    validate_source_reference(
        &request.source_reference,
        "source-reference relationship cleanup",
    )?;

    let graph_commit_epoch_before = db.store.commit_epoch();
    let Some(rel_type_id) = db.catalog.rel_type_id("RELATES_TO") else {
        return Ok(KnowledgeSourceReferenceRelationshipCleanupOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows: Vec::new(),
            candidate_count: 0,
            deleted_relationship_count: 0,
        });
    };

    let mut candidates = Vec::new();
    db.store
        .try_visit_relationships_owned(Some(rel_type_id), |relationship| {
            if relationship
                .properties
                .get("source_reference")
                .is_some_and(|value| value_to_external_id(value) == request.source_reference)
            {
                candidates.push(KnowledgeSourceReferenceRelationshipDeleteCandidate {
                    relationship_id: relationship.id.0,
                    source_node_id: relationship.source.0,
                    target_node_id: relationship.target.0,
                    source_external_id: db
                        .store
                        .node_owned(relationship.source)?
                        .as_ref()
                        .and_then(node_external_id),
                    target_external_id: db
                        .store
                        .node_owned(relationship.target)?
                        .as_ref()
                        .and_then(node_external_id),
                });
            }
            Ok(crate::store::GraphScanControl::Continue)
        })?;
    candidates.sort_by_key(|candidate| candidate.relationship_id);

    if candidates.is_empty() {
        return Ok(KnowledgeSourceReferenceRelationshipCleanupOutput {
            graph_commit_epoch_before,
            graph_commit_epoch_after: graph_commit_epoch_before,
            rows: Vec::new(),
            candidate_count: 0,
            deleted_relationship_count: 0,
        });
    }

    let mut tx = db.begin_transaction();
    for candidate in &candidates {
        let (cypher, parameters) =
            knowledge_source_reference_relationship_delete_statement(candidate.relationship_id)?;
        tx.query_with_params(cypher.as_str(), &parameters)?;
    }
    tx.commit()?;
    let deleted_relationship_count = candidates.len();
    let rows = candidates
        .into_iter()
        .map(|candidate| KnowledgeSourceReferenceRelationshipCleanupRow {
            relationship_id: candidate.relationship_id,
            source_node_id: candidate.source_node_id,
            target_node_id: candidate.target_node_id,
            source_external_id: candidate.source_external_id,
            target_external_id: candidate.target_external_id,
            deleted: true,
        })
        .collect::<Vec<_>>();

    Ok(KnowledgeSourceReferenceRelationshipCleanupOutput {
        graph_commit_epoch_before,
        graph_commit_epoch_after: db.store.commit_epoch(),
        candidate_count: rows.len(),
        rows,
        deleted_relationship_count,
    })
}

pub(super) fn knowledge_source_reference_relationship_delete_statement(
    relationship_id: u64,
) -> Result<(String, BTreeMap<String, Value>)> {
    let relationship_id = i64::try_from(relationship_id).map_err(|_| {
        SkeinError::Semantic("relationship id does not fit Cypher integer".to_string())
    })?;
    Ok((
        "MATCH (:Entity)-[r:RELATES_TO]->(:Entity) WHERE id(r) = $relationship_id DELETE r"
            .to_string(),
        BTreeMap::from([("relationship_id".to_string(), Value::Int(relationship_id))]),
    ))
}

pub(super) fn node_has_external_id_property(node: &NodeRecord, external_id: &str) -> bool {
    node.properties
        .get("id")
        .is_some_and(|value| value_to_external_id(value) == external_id)
}

pub(super) fn validate_cypher_identifier(value: &str, kind: &str) -> Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(SkeinError::Semantic(format!("{kind} identifier is empty")));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(SkeinError::Semantic(format!(
            "{kind} identifier {value:?} must start with an ASCII letter or underscore"
        )));
    }
    if chars.any(|ch| !(ch == '_' || ch.is_ascii_alphanumeric())) {
        return Err(SkeinError::Semantic(format!(
            "{kind} identifier {value:?} must contain only ASCII letters, digits, or underscores"
        )));
    }
    Ok(())
}

}

#[cfg(test)]
use legacy_business_api::*;

#[cfg(test)]
fn knowledge_neighbors_via_query_runtime(
    db: &Database,
    request: &KnowledgeNeighborsRequest,
) -> Result<KnowledgeNeighborsOutput> {
    knowledge_scoped_neighbors_via_query_runtime(
        db,
        &KnowledgeScopedNeighborsRequest {
            navigation: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

#[cfg(test)]
fn knowledge_scoped_neighbors_via_query_runtime(
    db: &Database,
    request: &KnowledgeScopedNeighborsRequest,
) -> Result<KnowledgeNeighborsOutput> {
    let navigation = &request.navigation;
    let graph_commit_epoch = db.store.commit_epoch();
    let seed_request = KnowledgeEntityRequest {
        label: navigation.label.clone(),
        external_id: navigation.external_id.clone(),
    };
    let seed = knowledge_relationship_seed_via_query_runtime(db, &seed_request)?;
    let Some(seed) = seed else {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch,
            seed_found: false,
            target_found: None,
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: Some(knowledge_identity_description(
                navigation.label.as_str(),
                navigation.external_id.as_str(),
            )),
            missing_target_identity: None,
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: Some(navigation.limit),
            node_limit: None,
            relationship_limit: None,
        });
        attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
        return Ok(KnowledgeNeighborsOutput {
            graph_commit_epoch,
            seed_node_id: None,
            paths: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        });
    };
    if !request.metadata_filters.is_empty()
        && !knowledge_entity_matches_filters(&seed, &request.metadata_filters)
    {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch,
            seed_found: false,
            target_found: None,
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: None,
            missing_target_identity: None,
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: Some(navigation.limit),
            node_limit: None,
            relationship_limit: None,
        });
        attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 1);
        return Ok(KnowledgeNeighborsOutput {
            graph_commit_epoch,
            seed_node_id: Some(seed.node_id),
            paths: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        });
    }

    let relationship_type_name = match navigation.relationship_type.as_deref() {
        Some(name) => match db.catalog.rel_type_id(name) {
            Some(_) => {
                validate_cypher_identifier(name, "relationship type")?;
                Some(name.to_string())
            }
            None => {
                let mut diagnostics =
                    knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
                        graph_commit_epoch,
                        seed_found: true,
                        target_found: None,
                        path_count: 0,
                        node_count: 0,
                        relationship_count: 0,
                        fanout_reason_details: Vec::new(),
                        missing_seed_identity: None,
                        missing_target_identity: None,
                        missing_relationship_type: Some(name.to_string()),
                        max_hops: navigation.max_hops,
                        path_limit: Some(navigation.limit),
                        node_limit: None,
                        relationship_limit: None,
                    });
                attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
                return Ok(KnowledgeNeighborsOutput {
                    graph_commit_epoch,
                    seed_node_id: Some(seed.node_id),
                    paths: Vec::new(),
                    fanout_reason_codes: Vec::new(),
                    fanout_reason_details: Vec::new(),
                    fanout_reasons: Vec::new(),
                    diagnostics,
                });
            }
        },
        None => None,
    };

    let (paths, fanout_reason_details) = expand_knowledge_neighbors_via_query_runtime(
        db,
        "seed",
        NodeId(seed.node_id),
        relationship_type_name.as_deref(),
        navigation.direction,
        navigation.limit,
        navigation.max_hops,
    )?;
    let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
        graph_commit_epoch,
        seed_found: true,
        target_found: None,
        path_count: paths.len(),
        node_count: knowledge_context_path_node_count(&paths),
        relationship_count: paths.len(),
        fanout_reason_details: fanout_reason_details.clone(),
        missing_seed_identity: None,
        missing_target_identity: None,
        missing_relationship_type: None,
        max_hops: navigation.max_hops,
        path_limit: Some(navigation.limit),
        node_limit: None,
        relationship_limit: None,
    });
    attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
    Ok(KnowledgeNeighborsOutput {
        graph_commit_epoch,
        seed_node_id: Some(seed.node_id),
        paths,
        fanout_reason_codes: knowledge_fanout_reason_codes(&fanout_reason_details),
        fanout_reasons: knowledge_fanout_reason_messages(&fanout_reason_details),
        fanout_reason_details,
        diagnostics,
    })
}

#[cfg(test)]
fn expand_knowledge_neighbors_via_query_runtime(
    db: &Database,
    seed_hit_id: &str,
    seed_node_id: NodeId,
    relationship_type_name: Option<&str>,
    direction: KnowledgeNeighborDirection,
    limit: usize,
    max_hops: usize,
) -> Result<(
    Vec<KnowledgeGraphContextPath>,
    Vec<KnowledgeFanoutReasonDetail>,
)> {
    let mut paths = Vec::new();
    let mut fanout_reason_details = Vec::new();
    let mut seen_relationships = BTreeSet::new();
    let mut seen_frontier_nodes = BTreeSet::new();
    let mut frontier = VecDeque::from([(seed_node_id, 0usize)]);
    seen_frontier_nodes.insert(seed_node_id.0);

    while let Some((current_node, depth)) = frontier.pop_front() {
        if depth >= max_hops {
            continue;
        }
        let remaining_limit = limit.saturating_sub(paths.len());
        let (mut next_paths, mut next_fanout_reason_details) =
            knowledge_relationship_rows_via_query_runtime(
                db,
                seed_hit_id,
                current_node.0,
                relationship_type_name,
                direction,
                remaining_limit,
            )?;
        let reached_limit = next_fanout_reason_details
            .iter()
            .any(|reason| matches!(reason.code, KnowledgeFanoutReasonCode::PathLimitReached));
        fanout_reason_details.append(&mut next_fanout_reason_details);

        for mut path in next_paths.drain(..) {
            if !seen_relationships.insert(path.relationship_id) {
                continue;
            }
            if paths.len() >= limit {
                fanout_reason_details.push(KnowledgeFanoutReasonDetail::path_limit(
                    "knowledge_neighbors",
                    limit,
                    seed_hit_id,
                ));
                return Ok((paths, fanout_reason_details));
            }
            path.hop = depth + 1;
            let next_node_id = match path.direction {
                KnowledgeGraphPathDirection::Outgoing => path.target_node_id,
                KnowledgeGraphPathDirection::Incoming => path.source_node_id,
            };
            paths.push(path);
            if seen_frontier_nodes.insert(next_node_id) {
                frontier.push_back((NodeId(next_node_id), depth + 1));
            }
        }

        if reached_limit {
            return Ok((paths, fanout_reason_details));
        }
    }

    Ok((paths, fanout_reason_details))
}

#[cfg(test)]
fn knowledge_relationships_via_query_runtime(
    db: &Database,
    request: &KnowledgeRelationshipsRequest,
) -> Result<KnowledgeRelationshipsOutput> {
    let scoped_request = KnowledgeScopedRelationshipsRequest {
        relationships: request.clone(),
        metadata_filters: BTreeMap::new(),
    };
    knowledge_scoped_relationships_via_query_runtime(db, &scoped_request)
}

#[cfg(test)]
fn knowledge_scoped_relationships_via_query_runtime(
    db: &Database,
    request: &KnowledgeScopedRelationshipsRequest,
) -> Result<KnowledgeRelationshipsOutput> {
    let relationship_type_name = match request.relationships.relationship_type.as_deref() {
        Some(name) => match db.catalog.rel_type_id(name) {
            Some(_) => {
                validate_cypher_identifier(name, "relationship type")?;
                Some(name.to_string())
            }
            None => {
                return Ok(knowledge_empty_relationship_groups_for_missing_type(
                    &db.store, request,
                ));
            }
        },
        None => None,
    };

    let mut groups = Vec::with_capacity(request.relationships.seeds.len());
    let mut found_seed_count = 0;
    let mut missing_seed_count = 0;
    let mut filtered_out_seed_count = 0;
    let mut relationship_count = 0;
    for (index, seed_request) in request.relationships.seeds.iter().enumerate() {
        let seed = knowledge_relationship_seed_via_query_runtime(db, seed_request)?;
        let Some(seed) = seed else {
            missing_seed_count += 1;
            groups.push(knowledge_empty_relationship_group(
                seed_request,
                None,
                false,
            ));
            continue;
        };
        if !request.metadata_filters.is_empty()
            && !knowledge_entity_matches_filters(&seed, &request.metadata_filters)
        {
            filtered_out_seed_count += 1;
            groups.push(knowledge_empty_relationship_group(
                seed_request,
                Some(seed.node_id),
                true,
            ));
            continue;
        }

        found_seed_count += 1;
        let seed_hit_id = format!("seed_{index}");
        let (relationships, fanout_reason_details) = knowledge_relationship_rows_via_query_runtime(
            db,
            &seed_hit_id,
            seed.node_id,
            relationship_type_name.as_deref(),
            request.relationships.direction,
            request.relationships.limit_per_seed,
        )?;
        relationship_count += relationships.len();
        groups.push(KnowledgeRelationshipGroup {
            seed: seed_request.clone(),
            seed_node_id: Some(seed.node_id),
            filtered_out: false,
            relationships,
            fanout_reason_codes: knowledge_fanout_reason_codes(&fanout_reason_details),
            fanout_reasons: knowledge_fanout_reason_messages(&fanout_reason_details),
            fanout_reason_details,
        });
    }

    Ok(KnowledgeRelationshipsOutput {
        graph_commit_epoch: db.store.commit_epoch(),
        groups,
        relationship_type_found: true,
        found_seed_count,
        missing_seed_count,
        filtered_out_seed_count,
        relationship_count,
    })
}

#[cfg(test)]
fn knowledge_relationship_seed_via_query_runtime(
    db: &Database,
    seed: &KnowledgeEntityRequest,
) -> Result<Option<KnowledgeEntity>> {
    validate_cypher_identifier(&seed.label, "seed label")?;
    if db.catalog.label_id(&seed.label).is_none() {
        return Ok(None);
    }
    let query = format!(
        "MATCH (s:{}) RETURN s AS seed ORDER BY id(s) ASC",
        seed.label
    );
    let output = db.query_read_only_with_params_bounded(&query, &BTreeMap::new(), None)?;
    Ok(output
        .rows
        .iter()
        .filter_map(|row| row.get("seed").and_then(knowledge_entity_from_value))
        .find(|entity| entity.external_id.as_deref() == Some(seed.external_id.as_str())))
}

#[cfg(test)]
fn knowledge_empty_relationship_group(
    seed: &KnowledgeEntityRequest,
    seed_node_id: Option<u64>,
    filtered_out: bool,
) -> KnowledgeRelationshipGroup {
    KnowledgeRelationshipGroup {
        seed: seed.clone(),
        seed_node_id,
        filtered_out,
        relationships: Vec::new(),
        fanout_reason_codes: Vec::new(),
        fanout_reason_details: Vec::new(),
        fanout_reasons: Vec::new(),
    }
}

#[cfg(test)]
fn knowledge_relationship_rows_via_query_runtime(
    db: &Database,
    seed_hit_id: &str,
    seed_node_id: u64,
    relationship_type_name: Option<&str>,
    direction: KnowledgeNeighborDirection,
    limit: usize,
) -> Result<(
    Vec<KnowledgeGraphContextPath>,
    Vec<KnowledgeFanoutReasonDetail>,
)> {
    let mut fanout_reason_details = Vec::new();
    let relationship_type = relationship_type_name.and_then(|name| db.catalog.rel_type_id(name));
    record_dense_adjacency_diagnostics(
        DenseAdjacencyDiagnosticContext {
            catalog: &db.catalog,
            store: &db.store,
            operation: "knowledge_neighbors",
            relationship_type,
            requested_direction: direction,
        },
        NodeId(seed_node_id),
        &mut BTreeSet::new(),
        &mut fanout_reason_details,
    );

    let mut paths = Vec::new();
    let mut seen_relationships = BTreeSet::new();
    let rel_pattern = relationship_type_name
        .map(|name| format!(":{name}"))
        .unwrap_or_default();
    let parameters = BTreeMap::from([(
        "seed_node_id".to_string(),
        Value::Int(i64::try_from(seed_node_id).map_err(|_| {
            SkeinError::Execution("knowledge relationship seed node id exceeds i64".to_string())
        })?),
    )]);

    if matches!(
        direction,
        KnowledgeNeighborDirection::Outgoing | KnowledgeNeighborDirection::Both
    ) {
        let query = format!(
            "MATCH (source)-[r{rel_pattern}]->(target) \
             WHERE id(source) = $seed_node_id \
             RETURN source AS source, target AS target, r AS relationship, id(r) AS relationship_id \
             ORDER BY relationship_id ASC"
        );
        knowledge_relationship_rows_for_direction_via_query_runtime(
            KnowledgeRelationshipDirectionQuery {
                db,
                query: &query,
                parameters: &parameters,
                seed_hit_id,
                direction: KnowledgeGraphPathDirection::Outgoing,
                limit,
                seen_relationships: &mut seen_relationships,
                paths: &mut paths,
                fanout_reason_details: &mut fanout_reason_details,
            },
        )?;
    }
    if matches!(
        direction,
        KnowledgeNeighborDirection::Incoming | KnowledgeNeighborDirection::Both
    ) {
        let query = format!(
            "MATCH (source)-[r{rel_pattern}]->(target) \
             WHERE id(target) = $seed_node_id \
             RETURN source AS source, target AS target, r AS relationship, id(r) AS relationship_id \
             ORDER BY relationship_id ASC"
        );
        knowledge_relationship_rows_for_direction_via_query_runtime(
            KnowledgeRelationshipDirectionQuery {
                db,
                query: &query,
                parameters: &parameters,
                seed_hit_id,
                direction: KnowledgeGraphPathDirection::Incoming,
                limit,
                seen_relationships: &mut seen_relationships,
                paths: &mut paths,
                fanout_reason_details: &mut fanout_reason_details,
            },
        )?;
    }

    Ok((paths, fanout_reason_details))
}

#[cfg(test)]
struct KnowledgeRelationshipDirectionQuery<'a> {
    db: &'a Database,
    query: &'a str,
    parameters: &'a BTreeMap<String, Value>,
    seed_hit_id: &'a str,
    direction: KnowledgeGraphPathDirection,
    limit: usize,
    seen_relationships: &'a mut BTreeSet<u64>,
    paths: &'a mut Vec<KnowledgeGraphContextPath>,
    fanout_reason_details: &'a mut Vec<KnowledgeFanoutReasonDetail>,
}

#[cfg(test)]
fn knowledge_relationship_rows_for_direction_via_query_runtime(
    context: KnowledgeRelationshipDirectionQuery<'_>,
) -> Result<()> {
    let output =
        context
            .db
            .query_read_only_with_params_bounded(context.query, context.parameters, None)?;
    for row in &output.rows {
        let relationship_id = row
            .get("relationship_id")
            .and_then(value_to_non_negative_u64)
            .ok_or_else(|| {
                SkeinError::Execution(
                    "knowledge relationship row is missing relationship_id".to_string(),
                )
            })?;
        if !context.seen_relationships.insert(relationship_id) {
            continue;
        }
        if context.paths.len() >= context.limit {
            context
                .fanout_reason_details
                .push(KnowledgeFanoutReasonDetail::path_limit(
                    "knowledge_neighbors",
                    context.limit,
                    context.seed_hit_id,
                ));
            return Ok(());
        }
        context.paths.push(knowledge_context_path_from_query_row(
            row,
            context.seed_hit_id,
            context.direction,
            relationship_id,
        )?);
    }
    Ok(())
}

#[cfg(test)]
fn knowledge_context_path_from_query_row(
    row: impl QueryRowLookup,
    seed_hit_id: &str,
    direction: KnowledgeGraphPathDirection,
    relationship_id: u64,
) -> Result<KnowledgeGraphContextPath> {
    let source = row
        .get("source")
        .and_then(knowledge_entity_from_value)
        .ok_or_else(|| {
            SkeinError::Execution("knowledge relationship row is missing source map".to_string())
        })?;
    let target = row
        .get("target")
        .and_then(knowledge_entity_from_value)
        .ok_or_else(|| {
            SkeinError::Execution("knowledge relationship row is missing target map".to_string())
        })?;
    let relationship = row
        .get("relationship")
        .and_then(value_to_map)
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge relationship row is missing relationship map".to_string(),
            )
        })?;
    let relationship_type = relationship
        .get("type")
        .map(value_to_external_id)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "<unknown>".to_string());
    let mut relationship_properties = relationship.clone();
    relationship_properties.remove("_id");
    relationship_properties.remove("source_id");
    relationship_properties.remove("target_id");
    relationship_properties.remove("type");

    Ok(KnowledgeGraphContextPath {
        seed_hit_id: seed_hit_id.to_string(),
        hop: 1,
        direction,
        relationship_id,
        relationship_type,
        relationship_properties,
        source_node_id: source.node_id,
        source_labels: source.labels,
        source_external_id: source.external_id,
        target_node_id: target.node_id,
        target_labels: target.labels,
        target_external_id: target.external_id,
    })
}

#[cfg(test)]
fn knowledge_empty_relationship_groups_for_missing_type(
    store: &GraphStore,
    request: &KnowledgeScopedRelationshipsRequest,
) -> KnowledgeRelationshipsOutput {
    KnowledgeRelationshipsOutput {
        graph_commit_epoch: store.commit_epoch(),
        groups: request
            .relationships
            .seeds
            .iter()
            .cloned()
            .map(|seed| KnowledgeRelationshipGroup {
                seed,
                seed_node_id: None,
                filtered_out: false,
                relationships: Vec::new(),
                fanout_reason_codes: Vec::new(),
                fanout_reason_details: Vec::new(),
                fanout_reasons: Vec::new(),
            })
            .collect(),
        relationship_type_found: false,
        found_seed_count: 0,
        missing_seed_count: 0,
        filtered_out_seed_count: 0,
        relationship_count: 0,
    }
}

#[cfg(test)]
fn knowledge_paths_via_query_runtime(
    db: &Database,
    request: &KnowledgePathRequest,
) -> Result<KnowledgePathOutput> {
    knowledge_scoped_paths_via_query_runtime(
        db,
        &KnowledgeScopedPathRequest {
            navigation: request.clone(),
            source_metadata_filters: BTreeMap::new(),
            target_metadata_filters: BTreeMap::new(),
        },
    )
}

#[cfg(test)]
fn knowledge_scoped_paths_via_query_runtime(
    db: &Database,
    request: &KnowledgeScopedPathRequest,
) -> Result<KnowledgePathOutput> {
    let navigation = &request.navigation;

    let graph_commit_epoch = db.store.commit_epoch();
    let source_request = KnowledgeEntityRequest {
        label: navigation.source_label.clone(),
        external_id: navigation.source_external_id.clone(),
    };
    let target_request = KnowledgeEntityRequest {
        label: navigation.target_label.clone(),
        external_id: navigation.target_external_id.clone(),
    };
    let source = knowledge_relationship_seed_via_query_runtime(db, &source_request)?;
    let target = knowledge_relationship_seed_via_query_runtime(db, &target_request)?;
    let source_node_id = source.as_ref().map(|entity| entity.node_id);
    let target_node_id = target.as_ref().map(|entity| entity.node_id);

    let Some(source) = source else {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch,
            seed_found: false,
            target_found: Some(target_node_id.is_some()),
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: Some(knowledge_identity_description(
                navigation.source_label.as_str(),
                navigation.source_external_id.as_str(),
            )),
            missing_target_identity: target_node_id.is_none().then(|| {
                knowledge_identity_description(
                    navigation.target_label.as_str(),
                    navigation.target_external_id.as_str(),
                )
            }),
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: Some(navigation.limit),
            node_limit: None,
            relationship_limit: None,
        });
        attach_path_endpoint_metadata_filters(
            &mut diagnostics,
            &request.source_metadata_filters,
            &request.target_metadata_filters,
            0,
        );
        return Ok(KnowledgePathOutput {
            graph_commit_epoch,
            source_node_id,
            target_node_id,
            paths: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        });
    };
    let Some(target) = target else {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch,
            seed_found: true,
            target_found: Some(false),
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: None,
            missing_target_identity: Some(knowledge_identity_description(
                navigation.target_label.as_str(),
                navigation.target_external_id.as_str(),
            )),
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: Some(navigation.limit),
            node_limit: None,
            relationship_limit: None,
        });
        attach_path_endpoint_metadata_filters(
            &mut diagnostics,
            &request.source_metadata_filters,
            &request.target_metadata_filters,
            0,
        );
        return Ok(KnowledgePathOutput {
            graph_commit_epoch,
            source_node_id: Some(source.node_id),
            target_node_id,
            paths: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        });
    };

    let source_filtered = !request.source_metadata_filters.is_empty()
        && !knowledge_entity_matches_filters(&source, &request.source_metadata_filters);
    let target_filtered = !request.target_metadata_filters.is_empty()
        && !knowledge_entity_matches_filters(&target, &request.target_metadata_filters);
    if source_filtered || target_filtered {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch,
            seed_found: !source_filtered,
            target_found: Some(!target_filtered),
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: None,
            missing_target_identity: None,
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: Some(navigation.limit),
            node_limit: None,
            relationship_limit: None,
        });
        attach_path_endpoint_metadata_filters(
            &mut diagnostics,
            &request.source_metadata_filters,
            &request.target_metadata_filters,
            usize::from(source_filtered) + usize::from(target_filtered),
        );
        return Ok(KnowledgePathOutput {
            graph_commit_epoch,
            source_node_id: Some(source.node_id),
            target_node_id: Some(target.node_id),
            paths: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        });
    }

    let relationship_type_name = match navigation.relationship_type.as_deref() {
        Some(name) => match db.catalog.rel_type_id(name) {
            Some(_) => {
                validate_cypher_identifier(name, "relationship type")?;
                Some(name.to_string())
            }
            None => {
                let mut diagnostics =
                    knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
                        graph_commit_epoch,
                        seed_found: true,
                        target_found: Some(true),
                        path_count: 0,
                        node_count: 0,
                        relationship_count: 0,
                        fanout_reason_details: Vec::new(),
                        missing_seed_identity: None,
                        missing_target_identity: None,
                        missing_relationship_type: Some(name.to_string()),
                        max_hops: navigation.max_hops,
                        path_limit: Some(navigation.limit),
                        node_limit: None,
                        relationship_limit: None,
                    });
                attach_path_endpoint_metadata_filters(
                    &mut diagnostics,
                    &request.source_metadata_filters,
                    &request.target_metadata_filters,
                    0,
                );
                return Ok(KnowledgePathOutput {
                    graph_commit_epoch,
                    source_node_id: Some(source.node_id),
                    target_node_id: Some(target.node_id),
                    paths: Vec::new(),
                    fanout_reason_codes: Vec::new(),
                    fanout_reason_details: Vec::new(),
                    fanout_reasons: Vec::new(),
                    diagnostics,
                });
            }
        },
        None => None,
    };
    let (paths, fanout_reason_details) = expand_knowledge_paths_via_query_runtime(
        db,
        source.node_id,
        target.node_id,
        relationship_type_name.as_deref(),
        navigation.direction,
        navigation.max_hops,
        navigation.limit,
    )?;
    let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
        graph_commit_epoch,
        seed_found: true,
        target_found: Some(true),
        path_count: paths.len(),
        node_count: knowledge_graph_path_node_count(&paths),
        relationship_count: paths.iter().map(|path| path.segments.len()).sum::<usize>(),
        fanout_reason_details: fanout_reason_details.clone(),
        missing_seed_identity: None,
        missing_target_identity: None,
        missing_relationship_type: None,
        max_hops: navigation.max_hops,
        path_limit: Some(navigation.limit),
        node_limit: None,
        relationship_limit: None,
    });
    attach_path_endpoint_metadata_filters(
        &mut diagnostics,
        &request.source_metadata_filters,
        &request.target_metadata_filters,
        0,
    );
    Ok(KnowledgePathOutput {
        graph_commit_epoch,
        source_node_id: Some(source.node_id),
        target_node_id: Some(target.node_id),
        diagnostics,
        paths,
        fanout_reason_codes: knowledge_fanout_reason_codes(&fanout_reason_details),
        fanout_reasons: knowledge_fanout_reason_messages(&fanout_reason_details),
        fanout_reason_details,
    })
}

#[cfg(test)]
fn expand_knowledge_paths_via_query_runtime(
    db: &Database,
    source_node_id: u64,
    target_node_id: u64,
    relationship_type_name: Option<&str>,
    direction: KnowledgeNeighborDirection,
    max_hops: usize,
    limit: usize,
) -> Result<(Vec<KnowledgeGraphPath>, Vec<KnowledgeFanoutReasonDetail>)> {
    let relationship_type = relationship_type_name.and_then(|name| db.catalog.rel_type_id(name));
    let mut paths = Vec::new();
    let mut fanout_reason_details = Vec::new();
    let mut reported_dense_groups = BTreeSet::new();
    let mut frontier = VecDeque::from([(
        NodeId(source_node_id),
        Vec::<KnowledgeGraphContextPath>::new(),
        BTreeSet::from([source_node_id]),
    )]);

    while let Some((current_node, current_path, visited_nodes)) = frontier.pop_front() {
        if current_path.len() >= max_hops {
            continue;
        }
        record_dense_adjacency_diagnostics(
            DenseAdjacencyDiagnosticContext {
                catalog: &db.catalog,
                store: &db.store,
                operation: "knowledge_paths",
                relationship_type,
                requested_direction: direction,
            },
            current_node,
            &mut reported_dense_groups,
            &mut fanout_reason_details,
        );
        for mut segment in knowledge_path_segments_from_node_via_query_runtime(
            db,
            current_node.0,
            relationship_type_name,
            direction,
        )? {
            let next_node_id = match segment.direction {
                KnowledgeGraphPathDirection::Outgoing => segment.target_node_id,
                KnowledgeGraphPathDirection::Incoming => segment.source_node_id,
            };
            if visited_nodes.contains(&next_node_id) && next_node_id != target_node_id {
                continue;
            }
            segment.hop = current_path.len() + 1;
            let mut next_path = current_path.clone();
            next_path.push(segment);
            if next_node_id == target_node_id {
                if paths.len() >= limit {
                    fanout_reason_details.push(KnowledgeFanoutReasonDetail::path_limit(
                        "knowledge_paths",
                        limit,
                        "path",
                    ));
                    return Ok((paths, fanout_reason_details));
                }
                paths.push(KnowledgeGraphPath {
                    segments: next_path,
                });
                continue;
            }
            let mut next_visited = visited_nodes.clone();
            next_visited.insert(next_node_id);
            frontier.push_back((NodeId(next_node_id), next_path, next_visited));
        }
    }

    Ok((paths, fanout_reason_details))
}

#[cfg(test)]
fn knowledge_path_segments_from_node_via_query_runtime(
    db: &Database,
    current_node_id: u64,
    relationship_type_name: Option<&str>,
    direction: KnowledgeNeighborDirection,
) -> Result<Vec<KnowledgeGraphContextPath>> {
    let rel_pattern = relationship_type_name
        .map(|name| format!(":{name}"))
        .unwrap_or_default();
    let parameters = BTreeMap::from([(
        "current_node_id".to_string(),
        Value::Int(i64::try_from(current_node_id).map_err(|_| {
            SkeinError::Execution("knowledge path frontier node id exceeds i64".to_string())
        })?),
    )]);
    let mut segments = Vec::new();
    let mut seen_relationships = BTreeSet::new();

    if matches!(
        direction,
        KnowledgeNeighborDirection::Outgoing | KnowledgeNeighborDirection::Both
    ) {
        let query = format!(
            "MATCH (source)-[r{rel_pattern}]->(target) \
             WHERE id(source) = $current_node_id \
             RETURN source AS source, target AS target, r AS relationship, id(r) AS relationship_id \
             ORDER BY relationship_id ASC"
        );
        knowledge_path_segments_for_direction_via_query_runtime(
            db,
            &query,
            &parameters,
            KnowledgeGraphPathDirection::Outgoing,
            &mut seen_relationships,
            &mut segments,
        )?;
    }
    if matches!(
        direction,
        KnowledgeNeighborDirection::Incoming | KnowledgeNeighborDirection::Both
    ) {
        let query = format!(
            "MATCH (source)-[r{rel_pattern}]->(target) \
             WHERE id(target) = $current_node_id \
             RETURN source AS source, target AS target, r AS relationship, id(r) AS relationship_id \
             ORDER BY relationship_id ASC"
        );
        knowledge_path_segments_for_direction_via_query_runtime(
            db,
            &query,
            &parameters,
            KnowledgeGraphPathDirection::Incoming,
            &mut seen_relationships,
            &mut segments,
        )?;
    }

    Ok(segments)
}

#[cfg(test)]
fn knowledge_path_segments_for_direction_via_query_runtime(
    db: &Database,
    query: &str,
    parameters: &BTreeMap<String, Value>,
    direction: KnowledgeGraphPathDirection,
    seen_relationships: &mut BTreeSet<u64>,
    segments: &mut Vec<KnowledgeGraphContextPath>,
) -> Result<()> {
    let output = db.query_read_only_with_params_bounded(query, parameters, None)?;
    for row in &output.rows {
        let relationship_id = row
            .get("relationship_id")
            .and_then(value_to_non_negative_u64)
            .ok_or_else(|| {
                SkeinError::Execution("knowledge path row is missing relationship_id".to_string())
            })?;
        if !seen_relationships.insert(relationship_id) {
            continue;
        }
        segments.push(knowledge_context_path_from_query_row(
            row,
            "path",
            direction,
            relationship_id,
        )?);
    }
    Ok(())
}

#[cfg(test)]
fn knowledge_subgraph_via_query_runtime(
    db: &Database,
    request: &KnowledgeSubgraphRequest,
) -> Result<KnowledgeSubgraphOutput> {
    knowledge_scoped_subgraph_via_query_runtime(
        db,
        &KnowledgeScopedSubgraphRequest {
            navigation: request.clone(),
            metadata_filters: BTreeMap::new(),
        },
    )
}

#[cfg(test)]
fn knowledge_scoped_subgraph_via_query_runtime(
    db: &Database,
    request: &KnowledgeScopedSubgraphRequest,
) -> Result<KnowledgeSubgraphOutput> {
    let navigation = &request.navigation;

    let graph_commit_epoch = db.store.commit_epoch();
    let seed_request = KnowledgeEntityRequest {
        label: navigation.label.clone(),
        external_id: navigation.external_id.clone(),
    };
    let seed = knowledge_relationship_seed_via_query_runtime(db, &seed_request)?;
    let Some(seed) = seed else {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch,
            seed_found: false,
            target_found: None,
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: Some(knowledge_identity_description(
                navigation.label.as_str(),
                navigation.external_id.as_str(),
            )),
            missing_target_identity: None,
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: None,
            node_limit: Some(navigation.node_limit),
            relationship_limit: Some(navigation.relationship_limit),
        });
        attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
        return Ok(KnowledgeSubgraphOutput {
            graph_commit_epoch,
            seed_node_id: None,
            nodes: Vec::new(),
            relationships: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        });
    };
    if !request.metadata_filters.is_empty()
        && !knowledge_entity_matches_filters(&seed, &request.metadata_filters)
    {
        let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
            graph_commit_epoch,
            seed_found: false,
            target_found: None,
            path_count: 0,
            node_count: 0,
            relationship_count: 0,
            fanout_reason_details: Vec::new(),
            missing_seed_identity: None,
            missing_target_identity: None,
            missing_relationship_type: None,
            max_hops: navigation.max_hops,
            path_limit: None,
            node_limit: Some(navigation.node_limit),
            relationship_limit: Some(navigation.relationship_limit),
        });
        attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 1);
        return Ok(KnowledgeSubgraphOutput {
            graph_commit_epoch,
            seed_node_id: Some(seed.node_id),
            nodes: Vec::new(),
            relationships: Vec::new(),
            fanout_reason_codes: Vec::new(),
            fanout_reason_details: Vec::new(),
            fanout_reasons: Vec::new(),
            diagnostics,
        });
    }

    let relationship_type_name = match navigation.relationship_type.as_deref() {
        Some(name) => match db.catalog.rel_type_id(name) {
            Some(_) => {
                validate_cypher_identifier(name, "relationship type")?;
                Some(name.to_string())
            }
            None => {
                let mut diagnostics =
                    knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
                        graph_commit_epoch,
                        seed_found: true,
                        target_found: None,
                        path_count: 0,
                        node_count: 0,
                        relationship_count: 0,
                        fanout_reason_details: Vec::new(),
                        missing_seed_identity: None,
                        missing_target_identity: None,
                        missing_relationship_type: Some(name.to_string()),
                        max_hops: navigation.max_hops,
                        path_limit: None,
                        node_limit: Some(navigation.node_limit),
                        relationship_limit: Some(navigation.relationship_limit),
                    });
                attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
                return Ok(KnowledgeSubgraphOutput {
                    graph_commit_epoch,
                    seed_node_id: Some(seed.node_id),
                    nodes: Vec::new(),
                    relationships: Vec::new(),
                    fanout_reason_codes: Vec::new(),
                    fanout_reason_details: Vec::new(),
                    fanout_reasons: Vec::new(),
                    diagnostics,
                });
            }
        },
        None => None,
    };

    let (nodes, relationships, fanout_reason_details) =
        expand_knowledge_subgraph_via_query_runtime(
            db,
            &seed,
            relationship_type_name.as_deref(),
            navigation.direction,
            navigation.max_hops,
            navigation.node_limit,
            navigation.relationship_limit,
        )?;
    let mut diagnostics = knowledge_traversal_diagnostics(KnowledgeTraversalDiagnosticInput {
        graph_commit_epoch,
        seed_found: true,
        target_found: None,
        path_count: relationships.len(),
        node_count: nodes.len(),
        relationship_count: relationships.len(),
        fanout_reason_details: fanout_reason_details.clone(),
        missing_seed_identity: None,
        missing_target_identity: None,
        missing_relationship_type: None,
        max_hops: navigation.max_hops,
        path_limit: None,
        node_limit: Some(navigation.node_limit),
        relationship_limit: Some(navigation.relationship_limit),
    });
    attach_traversal_metadata_filters(&mut diagnostics, &request.metadata_filters, 0);
    Ok(KnowledgeSubgraphOutput {
        graph_commit_epoch,
        seed_node_id: Some(seed.node_id),
        diagnostics,
        nodes,
        relationships,
        fanout_reason_codes: knowledge_fanout_reason_codes(&fanout_reason_details),
        fanout_reasons: knowledge_fanout_reason_messages(&fanout_reason_details),
        fanout_reason_details,
    })
}

#[cfg(test)]
fn expand_knowledge_subgraph_via_query_runtime(
    db: &Database,
    seed: &KnowledgeEntity,
    relationship_type_name: Option<&str>,
    direction: KnowledgeNeighborDirection,
    max_hops: usize,
    node_limit: usize,
    relationship_limit: usize,
) -> Result<(
    Vec<KnowledgeEntity>,
    Vec<KnowledgeGraphContextPath>,
    Vec<KnowledgeFanoutReasonDetail>,
)> {
    let mut nodes = Vec::new();
    let mut relationships = Vec::new();
    let mut fanout_reason_details = Vec::new();
    let mut seen_nodes = BTreeSet::new();
    let mut seen_relationships = BTreeSet::new();
    let mut reported_dense_groups = BTreeSet::new();
    let mut frontier = VecDeque::from([(NodeId(seed.node_id), 0usize)]);

    if node_limit == 0 {
        fanout_reason_details.push(KnowledgeFanoutReasonDetail::node_limit(0));
        return Ok((nodes, relationships, fanout_reason_details));
    }
    nodes.push(seed.clone());
    seen_nodes.insert(seed.node_id);

    let relationship_type = relationship_type_name.and_then(|name| db.catalog.rel_type_id(name));

    while let Some((current_node, depth)) = frontier.pop_front() {
        if depth >= max_hops {
            continue;
        }
        record_dense_adjacency_diagnostics(
            DenseAdjacencyDiagnosticContext {
                catalog: &db.catalog,
                store: &db.store,
                operation: "knowledge_subgraph",
                relationship_type,
                requested_direction: direction,
            },
            current_node,
            &mut reported_dense_groups,
            &mut fanout_reason_details,
        );
        if expand_knowledge_subgraph_node_via_query_runtime(ExpandKnowledgeSubgraphNodeQuery {
            db,
            current_node,
            relationship_type_name,
            direction,
            next_depth: depth + 1,
            max_hops,
            node_limit,
            relationship_limit,
            seen_nodes: &mut seen_nodes,
            seen_relationships: &mut seen_relationships,
            frontier: &mut frontier,
            nodes: &mut nodes,
            relationships: &mut relationships,
            fanout_reason_details: &mut fanout_reason_details,
        })? {
            break;
        }
    }

    Ok((nodes, relationships, fanout_reason_details))
}

#[derive(Debug, Clone, Copy)]
#[cfg(test)]
enum KnowledgeSubgraphNextEndpoint {
    Source,
    Target,
}

#[cfg(test)]
struct ExpandKnowledgeSubgraphNodeQuery<'a> {
    db: &'a Database,
    current_node: NodeId,
    relationship_type_name: Option<&'a str>,
    direction: KnowledgeNeighborDirection,
    next_depth: usize,
    max_hops: usize,
    node_limit: usize,
    relationship_limit: usize,
    seen_nodes: &'a mut BTreeSet<u64>,
    seen_relationships: &'a mut BTreeSet<u64>,
    frontier: &'a mut VecDeque<(NodeId, usize)>,
    nodes: &'a mut Vec<KnowledgeEntity>,
    relationships: &'a mut Vec<KnowledgeGraphContextPath>,
    fanout_reason_details: &'a mut Vec<KnowledgeFanoutReasonDetail>,
}

#[cfg(test)]
struct KnowledgeSubgraphDirectionQuery<'a> {
    db: &'a Database,
    query: &'a str,
    parameters: &'a BTreeMap<String, Value>,
    direction: KnowledgeGraphPathDirection,
    next_endpoint: KnowledgeSubgraphNextEndpoint,
    next_depth: usize,
    max_hops: usize,
    node_limit: usize,
    relationship_limit: usize,
    seen_nodes: &'a mut BTreeSet<u64>,
    seen_relationships: &'a mut BTreeSet<u64>,
    frontier: &'a mut VecDeque<(NodeId, usize)>,
    nodes: &'a mut Vec<KnowledgeEntity>,
    relationships: &'a mut Vec<KnowledgeGraphContextPath>,
    fanout_reason_details: &'a mut Vec<KnowledgeFanoutReasonDetail>,
}

#[cfg(test)]
fn expand_knowledge_subgraph_node_via_query_runtime(
    context: ExpandKnowledgeSubgraphNodeQuery<'_>,
) -> Result<bool> {
    let rel_pattern = context
        .relationship_type_name
        .map(|name| format!(":{name}"))
        .unwrap_or_default();
    let parameters = BTreeMap::from([(
        "current_node_id".to_string(),
        Value::Int(i64::try_from(context.current_node.0).map_err(|_| {
            SkeinError::Execution("knowledge subgraph frontier node id exceeds i64".to_string())
        })?),
    )]);

    if matches!(
        context.direction,
        KnowledgeNeighborDirection::Outgoing | KnowledgeNeighborDirection::Both
    ) {
        let query = format!(
            "MATCH (source)-[r{rel_pattern}]->(target) \
             WHERE id(source) = $current_node_id \
             RETURN source AS source, target AS target, r AS relationship, id(r) AS relationship_id \
             ORDER BY relationship_id ASC"
        );
        if knowledge_subgraph_for_direction_via_query_runtime(KnowledgeSubgraphDirectionQuery {
            db: context.db,
            query: &query,
            parameters: &parameters,
            direction: KnowledgeGraphPathDirection::Outgoing,
            next_endpoint: KnowledgeSubgraphNextEndpoint::Target,
            next_depth: context.next_depth,
            max_hops: context.max_hops,
            node_limit: context.node_limit,
            relationship_limit: context.relationship_limit,
            seen_nodes: context.seen_nodes,
            seen_relationships: context.seen_relationships,
            frontier: context.frontier,
            nodes: context.nodes,
            relationships: context.relationships,
            fanout_reason_details: context.fanout_reason_details,
        })? {
            return Ok(true);
        }
    }
    if matches!(
        context.direction,
        KnowledgeNeighborDirection::Incoming | KnowledgeNeighborDirection::Both
    ) {
        let query = format!(
            "MATCH (source)-[r{rel_pattern}]->(target) \
             WHERE id(target) = $current_node_id \
             RETURN source AS source, target AS target, r AS relationship, id(r) AS relationship_id \
             ORDER BY relationship_id ASC"
        );
        if knowledge_subgraph_for_direction_via_query_runtime(KnowledgeSubgraphDirectionQuery {
            db: context.db,
            query: &query,
            parameters: &parameters,
            direction: KnowledgeGraphPathDirection::Incoming,
            next_endpoint: KnowledgeSubgraphNextEndpoint::Source,
            next_depth: context.next_depth,
            max_hops: context.max_hops,
            node_limit: context.node_limit,
            relationship_limit: context.relationship_limit,
            seen_nodes: context.seen_nodes,
            seen_relationships: context.seen_relationships,
            frontier: context.frontier,
            nodes: context.nodes,
            relationships: context.relationships,
            fanout_reason_details: context.fanout_reason_details,
        })? {
            return Ok(true);
        }
    }

    Ok(false)
}

#[cfg(test)]
fn knowledge_subgraph_for_direction_via_query_runtime(
    context: KnowledgeSubgraphDirectionQuery<'_>,
) -> Result<bool> {
    let output =
        context
            .db
            .query_read_only_with_params_bounded(context.query, context.parameters, None)?;
    for row in &output.rows {
        let relationship_id = row
            .get("relationship_id")
            .and_then(value_to_non_negative_u64)
            .ok_or_else(|| {
                SkeinError::Execution(
                    "knowledge subgraph row is missing relationship_id".to_string(),
                )
            })?;
        if !context.seen_relationships.insert(relationship_id) {
            continue;
        }
        let next_node = match context.next_endpoint {
            KnowledgeSubgraphNextEndpoint::Source => {
                row.get("source").and_then(knowledge_entity_from_value)
            }
            KnowledgeSubgraphNextEndpoint::Target => {
                row.get("target").and_then(knowledge_entity_from_value)
            }
        }
        .ok_or_else(|| {
            SkeinError::Execution("knowledge subgraph row is missing next node".to_string())
        })?;
        let new_node = !context.seen_nodes.contains(&next_node.node_id);
        if new_node && context.nodes.len() >= context.node_limit {
            context
                .fanout_reason_details
                .push(KnowledgeFanoutReasonDetail::node_limit(context.node_limit));
            return Ok(true);
        }
        if context.relationships.len() >= context.relationship_limit {
            context
                .fanout_reason_details
                .push(KnowledgeFanoutReasonDetail::relationship_limit(
                    context.relationship_limit,
                ));
            return Ok(true);
        }
        let mut relationship = knowledge_context_path_from_query_row(
            row,
            "subgraph",
            context.direction,
            relationship_id,
        )?;
        relationship.hop = context.next_depth;
        context.relationships.push(relationship);
        if new_node && context.seen_nodes.insert(next_node.node_id) {
            let next_node_id = NodeId(next_node.node_id);
            context.nodes.push(next_node);
            if context.next_depth < context.max_hops {
                context
                    .frontier
                    .push_back((next_node_id, context.next_depth));
            }
        }
    }
    Ok(false)
}

#[cfg(test)]
struct KnowledgeTraversalDiagnosticInput {
    graph_commit_epoch: u64,
    seed_found: bool,
    target_found: Option<bool>,
    path_count: usize,
    node_count: usize,
    relationship_count: usize,
    fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    missing_seed_identity: Option<String>,
    missing_target_identity: Option<String>,
    missing_relationship_type: Option<String>,
    max_hops: usize,
    path_limit: Option<usize>,
    node_limit: Option<usize>,
    relationship_limit: Option<usize>,
}

#[cfg(test)]
fn knowledge_traversal_diagnostics(
    input: KnowledgeTraversalDiagnosticInput,
) -> KnowledgeTraversalDiagnostics {
    let fallback_reason_codes = knowledge_traversal_fallback_reason_codes(&input);
    let fallback_reasons = knowledge_traversal_fallback_reasons(&input);
    let input_candidate_set = knowledge_traversal_input_candidate_set_report(&input);
    let candidate_set = knowledge_traversal_candidate_set_report(&input);
    KnowledgeTraversalDiagnostics {
        seed_found: input.seed_found,
        target_found: input.target_found,
        input_candidate_set,
        candidate_set,
        path_count: input.path_count,
        node_count: input.node_count,
        relationship_count: input.relationship_count,
        fanout_reason_count: input.fanout_reason_details.len(),
        fanout_reason_codes: knowledge_fanout_reason_codes(&input.fanout_reason_details),
        fanout_reasons: knowledge_fanout_reason_messages(&input.fanout_reason_details),
        fanout_reason_details: input.fanout_reason_details,
        fallback_reason_codes,
        fallback_reasons,
        max_hops: input.max_hops,
        path_limit: input.path_limit,
        node_limit: input.node_limit,
        relationship_limit: input.relationship_limit,
    }
}

#[cfg(test)]
fn attach_traversal_metadata_filters(
    diagnostics: &mut KnowledgeTraversalDiagnostics,
    metadata_filters: &BTreeMap<String, String>,
    filtered_out_count: usize,
) {
    if metadata_filters.is_empty() && filtered_out_count == 0 {
        return;
    }
    diagnostics.input_candidate_set.metadata_filters = metadata_filters.clone();
    diagnostics.input_candidate_set.filtered_out_count = filtered_out_count;
}

#[cfg(test)]
fn attach_path_endpoint_metadata_filters(
    diagnostics: &mut KnowledgeTraversalDiagnostics,
    source_metadata_filters: &BTreeMap<String, String>,
    target_metadata_filters: &BTreeMap<String, String>,
    filtered_out_count: usize,
) {
    if source_metadata_filters.is_empty()
        && target_metadata_filters.is_empty()
        && filtered_out_count == 0
    {
        return;
    }
    diagnostics.input_candidate_set.metadata_filters =
        prefixed_path_endpoint_metadata_filters(source_metadata_filters, target_metadata_filters);
    diagnostics.input_candidate_set.filtered_out_count = filtered_out_count;
}

#[cfg(test)]
fn prefixed_path_endpoint_metadata_filters(
    source_metadata_filters: &BTreeMap<String, String>,
    target_metadata_filters: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    source_metadata_filters
        .iter()
        .map(|(key, value)| (format!("source.{key}"), value.clone()))
        .chain(
            target_metadata_filters
                .iter()
                .map(|(key, value)| (format!("target.{key}"), value.clone())),
        )
        .collect()
}

#[cfg(test)]
fn knowledge_traversal_input_candidate_set_report(
    input: &KnowledgeTraversalDiagnosticInput,
) -> SearchCandidateSetReport {
    let cardinality = match input.target_found {
        Some(target_found) => usize::from(input.seed_found) + usize::from(target_found),
        None => usize::from(input.seed_found),
    };
    SearchCandidateSetReport {
        id_space: "canonical_graph_node_id".to_string(),
        representation: "traversal_seed_node_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(input.graph_commit_epoch),
        policy_epoch: None,
        filtered_out_count: 0,
        metadata_filters: BTreeMap::new(),
        metadata_predicate_pushdown: SearchPredicatePushdownReport::default(),
    }
}

#[cfg(test)]
fn knowledge_traversal_candidate_set_report(
    input: &KnowledgeTraversalDiagnosticInput,
) -> SearchRetrieverCandidateSetReport {
    let (id_space, representation, cardinality) =
        if input.node_limit.is_some() || input.relationship_limit.is_some() {
            (
                "mixed_graph_id",
                "subgraph_node_and_relationship_ids",
                input.node_count + input.relationship_count,
            )
        } else if input.target_found.is_some() {
            ("canonical_graph_path", "bounded_paths", input.path_count)
        } else {
            (
                "canonical_graph_relationship_id",
                "neighbor_relationship_ids",
                input.relationship_count,
            )
        };
    SearchRetrieverCandidateSetReport {
        id_space: id_space.to_string(),
        representation: representation.to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(input.graph_commit_epoch),
        policy_epoch: None,
    }
}

#[cfg(test)]
fn knowledge_traversal_fallback_reason_codes(
    input: &KnowledgeTraversalDiagnosticInput,
) -> Vec<KnowledgeTraversalFallbackReasonCode> {
    let mut codes = Vec::new();
    if input.missing_seed_identity.is_some() {
        codes.push(KnowledgeTraversalFallbackReasonCode::SeedNotFound);
    }
    if input.missing_target_identity.is_some() {
        codes.push(KnowledgeTraversalFallbackReasonCode::TargetNotFound);
    }
    if input.max_hops == 0 {
        codes.push(KnowledgeTraversalFallbackReasonCode::MaxHopsZero);
    }
    if input.path_limit == Some(0) {
        codes.push(KnowledgeTraversalFallbackReasonCode::PathLimitZero);
    }
    if input.node_limit == Some(0) {
        codes.push(KnowledgeTraversalFallbackReasonCode::NodeLimitZero);
    }
    if input.relationship_limit == Some(0) {
        codes.push(KnowledgeTraversalFallbackReasonCode::RelationshipLimitZero);
    }
    if input.missing_relationship_type.is_some() {
        codes.push(KnowledgeTraversalFallbackReasonCode::RelationshipTypeNotFound);
    }
    codes
}

#[cfg(test)]
fn knowledge_traversal_fallback_reasons(input: &KnowledgeTraversalDiagnosticInput) -> Vec<String> {
    let mut reasons = Vec::new();
    if let Some(seed_identity) = &input.missing_seed_identity {
        reasons.push(format!("seed {seed_identity} not found"));
    }
    if let Some(target_identity) = &input.missing_target_identity {
        reasons.push(format!("target {target_identity} not found"));
    }
    if input.max_hops == 0 {
        reasons.push("traversal disabled by max_hops 0".to_string());
    }
    if input.path_limit == Some(0) {
        reasons.push("path traversal disabled by limit 0".to_string());
    }
    if input.node_limit == Some(0) {
        reasons.push("subgraph traversal disabled by node_limit 0".to_string());
    }
    if input.relationship_limit == Some(0) {
        reasons.push("subgraph traversal disabled by relationship_limit 0".to_string());
    }
    if let Some(relationship_type) = &input.missing_relationship_type {
        reasons.push(format!("relationship type {relationship_type} not found"));
    }
    reasons
}

#[cfg(test)]
fn knowledge_identity_description(label: &str, external_id: &str) -> String {
    format!("{label}:{external_id}")
}

fn knowledge_context_path_node_count(paths: &[KnowledgeGraphContextPath]) -> usize {
    paths
        .iter()
        .flat_map(|path| [path.source_node_id, path.target_node_id])
        .collect::<BTreeSet<_>>()
        .len()
}

#[cfg(test)]
fn knowledge_graph_path_node_count(paths: &[KnowledgeGraphPath]) -> usize {
    paths
        .iter()
        .flat_map(|path| path.segments.iter())
        .flat_map(|segment| [segment.source_node_id, segment.target_node_id])
        .collect::<BTreeSet<_>>()
        .len()
}

struct KnowledgeExpansionEdge {
    direction: KnowledgeGraphPathDirection,
    next_node: NodeId,
    relationship: RelRecord,
}

struct DenseAdjacencyDiagnosticContext<'a> {
    catalog: &'a Catalog,
    store: &'a GraphStore,
    operation: &'a str,
    relationship_type: Option<crate::schema::RelTypeId>,
    requested_direction: KnowledgeNeighborDirection,
}

fn knowledge_expansion_edges_for_node(
    store: &GraphStore,
    node_id: NodeId,
    relationship_type: Option<crate::schema::RelTypeId>,
    requested_direction: KnowledgeNeighborDirection,
    max_edges_per_direction: usize,
) -> Result<Vec<KnowledgeExpansionEdge>> {
    let mut edges = Vec::new();
    let mut seen_relationships = BTreeSet::new();
    for adjacency_direction in adjacency_directions_for_request(requested_direction) {
        let mut direction_edges = BTreeMap::new();
        store.visit_adjacent_relationships_owned(
            node_id,
            relationship_type,
            adjacency_direction,
            |relationship| {
                let next_node = match adjacency_direction {
                    AdjacencyDirection::Outgoing => relationship.target,
                    AdjacencyDirection::Incoming => relationship.source,
                };
                let key = (next_node, relationship.id);
                direction_edges.insert(
                    key,
                    KnowledgeExpansionEdge {
                        direction: knowledge_path_direction_for_adjacency(adjacency_direction),
                        next_node,
                        relationship,
                    },
                );
                if direction_edges.len() > max_edges_per_direction
                    && let Some(last_key) = direction_edges.keys().next_back().copied()
                {
                    direction_edges.remove(&last_key);
                }
                crate::store::GraphScanControl::Continue
            },
        )?;
        edges.extend(
            direction_edges
                .into_values()
                .filter(|edge| seen_relationships.insert(edge.relationship.id.0)),
        );
    }
    Ok(edges)
}

fn record_dense_adjacency_diagnostics(
    context: DenseAdjacencyDiagnosticContext<'_>,
    node_id: NodeId,
    reported_dense_groups: &mut BTreeSet<String>,
    fanout_reasons: &mut Vec<KnowledgeFanoutReasonDetail>,
) {
    for adjacency_direction in adjacency_directions_for_request(context.requested_direction) {
        let stats = match context.relationship_type {
            Some(rel_type) => {
                vec![context
                    .store
                    .adjacency_group_stats(node_id, rel_type, adjacency_direction)]
            }
            None => context
                .store
                .adjacency_group_stats_for_node(node_id, adjacency_direction),
        };
        for stats in stats {
            if stats.layout != AdjacencyLayout::Dense {
                continue;
            }
            let rel_type_name = context
                .catalog
                .rel_type_name(stats.rel_type)
                .unwrap_or("<unknown>");
            let direction = adjacency_direction_name(adjacency_direction);
            let key = format!(
                "{}:{rel_type_name}:{direction}:{}",
                context.operation, node_id.0
            );
            if reported_dense_groups.insert(key) {
                fanout_reasons.push(KnowledgeFanoutReasonDetail::dense_adjacency(
                    context.operation,
                    rel_type_name,
                    direction,
                    node_id.0,
                    stats.degree,
                ));
            }
        }
    }
}

fn adjacency_directions_for_request(
    requested_direction: KnowledgeNeighborDirection,
) -> Vec<AdjacencyDirection> {
    match requested_direction {
        #[cfg(test)]
        KnowledgeNeighborDirection::Outgoing => vec![AdjacencyDirection::Outgoing],
        #[cfg(test)]
        KnowledgeNeighborDirection::Incoming => vec![AdjacencyDirection::Incoming],
        KnowledgeNeighborDirection::Both => {
            vec![AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming]
        }
    }
}

fn knowledge_path_direction_for_adjacency(
    adjacency_direction: AdjacencyDirection,
) -> KnowledgeGraphPathDirection {
    match adjacency_direction {
        AdjacencyDirection::Outgoing => KnowledgeGraphPathDirection::Outgoing,
        AdjacencyDirection::Incoming => KnowledgeGraphPathDirection::Incoming,
    }
}

fn adjacency_direction_name(adjacency_direction: AdjacencyDirection) -> &'static str {
    match adjacency_direction {
        AdjacencyDirection::Outgoing => "outgoing",
        AdjacencyDirection::Incoming => "incoming",
    }
}

fn qos_admission_name(admission: &QosAdmission) -> &'static str {
    match admission {
        QosAdmission::Admit => "admit",
        QosAdmission::Defer { .. } => "defer",
        QosAdmission::Reject { .. } => "reject",
    }
}

fn try_seed_node_by_label_and_external_id(
    catalog: &Catalog,
    store: &GraphStore,
    label: &str,
    external_id: &str,
) -> Result<Option<NodeRecord>> {
    let Some(label_id) = catalog.label_id(label) else {
        return Ok(None);
    };
    let mut found = None;
    let mut values = vec![Value::String(external_id.to_string())];
    if let Ok(value) = external_id.parse::<i64>() {
        values.push(Value::Int(value));
    }
    store.visit_nodes_by_property_owned(label_id, "id", &values, |node| {
        if projected_node_external_id(&node) != external_id {
            return crate::store::GraphScanControl::Continue;
        }
        found = Some(node);
        crate::store::GraphScanControl::Stop
    })?;
    if found.is_some() {
        return Ok(found);
    }
    if let Ok(node_id) = external_id.parse::<u64>()
        && let Some(node) = store.node_owned(NodeId(node_id))?
        && node.labels.contains(&label_id)
        && projected_node_external_id(&node) == external_id
    {
        return Ok(Some(node));
    }
    Ok(found)
}

#[cfg(test)]
fn try_node_by_label_property_external_id(
    catalog: &Catalog,
    store: &GraphStore,
    label: &str,
    property: &str,
    external_id: &str,
) -> Result<Option<NodeRecord>> {
    let Some(label_id) = catalog.label_id(label) else {
        return Ok(None);
    };
    let mut found = None;
    store.visit_nodes_owned(Some(label_id), |node| {
        if node
            .properties
            .get(property)
            .is_some_and(|value| value_to_external_id(value) == external_id)
        {
            found = Some(node);
            crate::store::GraphScanControl::Stop
        } else {
            crate::store::GraphScanControl::Continue
        }
    })?;
    Ok(found)
}

#[cfg(test)]
fn knowledge_induced_edges_via_query_runtime(
    db: &Database,
    request: &KnowledgeInducedEdgeListRequest,
) -> Result<KnowledgeInducedEdgeListOutput> {
    validate_knowledge_induced_edges_request(request)?;

    let graph_commit_epoch = db.store.commit_epoch();
    let requested_ids = request
        .external_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let parameters = BTreeMap::from([(
        "external_ids".to_string(),
        Value::List(
            requested_ids
                .iter()
                .cloned()
                .map(Value::String)
                .collect::<Vec<_>>(),
        ),
    )]);
    let node_output = db.query_read_only_with_params_bounded(
        "MATCH (n) WHERE n.id IN $external_ids RETURN n AS node ORDER BY id(n) ASC",
        &parameters,
        None,
    )?;
    let matched_nodes = node_output
        .rows
        .iter()
        .filter_map(|row| row.get("node").and_then(knowledge_entity_from_value))
        .filter_map(|entity| {
            knowledge_entity_id_property(&entity).and_then(|external_id| {
                requested_ids
                    .contains(&external_id)
                    .then_some((external_id, entity.node_id))
            })
        })
        .collect::<Vec<_>>();

    let matched_external_ids = matched_nodes
        .iter()
        .map(|(external_id, _)| external_id.clone())
        .collect::<BTreeSet<_>>();
    let matched_node_ids = matched_nodes
        .iter()
        .map(|(_, node_id)| *node_id)
        .collect::<BTreeSet<_>>();
    let mut missing_external_ids = Vec::new();
    let mut seen_missing = BTreeSet::new();
    for external_id in &request.external_ids {
        if !matched_external_ids.contains(external_id) && seen_missing.insert(external_id.clone()) {
            missing_external_ids.push(external_id.clone());
        }
    }

    let mut rows = if matched_node_ids.is_empty() {
        Vec::new()
    } else {
        let relationship_parameters = BTreeMap::from([(
            "node_ids".to_string(),
            Value::List(
                matched_node_ids
                    .iter()
                    .map(|node_id| {
                        i64::try_from(*node_id).map(Value::Int).map_err(|_| {
                            SkeinError::Execution(
                                "knowledge induced edge node id exceeds i64".to_string(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        )]);
        let relationship_output = db.query_read_only_with_params_bounded(
            "MATCH (source)-[r]->(target) \
             WHERE id(source) IN $node_ids AND id(target) IN $node_ids \
             RETURN source AS source, target AS target, r AS relationship, id(r) AS relationship_id",
            &relationship_parameters,
            None,
        )?;
        relationship_output
            .rows
            .iter()
            .map(knowledge_induced_edge_row_from_query_row)
            .collect::<Result<Vec<_>>>()?
    };
    rows.sort_by(|left, right| {
        left.source_id
            .cmp(&right.source_id)
            .then_with(|| left.target_id.cmp(&right.target_id))
            .then_with(|| left.relationship_type.cmp(&right.relationship_type))
            .then_with(|| left.relationship_id.cmp(&right.relationship_id))
    });
    let matched_count = rows.len();
    if request.limit > 0 {
        rows.truncate(request.limit);
    }
    let returned_count = rows.len();

    Ok(KnowledgeInducedEdgeListOutput {
        graph_commit_epoch,
        rows,
        matched_node_count: matched_node_ids.len(),
        missing_external_ids,
        matched_count,
        returned_count,
    })
}

#[cfg(test)]
fn validate_knowledge_induced_edges_request(
    request: &KnowledgeInducedEdgeListRequest,
) -> Result<()> {
    if request.external_ids.is_empty() || request.external_ids.iter().any(String::is_empty) {
        return Err(SkeinError::Semantic(
            "knowledge induced edge read requires non-empty external ids".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
fn knowledge_entity_id_property(entity: &KnowledgeEntity) -> Option<String> {
    entity
        .properties
        .get("id")
        .map(value_to_external_id)
        .filter(|external_id| !external_id.is_empty())
}

#[cfg(test)]
fn knowledge_induced_edge_row_from_query_row(
    row: impl QueryRowLookup,
) -> Result<KnowledgeInducedEdgeRow> {
    let source = row
        .get("source")
        .and_then(knowledge_entity_from_value)
        .ok_or_else(|| {
            SkeinError::Execution("knowledge induced edge row is missing source".to_string())
        })?;
    let target = row
        .get("target")
        .and_then(knowledge_entity_from_value)
        .ok_or_else(|| {
            SkeinError::Execution("knowledge induced edge row is missing target".to_string())
        })?;
    let relationship = row
        .get("relationship")
        .and_then(value_to_map)
        .ok_or_else(|| {
            SkeinError::Execution("knowledge induced edge row is missing relationship".to_string())
        })?;
    let relationship_id = row
        .get("relationship_id")
        .and_then(value_to_non_negative_u64)
        .or_else(|| relationship.get("_id").and_then(value_to_non_negative_u64))
        .ok_or_else(|| {
            SkeinError::Execution(
                "knowledge induced edge row is missing relationship_id".to_string(),
            )
        })?;
    Ok(KnowledgeInducedEdgeRow {
        source_id: knowledge_entity_id_property(&source),
        source_node_id: source.node_id,
        target_id: knowledge_entity_id_property(&target),
        target_node_id: target.node_id,
        relationship_id,
        relationship_type: relationship
            .get("type")
            .map(value_to_external_id)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "<unknown>".to_string()),
        strength: relationship
            .get("strength")
            .or_else(|| relationship.get("confidence"))
            .cloned()
            .unwrap_or(Value::Float(0.5)),
    })
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
        return Err(SkeinError::Storage(format!(
                "search projection graph delta operation count {operation_count} exceeded configured limit {limit}"
            )));
    }
    if let Some(epoch) = request.complete_through_graph_commit_epoch {
        let current_epoch = store.commit_epoch();
        if epoch > current_epoch {
            return Err(SkeinError::Storage(format!(
                "search projection graph delta complete-through epoch {epoch} is ahead of graph commit epoch {current_epoch}"
            )));
        }
    }

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

#[cfg(test)]
fn knowledge_entity_from_value(value: &Value) -> Option<KnowledgeEntity> {
    let Value::Map(values) = value else {
        return None;
    };
    let node_id = values.get("_id").and_then(value_to_non_negative_u64)?;
    let labels = values
        .get("labels")
        .and_then(value_to_string_list)
        .unwrap_or_default();
    let mut properties = values.clone();
    properties.remove("_id");
    properties.remove("labels");
    let external_id = properties
        .get("id")
        .map(value_to_external_id)
        .filter(|external_id| !external_id.is_empty())
        .or_else(|| Some(node_id.to_string()));
    Some(KnowledgeEntity {
        node_id,
        labels,
        external_id,
        properties,
    })
}

#[cfg(test)]
fn knowledge_entity_matches_filters(
    entity: &KnowledgeEntity,
    metadata_filters: &BTreeMap<String, String>,
) -> bool {
    metadata_filters
        .iter()
        .all(|(key, value)| knowledge_entity_matches_filter_value(entity, key, value))
}

#[cfg(test)]
fn knowledge_entity_matches_filter_value(entity: &KnowledgeEntity, key: &str, value: &str) -> bool {
    match key {
        "kind" => search_kind_to_label(value)
            .is_some_and(|label| entity.labels.iter().any(|node_label| node_label == label)),
        "external_id" => entity.external_id.as_deref() == Some(value),
        "source_id" => knowledge_entity_projection_source_id(entity).as_deref() == Some(value),
        "space_id" => knowledge_entity_normalized_space_id(entity) == value,
        _ => entity
            .properties
            .get(key)
            .is_some_and(|property| value_to_external_id(property) == value),
    }
}

#[cfg(test)]
fn knowledge_entity_normalized_space_id(entity: &KnowledgeEntity) -> String {
    entity
        .properties
        .get("space_id")
        .map(value_to_external_id)
        .filter(|space_id| !space_id.is_empty())
        .unwrap_or_else(|| "default".to_string())
}

#[cfg(test)]
fn knowledge_entity_projection_source_id(entity: &KnowledgeEntity) -> Option<String> {
    ["source_id", "thread_id", "source"]
        .into_iter()
        .filter_map(|key| entity.properties.get(key).map(value_to_external_id))
        .find(|source_id| !source_id.is_empty())
}

impl ReaderPins {
    fn oldest_epoch(&self) -> Option<u64> {
        self.active_views
            .values()
            .map(|view| view.visible_commit_epoch())
            .min()
    }
}

fn elapsed_micros(started: std::time::Instant) -> u64 {
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

fn optimizer_catalog(
    catalog: &Catalog,
    statistics: &GraphStatistics,
    graph_commit_epoch: u64,
) -> OptimizerCatalog {
    let advanced_statistics = (statistics.advanced_statistics_freshness(graph_commit_epoch)
        == AdvancedStatisticsFreshness::Fresh)
        .then_some(statistics);
    let equality_property_indexes = catalog.property_indexes().filter_map(|index| {
        if index.kind != IndexKind::Equality {
            return None;
        }
        catalog
            .label_name(index.label_id)
            .map(|label| (label.to_string(), index.property.clone()))
    });
    let composite_property_indexes = catalog.composite_property_indexes().filter_map(|index| {
        catalog
            .label_name(index.label_id)
            .map(|label| (label.to_string(), index.properties.clone()))
    });
    let range_property_indexes = catalog.property_indexes().filter_map(|index| {
        if index.kind != IndexKind::Range {
            return None;
        }
        catalog
            .label_name(index.label_id)
            .map(|label| (label.to_string(), index.property.clone()))
    });
    let full_text_property_indexes = catalog.property_indexes().filter_map(|index| {
        if index.kind != IndexKind::FullText {
            return None;
        }
        catalog
            .label_name(index.label_id)
            .map(|label| (label.to_string(), index.property.clone()))
    });
    let label_counts = statistics
        .label_counts
        .iter()
        .filter_map(|(label_id, count)| {
            catalog
                .label_name(*label_id)
                .map(|label| (label.to_string(), *count))
        });
    let rel_type_counts = statistics
        .rel_type_counts
        .iter()
        .filter_map(|(rel_type_id, count)| {
            catalog
                .rel_type_name(*rel_type_id)
                .map(|rel_type| (rel_type.to_string(), *count))
        });
    let rel_type_source_counts = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.rel_type_source_counts.iter())
        .filter_map(|(rel_type_id, count)| {
            catalog
                .rel_type_name(*rel_type_id)
                .map(|rel_type| (rel_type.to_string(), *count))
        });
    let rel_type_target_counts = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.rel_type_target_counts.iter())
        .filter_map(|(rel_type_id, count)| {
            catalog
                .rel_type_name(*rel_type_id)
                .map(|rel_type| (rel_type.to_string(), *count))
        });
    let path_counts = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.path_counts.iter())
        .filter_map(|((source_label_id, rel_type_id, target_label_id), count)| {
            Some((
                (
                    catalog.label_name(*source_label_id)?.to_string(),
                    catalog.rel_type_name(*rel_type_id)?.to_string(),
                    catalog.label_name(*target_label_id)?.to_string(),
                ),
                *count,
            ))
        });
    let path_source_distinct_counts = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.path_source_distinct_counts.iter())
        .filter_map(|((source_label_id, rel_type_id, target_label_id), count)| {
            Some((
                (
                    catalog.label_name(*source_label_id)?.to_string(),
                    catalog.rel_type_name(*rel_type_id)?.to_string(),
                    catalog.label_name(*target_label_id)?.to_string(),
                ),
                *count,
            ))
        });
    let path_target_distinct_counts = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.path_target_distinct_counts.iter())
        .filter_map(|((source_label_id, rel_type_id, target_label_id), count)| {
            Some((
                (
                    catalog.label_name(*source_label_id)?.to_string(),
                    catalog.rel_type_name(*rel_type_id)?.to_string(),
                    catalog.label_name(*target_label_id)?.to_string(),
                ),
                *count,
            ))
        });
    let bounded_path_counts = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.bounded_path_counts.iter())
        .filter_map(
            |((source_label_id, rel_type_id, target_label_id, hops), count)| {
                Some((
                    (
                        catalog.label_name(*source_label_id)?.to_string(),
                        catalog.rel_type_name(*rel_type_id)?.to_string(),
                        catalog.label_name(*target_label_id)?.to_string(),
                        *hops,
                    ),
                    *count,
                ))
            },
        );
    let bounded_path_source_distinct_counts = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.bounded_path_source_distinct_counts.iter())
        .filter_map(
            |((source_label_id, rel_type_id, target_label_id, hops), count)| {
                Some((
                    (
                        catalog.label_name(*source_label_id)?.to_string(),
                        catalog.rel_type_name(*rel_type_id)?.to_string(),
                        catalog.label_name(*target_label_id)?.to_string(),
                        *hops,
                    ),
                    *count,
                ))
            },
        );
    let bounded_path_target_distinct_counts = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.bounded_path_target_distinct_counts.iter())
        .filter_map(
            |((source_label_id, rel_type_id, target_label_id, hops), count)| {
                Some((
                    (
                        catalog.label_name(*source_label_id)?.to_string(),
                        catalog.rel_type_name(*rel_type_id)?.to_string(),
                        catalog.label_name(*target_label_id)?.to_string(),
                        *hops,
                    ),
                    *count,
                ))
            },
        );
    let property_distinct_counts = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.property_distinct_counts.iter())
        .filter_map(|((label_id, property), count)| {
            catalog
                .label_name(*label_id)
                .map(|label| ((label.to_string(), property.clone()), *count))
        });
    let property_histograms = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.property_histograms.iter())
        .filter_map(|((label_id, property), values)| {
            catalog
                .label_name(*label_id)
                .map(|label| ((label.to_string(), property.clone()), values.clone()))
        });
    let rel_property_distinct_counts = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.rel_property_distinct_counts.iter())
        .filter_map(|((rel_type_id, property), count)| {
            catalog
                .rel_type_name(*rel_type_id)
                .map(|rel_type| ((rel_type.to_string(), property.clone()), *count))
        });
    let rel_property_histograms = advanced_statistics
        .into_iter()
        .flat_map(|statistics| statistics.rel_property_histograms.iter())
        .filter_map(|((rel_type_id, property), values)| {
            catalog
                .rel_type_name(*rel_type_id)
                .map(|rel_type| ((rel_type.to_string(), property.clone()), values.clone()))
        });
    let property_index_statistics = catalog.property_indexes().filter_map(|index| {
        if index.kind == IndexKind::FullText {
            return None;
        }
        let statistics = advanced_statistics?;
        let sample = statistics.index_samples.get(&index.id)?;
        let distinct_count = sample.estimated_unique_values()?;
        let label = catalog.label_name(index.label_id)?;
        Some((
            (label.to_string(), index.property.clone()),
            OptimizerIndexStatistics {
                index_size: sample.index_size,
                distinct_count,
            },
        ))
    });
    let composite_index_statistics = catalog.composite_property_indexes().filter_map(|index| {
        let statistics = advanced_statistics?;
        let sample = statistics.index_samples.get(&index.id)?;
        let distinct_count = sample.estimated_unique_values()?;
        let label = catalog.label_name(index.label_id)?;
        Some((
            (label.to_string(), index.properties.clone()),
            OptimizerIndexStatistics {
                index_size: sample.index_size,
                distinct_count,
            },
        ))
    });
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            equality_property_indexes,
            composite_property_indexes,
            range_property_indexes,
            full_text_property_indexes,
        ),
        OptimizerCatalogStatistics::new(
            label_counts,
            rel_type_counts,
            rel_type_source_counts,
            path_counts,
            bounded_path_counts,
            property_distinct_counts,
            property_histograms,
        )
        .with_property_index_statistics(property_index_statistics)
        .with_composite_index_statistics(composite_index_statistics)
        .with_relationship_type_target_counts(rel_type_target_counts)
        .with_path_source_distinct_counts(path_source_distinct_counts)
        .with_path_target_distinct_counts(path_target_distinct_counts)
        .with_bounded_path_source_distinct_counts(bounded_path_source_distinct_counts)
        .with_bounded_path_target_distinct_counts(bounded_path_target_distinct_counts)
        .with_relationship_property_distinct_counts(rel_property_distinct_counts)
        .with_relationship_property_histograms(rel_property_histograms),
    )
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
        cypher::Statement::MatchCreateRelationship(_) => "match_create_relationship",
        cypher::Statement::MatchDelete(_) => "match_delete",
        cypher::Statement::MatchExpandMatchMergeRelationship(_) => {
            "match_expand_match_merge_relationship"
        }
        cypher::Statement::MatchExpandMergeRelationship(_) => "match_expand_merge_relationship",
        cypher::Statement::MatchMergeRelationship(_) => "match_merge_relationship",
        cypher::Statement::MatchNodesReturn(_) => "match_nodes_return",
        cypher::Statement::MatchOptionalRelationshipCountSum(_) => {
            "match_optional_relationship_count_sum"
        }
        cypher::Statement::MatchReturn(query) if query.vector_seed.is_some() => {
            "vector_graph_search"
        }
        cypher::Statement::MatchReturn(_) => "match_return",
        cypher::Statement::MatchSet(_) => "match_set",
        cypher::Statement::MatchSetReturn(_) => "match_set_return",
        cypher::Statement::MatchThreadRepairStats(_) => "match_thread_repair_stats",
        cypher::Statement::MergeNode(_) => "merge_node",
        cypher::Statement::MergeRelationship(_) => "merge_relationship",
        cypher::Statement::ProjectGraph(_) => "project_graph",
        cypher::Statement::Rollback => "rollback",
        cypher::Statement::SetSystemVariable(_) => "set_system_variable",
        cypher::Statement::ShortestPathReturn(_) => "shortest_path_return",
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

impl<'a> NowledgeGraphAdapter<'a> {
    pub fn new(db: &'a mut Database) -> Self {
        Self { db }
    }

    pub fn query(&mut self, statement: &NowledgeGraphStatement) -> Result<QueryOutput> {
        self.db
            .query_with_params(&statement.cypher, &statement.parameters)
    }

    pub fn query_work_request(&self, statement: &NowledgeGraphStatement) -> Result<WorkRequest> {
        self.db.query_work_request_for(&statement.cypher)
    }

    pub fn explain(
        &self,
        statement: &NowledgeGraphStatement,
    ) -> Result<NowledgeGraphExplainOutput> {
        let output = self
            .db
            .explain_query_with_params(&statement.cypher, &statement.parameters)?;
        Ok(NowledgeGraphExplainOutput {
            plan: output.physical_plan.explain(0),
            trace: output.trace,
            work_request: output.work_request,
            plan_cache_lookup: output.plan_cache_lookup,
            statement_kind: output.statement_kind,
        })
    }

    pub fn transaction(
        &mut self,
        statements: &[NowledgeGraphStatement],
    ) -> Result<NowledgeGraphTransactionOutput> {
        let mut tx = self.db.begin_transaction();
        let mut statement_outputs = Vec::with_capacity(statements.len());
        for statement in statements {
            statement_outputs.push(tx.query_with_params(&statement.cypher, &statement.parameters)?);
        }
        let commit_output = tx.commit()?;
        Ok(NowledgeGraphTransactionOutput {
            statement_outputs,
            commit_output,
        })
    }

    pub fn retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> KnowledgeRetrievalOutput {
        self.db.retrieve_knowledge(search_index, request)
    }

    pub fn try_retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        self.db.try_retrieve_knowledge(search_index, request)
    }
}

impl DatabaseTransactionRuntime {
    fn from_database(db: &Database) -> Self {
        Self::from_database_with_system_variables(db, db.system_variables.clone())
    }

    fn from_database_with_system_variables(
        db: &Database,
        system_variables: QuerySystemVariables,
    ) -> Self {
        Self {
            optimizer: db.optimizer.clone(),
            plan_cache: SharedState::new(PlanCache::new(db.config.max_plan_cache_entries)),
            optimizer_planning_cache: SharedState::new(
                db.optimizer_planning_cache.borrow().clone(),
            ),
            config: db.config.clone(),
            system_variables,
        }
    }

    fn ensure_writable(&self, store: &GraphStore) -> Result<()> {
        store.ensure_usable()?;
        if self.config.read_only {
            return Err(SkeinError::Execution(
                "database is opened in read-only mode".to_string(),
            ));
        }
        Ok(())
    }
}

impl DatabaseTransactionState {
    fn from_database(db: &Database) -> Self {
        Self {
            graph_transaction: Some(db.store.begin_mutation_transaction(&db.catalog)),
            relational_transaction: skein_storage::RelationalTransaction::default(),
            relational_state: db.store.relational_state().clone(),
            append_transaction: skein_storage::AppendTransaction::default(),
            append_state: db.store.append_state().clone(),
            relational_index: db
                .store
                .begin_authoritative_relational_transaction_index()
                .map_err(|error| error.to_string()),
            relational_rows: db
                .store
                .begin_authoritative_relational_transaction_rows()
                .map_err(|error| error.to_string()),
        }
    }

    fn rollback(&mut self) {
        self.graph_transaction.take();
        self.relational_transaction.writes.clear();
        self.append_transaction.writes.clear();
        self.relational_index = Ok(None);
        self.relational_rows = Ok(None);
    }

    pub(crate) fn restore_graph_statement(&mut self, savepoint: GraphMutationSavepoint) {
        self.graph_transaction
            .as_mut()
            .expect("database transaction must own a graph workspace")
            .restore(savepoint);
    }

    fn take_for_commit(&mut self) -> Self {
        Self {
            graph_transaction: self.graph_transaction.take(),
            relational_transaction: std::mem::take(&mut self.relational_transaction),
            relational_state: std::mem::take(&mut self.relational_state),
            append_transaction: std::mem::take(&mut self.append_transaction),
            append_state: std::mem::take(&mut self.append_state),
            relational_index: std::mem::replace(&mut self.relational_index, Ok(None)),
            relational_rows: std::mem::replace(&mut self.relational_rows, Ok(None)),
        }
    }

    fn authoritative_relational_index(
        &self,
    ) -> Result<&crate::store::RelationalTransactionIndexView> {
        match &self.relational_index {
            Ok(Some(index)) => Ok(index),
            Ok(None) => Err(SkeinError::StorageIntegrity(
                "authoritative transaction index view is unavailable".to_string(),
            )),
            Err(error) => Err(SkeinError::StorageIntegrity(format!(
                "authoritative transaction index view could not be pinned: {error}"
            ))),
        }
    }

    fn stage_sparse_authoritative_relational_statement(
        &mut self,
        transaction: skein_storage::RelationalTransaction,
    ) -> Result<Option<skein_storage::RelationalState>> {
        let rows = match &self.relational_rows {
            Ok(Some(rows)) => rows,
            Ok(None) => return Ok(None),
            Err(error) => {
                return Err(SkeinError::StorageIntegrity(format!(
                    "authoritative transaction row view could not be pinned: {error}"
                )));
            }
        };
        let index = match &mut self.relational_index {
            Ok(Some(index)) => index,
            Ok(None) => {
                return Err(SkeinError::StorageIntegrity(
                    "authoritative transaction index view is unavailable".to_string(),
                ));
            }
            Err(error) => {
                return Err(SkeinError::StorageIntegrity(format!(
                    "authoritative transaction index view could not be pinned: {error}"
                )));
            }
        };
        let store = self
            .graph_transaction
            .as_ref()
            .expect("database transaction must own a graph workspace")
            .store();
        let (next, index_capture, row_capture) = store
            .stage_sparse_relational_transaction_statement(
                &self.relational_state,
                rows,
                transaction,
                index,
            )
            .map_err(map_transaction_relational_error)?;
        let next_rows = rows
            .stage_advance(row_capture)
            .map_err(map_transaction_relational_error)?;
        index
            .append(index_capture)
            .map_err(map_transaction_relational_error)?;
        self.relational_rows = Ok(Some(next_rows));
        Ok(Some(next))
    }
}

fn execute_graph_transaction_statement(
    runtime: &DatabaseTransactionRuntime,
    transaction: &mut GraphMutationTransaction,
    system_variables: &QuerySystemVariables,
    cypher_text: &str,
    statement: &cypher::Statement,
    parameters: &BTreeMap<String, Value>,
) -> Result<GraphTransactionStatementOutcome> {
    query_work_request_for_statement(system_variables, statement)?;
    let optimizer_search =
        query_statement_variables_for_statement(system_variables, statement)?.optimizer_search;
    let optimized = optimized_query_plan_for(
        cypher_text,
        statement,
        parameters,
        PlanCacheMode::Bypass(PlanCacheBypassReason::MutationPlanning),
        PlanCacheContext {
            catalog: transaction.catalog(),
            store: transaction.store(),
            optimizer: &runtime.optimizer,
            config: &runtime.config,
            cache: &runtime.plan_cache,
            planning_cache: &runtime.optimizer_planning_cache,
            access_control: None,
            optimizer_search,
        },
    )?;

    if executor::is_mutation_plan(&optimized.physical_plan)? {
        runtime.ensure_writable(transaction.store())?;
        let mutation = executor::mutation_command(&optimized.physical_plan)?.ok_or_else(|| {
            SkeinError::Execution(
                "transaction mutation plan cannot be represented as a staged mutation".to_string(),
            )
        })?;
        let is_mutation_return = matches!(
            optimized.physical_plan,
            PhysicalPlan::SetNodePropertiesReturn { .. }
        );
        let mutation_limits = match &optimized.physical_plan {
            PhysicalPlan::SetNodePropertiesReturn {
                returns: crate::planner::SetNodePropertiesReturnMode::Count { .. },
                ..
            } => skein_storage::MutationLimits {
                max_result_rows: runtime.config.mutation_limits.max_affected_rows,
                max_result_payload_bytes: std::num::NonZeroUsize::new(usize::MAX)
                    .expect("usize::MAX is non-zero"),
                ..runtime.config.mutation_limits
            },
            _ => runtime.config.mutation_limits,
        };
        let statement_savepoint = transaction.savepoint();
        let execution = (|| {
            let staged = if is_mutation_return {
                transaction.stage_mutation_without_commit_rows(mutation, mutation_limits)
            } else {
                transaction.stage_mutation_with_limits(mutation, mutation_limits)
            };
            let summary = staged?;
            let returned_rows = executor::project_staged_mutation_return_rows(
                &optimized.physical_plan,
                transaction.catalog(),
                transaction.store(),
                &summary.rows,
                runtime.config.mutation_limits,
            )?;
            Ok((
                QueryOutput {
                    rows: returned_rows.unwrap_or_default().into(),
                },
                transaction.lock_footprint_since(&statement_savepoint)?,
            ))
        })();
        return match execution {
            Ok((output, lock_footprint)) => Ok(GraphTransactionStatementOutcome {
                output,
                lock_footprint,
                savepoint: Some(statement_savepoint),
            }),
            Err(error) => {
                transaction.restore(statement_savepoint);
                Err(error)
            }
        };
    }

    let query_result = {
        let (catalog, store) = transaction.catalog_and_store_mut();
        let mut external = executor::NoExternalReadOperator;
        executor::execute_with_output_limits_profile_and_external_and_memory(
            &optimized.physical_plan,
            catalog,
            store,
            parameters,
            &mut external,
            runtime.config.max_read_result_rows,
            runtime.config.max_read_result_payload_bytes,
            &runtime.config.execution_memory,
        )
        .map(|profiled| QueryOutput {
            rows: profiled.rows,
        })
    };
    transaction.store().poison_on_storage_error(&query_result);
    query_result.map(|output| GraphTransactionStatementOutcome {
        output,
        savepoint: None,
        lock_footprint: GraphMutationLockFootprint::default(),
    })
}

fn execute_database_transaction_query(
    runtime: &DatabaseTransactionRuntime,
    state: &mut DatabaseTransactionState,
    cypher_text: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<QueryOutput> {
    let statement = cypher::parse(cypher_text)?;
    let body = statement_body(&statement);
    if matches!(body, cypher::Statement::SetSystemVariable(_)) {
        reject_system_variable_parameters(parameters)?;
        return Err(SkeinError::Execution(
            "SET system variable is not allowed inside a transaction".to_string(),
        ));
    }
    if matches!(body, cypher::Statement::Explain(_)) {
        return Err(SkeinError::Execution(
            "EXPLAIN is not allowed inside a transaction".to_string(),
        ));
    }
    let transaction = state
        .graph_transaction
        .as_mut()
        .expect("database transaction must own a graph workspace");
    execute_graph_transaction_statement(
        runtime,
        transaction,
        &runtime.system_variables,
        cypher_text,
        &statement,
        parameters,
    )
    .map(|outcome| outcome.output)
}

pub(super) fn execute_concurrent_graph_transaction_query(
    runtime: &DatabaseTransactionRuntime,
    state: &mut DatabaseTransactionState,
    cypher_text: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<GraphTransactionStatementOutcome> {
    let statement = cypher::parse(cypher_text)?;
    let body = statement_body(&statement);
    if matches!(body, cypher::Statement::SetSystemVariable(_)) {
        reject_system_variable_parameters(parameters)?;
        return Err(SkeinError::Execution(
            "SET system variable is not allowed inside a transaction".to_string(),
        ));
    }
    if matches!(body, cypher::Statement::Explain(_)) {
        return Err(SkeinError::Execution(
            "EXPLAIN is not allowed inside a transaction".to_string(),
        ));
    }
    let transaction = state
        .graph_transaction
        .as_mut()
        .expect("database transaction must own a graph workspace");
    execute_graph_transaction_statement(
        runtime,
        transaction,
        &runtime.system_variables,
        cypher_text,
        &statement,
        parameters,
    )
}

fn execute_database_transaction_sql(
    runtime: &DatabaseTransactionRuntime,
    state: &mut DatabaseTransactionState,
    sql_text: &str,
    parameters: &[Value],
    allow_system_schema_registry_write: bool,
    allow_locking_select: bool,
) -> Result<QueryOutput> {
    let prepared = skein_sql::prepare_postgres_sql(sql_text)?;
    reject_locking_select_without_manager(&prepared.statement, allow_locking_select)?;
    if !allow_system_schema_registry_write
        && crate::relational_sql::statement_writes_system_schema_registry(&prepared.statement)
    {
        return Err(SkeinError::Semantic(
            "skein_schema_migrations is read-only outside system schema upgrade".to_string(),
        ));
    }
    if matches!(
        &prepared.statement,
        crate::sql::SqlStatement::Select(select)
            if system_sql::is_virtual_catalog_select(select)
    ) {
        let graph_transaction = state
            .graph_transaction
            .as_ref()
            .expect("database transaction must own a graph workspace");
        let plan_cache_stats = runtime.plan_cache.borrow().stats();
        return system_sql::query_sql_with_params(
            sql_text,
            parameters,
            runtime.config.max_read_result_rows,
            runtime.config.max_read_result_payload_bytes,
            &system_sql::SystemSqlContext {
                catalog: graph_transaction.catalog(),
                store: graph_transaction.store(),
                relational_state: &state.relational_state,
                append_state: &state.append_state,
                runtime: system_sql::SystemRuntimeSnapshot::from_config(&runtime.config),
                plan_cache_stats: &plan_cache_stats,
                slow_queries: &[],
                statement_summaries: &[],
            },
        );
    }
    if let Some(plan) = crate::relational_sql::compile_append_select_sql(
        sql_text,
        parameters,
        &state.append_state,
        runtime.config.max_read_result_rows.unwrap_or(usize::MAX),
    )? {
        let store = state
            .graph_transaction
            .as_ref()
            .expect("database transaction must own a graph workspace")
            .store();
        let output = store.read_append_partition_from_state_bounded(
            &state.append_state,
            &plan.table,
            &plan.partition,
            plan.after.as_ref(),
            plan.max_rows,
            runtime
                .config
                .max_read_result_payload_bytes
                .unwrap_or(usize::MAX),
        )?;
        return Ok(QueryOutput {
            rows: crate::relational_sql::project_append_rows(&plan, &output.rows)?.into(),
        });
    }
    if let Some(plan) = crate::relational_sql::compile_append_explain_sql(
        sql_text,
        parameters,
        &state.append_state,
        runtime.config.max_read_result_rows.unwrap_or(usize::MAX),
    )? {
        let report = if plan.analyze {
            let store = state
                .graph_transaction
                .as_ref()
                .expect("database transaction must own a graph workspace")
                .store();
            Some(
                store
                    .read_append_partition_from_state_bounded(
                        &state.append_state,
                        &plan.select.table,
                        &plan.select.partition,
                        plan.select.after.as_ref(),
                        plan.select.max_rows,
                        runtime
                            .config
                            .max_read_result_payload_bytes
                            .unwrap_or(usize::MAX),
                    )?
                    .report,
            )
        } else {
            None
        };
        return Ok(QueryOutput {
            rows: crate::relational_sql::format_append_explain(&plan, report.as_ref()).into(),
        });
    }
    if matches!(
        prepared.statement,
        crate::sql::SqlStatement::Select(_) | crate::sql::SqlStatement::Explain(_)
    ) {
        let index_read_mode = if runtime
            .config
            .relational_index_mode
            .requires_authoritative_indexes()
        {
            crate::relational_sql::RelationalIndexReadMode::AuthoritativeTransaction(
                state.authoritative_relational_index()?,
            )
        } else if runtime
            .config
            .relational_index_mode
            .serves_demand_paged_reads()
        {
            crate::relational_sql::RelationalIndexReadMode::TransactionWorkspace
        } else {
            crate::relational_sql::RelationalIndexReadMode::Materialized
        };
        let row_read_mode = match &state.relational_rows {
            Ok(Some(rows)) => crate::relational_sql::RelationalRowReadMode::Transaction {
                store: state
                    .graph_transaction
                    .as_ref()
                    .expect("database transaction must own a graph workspace")
                    .store(),
                rows,
            },
            Ok(None) => crate::relational_sql::RelationalRowReadMode::CanonicalMemory,
            Err(error) => {
                return Err(SkeinError::StorageIntegrity(format!(
                    "authoritative transaction row view could not be pinned: {error}"
                )));
            }
        };
        let output = crate::relational_sql::execute_relational_query_sql_with_runtime(
            sql_text,
            parameters,
            &state.relational_state,
            crate::relational_sql::RelationalQueryReadModes::new(index_read_mode, row_read_mode),
            relational_query_limits(&runtime.config, runtime.config.max_read_result_rows),
            &runtime.config.execution_memory,
            None,
        )?;
        return Ok(QueryOutput { rows: output.rows });
    }

    runtime.ensure_writable(
        state
            .graph_transaction
            .as_ref()
            .expect("database transaction must own a graph workspace")
            .store(),
    )?;
    if let Some(transaction) = crate::relational_sql::compile_append_statement_sql(
        sql_text,
        parameters,
        &state.append_state,
    )? {
        if let crate::sql::SqlStatement::CreateTable(create) = &prepared.statement
            && state
                .relational_state
                .table_schema(&create.table.name)
                .is_some()
        {
            return Err(SkeinError::Semantic(format!(
                "table {} already exists as a RowPage table",
                create.table.name
            )));
        }
        state.append_state = state
            .append_state
            .stage_transaction(&transaction, skein_storage::AppendMutationLimits::default())
            .map_err(map_transaction_append_error)?;
        state.append_transaction.writes.extend(transaction.writes);
        return Ok(QueryOutput {
            rows: Vec::new().into(),
        });
    }
    if let crate::sql::SqlStatement::CreateTable(create) = &prepared.statement
        && state.append_state.schema(&create.table.name).is_some()
    {
        return Err(SkeinError::Semantic(format!(
            "table {} already exists as a strict append table",
            create.table.name
        )));
    }
    let transaction = crate::relational_sql::compile_relational_statement_sql(
        sql_text,
        parameters,
        &state.relational_state,
    )?;
    let next_relational_state = if runtime
        .config
        .relational_index_mode
        .requires_authoritative_indexes()
    {
        if let Some(next) =
            state.stage_sparse_authoritative_relational_statement(transaction.clone())?
        {
            next
        } else {
            let index = match &mut state.relational_index {
                Ok(Some(index)) => index,
                Ok(None) => {
                    return Err(SkeinError::StorageIntegrity(
                        "authoritative transaction index view is unavailable".to_string(),
                    ));
                }
                Err(error) => {
                    return Err(SkeinError::StorageIntegrity(format!(
                        "authoritative transaction index view could not be pinned: {error}"
                    )));
                }
            };
            let (next, capture) = state
                .relational_state
                .stage_transaction_with_authoritative_index(
                    transaction.clone(),
                    skein_storage::RelationalMutationLimits::default(),
                    skein_storage::RelationalOverflowConfig::default(),
                    index.capture_limits(),
                    index,
                )
                .map_err(map_transaction_relational_error)?;
            index
                .append(capture)
                .map_err(map_transaction_relational_error)?;
            next
        }
    } else {
        state
            .relational_state
            .stage_transaction(
                transaction.clone(),
                skein_storage::RelationalMutationLimits::default(),
                skein_storage::RelationalOverflowConfig::default(),
            )
            .map_err(map_transaction_relational_error)?
    };
    state.relational_state = next_relational_state;
    state
        .relational_transaction
        .writes
        .extend(transaction.writes);
    Ok(QueryOutput {
        rows: Vec::new().into(),
    })
}

fn map_transaction_append_error(error: skein_storage::AppendTableError) -> SkeinError {
    match error {
        skein_storage::AppendTableError::Corruption(message)
        | skein_storage::AppendTableError::Durability(message) => {
            SkeinError::StorageIntegrity(message)
        }
        error @ (skein_storage::AppendTableError::Admission(_)
        | skein_storage::AppendTableError::Schema(_)
        | skein_storage::AppendTableError::Constraint(_)) => {
            SkeinError::Execution(error.to_string())
        }
    }
}

fn map_transaction_relational_error(error: skein_storage::RelationalError) -> SkeinError {
    match error {
        skein_storage::RelationalError::Corruption(message)
        | skein_storage::RelationalError::Durability(message) => {
            SkeinError::StorageIntegrity(message)
        }
        error @ (skein_storage::RelationalError::Admission(_)
        | skein_storage::RelationalError::Schema(_)
        | skein_storage::RelationalError::Constraint(_)) => {
            SkeinError::Execution(error.to_string())
        }
    }
}

fn reject_locking_select_without_manager(
    statement: &crate::sql::SqlStatement,
    allow_locking_select: bool,
) -> Result<()> {
    let locking_select = match statement {
        crate::sql::SqlStatement::Select(select) => select.lock_strength.is_some(),
        crate::sql::SqlStatement::Explain(explain) => {
            matches!(explain.statement.as_ref(), crate::sql::SqlStatement::Select(select) if select.lock_strength.is_some())
        }
        _ => false,
    };
    if locking_select && !allow_locking_select {
        return Err(SkeinError::Semantic(
            "FOR UPDATE/SHARE requires a pessimistic concurrent transaction".to_string(),
        ));
    }
    Ok(())
}

fn commit_database_transaction_state(
    db: &mut Database,
    state: &mut DatabaseTransactionState,
    allow_stale_rebase: bool,
) -> Result<QueryOutput> {
    db.ensure_writable()?;
    let graph_transaction = state
        .graph_transaction
        .take()
        .expect("database transaction must own a graph workspace");
    let relational_transaction = std::mem::take(&mut state.relational_transaction);
    let append_transaction = std::mem::take(&mut state.append_transaction);
    let summary = if allow_stale_rebase {
        db.store
            .commit_rebased_mutation_transaction_relational_and_append(
                &mut db.catalog,
                graph_transaction,
                relational_transaction,
                append_transaction,
                db.config.mutation_limits,
            )?
    } else {
        db.store.commit_mutation_transaction_relational_and_append(
            &mut db.catalog,
            graph_transaction,
            relational_transaction,
            append_transaction,
            db.config.mutation_limits,
        )?
    };
    db.complete_required_relational_row_checkpoint("transaction commit")?;
    Ok(QueryOutput {
        rows: summary.rows.into(),
    })
}

impl DatabaseTransaction<'_> {
    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        execute_database_transaction_query(&self.runtime, &mut self.state, cypher_text, parameters)
    }

    pub fn query_sql(&mut self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_with_params(sql_text, &[])
    }

    pub fn query_sql_with_params(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        execute_database_transaction_sql(
            &self.runtime,
            &mut self.state,
            sql_text,
            parameters,
            false,
            false,
        )
    }

    pub(super) fn query_system_schema_sql(&mut self, sql_text: &str) -> Result<QueryOutput> {
        self.query_system_schema_sql_with_params(sql_text, &[])
    }

    pub(super) fn query_system_schema_sql_with_params(
        &mut self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        execute_database_transaction_sql(
            &self.runtime,
            &mut self.state,
            sql_text,
            parameters,
            true,
            false,
        )
    }

    pub fn commit(mut self) -> Result<QueryOutput> {
        commit_database_transaction_state(self.db, &mut self.state, false)
    }

    pub fn rollback(mut self) {
        self.state.rollback();
    }
}

impl DatabaseSession<'_> {
    pub fn system_variables(&self) -> &QuerySystemVariables {
        &self.system_variables
    }

    pub fn query_work_request(&self) -> WorkRequest {
        self.system_variables.query_work_request()
    }

    pub fn query_work_request_for(&self, cypher_text: &str) -> Result<WorkRequest> {
        let statement = cypher::parse(cypher_text)?;
        query_work_request_for_statement(&self.system_variables, &statement)
    }

    pub fn explain_query(&self, cypher_text: &str) -> Result<ExplainOutput> {
        self.explain_query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn explain_query_with_params(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<ExplainOutput> {
        if self.graph_transaction.is_some() {
            return Err(SkeinError::Execution(
                "EXPLAIN is not allowed inside an active transaction".to_string(),
            ));
        }
        let statement = cypher::parse(cypher_text)?;
        let work_request = query_work_request_for_statement(&self.system_variables, &statement)?;
        let optimized = self
            .db
            .optimized_query_plan(cypher_text, &statement, parameters)?;
        Ok(ExplainOutput {
            physical_plan: optimized.physical_plan,
            trace: optimized.trace,
            work_request,
            plan_cache_lookup: optimized.plan_cache_lookup,
            statement_kind: statement_kind(statement_body(&statement)),
        })
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        let statement = cypher::parse(cypher_text)?;
        let body = statement_body(&statement);
        match body {
            cypher::Statement::BeginTransaction => {
                reject_transaction_control_parameters("BEGIN TRANSACTION", parameters)?;
                if self.graph_transaction.is_some() {
                    return Err(SkeinError::Execution(
                        "transaction is already active".to_string(),
                    ));
                }
                self.db.ensure_writable()?;
                self.transaction_runtime = Some(
                    DatabaseTransactionRuntime::from_database_with_system_variables(
                        self.db,
                        self.system_variables.clone(),
                    ),
                );
                self.graph_transaction =
                    Some(self.db.store.begin_mutation_transaction(&self.db.catalog));
                Ok(QueryOutput {
                    rows: Vec::new().into(),
                })
            }
            cypher::Statement::Commit => {
                reject_transaction_control_parameters("COMMIT", parameters)?;
                let Some(transaction) = self.graph_transaction.take() else {
                    return Err(SkeinError::Execution(
                        "COMMIT requires an active transaction".to_string(),
                    ));
                };
                self.transaction_runtime.take();
                self.db.ensure_writable()?;
                let summary = self.db.store.commit_mutation_transaction_and_relational(
                    &mut self.db.catalog,
                    transaction,
                    skein_storage::RelationalTransaction::default(),
                    self.db.config.mutation_limits,
                )?;
                Ok(QueryOutput {
                    rows: summary.rows.into(),
                })
            }
            cypher::Statement::Rollback => {
                reject_transaction_control_parameters("ROLLBACK", parameters)?;
                if self.graph_transaction.take().is_none() {
                    return Err(SkeinError::Execution(
                        "ROLLBACK requires an active transaction".to_string(),
                    ));
                }
                self.transaction_runtime.take();
                Ok(QueryOutput {
                    rows: Vec::new().into(),
                })
            }
            cypher::Statement::Checkpoint if self.graph_transaction.is_some() => {
                Err(SkeinError::Execution(
                    "CHECKPOINT is not allowed inside an active transaction".to_string(),
                ))
            }
            cypher::Statement::SetSystemVariable(_) if self.graph_transaction.is_some() => {
                Err(SkeinError::Execution(
                    "SET system variable is not allowed inside an active transaction".to_string(),
                ))
            }
            cypher::Statement::SetSystemVariable(set) => {
                reject_system_variable_parameters(parameters)?;
                self.system_variables.apply_set_system_variable(set)
            }
            cypher::Statement::Explain(_) if self.graph_transaction.is_some() => {
                Err(SkeinError::Execution(
                    "EXPLAIN is not allowed inside an active transaction".to_string(),
                ))
            }
            cypher::Statement::Explain(explain) => {
                self.execute_explain_statement(cypher_text, explain, parameters)
            }
            statement if self.graph_transaction.is_some() => {
                let transaction = self
                    .graph_transaction
                    .as_mut()
                    .expect("checked active transaction");
                let runtime = self
                    .transaction_runtime
                    .as_ref()
                    .expect("active session transaction must own a query runtime");
                execute_graph_transaction_statement(
                    runtime,
                    transaction,
                    &self.system_variables,
                    cypher_text,
                    statement,
                    parameters,
                )
                .map(|outcome| outcome.output)
            }
            _ => {
                query_work_request_for_statement(&self.system_variables, &statement)?;
                self.db.query_with_params(cypher_text, parameters)
            }
        }
    }

    fn execute_explain_statement(
        &mut self,
        cypher_text: &str,
        explain: &cypher::Explain,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        let work_request =
            query_work_request_for_statement(&self.system_variables, &explain.statement)?;
        let optimized =
            self.db
                .optimized_query_plan(cypher_text, &explain.statement, parameters)?;
        let inner_statement_kind = statement_kind(statement_body(&explain.statement));
        if explain.analyze {
            if executor::is_mutation_plan(&optimized.physical_plan)? {
                return Err(SkeinError::Execution(
                    "EXPLAIN ANALYZE only supports read queries".to_string(),
                ));
            }
            let mut external = executor::NoExternalReadOperator;
            let profiled = executor::execute_with_output_limits_profile_and_external_and_memory(
                &optimized.physical_plan,
                &mut self.db.catalog,
                &mut self.db.store,
                parameters,
                &mut external,
                self.db.config.max_read_result_rows,
                self.db.config.max_read_result_payload_bytes,
                &self.db.config.execution_memory,
            );
            self.db.store.poison_on_storage_error(&profiled);
            let profiled = profiled?;
            return Ok(QueryOutput {
                rows: vec![explain_analyze_output_row(
                    &optimized,
                    work_request,
                    inner_statement_kind,
                    profiled.rows.len(),
                    &profiled.profile,
                )]
                .into(),
            });
        }
        Ok(QueryOutput {
            rows: vec![explain_output_row(
                &optimized,
                work_request,
                inner_statement_kind,
            )]
            .into(),
        })
    }
}

fn reject_transaction_control_parameters(
    statement: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    if parameters.is_empty() {
        Ok(())
    } else {
        Err(SkeinError::Semantic(format!(
            "{statement} does not accept parameters"
        )))
    }
}

fn profiled_relational_sql_output(
    output: crate::relational_sql::RelationalQueryOutput,
) -> ProfiledRelationalSqlQueryOutput {
    let crate::relational_sql::RelationalQueryOutput {
        rows,
        intermediate_rows,
        hydration,
        index_execution_evidence,
        row_execution_evidence,
        ..
    } = output;
    let profile = RelationalSqlReadProfile {
        intermediate_rows,
        hydrated_rows: hydration.hydrated_rows,
        hydrated_compressed_bytes: hydration.compressed_bytes,
        hydrated_decompressed_bytes: hydration.decompressed_bytes,
        index_reads: index_execution_evidence
            .into_iter()
            .map(|evidence| {
                let runtime_path = evidence.runtime_path().to_string();
                RelationalSqlIndexReadProfile {
                    table: evidence.table,
                    index: evidence.index,
                    runtime_path,
                    logical_pages: evidence.logical_pages,
                    logical_bytes: evidence.logical_bytes,
                    physical_pages: evidence.file_pages,
                    physical_bytes: evidence.file_bytes,
                    cache_hits: evidence.cache_hits,
                    cache_misses: evidence.cache_misses,
                    cache_admission_rejections: evidence.cache_admission_rejections,
                    rows_visited: evidence.rows_visited,
                }
            })
            .collect(),
        row_read: RelationalSqlRowReadProfile {
            runtime_path: row_execution_evidence.runtime_path.to_string(),
            base_generation: row_execution_evidence.base_generation,
            delta_generation: row_execution_evidence.delta_generation,
            base_commit_epoch: row_execution_evidence.base_commit_epoch,
            visible_commit_epoch: row_execution_evidence.visible_commit_epoch,
            root_set_digest: row_execution_evidence.root_set_digest,
            descriptor_reads: row_execution_evidence.descriptor_reads,
            logical_pages: row_execution_evidence.logical_pages,
            logical_bytes: row_execution_evidence.logical_bytes,
            physical_pages: row_execution_evidence.file_pages,
            physical_bytes: row_execution_evidence.file_bytes,
            cache_hits: row_execution_evidence.cache_hits,
            cache_misses: row_execution_evidence.cache_misses,
            cache_admission_rejections: row_execution_evidence.cache_admission_rejections,
            rows_visited: row_execution_evidence.rows_visited,
            overlay_entries: row_execution_evidence.overlay_entries,
            overlay_resident_bytes: row_execution_evidence.overlay_resident_bytes,
        },
    };
    ProfiledRelationalSqlQueryOutput {
        output: QueryOutput { rows },
        profile,
    }
}

impl DatabaseReadTransaction {
    pub fn commit_epoch(&self) -> u64 {
        self.published_read_view.visible_commit_epoch()
    }

    pub const fn published_read_view(&self) -> PublishedReadView {
        self.published_read_view
    }

    pub fn query(&mut self, cypher_text: &str) -> Result<QueryOutput> {
        self.query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        self.query_with_params_bounded(cypher_text, parameters, self.config.max_read_result_rows)
    }

    pub fn query_with_params_context(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        task_context: &skein_core::RuntimeTaskContext,
    ) -> Result<QueryOutput> {
        Ok(self
            .query_with_params_bounded_profile_internal(
                cypher_text,
                parameters,
                self.config.max_read_result_rows,
                None,
                Some(task_context),
            )?
            .output)
    }

    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    pub(crate) fn query_prepared_with_params_context(
        &mut self,
        prepared: PreparedRuntimeQuery,
        parameters: &BTreeMap<String, Value>,
        task_context: &skein_core::RuntimeTaskContext,
    ) -> Result<QueryOutput> {
        let (cypher_text, prepared) = prepared.into_execution(&self.catalog, &self.store);
        Ok(self
            .query_with_params_bounded_profile_prepared_internal(
                &cypher_text,
                prepared,
                parameters,
                self.config.max_read_result_rows,
                None,
                Some(task_context),
            )?
            .output)
    }

    pub fn query_with_params_access_control(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        access_control: QueryAccessControlContext,
    ) -> Result<QueryOutput> {
        self.query_with_params_bounded_profile_access_control(
            cypher_text,
            parameters,
            self.config.max_read_result_rows,
            access_control,
        )
        .map(|output| output.output)
    }

    pub fn query_with_params_bounded(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
    ) -> Result<QueryOutput> {
        Ok(self
            .query_with_params_bounded_profile(cypher_text, parameters, max_rows)?
            .output)
    }

    pub fn query_with_params_bounded_profile(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
    ) -> Result<BoundedReadQueryOutput> {
        self.query_with_params_bounded_profile_internal(
            cypher_text,
            parameters,
            max_rows,
            None,
            None,
        )
    }

    pub fn query_streaming(
        &mut self,
        cypher_text: &str,
        options: QueryStreamOptions,
        consumer: impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.query_with_params_streaming(cypher_text, &BTreeMap::new(), options, consumer)
    }

    /// Streams rows from a read plan through a budgeted consumer boundary.
    ///
    /// Consumer calls are provisional until this method returns `Ok`. A host
    /// that cannot surface a terminal query error must not publish consumed
    /// rows before the final report is available.
    pub fn query_with_params_streaming(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        mut consumer: impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.store.ensure_usable()?;
        self.query_with_params_streaming_prepared_internal(
            cypher_text,
            query_runtime::parse_runtime_execution(cypher_text)?,
            parameters,
            options,
            None,
            &mut consumer,
        )
    }

    /// Streams borrowed row views to a synchronous host consumer.
    ///
    /// The view cannot outlive the consumer call. Hosts can inspect, serialize,
    /// or load values without an additional clone; hosts that retain a row
    /// must call [`RowRef::to_owned_row`]. The scalar fallback currently
    /// borrows an already materialized row, while the same callback contract
    /// can receive direct columnar row views after that production path is
    /// qualified end to end.
    pub fn query_with_params_streaming_ref(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        mut consumer: impl for<'row> FnMut(RowRef<'row>) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.query_with_params_streaming(cypher_text, parameters, options, |row| {
            consumer(RowRef::from(&row))
        })
    }

    pub fn query_with_params_streaming_context(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        task_context: &skein_core::RuntimeTaskContext,
        mut consumer: impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.store.ensure_usable()?;
        self.query_with_params_streaming_prepared_internal(
            cypher_text,
            query_runtime::parse_runtime_execution(cypher_text)?,
            parameters,
            options,
            Some(task_context),
            &mut consumer,
        )
    }

    pub(crate) fn query_with_params_streaming_external(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        external: &mut dyn executor::ExternalReadOperator,
        mut consumer: impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.store.ensure_usable()?;
        self.query_with_params_streaming_prepared_external_internal(
            cypher_text,
            query_runtime::parse_runtime_execution(cypher_text)?,
            parameters,
            options,
            ReadStreamingExecutionContext {
                task_context: None,
                external,
            },
            &mut consumer,
        )
    }

    #[cfg_attr(not(feature = "tokio-runtime"), allow(dead_code))]
    pub(crate) fn query_prepared_with_params_streaming_context(
        &mut self,
        prepared: PreparedRuntimeQuery,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        task_context: &skein_core::RuntimeTaskContext,
        mut consumer: impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        let (cypher_text, prepared) = prepared.into_execution(&self.catalog, &self.store);
        self.query_with_params_streaming_prepared_internal(
            &cypher_text,
            prepared,
            parameters,
            options,
            Some(task_context),
            &mut consumer,
        )
    }

    fn query_with_params_streaming_prepared_internal(
        &mut self,
        cypher_text: &str,
        prepared: query_runtime::PreparedRuntimeExecution,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        task_context: Option<&skein_core::RuntimeTaskContext>,
        consumer: &mut impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        let mut external = executor::NoExternalReadOperator;
        self.query_with_params_streaming_prepared_external_internal(
            cypher_text,
            prepared,
            parameters,
            options,
            ReadStreamingExecutionContext {
                task_context,
                external: &mut external,
            },
            consumer,
        )
    }

    fn query_with_params_streaming_prepared_external_internal(
        &mut self,
        cypher_text: &str,
        prepared: query_runtime::PreparedRuntimeExecution,
        parameters: &BTreeMap<String, Value>,
        options: QueryStreamOptions,
        context: ReadStreamingExecutionContext<'_>,
        consumer: &mut impl FnMut(Row) -> Result<()>,
    ) -> Result<QueryStreamReport> {
        self.store.ensure_usable()?;
        let max_rows = restrictive_query_limit(self.config.max_read_result_rows, options.max_rows);
        let max_payload_bytes = restrictive_query_limit(
            self.config.max_read_result_payload_bytes,
            options.max_payload_bytes,
        );
        let query_runtime::PreparedRuntimeExecution {
            statement,
            optimized: prepared_optimized,
            ..
        } = prepared;
        let body = statement_body(&statement);
        if matches!(statement, cypher::Statement::Explain(_)) {
            return Err(SkeinError::Execution(
                "streaming query does not support EXPLAIN".to_string(),
            ));
        }
        if matches!(body, cypher::Statement::Checkpoint) {
            return Err(SkeinError::Execution(
                "CHECKPOINT is not allowed inside a read transaction".to_string(),
            ));
        }
        if matches!(body, cypher::Statement::SetSystemVariable(_)) {
            return Err(SkeinError::Execution(
                "SET system variable is not allowed inside a read transaction".to_string(),
            ));
        }
        query_work_request_for_statement(&QuerySystemVariables::default(), &statement)?;
        let optimized = match prepared_optimized {
            Some(optimized) => optimized,
            None => self.optimized_query_plan_with_access_control(
                cypher_text,
                &statement,
                parameters,
                None,
            )?,
        };
        if executor::is_mutation_plan(&optimized.physical_plan)? {
            return Err(SkeinError::Execution(
                "read transaction query must not be a mutation".to_string(),
            ));
        }
        let streamed = match context.task_context {
            Some(task_context) => {
                executor::execute_with_row_consumer_profile_and_external_and_context_and_memory(
                    &optimized.physical_plan,
                    &mut self.catalog,
                    &mut self.store,
                    parameters,
                    context.external,
                    max_rows,
                    max_payload_bytes,
                    consumer,
                    task_context,
                    &self.config.execution_memory,
                )
            }
            None => executor::execute_with_row_consumer_profile_and_external_and_memory(
                &optimized.physical_plan,
                &mut self.catalog,
                &mut self.store,
                parameters,
                context.external,
                max_rows,
                max_payload_bytes,
                consumer,
                &self.config.execution_memory,
            ),
        };
        self.store.poison_on_storage_error(&streamed);
        let streamed = streamed?;
        let pipeline = &streamed.profile.pipeline_memory_report;
        Ok(QueryStreamReport {
            fully_streamed: streamed.fully_streamed,
            output_rows: pipeline.output_rows,
            output_payload_bytes: pipeline.output_payload_bytes,
            execution_profile: streamed.profile,
        })
    }

    pub fn query_with_params_bounded_profile_access_control(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
        access_control: QueryAccessControlContext,
    ) -> Result<BoundedReadQueryOutput> {
        self.query_with_params_bounded_profile_internal(
            cypher_text,
            parameters,
            max_rows,
            Some(access_control),
            None,
        )
    }

    fn query_with_params_bounded_profile_internal(
        &mut self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
        access_control: Option<QueryAccessControlContext>,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<BoundedReadQueryOutput> {
        self.store.ensure_usable()?;
        self.query_with_params_bounded_profile_prepared_internal(
            cypher_text,
            query_runtime::parse_runtime_execution(cypher_text)?,
            parameters,
            max_rows,
            access_control,
            task_context,
        )
    }

    fn query_with_params_bounded_profile_prepared_internal(
        &mut self,
        cypher_text: &str,
        prepared: query_runtime::PreparedRuntimeExecution,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
        access_control: Option<QueryAccessControlContext>,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<BoundedReadQueryOutput> {
        self.store.ensure_usable()?;
        query_runtime::query_runtime_checkpoint(task_context)?;
        let max_rows = restrictive_query_limit(self.config.max_read_result_rows, max_rows);
        let max_payload_bytes = self.config.max_read_result_payload_bytes;
        let query_runtime::PreparedRuntimeExecution {
            statement,
            optimized: prepared_optimized,
            ..
        } = prepared;
        let body = statement_body(&statement);
        if let cypher::Statement::Explain(explain) = &statement {
            return self.execute_explain_statement(
                cypher_text,
                explain,
                parameters,
                max_rows,
                access_control,
                task_context,
            );
        }
        if matches!(body, cypher::Statement::Checkpoint) {
            reject_transaction_control_parameters("CHECKPOINT", parameters)?;
            return Err(SkeinError::Execution(
                "CHECKPOINT is not allowed inside a read transaction".to_string(),
            ));
        }
        if matches!(body, cypher::Statement::SetSystemVariable(_)) {
            reject_system_variable_parameters(parameters)?;
            return Err(SkeinError::Execution(
                "SET system variable is not allowed inside a read transaction".to_string(),
            ));
        }
        query_work_request_for_statement(&QuerySystemVariables::default(), &statement)?;
        let optimized = match prepared_optimized {
            Some(optimized) if access_control.is_none() => optimized,
            _ => self.optimized_query_plan_with_access_control(
                cypher_text,
                &statement,
                parameters,
                access_control.as_ref(),
            )?,
        };
        if executor::is_mutation_plan(&optimized.physical_plan)? {
            return Err(SkeinError::Execution(
                "read transaction query must not be a mutation".to_string(),
            ));
        }
        let profiled = match task_context {
            Some(task_context) => {
                let mut external = executor::NoExternalReadOperator;
                executor::execute_with_output_limits_profile_and_external_and_context_and_memory(
                    &optimized.physical_plan,
                    &mut self.catalog,
                    &mut self.store,
                    parameters,
                    &mut external,
                    max_rows,
                    max_payload_bytes,
                    task_context,
                    &self.config.execution_memory,
                )
            }
            None => {
                let mut external = executor::NoExternalReadOperator;
                executor::execute_with_output_limits_profile_and_external_and_memory(
                    &optimized.physical_plan,
                    &mut self.catalog,
                    &mut self.store,
                    parameters,
                    &mut external,
                    max_rows,
                    max_payload_bytes,
                    &self.config.execution_memory,
                )
            }
        };
        self.store.poison_on_storage_error(&profiled);
        let profiled = profiled?;
        query_runtime::query_runtime_checkpoint(task_context)?;
        Ok(BoundedReadQueryOutput {
            output: QueryOutput {
                rows: profiled.rows,
            },
            execution_profile: profiled.profile,
        })
    }

    fn execute_explain_statement(
        &mut self,
        cypher_text: &str,
        explain: &cypher::Explain,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
        access_control: Option<QueryAccessControlContext>,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<BoundedReadQueryOutput> {
        query_runtime::query_runtime_checkpoint(task_context)?;
        let work_request =
            query_work_request_for_statement(&QuerySystemVariables::default(), &explain.statement)?;
        let optimized = self.optimized_query_plan_with_access_control(
            cypher_text,
            &explain.statement,
            parameters,
            access_control.as_ref(),
        )?;
        let inner_statement_kind = statement_kind(statement_body(&explain.statement));
        if executor::is_mutation_plan(&optimized.physical_plan)? {
            if explain.analyze {
                return Err(SkeinError::Execution(
                    "EXPLAIN ANALYZE only supports read queries".to_string(),
                ));
            }
            return Err(SkeinError::Execution(
                "read transaction query must not be a mutation".to_string(),
            ));
        }
        if explain.analyze {
            let profiled = match task_context {
                Some(task_context) => {
                    let mut external = executor::NoExternalReadOperator;
                    executor::execute_with_output_limits_profile_and_external_and_context_and_memory(
                        &optimized.physical_plan,
                        &mut self.catalog,
                        &mut self.store,
                        parameters,
                        &mut external,
                        max_rows,
                        self.config.max_read_result_payload_bytes,
                        task_context,
                        &self.config.execution_memory,
                    )
                }
                None => {
                    let mut external = executor::NoExternalReadOperator;
                    executor::execute_with_output_limits_profile_and_external_and_memory(
                        &optimized.physical_plan,
                        &mut self.catalog,
                        &mut self.store,
                        parameters,
                        &mut external,
                        max_rows,
                        self.config.max_read_result_payload_bytes,
                        &self.config.execution_memory,
                    )
                }
            };
            self.store.poison_on_storage_error(&profiled);
            let profiled = profiled?;
            query_runtime::query_runtime_checkpoint(task_context)?;
            let row_count = profiled.rows.len();
            return Ok(BoundedReadQueryOutput {
                output: QueryOutput {
                    rows: vec![explain_analyze_output_row(
                        &optimized,
                        work_request,
                        inner_statement_kind,
                        row_count,
                        &profiled.profile,
                    )]
                    .into(),
                },
                execution_profile: profiled.profile,
            });
        }
        Ok(BoundedReadQueryOutput {
            output: QueryOutput {
                rows: vec![explain_output_row(
                    &optimized,
                    work_request,
                    inner_statement_kind,
                )]
                .into(),
            },
            execution_profile: empty_read_execution_profile(),
        })
    }

    pub fn query_sql(&self, sql_text: &str) -> Result<QueryOutput> {
        self.query_sql_bounded(sql_text, self.config.max_read_result_rows)
    }

    pub fn query_sql_with_params(
        &self,
        sql_text: &str,
        parameters: &[Value],
    ) -> Result<QueryOutput> {
        self.query_sql_with_params_bounded(sql_text, parameters, self.config.max_read_result_rows)
    }

    pub fn query_sql_bounded(
        &self,
        sql_text: &str,
        max_rows: Option<usize>,
    ) -> Result<QueryOutput> {
        self.query_sql_with_params_bounded(sql_text, &[], max_rows)
    }

    pub fn query_sql_with_params_bounded(
        &self,
        sql_text: &str,
        parameters: &[Value],
        max_rows: Option<usize>,
    ) -> Result<QueryOutput> {
        self.query_sql_with_params_options(
            sql_text,
            parameters,
            QueryStreamOptions {
                max_rows,
                max_payload_bytes: self.config.max_read_result_payload_bytes,
            },
        )
    }

    /// Executes PostgreSQL-dialect SQL with per-statement row and payload
    /// admission against this pinned read transaction. Configured database
    /// limits remain hard upper bounds.
    pub fn query_sql_with_params_options(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
    ) -> Result<QueryOutput> {
        self.query_sql_with_params_options_context(
            sql_text,
            parameters,
            options,
            &skein_core::RuntimeTaskContext::default(),
        )
    }

    /// Executes one bounded relational `SELECT` against this pinned snapshot
    /// and returns result rows together with the storage accounting from that
    /// exact execution. Virtual system-catalog queries and `EXPLAIN` are kept
    /// on their dedicated output paths.
    pub fn query_sql_with_params_options_profiled(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
    ) -> Result<ProfiledRelationalSqlQueryOutput> {
        self.query_sql_with_params_options_profiled_context(
            sql_text,
            parameters,
            options,
            &skein_core::RuntimeTaskContext::default(),
        )
    }

    /// Profiled relational `SELECT` with host cancellation and deadline
    /// propagation. The returned rows and profile always belong to one
    /// execution of the same pinned read view.
    pub fn query_sql_with_params_options_profiled_context(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
        task_context: &skein_core::RuntimeTaskContext,
    ) -> Result<ProfiledRelationalSqlQueryOutput> {
        self.store.ensure_usable()?;
        query_runtime::query_runtime_checkpoint(Some(task_context))?;
        let max_rows = restrictive_query_limit(self.config.max_read_result_rows, options.max_rows);
        let max_payload_bytes = restrictive_query_limit(
            self.config.max_read_result_payload_bytes,
            options.max_payload_bytes,
        );
        let prepared = skein_sql::prepare_postgres_sql(sql_text)?;
        reject_locking_select_without_manager(&prepared.statement, false)?;
        let crate::sql::SqlStatement::Select(select) = &prepared.statement else {
            return Err(SkeinError::Semantic(
                "profiled relational SQL requires SELECT".to_string(),
            ));
        };
        if system_sql::is_virtual_catalog_select(select) {
            return Err(SkeinError::Semantic(
                "profiled relational SQL does not support virtual system catalogs".to_string(),
            ));
        }
        self.execute_profiled_relational_sql(
            sql_text,
            parameters,
            max_rows,
            max_payload_bytes,
            task_context,
        )
    }

    /// Executes bounded PostgreSQL-dialect SQL against this pinned read
    /// transaction while propagating host cancellation and deadlines through
    /// planning, index traversal, row hydration, and result construction.
    pub fn query_sql_with_params_options_context(
        &self,
        sql_text: &str,
        parameters: &[Value],
        options: QueryStreamOptions,
        task_context: &skein_core::RuntimeTaskContext,
    ) -> Result<QueryOutput> {
        self.store.ensure_usable()?;
        query_runtime::query_runtime_checkpoint(Some(task_context))?;
        let max_rows = restrictive_query_limit(self.config.max_read_result_rows, options.max_rows);
        let max_payload_bytes = restrictive_query_limit(
            self.config.max_read_result_payload_bytes,
            options.max_payload_bytes,
        );
        let prepared = skein_sql::prepare_postgres_sql(sql_text)?;
        reject_locking_select_without_manager(&prepared.statement, false)?;
        if matches!(
            &prepared.statement,
            crate::sql::SqlStatement::Select(select)
                if system_sql::is_virtual_catalog_select(select)
        ) {
            let output = system_sql::query_sql_with_params(
                sql_text,
                parameters,
                max_rows,
                max_payload_bytes,
                &system_sql::SystemSqlContext {
                    catalog: &self.catalog,
                    store: &self.store,
                    relational_state: self.store.relational_state(),
                    append_state: self.store.append_state(),
                    runtime: system_sql::SystemRuntimeSnapshot::from_config(&self.config),
                    plan_cache_stats: &self.plan_cache.borrow().stats(),
                    slow_queries: &self.slow_query_snapshot,
                    statement_summaries: &self.statement_summary_snapshot,
                },
            )?;
            query_runtime::query_runtime_checkpoint(Some(task_context))?;
            return Ok(output);
        }

        if let Some(plan) = crate::relational_sql::compile_append_select_sql(
            sql_text,
            parameters,
            self.store.append_state(),
            max_rows.unwrap_or(usize::MAX),
        )? {
            let output = self.store.read_append_partition_bounded(
                &plan.table,
                &plan.partition,
                plan.after.as_ref(),
                plan.max_rows,
                max_payload_bytes.unwrap_or(usize::MAX),
            )?;
            query_runtime::query_runtime_checkpoint(Some(task_context))?;
            return Ok(QueryOutput {
                rows: crate::relational_sql::project_append_rows(&plan, &output.rows)?.into(),
            });
        }
        if let Some(plan) = crate::relational_sql::compile_append_explain_sql(
            sql_text,
            parameters,
            self.store.append_state(),
            max_rows.unwrap_or(usize::MAX),
        )? {
            let report = if plan.analyze {
                Some(
                    self.store
                        .read_append_partition_bounded(
                            &plan.select.table,
                            &plan.select.partition,
                            plan.select.after.as_ref(),
                            plan.select.max_rows,
                            max_payload_bytes.unwrap_or(usize::MAX),
                        )?
                        .report,
                )
            } else {
                None
            };
            query_runtime::query_runtime_checkpoint(Some(task_context))?;
            return Ok(QueryOutput {
                rows: crate::relational_sql::format_append_explain(&plan, report.as_ref()).into(),
            });
        }

        self.execute_profiled_relational_sql(
            sql_text,
            parameters,
            max_rows,
            max_payload_bytes,
            task_context,
        )
        .map(|profiled| profiled.output)
    }

    fn execute_profiled_relational_sql(
        &self,
        sql_text: &str,
        parameters: &[Value],
        max_rows: Option<usize>,
        max_payload_bytes: Option<usize>,
        task_context: &skein_core::RuntimeTaskContext,
    ) -> Result<ProfiledRelationalSqlQueryOutput> {
        let query_result = crate::relational_sql::execute_relational_query_sql_with_runtime(
            sql_text,
            parameters,
            self.store.relational_state(),
            crate::relational_sql::RelationalQueryReadModes::new(
                relational_index_read_mode(&self.config, &self.store),
                crate::relational_sql::RelationalRowReadMode::Store(&self.store),
            ),
            relational_query_limits_with_payload(&self.config, max_rows, max_payload_bytes),
            &self.config.execution_memory,
            Some(task_context),
        );
        self.store.poison_on_storage_error(&query_result);
        let output = query_result?;
        query_runtime::query_runtime_checkpoint(Some(task_context))?;
        Ok(profiled_relational_sql_output(output))
    }

    pub fn explain_query(&self, cypher_text: &str) -> Result<ExplainOutput> {
        self.explain_query_with_params(cypher_text, &BTreeMap::new())
    }

    pub fn explain_query_with_params(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<ExplainOutput> {
        self.store.ensure_usable()?;
        let statement = cypher::parse(cypher_text)?;
        let work_request =
            query_work_request_for_statement(&QuerySystemVariables::default(), &statement)?;
        let optimized = self.optimized_query_plan(cypher_text, &statement, parameters)?;
        if executor::is_mutation_plan(&optimized.physical_plan)? {
            return Err(SkeinError::Execution(
                "read transaction query must not be a mutation".to_string(),
            ));
        }
        Ok(ExplainOutput {
            physical_plan: optimized.physical_plan,
            trace: optimized.trace,
            work_request,
            plan_cache_lookup: optimized.plan_cache_lookup,
            statement_kind: statement_kind(statement_body(&statement)),
        })
    }

    pub fn plan_cache_stats(&self) -> PlanCacheStats {
        self.plan_cache.borrow().stats()
    }

    fn optimized_query_plan(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<OptimizedQueryPlan> {
        self.optimized_query_plan_with_access_control(cypher_text, statement, parameters, None)
    }

    fn optimized_query_plan_with_access_control(
        &self,
        cypher_text: &str,
        statement: &cypher::Statement,
        parameters: &BTreeMap<String, Value>,
        access_control: Option<&QueryAccessControlContext>,
    ) -> Result<OptimizedQueryPlan> {
        let optimizer_search =
            query_statement_variables_for_statement(&QuerySystemVariables::default(), statement)?
                .optimizer_search;
        let cache_mode = if optimizer_search != OptimizerSearchDirective::Auto {
            PlanCacheMode::Bypass(PlanCacheBypassReason::OptimizerDirective)
        } else if statement_uses_plan_cache(statement) {
            PlanCacheMode::Use
        } else {
            PlanCacheMode::Bypass(PlanCacheBypassReason::StatementNotCacheable)
        };
        optimized_query_plan_for(
            cypher_text,
            statement,
            parameters,
            cache_mode,
            PlanCacheContext {
                catalog: &self.catalog,
                store: &self.store,
                optimizer: &self.optimizer,
                config: &self.config,
                cache: &self.plan_cache,
                planning_cache: &self.optimizer_planning_cache,
                access_control,
                optimizer_search,
            },
        )
    }

    pub fn project_graph(&self, rel_type: Option<&str>) -> ProjectedGraph {
        match rel_type {
            Some(name) => self
                .catalog
                .rel_type_id(name)
                .map(|rel_type_id| ProjectedGraph::from_store(&self.store, Some(rel_type_id)))
                .unwrap_or_else(|| ProjectedGraph::from_store_without_edges(&self.store)),
            None => ProjectedGraph::from_store(&self.store, None),
        }
    }

    pub fn export_canonical_graph_snapshot(&self) -> CanonicalGraphSnapshotExport {
        export_canonical_graph_snapshot_for(&self.catalog, &self.store)
    }

    pub fn try_export_canonical_graph_snapshot(&self) -> Result<CanonicalGraphSnapshotExport> {
        canonical_snapshot::try_export_canonical_graph_snapshot_for(&self.catalog, &self.store)
    }

    pub fn rebuild_search_projection(
        &self,
        search_index: &mut SearchIndex,
        options: SearchRebuildOptions,
    ) -> Result<SearchRebuildSummary> {
        search_index.rebuild_from_graph(&self.catalog, &self.store, options)
    }

    pub fn repair_search_projection_metadata(
        &self,
        search_index: &mut SearchIndex,
        options: MetadataRepairOptions,
    ) -> Result<MetadataRepairSummary> {
        search_index.repair_metadata_from_graph(&self.catalog, &self.store, options)
    }

    pub fn retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> KnowledgeRetrievalOutput {
        KnowledgeRetrievalGraphContext {
            catalog: &self.catalog,
            store: &self.store,
            compressed_vector_search_mode: self.config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.config.adaptive_vector_backend_policy,
            query_memory_budget: self.config.execution_memory.query_memory_bytes,
            result_payload_budget: self
                .config
                .max_read_result_payload_bytes
                .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
        }
        .retrieve_knowledge(search_index, request)
    }

    pub fn try_retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        KnowledgeRetrievalGraphContext {
            catalog: &self.catalog,
            store: &self.store,
            compressed_vector_search_mode: self.config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.config.adaptive_vector_backend_policy,
            query_memory_budget: self.config.execution_memory.query_memory_bytes,
            result_payload_budget: self
                .config
                .max_read_result_payload_bytes
                .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
        }
        .try_retrieve_knowledge(search_index, request)
    }

    #[cfg(test)]
    pub(crate) fn statistics(&self) -> GraphStatistics {
        self.store.statistics(&self.catalog)
    }

    #[cfg(test)]
    pub(crate) fn basic_statistics(&self) -> crate::schema::BasicGraphStatistics {
        self.store.basic_statistics()
    }
}

fn single_import_label(node: &CanonicalSnapshotNode) -> Result<String> {
    match node.labels.as_slice() {
        [label] => Ok(label.clone()),
        [] => Err(SkeinError::Storage(
            "Skein Lightning initial import node has no label".to_string(),
        )),
        _ => Err(SkeinError::Storage(
            "Skein Lightning initial import multi-label node is unsupported".to_string(),
        )),
    }
}

fn relational_state_counts(state: &skein_storage::RelationalState) -> (usize, usize) {
    let table_count = state.table_schemas().count();
    let row_count = state
        .table_schemas()
        .map(|schema| state.row_count(&schema.name))
        .sum();
    (table_count, row_count)
}

#[cfg(test)]
mod tests;
