use crate::analytics::ProjectedGraph;
use crate::error::{Result, SkeinError};
use crate::schema::{
    AdvancedStatisticsFreshness, BasicGraphStatistics, Catalog, CompositeIndexDescriptor,
    ConstraintId, GraphStatistics, IndexId, IndexKind, IndexStatisticsSample, LabelId, PropertyId,
    PropertyType, RelTypeId, SchemaObjectState, TableDescriptor, TableId, TableKind,
};
use crate::telemetry::TelemetrySink;
use crate::value::Value;
use skein_core::RuntimeTaskContext;
use skein_integrity::{checksum_u64, integrity_digest, Sha256Digest};
use skein_storage::projection_document_id_for_node as search_projection_document_id_for_node;

#[derive(Debug)]
struct RuntimeGovernorBackgroundAdmission(skein_qos::RuntimeGovernor);

struct RootStorageTelemetry(Arc<dyn TelemetrySink>);

impl std::fmt::Debug for RootStorageTelemetry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RootStorageTelemetry")
    }
}

impl skein_storage::StorageTelemetrySink for RootStorageTelemetry {
    fn record_wal_append(&self, event: skein_storage::WalAppendTelemetry) {
        self.0.record_kernel(skein_telemetry::KernelTelemetry {
            operation: skein_telemetry::KernelTelemetryOperation::WalAppend,
            success: event.success,
            elapsed_micros: event.elapsed_micros,
            item_count: event.operation_count,
            byte_count: event.byte_count,
            fsync_micros: event.fsync_micros,
            generation: Some(event.generation),
        });
    }
}

impl skein_storage::BackgroundWorkAdmission for RuntimeGovernorBackgroundAdmission {
    fn try_admit(
        &self,
        request: skein_storage::BackgroundWorkRequest,
    ) -> std::result::Result<Box<dyn skein_storage::BackgroundWorkPermit>, String> {
        self.0
            .try_admit(skein_qos::RuntimeWorkRequest {
                priority: skein_qos::RuntimeWorkPriority::Background,
                kind: skein_qos::RuntimeWorkKind::Control,
                cpu_slots: request.cpu_slots,
                memory_bytes: request.memory_bytes,
                io_slots: request.io_slots,
                io_reservation_scope: skein_qos::RuntimeIoReservationScope::Task,
                result_bytes: 0,
                blocking: false,
            })
            .map(|permit| Box::new(permit) as Box<dyn skein_storage::BackgroundWorkPermit>)
            .map_err(|error| error.to_string())
    }
}
#[path = "store/append_tables.rs"]
mod append_tables;
#[path = "store/backup.rs"]
mod backup;
#[path = "store/derived_repair.rs"]
mod derived_repair;
#[path = "store/doctor.rs"]
mod doctor;
#[path = "store/durable.rs"]
mod durable;
#[path = "store/graph_apply.rs"]
mod graph_apply;
#[path = "store/graph_checkpoint.rs"]
mod graph_checkpoint;
#[path = "store/graph_columnar_shadow.rs"]
mod graph_columnar_shadow;
#[path = "store/graph_commit.rs"]
mod graph_commit;
#[path = "store/graph_indexes.rs"]
mod graph_indexes;
#[path = "store/graph_mutation.rs"]
mod graph_mutation;
#[path = "store/graph_read.rs"]
mod graph_read;
#[path = "store/graph_recovery.rs"]
mod graph_recovery;
#[path = "store/relational_index_shadow.rs"]
mod relational_index_shadow;
#[path = "store/relational_row_pages.rs"]
mod relational_row_pages;
#[path = "store/source_scan.rs"]
mod source_scan;
#[path = "store/statistics_refresh.rs"]
mod statistics_refresh;
#[path = "store/wal_codec.rs"]
mod wal_codec;
pub use backup::restore_storage_backup;
use backup::{
    copy_backup_file, copy_file_with_checksum, file_checksum, remove_source_scan_artifacts,
    validate_backup_files, validate_new_backup_destination,
};
pub use derived_repair::{
    DerivedArtifactHealth, DerivedArtifactHealthReport, DerivedArtifactHealthState,
    DerivedArtifactKind, DerivedArtifactRebuildOptions, DerivedArtifactRepairPlan,
    DerivedArtifactRepairReport, DERIVED_ARTIFACT_REPAIR_PROTOCOL,
};
pub use doctor::{
    DatabaseDoctor, WalDoctorOptions, WalRepairAcknowledgement, WalTailRepairPlan,
    WalTailRepairReason, WalTailRepairReport, WAL_DOCTOR_REPAIR_PROTOCOL,
};
pub(crate) use durable::PreparedCheckpoint;
use durable::{
    artifact_metadata_presence_consistent, load_published_canonical_adjacency,
    load_published_property_projection, CheckpointImage, CheckpointManifestArtifacts,
    DerivedArtifactBuildConfig, DurableArtifactMetadata, DurableManifest, DurableOpenMode,
    DurableStore, GraphManifestOpenBudget,
};
use graph_columnar_shadow::ColumnarShadowState;
pub use graph_columnar_shadow::{
    ColumnarShadowAdmission, ColumnarShadowCheckpointReport, ColumnarShadowCheckpointStatus,
    ColumnarShadowRecoveryStatus, COLUMN_GROUP_SHADOW_DIR,
};
use relational_index_shadow::RelationalIndexShadowState;
pub use relational_index_shadow::{
    RelationalConstraintQualificationProbeReport, RelationalConstraintQualificationReport,
    RelationalConstraintQualificationUse, RelationalIndexQualificationProbeKind,
    RelationalIndexQualificationProbeReport, RelationalIndexReadViewBackendReport,
    RelationalIndexReadViewReport, RelationalIndexShadowCheckpointReport,
    RelationalIndexShadowCheckpointStatus, RelationalIndexShadowRecoveryStatus,
    RelationalIndexViewQualificationOptions, RelationalIndexViewQualificationReport,
    RELATIONAL_CONSTRAINT_QUALIFICATION_PROTOCOL, RELATIONAL_INDEX_VIEW_QUALIFICATION_PROTOCOL,
};
pub(crate) use relational_index_shadow::{
    RelationalIndexProbeStatistics, RelationalTransactionIndexView,
};
pub use relational_row_pages::RelationalRowPageRecoveryStatus;
use relational_row_pages::RelationalRowPageState;
pub(crate) use relational_row_pages::RelationalTransactionRowView;
use skein_storage::artifact_files::{
    canonical_adjacency_artifact_generation_file, canonical_artifact_generation_file,
    canonical_manifest_generation_file, checkpoint_generation_file,
    cleanup_abandoned_checkpoint_preparations, has_storage_artifacts,
    parse_append_manifest_generation_file, parse_append_segment_generation_file,
    parse_generation_file, parse_relational_index_artifact_generation_file,
    parse_relational_index_manifest_generation_file,
    parse_relational_overflow_extent_generation_file, parse_relational_overflow_generation_file,
    parse_relational_row_generation_file, parse_relational_row_page_artifact_generation_file,
    property_projection_artifact_generation_file, property_projection_manifest_generation_file,
    property_spill_artifact_generation_file, property_spill_manifest_generation_file,
    relational_checkpoint_generation_file, storage_generation_for_file, store_id_for_path,
    wal_generation_file,
};
use skein_storage::GraphIndexReadMetrics;
#[cfg(test)]
use skein_storage::COW_MAP_TARGET_SEGMENT_BYTES;
use skein_storage::{
    available_storage_space, decode_append_wal_batch,
    decode_relational_checkpoint_file_with_index_load,
    decode_relational_checkpoint_with_index_load, decode_relational_wal_batch,
    encode_append_wal_batch, encode_relational_checkpoint, persistent_composite_property_identity,
    sync_parent_directory, AdjacencyPostingList, AppendDecodeLimits, AppendGenerationReader,
    AppendMutationLimits, AppendPublicationConfig, AppendPublicationState, AppendPublisher,
    AppendState, CanonicalEndpointDirection, CanonicalNodeIterator, CanonicalRelationshipIterator,
    CanonicalSegmentError, PersistentPropertyProjectionDefinitionAdmission,
    PersistentPropertyProjectionRecord, RelationalCheckpointIndexLoad, RelationalDecodeLimits,
    RelationalMutationLimits, RelationalOverflowConfig, RelationalOverflowPublicationConfig,
    RelationalOverflowPublisher, RelationalRecoverySourceBuilder,
    RelationalRowPageGenerationRequest, RelationalRowPagePublicationConfig,
    RelationalRowPagePublisher, RelationalSparseLiveStage, RelationalState, RelationalTransaction,
};
pub use skein_storage::{
    AdjacencyDirection, AdjacencyGroupConsistencyMismatch, AdjacencyGroupKey, AdjacencyGroupStats,
    AdjacencyLayout, AppendGeneratedRow, AppendMutationOutcome, AppendOrderMode,
    AppendSegmentReadOutput, AppendStorageResidencyReport, AppendTableRow, AppendTableSchema,
    AppendTransaction, AppendWrite, BackupFileEntry, BackupManifest, CanonicalAdjacencyBuildReport,
    CanonicalAdjacencyConfig, CanonicalAdjacencyEntry, CanonicalAdjacencyReadReport,
    CanonicalAdjacencyReader, CanonicalAdjacencyWriter, CanonicalScanControl,
    CanonicalSegmentConfig, CanonicalSegmentManifest, CanonicalSegmentReader,
    CanonicalSegmentWriter, ConnectedNodesCreate, DurabilityPolicy, DurableCompression,
    FileSegmentRangeReader, GraphMutation, ManifestGeneration, MatchedRelationshipCopyMerge,
    MatchedRelationshipCreate, MatchedRelationshipMerge, MatchedRelationshipRetargetMerge,
    MatchedRelationshipSourceRetargetMerge, MutationLimits, MutationSummary, NodeId, NodeRecord,
    NodeSetAssignment, NodeSetValue, OrderedAdjacencyEntry, PersistentPropertyProjectionConfig,
    PersistentPropertyProjectionDefinition, PersistentPropertyProjectionError,
    PersistentPropertyProjectionKind, PersistentPropertyProjectionManifest,
    PersistentPropertyProjectionReader, PersistentPropertyProjectionWriter,
    ProjectedGraphDefinition, ProjectedGraphStatus, ProjectedNodeRecord, PropertyFilter,
    PropertyIndexProjectionRebuildAction, PropertySpillConfig, PropertySpillManifest,
    PropertySpillReader, RecoveryMode, RelId, RelRecord, RelationalColumnSchema,
    RelationalIndexMode, RelationalIndexReadLimits, RelationalIndexReadReport,
    RelationalIndexRecoveryReadReport, RelationalKey, RelationalRow, RelationalScalarType,
    RelationalValue, RelationshipDeleteRequest, RelationshipOnCreatePropertyValue,
    RelationshipPropertiesUpdate, RelationshipPropertyUpdate, RelationshipSetAssignment,
    RelationshipTargetNodeDelete, ScanPredicate, ScanPruningReport, ScanPruningStrategy,
    ScanPruningTargetKind, ScanSegmentAccessPlan, ScanSegmentFallback, ScanSegmentManifest,
    SchemaMaintenanceAction, SchemaMaintenancePlanItem, SearchProjectionChangefeedReadiness,
    SearchProjectionChangefeedStatus, SearchProjectionGraphChange, SearchProjectionMutationId,
    SegmentCache, SegmentCacheSnapshot, SegmentRangeReader, SegmentReadError,
    SegmentReadExecutionError, SegmentReadExecutionReport, SegmentReadExecutor, SegmentReadPayload,
    SegmentReadRange, SegmentReadSchedule, SegmentReadScheduler, SegmentReadWave,
    StorageBackupReport, StorageDebtController, StorageOpenTimings, StoragePressureReasonCode,
    StoragePressureSignals, StoragePressureSnapshot, StoragePressureState,
    StorageReclamationWatermark, StorageRecoveryReport, StorageResidencyMode, StorageRestoreReport,
    StorageScrubReport, StoreId, StoreStableIdMapping, WalReplayConfig,
    STORAGE_PRESSURE_DELAY_RATIO_PER_MILLION, STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION,
};
use skein_storage::{
    CowSegment, CowSegmentedMap, ProjectedGraphArtifact, ProjectedGraphArtifactData,
};
pub use skein_storage::{
    GraphIndexReadMetricsSnapshot, PersistentGraphIndexClass, PublishedReadView,
};
pub use skein_storage::{RelationalIndexArtifactMetadata, RelationalIndexGenerationArtifacts};
pub(crate) use skein_storage::{WalSyncGroupFlush, WalSyncGroupProgress};
pub use source_scan::SourceScanRow;
pub(crate) use statistics_refresh::OptimizerStatisticsRefreshWork;
pub use statistics_refresh::{OptimizerStatisticsRefreshOptions, OptimizerStatisticsRefreshReport};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use wal_codec::binary::encode_binary_wal_record;
use wal_codec::frame::{
    encode_binary_wal_header, frame_binary_wal_record, WAL_BINARY_FILE_HEADER_BYTES,
};
use wal_codec::{
    quarantine_corrupt_wal, reject_corrupt_wal_record, WalCursorEvent, WalEntry, WalOp,
    WalOpenOutcome, WalRecordCursor,
};

const STORAGE_VERSION: &str = "skein-storage-v1";
const MANIFEST_FILE: &str = "manifest.skein";
const PROJECTED_GRAPHS_FILE: &str = "projected_graphs.skein";
const STABLE_ID_MAPPING_FILE: &str = "stable_ids.skein";
const PROJECTED_GRAPH_ARTIFACT_VERSION: u64 = 1;
const CHECKPOINT_HEADER_V1: &str = "SKEIN_CHECKPOINT_V1";
const MANIFEST_HEADER_V1: &str = "SKEIN_MANIFEST_V1";
const BACKUP_MANIFEST_FILE: &str = "backup.skein";
const CANONICAL_MANIFEST_MAX_BYTES: u64 = 256 * 1024 * 1024;
const PROPERTY_SPILL_MANIFEST_MAX_BYTES: u64 = 64 * 1024;
const PROPERTY_PROJECTION_MANIFEST_MAX_BYTES: u64 = 32 * 1024 * 1024;
const CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER: u64 = 4;
const MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES: u64 = 64 * 1024;
const MIN_PROPERTY_HISTOGRAM_VALUES: usize = 128;
const MID_PROPERTY_HISTOGRAM_VALUES: usize = 256;
const MAX_PROPERTY_HISTOGRAM_VALUES: usize = 512;
const MID_PROPERTY_HISTOGRAM_DISTINCT_VALUES: usize = 1_024;
const MAX_PROPERTY_HISTOGRAM_DISTINCT_VALUES: usize = 4_096;
const MAX_BOUNDED_PATH_STAT_HOPS: usize = 3;
const DURABLE_COMPRESSION_HEADER: &str = "SKEIN_COMPRESSED_V1";
const DEFAULT_COMPRESSION_LEVEL: i32 = 3;
pub const DENSE_ADJACENCY_DEGREE_THRESHOLD: usize = 64;
const MAX_ADJACENCY_CONSISTENCY_SAMPLES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckpointPublishStage {
    CheckpointPersisted,
    WalPrepared,
    ManifestPublished,
}

#[cfg(test)]
const PROCESS_CRASH_POINT_ENV: &str = "SKEIN_TEST_PROCESS_CRASH_POINT";

fn process_crash_failpoint(point: &str) {
    #[cfg(test)]
    if std::env::var(PROCESS_CRASH_POINT_ENV).as_deref() == Ok(point) {
        std::process::exit(86);
    }
    let _ = point;
}

#[cfg(test)]
thread_local! {
    static CHECKPOINT_FAILPOINT: std::cell::Cell<Option<CheckpointPublishStage>> = const {
        std::cell::Cell::new(None)
    };
}

fn checkpoint_publish_failpoint(stage: CheckpointPublishStage) -> Result<()> {
    match stage {
        CheckpointPublishStage::CheckpointPersisted => {
            process_crash_failpoint("during_checkpoint_publication");
        }
        CheckpointPublishStage::ManifestPublished => {
            process_crash_failpoint("after_manifest_publication");
        }
        CheckpointPublishStage::WalPrepared => {}
    }
    #[cfg(test)]
    if CHECKPOINT_FAILPOINT.with(|failpoint| failpoint.get()) == Some(stage) {
        return Err(SkeinError::Storage(format!(
            "injected checkpoint failure at {stage:?}"
        )));
    }
    let _ = stage;
    Ok(())
}

#[cfg(test)]
fn set_checkpoint_failpoint(stage: Option<CheckpointPublishStage>) {
    CHECKPOINT_FAILPOINT.with(|failpoint| failpoint.set(stage));
}

#[cfg(test)]
thread_local! {
    static GENERATION_RECLAMATION_REMOVE_FAILPOINT: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

fn remove_generation_reclamation_candidate(path: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    {
        let file_name = path.file_name().and_then(|name| name.to_str());
        let should_fail = GENERATION_RECLAMATION_REMOVE_FAILPOINT
            .with(|failpoint| failpoint.borrow().as_deref() == file_name);
        if should_fail {
            GENERATION_RECLAMATION_REMOVE_FAILPOINT.with(|failpoint| {
                failpoint.borrow_mut().take();
            });
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "injected generation reclamation failure for {}",
                    path.display()
                ),
            ));
        }
    }
    fs::remove_file(path)
}

#[cfg(test)]
fn set_generation_reclamation_remove_failpoint(file_name: Option<String>) {
    GENERATION_RECLAMATION_REMOVE_FAILPOINT.with(|failpoint| {
        *failpoint.borrow_mut() = file_name;
    });
}

#[cfg(test)]
thread_local! {
    static WAL_APPLY_FAILPOINT_REMAINING: std::cell::Cell<Option<usize>> = const {
        std::cell::Cell::new(None)
    };
    static WAL_GROUP_SYNC_FAILPOINT: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

fn wal_apply_failpoint() -> Result<()> {
    #[cfg(test)]
    {
        let should_fail = WAL_APPLY_FAILPOINT_REMAINING.with(|remaining| match remaining.get() {
            Some(0) => true,
            Some(value) => {
                remaining.set(Some(value - 1));
                false
            }
            None => false,
        });
        if should_fail {
            return Err(SkeinError::Storage(
                "injected failure while applying a durable WAL batch".to_string(),
            ));
        }
    }
    Ok(())
}

/// Test support: renders binary WAL records as deterministic text lines.
/// The on-disk v1 format remains binary; this representation is never read
/// by recovery. A torn tail ends the rendering, while corruption appends a
/// terminal marker so damaged-file comparisons remain deterministic.
#[cfg(test)]
pub(crate) fn render_wal_records_for_test(path: &Path) -> std::io::Result<String> {
    use std::io::{Error, ErrorKind};
    let invalid = |reason: String| Error::new(ErrorKind::InvalidData, reason);
    let mut cursor =
        match WalRecordCursor::open(path, None).map_err(|error| invalid(error.to_string()))? {
            WalOpenOutcome::Cursor(cursor) => cursor,
            WalOpenOutcome::MissingHeader => {
                return Err(invalid("WAL is missing its header".to_string()));
            }
            WalOpenOutcome::HeaderTorn { reason } | WalOpenOutcome::HeaderCorrupt { reason } => {
                return Err(invalid(reason));
            }
        };
    let mut out = String::new();
    loop {
        match cursor.next().map_err(|error| invalid(error.to_string()))? {
            WalCursorEvent::Entry { entry, .. } => {
                out.push_str(&entry.encode().map_err(|error| invalid(error.to_string()))?);
                out.push('\n');
            }
            WalCursorEvent::Corrupt { offset, reason } => {
                out.push_str(&format!("<corrupt at {offset}: {reason}>\n"));
                break;
            }
            WalCursorEvent::TornTail { .. } | WalCursorEvent::Eof => break,
        }
    }
    Ok(out)
}

/// Test support: appends one well-formed framed record carrying a stale
/// WAL generation, emulating a recycled-log region past the logical tail.
/// Recovery must read it as clean end of log.
#[cfg(test)]
pub(crate) fn append_stale_generation_wal_fragment(path: &Path) -> Result<()> {
    let bytes = fs::read(path)?;
    let (generation, _) = wal_codec::frame::decode_binary_wal_header(&bytes)?;
    let position = bytes.len() as u64 - WAL_BINARY_FILE_HEADER_BYTES as u64;
    let stale_generation = generation.wrapping_sub(1);
    let framed = frame_binary_wal_record(
        stale_generation,
        b"recycled-region-record-from-a-previous-generation",
        position,
    );
    let mut file = OpenOptions::new().append(true).open(path)?;
    file.write_all(&framed)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn set_wal_apply_failpoint(operations_before_failure: Option<usize>) {
    WAL_APPLY_FAILPOINT_REMAINING.with(|remaining| remaining.set(operations_before_failure));
}

#[cfg(test)]
pub(crate) fn set_wal_group_sync_failpoint(enabled: bool) {
    WAL_GROUP_SYNC_FAILPOINT.with(|failpoint| failpoint.set(enabled));
}

fn wal_group_sync_failpoint() -> Result<()> {
    #[cfg(test)]
    if WAL_GROUP_SYNC_FAILPOINT.with(std::cell::Cell::take) {
        return Err(SkeinError::Storage(
            "injected WAL group sync failure".to_string(),
        ));
    }
    Ok(())
}

type PendingNode = (NodeId, LabelId, BTreeMap<String, Value>);
type PendingRelationship = (RelId, NodeId, NodeId, RelTypeId, BTreeMap<String, Value>);
pub type GraphSnapshotNodeImport = (NodeId, String, BTreeMap<String, Value>);
pub type GraphSnapshotRelationshipImport = (RelId, NodeId, NodeId, String, BTreeMap<String, Value>);

pub(crate) struct SkeinSnapshotRowsImport {
    pub stable_id_mapping: StoreStableIdMapping,
    pub source_fingerprint: String,
    pub nodes: Vec<GraphSnapshotNodeImport>,
    pub relationships: Vec<GraphSnapshotRelationshipImport>,
    pub relational_state: RelationalState,
    pub target_has_only_engine_bootstrap: bool,
}

#[derive(Debug, Clone)]
struct RelationshipCandidate {
    source: NodeId,
    target: NodeId,
    properties: BTreeMap<String, Value>,
}

struct RelationshipMatchRequest<'a> {
    rel_type_id: RelTypeId,
    source_label_id: Option<LabelId>,
    source_filter: Option<&'a PropertyFilter>,
    target_label_id: Option<LabelId>,
    target_filter: Option<&'a PropertyFilter>,
    rel_properties: &'a BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdjacencyConsistencyReport {
    pub ready: bool,
    pub computed_at_commit_epoch: u64,
    pub relationship_count: usize,
    pub maintained_group_count: usize,
    pub recomputed_group_count: usize,
    pub dense_group_count: usize,
    pub missing_group_count: usize,
    pub extra_group_count: usize,
    pub mismatched_group_count: usize,
    pub dangling_relationship_count: usize,
    pub mismatches: Vec<AdjacencyGroupConsistencyMismatch>,
    pub dangling_relationship_ids: Vec<RelId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DegreeStatisticsKey {
    pub label_id: LabelId,
    pub rel_type: RelTypeId,
    pub direction: AdjacencyDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DegreeStatisticsEntry {
    pub node_count: u64,
    pub non_zero_node_count: u64,
    pub relationship_count: u64,
    pub max_degree: u64,
    pub dense_node_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DegreeStatisticsConsistencyReport {
    pub ready: bool,
    pub computed_at_commit_epoch: u64,
    pub maintained: BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry>,
    pub recomputed: BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry>,
    pub mismatched_keys: Vec<DegreeStatisticsKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistinctValueStatisticsConsistencyReport {
    pub ready: bool,
    pub computed_at_commit_epoch: u64,
    pub maintained_property_distinct_counts: BTreeMap<(LabelId, String), u64>,
    pub recomputed_property_distinct_counts: BTreeMap<(LabelId, String), u64>,
    pub maintained_rel_property_distinct_counts: BTreeMap<(RelTypeId, String), u64>,
    pub recomputed_rel_property_distinct_counts: BTreeMap<(RelTypeId, String), u64>,
    pub mismatched_property_keys: Vec<(LabelId, String)>,
    pub mismatched_rel_property_keys: Vec<(RelTypeId, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyIndexConsistencyReport {
    pub ready: bool,
    pub computed_at_commit_epoch: u64,
    pub node_index_entry_count: usize,
    pub recomputed_node_index_entry_count: usize,
    pub node_index_reference_count: usize,
    pub recomputed_node_index_reference_count: usize,
    pub missing_node_key_count: usize,
    pub extra_node_key_count: usize,
    pub mismatched_node_key_count: usize,
    pub relationship_index_entry_count: usize,
    pub recomputed_relationship_index_entry_count: usize,
    pub relationship_index_reference_count: usize,
    pub recomputed_relationship_index_reference_count: usize,
    pub missing_relationship_key_count: usize,
    pub extra_relationship_key_count: usize,
    pub mismatched_relationship_key_count: usize,
    pub mismatched_node_keys: Vec<(LabelId, String, Value)>,
    pub mismatched_relationship_keys: Vec<(RelTypeId, String, Value)>,
}

type CompositePropertyKey = Vec<(String, Value)>;
type NodeIdPostingList = CowSegment<BTreeSet<NodeId>>;
type RelIdPropertyPostingList = CowSegment<BTreeSet<RelId>>;
type NodePropertyIndex = CowSegmentedMap<(LabelId, String, Value), NodeIdPostingList>;
type CompositePropertyIndex = CowSegmentedMap<(LabelId, CompositePropertyKey), NodeIdPostingList>;
type FullTextPropertyIndex = CowSegmentedMap<(LabelId, String, String), NodeIdPostingList>;
type RelationshipPropertyIndex =
    CowSegmentedMap<(RelTypeId, String, Value), RelIdPropertyPostingList>;

#[derive(Debug, Clone)]
pub struct ScanPrunedNodeScan<'a> {
    pub nodes: Vec<&'a NodeRecord>,
    pub report: ScanPruningReport,
}

#[derive(Debug, Clone)]
pub struct ScanPrunedRelationshipScan<'a> {
    pub relationships: Vec<&'a RelRecord>,
    pub report: ScanPruningReport,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KernelWriteBatch {
    pub graph: Vec<GraphMutation>,
    pub relational: RelationalTransaction,
    pub append: AppendTransaction,
}

#[derive(Default)]
struct MutationCommitOptions<'a> {
    relational: Option<RelationalTransaction>,
    append: Option<AppendTransaction>,
    preserve_single_create_wal: bool,
    captured_graph_ops: Option<&'a mut Vec<WalOp>>,
}

/// An isolated graph mutation workspace for a single explicit transaction.
///
/// The workspace applies each statement to a COW snapshot so subsequent reads
/// observe prior writes. It also retains the exact low-level operations chosen
/// by each statement; the live store publishes those operations as one WAL
/// batch instead of recomputing predicates against the pre-transaction state.
#[derive(Debug)]
pub(crate) struct GraphMutationTransaction {
    base_commit_epoch: u64,
    catalog: Catalog,
    store: GraphStore,
    ops: Vec<WalOp>,
    rows: Vec<BTreeMap<String, Value>>,
}

#[derive(Debug)]
pub(crate) struct GraphMutationSavepoint {
    catalog: Catalog,
    store: GraphStore,
    op_len: usize,
    row_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct GraphAdjacencyLockIdentity {
    pub(crate) node_id: NodeId,
    pub(crate) rel_type: Option<RelTypeId>,
    pub(crate) direction: AdjacencyDirection,
}

#[derive(Debug, Default)]
pub(crate) struct GraphMutationLockFootprint {
    pub(crate) requires_database_lock: bool,
    pub(crate) allocates_node_ids: bool,
    pub(crate) allocates_relationship_ids: bool,
    pub(crate) node_label_names: BTreeSet<String>,
    pub(crate) exclusive_node_label_names: BTreeSet<String>,
    pub(crate) node_label_read_names: BTreeSet<String>,
    pub(crate) relationship_type_names: BTreeSet<String>,
    pub(crate) exclusive_relationship_type_names: BTreeSet<String>,
    pub(crate) node_writes: BTreeSet<NodeId>,
    pub(crate) relationship_writes: BTreeSet<RelId>,
    pub(crate) node_delete_guard_reads: BTreeSet<NodeId>,
    pub(crate) node_delete_guard_writes: BTreeSet<NodeId>,
    pub(crate) adjacency_writes: BTreeSet<GraphAdjacencyLockIdentity>,
}

fn ensure_mutation_commit_limits(
    ops: &[WalOp],
    rows: &[BTreeMap<String, Value>],
    limits: MutationLimits,
) -> Result<()> {
    ensure_additional_mutation_limits(ops.len(), rows.len(), 0, 0, limits)?;
    if rows.len() > limits.max_result_rows.get() {
        return Err(SkeinError::Execution(format!(
            "mutation would exceed max_mutation_result_rows {}",
            limits.max_result_rows
        )));
    }
    let payload_bytes = rows.iter().fold(0u64, |total, row| {
        total.saturating_add(row.iter().fold(0u64, |row_total, (name, value)| {
            row_total
                .saturating_add(name.len() as u64)
                .saturating_add(estimated_value_bytes(value))
        }))
    });
    if payload_bytes > limits.max_result_payload_bytes.get() as u64 {
        return Err(SkeinError::Execution(format!(
            "mutation result payload would exceed max_mutation_result_payload_bytes {}",
            limits.max_result_payload_bytes
        )));
    }
    Ok(())
}

fn ensure_additional_mutation_limits(
    operation_count: usize,
    affected_row_count: usize,
    additional_operations: usize,
    additional_affected_rows: usize,
    limits: MutationLimits,
) -> Result<()> {
    let next_operations = operation_count
        .checked_add(additional_operations)
        .ok_or_else(|| SkeinError::Execution("mutation operation count overflow".to_string()))?;
    if next_operations > limits.max_operations.get() {
        return Err(SkeinError::Execution(format!(
            "mutation would exceed max_mutation_operations {}",
            limits.max_operations
        )));
    }
    let next_affected_rows = affected_row_count
        .checked_add(additional_affected_rows)
        .ok_or_else(|| SkeinError::Execution("mutation affected-row count overflow".to_string()))?;
    if next_affected_rows > limits.max_affected_rows.get() {
        return Err(SkeinError::Execution(format!(
            "mutation would exceed max_mutation_affected_rows {}",
            limits.max_affected_rows
        )));
    }
    Ok(())
}

fn remaining_mutation_affected_rows(current: usize, limits: MutationLimits) -> Result<usize> {
    limits
        .max_affected_rows
        .get()
        .checked_sub(current)
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "mutation would exceed max_mutation_affected_rows {}",
                limits.max_affected_rows
            ))
        })
}

fn remaining_mutation_operations(current: usize, limits: MutationLimits) -> Result<usize> {
    limits
        .max_operations
        .get()
        .checked_sub(current)
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "mutation would exceed max_mutation_operations {}",
                limits.max_operations
            ))
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasicStatisticsConsistencyReport {
    pub ready: bool,
    pub computed_at_commit_epoch: u64,
    pub incremental: BasicGraphStatistics,
    pub recomputed: BasicGraphStatistics,
    pub mismatched_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdjacencyConsolidationPlan {
    pub group_count: usize,
    pub delta_entry_count: usize,
    pub estimated_entries: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AdjacencyConsolidationReport {
    pub planned: AdjacencyConsolidationPlan,
    pub consolidated_group_count: usize,
    pub consolidated_delta_entry_count: usize,
    pub consolidated_estimated_entries: usize,
    pub remaining: AdjacencyConsolidationPlan,
}

#[derive(Debug, Clone, Copy)]
struct AdjacencyConsolidationCandidate {
    direction: AdjacencyDirection,
    key: (NodeId, RelTypeId),
    delta_entry_count: usize,
    estimated_entries: usize,
}

impl BasicStatisticsConsistencyReport {
    fn new(incremental: BasicGraphStatistics, recomputed: BasicGraphStatistics) -> Self {
        let mut mismatched_fields = Vec::new();
        if incremental.computed_at_commit_epoch != recomputed.computed_at_commit_epoch {
            mismatched_fields.push("computed_at_commit_epoch".to_string());
        }
        if incremental.node_count != recomputed.node_count {
            mismatched_fields.push("node_count".to_string());
        }
        if incremental.relationship_count != recomputed.relationship_count {
            mismatched_fields.push("relationship_count".to_string());
        }
        if incremental.label_counts != recomputed.label_counts {
            mismatched_fields.push("label_counts".to_string());
        }
        if incremental.rel_type_counts != recomputed.rel_type_counts {
            mismatched_fields.push("rel_type_counts".to_string());
        }
        Self {
            ready: mismatched_fields.is_empty(),
            computed_at_commit_epoch: incremental.computed_at_commit_epoch,
            incremental,
            recomputed,
            mismatched_fields,
        }
    }
}

type AdjacencyGroups = BTreeMap<AdjacencyGroupKey, BTreeSet<RelId>>;

impl AdjacencyConsistencyReport {
    fn new(
        computed_at_commit_epoch: u64,
        relationship_count: usize,
        maintained: AdjacencyGroups,
        recomputed: AdjacencyGroups,
        relationships: &CowSegmentedMap<RelId, RelRecord>,
    ) -> Self {
        let mut missing_group_count = 0;
        let mut extra_group_count = 0;
        let mut mismatched_group_count = 0;
        let mut mismatches = Vec::new();
        let keys = maintained
            .keys()
            .chain(recomputed.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        for key in keys {
            let maintained_ids = maintained.get(&key);
            let recomputed_ids = recomputed.get(&key);
            if maintained_ids == recomputed_ids {
                continue;
            }
            match (maintained_ids, recomputed_ids) {
                (None, Some(_)) => missing_group_count += 1,
                (Some(_), None) => extra_group_count += 1,
                (Some(_), Some(_)) => mismatched_group_count += 1,
                (None, None) => {}
            }
            if mismatches.len() < MAX_ADJACENCY_CONSISTENCY_SAMPLES {
                mismatches.push(AdjacencyGroupConsistencyMismatch {
                    key,
                    maintained_relationship_ids: maintained_ids
                        .map(sample_relationship_ids)
                        .unwrap_or_default(),
                    recomputed_relationship_ids: recomputed_ids
                        .map(sample_relationship_ids)
                        .unwrap_or_default(),
                });
            }
        }
        let dangling_relationships = maintained
            .values()
            .flat_map(|rel_ids| rel_ids.iter().copied())
            .filter(|rel_id| !relationships.contains_key(rel_id))
            .collect::<BTreeSet<_>>();
        let dangling_relationship_ids = dangling_relationships
            .iter()
            .copied()
            .take(MAX_ADJACENCY_CONSISTENCY_SAMPLES)
            .collect::<Vec<_>>();
        let dangling_relationship_count = dangling_relationships.len();
        let ready = missing_group_count == 0
            && extra_group_count == 0
            && mismatched_group_count == 0
            && dangling_relationship_count == 0;
        Self {
            ready,
            computed_at_commit_epoch,
            relationship_count,
            maintained_group_count: maintained.len(),
            recomputed_group_count: recomputed.len(),
            dense_group_count: maintained
                .values()
                .filter(|rel_ids| rel_ids.len() >= DENSE_ADJACENCY_DEGREE_THRESHOLD)
                .count(),
            missing_group_count,
            extra_group_count,
            mismatched_group_count,
            dangling_relationship_count,
            mismatches,
            dangling_relationship_ids,
        }
    }
}

impl DegreeStatisticsConsistencyReport {
    fn new(
        computed_at_commit_epoch: u64,
        maintained: BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry>,
        recomputed: BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry>,
    ) -> Self {
        let mismatched_keys = maintained
            .keys()
            .chain(recomputed.keys())
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|key| maintained.get(key) != recomputed.get(key))
            .take(MAX_ADJACENCY_CONSISTENCY_SAMPLES)
            .collect::<Vec<_>>();
        Self {
            ready: mismatched_keys.is_empty(),
            computed_at_commit_epoch,
            maintained,
            recomputed,
            mismatched_keys,
        }
    }
}

impl DistinctValueStatisticsConsistencyReport {
    fn new(
        computed_at_commit_epoch: u64,
        maintained_property_distinct_counts: BTreeMap<(LabelId, String), u64>,
        recomputed_property_distinct_counts: BTreeMap<(LabelId, String), u64>,
        maintained_rel_property_distinct_counts: BTreeMap<(RelTypeId, String), u64>,
        recomputed_rel_property_distinct_counts: BTreeMap<(RelTypeId, String), u64>,
    ) -> Self {
        let mismatched_property_keys = maintained_property_distinct_counts
            .keys()
            .chain(recomputed_property_distinct_counts.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|key| {
                maintained_property_distinct_counts.get(key)
                    != recomputed_property_distinct_counts.get(key)
            })
            .take(MAX_ADJACENCY_CONSISTENCY_SAMPLES)
            .collect::<Vec<_>>();
        let mismatched_rel_property_keys = maintained_rel_property_distinct_counts
            .keys()
            .chain(recomputed_rel_property_distinct_counts.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter(|key| {
                maintained_rel_property_distinct_counts.get(key)
                    != recomputed_rel_property_distinct_counts.get(key)
            })
            .take(MAX_ADJACENCY_CONSISTENCY_SAMPLES)
            .collect::<Vec<_>>();
        Self {
            ready: mismatched_property_keys.is_empty() && mismatched_rel_property_keys.is_empty(),
            computed_at_commit_epoch,
            maintained_property_distinct_counts,
            recomputed_property_distinct_counts,
            maintained_rel_property_distinct_counts,
            recomputed_rel_property_distinct_counts,
            mismatched_property_keys,
            mismatched_rel_property_keys,
        }
    }
}

impl PropertyIndexConsistencyReport {
    fn new(
        computed_at_commit_epoch: u64,
        maintained_node_index: &NodePropertyIndex,
        recomputed_node_index: &NodePropertyIndex,
        maintained_relationship_index: &RelationshipPropertyIndex,
        recomputed_relationship_index: &RelationshipPropertyIndex,
    ) -> Self {
        let (
            missing_node_key_count,
            extra_node_key_count,
            mismatched_node_key_count,
            mismatched_node_keys,
        ) = property_index_mismatch_summary(maintained_node_index, recomputed_node_index);
        let (
            missing_relationship_key_count,
            extra_relationship_key_count,
            mismatched_relationship_key_count,
            mismatched_relationship_keys,
        ) = relationship_property_index_mismatch_summary(
            maintained_relationship_index,
            recomputed_relationship_index,
        );
        Self {
            ready: missing_node_key_count == 0
                && extra_node_key_count == 0
                && mismatched_node_key_count == 0
                && missing_relationship_key_count == 0
                && extra_relationship_key_count == 0
                && mismatched_relationship_key_count == 0,
            computed_at_commit_epoch,
            node_index_entry_count: maintained_node_index.len(),
            recomputed_node_index_entry_count: recomputed_node_index.len(),
            node_index_reference_count: node_property_index_reference_count(maintained_node_index),
            recomputed_node_index_reference_count: node_property_index_reference_count(
                recomputed_node_index,
            ),
            missing_node_key_count,
            extra_node_key_count,
            mismatched_node_key_count,
            relationship_index_entry_count: maintained_relationship_index.len(),
            recomputed_relationship_index_entry_count: recomputed_relationship_index.len(),
            relationship_index_reference_count: relationship_property_index_reference_count(
                maintained_relationship_index,
            ),
            recomputed_relationship_index_reference_count:
                relationship_property_index_reference_count(recomputed_relationship_index),
            missing_relationship_key_count,
            extra_relationship_key_count,
            mismatched_relationship_key_count,
            mismatched_node_keys,
            mismatched_relationship_keys,
        }
    }
}

/// Mutation domains that invalidate an out-of-core advanced-statistics snapshot.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdvancedStatisticsDirtyState {
    node_topology: bool,
    relationship_topology: bool,
    node_properties: bool,
    relationship_properties: bool,
    mutation_operations: u64,
}

impl AdvancedStatisticsDirtyState {
    fn mark_node_topology(&mut self) {
        self.node_topology = true;
        self.node_properties = true;
        self.mutation_operations = self.mutation_operations.saturating_add(1);
    }

    fn mark_relationship_topology(&mut self) {
        self.relationship_topology = true;
        self.relationship_properties = true;
        self.mutation_operations = self.mutation_operations.saturating_add(1);
    }

    fn mark_node_properties(&mut self) {
        self.node_properties = true;
        self.mutation_operations = self.mutation_operations.saturating_add(1);
    }

    fn mark_relationship_properties(&mut self) {
        self.relationship_properties = true;
        self.mutation_operations = self.mutation_operations.saturating_add(1);
    }

    fn is_empty(self) -> bool {
        !self.node_topology
            && !self.relationship_topology
            && !self.node_properties
            && !self.relationship_properties
    }

    fn mutation_operations(self) -> u64 {
        self.mutation_operations
    }
}

#[derive(Debug, Default)]
pub struct GraphStore {
    next_node_id: u64,
    next_rel_id: u64,
    commit_epoch: u64,
    nodes: CowSegmentedMap<NodeId, NodeRecord>,
    relationships: CowSegmentedMap<RelId, RelRecord>,
    basic_statistics: BasicGraphStatistics,
    checkpoint_statistics: GraphStatistics,
    advanced_statistics_dirty: AdvancedStatisticsDirtyState,
    outgoing: CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
    incoming: CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
    property_index: NodePropertyIndex,
    composite_property_index: CompositePropertyIndex,
    full_text_property_index: FullTextPropertyIndex,
    relationship_property_index: RelationshipPropertyIndex,
    projected_graphs: CowSegment<BTreeMap<String, ProjectedGraphDefinition>>,
    projected_graph_artifacts: CowSegment<BTreeMap<String, ProjectedGraphArtifact>>,
    stable_id_mapping: CowSegment<StoreStableIdMapping>,
    initial_import_source_fingerprint: Option<String>,
    search_projection_change_log_start_epoch: u64,
    search_projection_graph_changes: CowSegment<Vec<SearchProjectionGraphChange>>,
    search_projection_change_log_retained_bytes: usize,
    max_search_projection_change_log_entries: Option<usize>,
    max_search_projection_change_log_bytes: Option<usize>,
    search_projection_primary_key_capture_limits:
        skein_storage::RelationalPrimaryKeyChangeCaptureLimits,
    source_scan_manifest: CowSegment<Option<ScanSegmentManifest>>,
    storage_recovery_report: StorageRecoveryReport,
    canonical_base: Option<CanonicalSegmentReader>,
    canonical_adjacency: Option<CanonicalAdjacencyReader>,
    persistent_property_projection: Option<PersistentPropertyProjectionReader>,
    graph_index_read_metrics: Arc<GraphIndexReadMetrics>,
    canonical_base_out_of_core: bool,
    node_tombstones: CowSegment<BTreeSet<NodeId>>,
    relationship_tombstones: CowSegment<BTreeSet<RelId>>,
    residency_mode: StorageResidencyMode,
    auto_materialize_checkpoint_bytes: u64,
    max_out_of_core_delta_bytes: Option<u64>,
    post_wal_apply_poisoned: bool,
    integrity_poisoned: Arc<AtomicBool>,
    relational_state: RelationalState,
    append_state: AppendState,
    append_mutation_limits: AppendMutationLimits,
    append_publication_config: AppendPublicationConfig,
    append_generation_reader: Option<AppendGenerationReader>,
    relational_mutation_limits: RelationalMutationLimits,
    relational_overflow_config: RelationalOverflowConfig,
    columnar_shadow: ColumnarShadowState,
    relational_index_shadow: RelationalIndexShadowState,
    relational_row_pages: RelationalRowPageState,
    projection_generations: Option<skein_storage::ProjectionGenerationStore>,
    /// The engine's runtime governor, threaded down from the embedding
    /// layer (`SkeinEmbedded` / `NowledgeMemGraph`) so background shadow
    /// work can request admission. The store never constructs its own.
    runtime_governor: Option<Arc<dyn skein_storage::BackgroundWorkAdmission>>,
    durable: Option<DurableStore>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphScanControl {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalOverflowCompactionConfig {
    pub max_scan_rows: NonZeroUsize,
    pub max_scan_pages: NonZeroUsize,
    pub max_scan_bytes: NonZeroUsize,
    pub max_overlay_entries: NonZeroUsize,
    pub max_overlay_bytes: NonZeroUsize,
    pub max_rewrite_bytes: NonZeroU64,
    pub reference_sort: skein_storage::RelationalOverflowReferenceSortConfig,
}

impl Default for RelationalOverflowCompactionConfig {
    fn default() -> Self {
        Self {
            max_scan_rows: NonZeroUsize::new(100_000_000)
                .expect("default overflow compaction row limit is non-zero"),
            max_scan_pages: NonZeroUsize::new(1_000_000)
                .expect("default overflow compaction page limit is non-zero"),
            max_scan_bytes: NonZeroUsize::new(1024usize.saturating_mul(1024 * 1024 * 1024))
                .expect("default overflow compaction read-byte limit is non-zero"),
            max_overlay_entries: NonZeroUsize::new(
                skein_storage::DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_ENTRIES,
            )
            .expect("default overflow compaction overlay entry limit is non-zero"),
            max_overlay_bytes: NonZeroUsize::new(
                skein_storage::DEFAULT_RELATIONAL_ROW_SNAPSHOT_OVERLAY_BYTES,
            )
            .expect("default overflow compaction overlay byte limit is non-zero"),
            max_rewrite_bytes: NonZeroU64::new(128 * 1024 * 1024 * 1024)
                .expect("default overflow compaction rewrite limit is non-zero"),
            reference_sort: skein_storage::RelationalOverflowReferenceSortConfig::default(),
        }
    }
}

impl RelationalOverflowCompactionConfig {
    pub fn admission_bytes(self) -> Result<u64> {
        let sort_bytes =
            u64::try_from(self.reference_sort.max_memory_bytes.get()).map_err(|_| {
                SkeinError::Storage(
                    "overflow compaction sort memory exceeds this target".to_string(),
                )
            })?;
        let overlay_bytes = u64::try_from(self.max_overlay_bytes.get()).map_err(|_| {
            SkeinError::Storage(
                "overflow compaction overlay memory exceeds this target".to_string(),
            )
        })?;
        let page_bytes = skein_storage::DEFAULT_RELATIONAL_ROW_PAGE_BYTES as u64;
        sort_bytes
            .checked_add(overlay_bytes)
            .and_then(|bytes| bytes.checked_add(page_bytes.saturating_mul(2)))
            .and_then(|bytes| {
                bytes.checked_add(
                    (skein_storage::DEFAULT_MAX_RELATIONAL_HYDRATION_BYTES as u64)
                        .saturating_mul(2),
                )
            })
            .ok_or_else(|| {
                SkeinError::Storage("overflow compaction admission byte count overflow".to_string())
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalOverflowCompactionReport {
    pub source_commit_epoch: u64,
    pub published_generation: u64,
    pub tables_scanned: usize,
    pub rows_scanned: usize,
    pub pages_read: usize,
    pub row_bytes_read: usize,
    pub hydrated_values: usize,
    pub overlay_entries: usize,
    pub overlay_bytes: usize,
    pub reference_occurrences: u64,
    pub unique_references: u64,
    pub spill_run_count: usize,
    pub spill_bytes: u64,
    pub peak_sort_memory_bytes: usize,
    pub previous_extent_count: u64,
    pub published_extent_count: u64,
    pub reclaimable_base_extent_count: u64,
    pub new_extent_count: u64,
    pub reused_extent_count: u64,
    pub copied_base_extent_count: u64,
    pub introduced_extent_count: u64,
    pub admitted_memory_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageResidencyReport {
    pub out_of_core: bool,
    pub canonical_generation: Option<u64>,
    pub canonical_artifact_bytes: u64,
    pub canonical_adjacency_artifact_bytes: u64,
    pub persistent_property_projection_artifact_bytes: u64,
    pub canonical_node_count: u64,
    pub canonical_relationship_count: u64,
    pub delta_node_count: usize,
    pub delta_relationship_count: usize,
    pub node_tombstone_count: usize,
    pub relationship_tombstone_count: usize,
    pub estimated_delta_resident_bytes: u64,
    pub max_out_of_core_delta_bytes: Option<u64>,
    pub delta_within_budget: bool,
    pub checkpoint_statistics_commit_epoch: u64,
    pub checkpoint_statistics_complete: bool,
    pub checkpoint_statistics_stale: bool,
    pub graph_manifest_open_budget_bytes: u64,
    pub graph_manifest_encoded_bytes: u64,
    pub segment_cache_capacity_bytes: u64,
    pub segment_cache_resident_bytes: u64,
    pub segment_cache_pinned_bytes: u64,
    pub segment_cache_hit_count: u64,
    pub segment_cache_miss_count: u64,
    pub segment_cache_eviction_count: u64,
    pub segment_cache_admission_rejection_count: u64,
    pub segment_cache_digest_mismatch_count: u64,
    pub graph_index_reads: GraphIndexReadMetricsSnapshot,
    pub relational_rows: RelationalRowStorageResidencyReport,
    pub relational_indexes: RelationalIndexStorageResidencyReport,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationalRowStorageResidencyReport {
    pub serving: bool,
    pub materialized_rows_resident: bool,
    pub checkpoint_state_metadata_only: bool,
    pub materialized_row_count: usize,
    pub materialized_row_bytes: u64,
    pub logical_row_count: usize,
    pub base_generation: Option<u64>,
    pub recovery_delta_generation: Option<u64>,
    pub base_commit_epoch: Option<u64>,
    pub visible_commit_epoch: Option<u64>,
    pub root_page_count: u64,
    pub page_artifact_bytes: u64,
    pub root_descriptor_artifact_bytes: u64,
    pub root_key_artifact_bytes: u64,
    pub overflow_extent_count: u64,
    pub overflow_extent_artifact_bytes: u64,
    pub overflow_descriptor_artifact_bytes: u64,
    pub recovery_delta_runs: usize,
    pub recovery_delta_checkpoint_runs: usize,
    pub recovery_delta_checkpoint_recommended: bool,
    pub recovery_delta_entries: u64,
    pub recovery_delta_artifact_bytes: u64,
    pub live_batches: usize,
    pub live_entries: usize,
    pub live_encoded_bytes: usize,
    pub live_resident_bytes: usize,
    pub monotonic_append_attempts: u64,
    pub monotonic_append_hits: u64,
    pub monotonic_append_fallbacks: u64,
    pub monotonic_append_proven_absent_primary_keys: u64,
}

impl RelationalRowStorageResidencyReport {
    pub fn canonical_artifact_bytes(&self) -> u64 {
        self.page_artifact_bytes
            .saturating_add(self.root_descriptor_artifact_bytes)
            .saturating_add(self.root_key_artifact_bytes)
            .saturating_add(self.overflow_extent_artifact_bytes)
            .saturating_add(self.overflow_descriptor_artifact_bytes)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationalIndexStorageResidencyReport {
    pub serving: bool,
    pub base_generation: Option<u64>,
    pub recovery_delta_generation: Option<u64>,
    pub base_commit_epoch: Option<u64>,
    pub visible_commit_epoch: Option<u64>,
    pub root_count: usize,
    pub base_page_count: u64,
    pub base_artifact_bytes: u64,
    pub recovery_delta_pages: usize,
    pub recovery_delta_entries: usize,
    pub recovery_delta_artifact_bytes: u64,
    pub live_batches: usize,
    pub live_entries: usize,
    pub live_encoded_bytes: usize,
}

impl RelationalIndexStorageResidencyReport {
    pub fn canonical_artifact_bytes(&self) -> u64 {
        self.base_artifact_bytes
    }
}

pub struct GraphNodeIterator {
    base: Option<std::iter::Peekable<CanonicalNodeIterator>>,
    delta: std::iter::Peekable<std::vec::IntoIter<NodeRecord>>,
    tombstones: CowSegment<BTreeSet<NodeId>>,
}

impl Iterator for GraphNodeIterator {
    type Item = Result<NodeRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let base_id = match self.base.as_mut().and_then(|base| base.peek()) {
                Some(Ok(node)) => Some(node.id),
                Some(Err(_)) => {
                    return self
                        .base
                        .as_mut()
                        .and_then(Iterator::next)
                        .map(|record| record.map_err(canonical_segment_error));
                }
                None => None,
            };
            let delta_id = self.delta.peek().map(|node| node.id);
            match (base_id, delta_id) {
                (None, None) => return None,
                (Some(_), None) => {
                    let record = self
                        .base
                        .as_mut()
                        .and_then(Iterator::next)
                        .expect("peeked base node exists")
                        .map_err(canonical_segment_error);
                    match record {
                        Ok(node) if self.tombstones.contains(&node.id) => continue,
                        other => return Some(other),
                    }
                }
                (None, Some(_)) => {
                    let node = self.delta.next().expect("peeked delta node exists");
                    if self.tombstones.contains(&node.id) {
                        continue;
                    }
                    return Some(Ok(node));
                }
                (Some(base_id), Some(delta_id)) if base_id < delta_id => {
                    let record = self
                        .base
                        .as_mut()
                        .and_then(Iterator::next)
                        .expect("peeked base node exists")
                        .map_err(canonical_segment_error);
                    match record {
                        Ok(node) if self.tombstones.contains(&node.id) => continue,
                        other => return Some(other),
                    }
                }
                (Some(base_id), Some(delta_id)) if base_id == delta_id => {
                    if let Err(error) = self
                        .base
                        .as_mut()
                        .and_then(Iterator::next)
                        .expect("peeked base node exists")
                        .map_err(canonical_segment_error)
                    {
                        return Some(Err(error));
                    }
                    let node = self.delta.next().expect("matching delta node exists");
                    if self.tombstones.contains(&node.id) {
                        continue;
                    }
                    return Some(Ok(node));
                }
                (Some(_), Some(_)) => {
                    let node = self.delta.next().expect("peeked delta node exists");
                    if self.tombstones.contains(&node.id) {
                        continue;
                    }
                    return Some(Ok(node));
                }
            }
        }
    }
}

pub struct GraphRelationshipIterator {
    base: Option<std::iter::Peekable<CanonicalRelationshipIterator>>,
    delta: std::iter::Peekable<std::vec::IntoIter<RelRecord>>,
    tombstones: CowSegment<BTreeSet<RelId>>,
}

impl Iterator for GraphRelationshipIterator {
    type Item = Result<RelRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let base_id = match self.base.as_mut().and_then(|base| base.peek()) {
                Some(Ok(relationship)) => Some(relationship.id),
                Some(Err(_)) => {
                    return self
                        .base
                        .as_mut()
                        .and_then(Iterator::next)
                        .map(|record| record.map_err(canonical_segment_error));
                }
                None => None,
            };
            let delta_id = self.delta.peek().map(|relationship| relationship.id);
            match (base_id, delta_id) {
                (None, None) => return None,
                (Some(_), None) => {
                    let record = self
                        .base
                        .as_mut()
                        .and_then(Iterator::next)
                        .expect("peeked base relationship exists")
                        .map_err(canonical_segment_error);
                    match record {
                        Ok(relationship) if self.tombstones.contains(&relationship.id) => continue,
                        other => return Some(other),
                    }
                }
                (None, Some(_)) => {
                    let relationship = self.delta.next().expect("peeked delta relationship exists");
                    if self.tombstones.contains(&relationship.id) {
                        continue;
                    }
                    return Some(Ok(relationship));
                }
                (Some(base_id), Some(delta_id)) if base_id < delta_id => {
                    let record = self
                        .base
                        .as_mut()
                        .and_then(Iterator::next)
                        .expect("peeked base relationship exists")
                        .map_err(canonical_segment_error);
                    match record {
                        Ok(relationship) if self.tombstones.contains(&relationship.id) => continue,
                        other => return Some(other),
                    }
                }
                (Some(base_id), Some(delta_id)) if base_id == delta_id => {
                    if let Err(error) = self
                        .base
                        .as_mut()
                        .and_then(Iterator::next)
                        .expect("peeked base relationship exists")
                        .map_err(canonical_segment_error)
                    {
                        return Some(Err(error));
                    }
                    let relationship = self
                        .delta
                        .next()
                        .expect("matching delta relationship exists");
                    if self.tombstones.contains(&relationship.id) {
                        continue;
                    }
                    return Some(Ok(relationship));
                }
                (Some(_), Some(_)) => {
                    let relationship = self.delta.next().expect("peeked delta relationship exists");
                    if self.tombstones.contains(&relationship.id) {
                        continue;
                    }
                    return Some(Ok(relationship));
                }
            }
        }
    }
}

fn canonical_segment_error(error: CanonicalSegmentError) -> SkeinError {
    SkeinError::StorageIntegrity(error.to_string())
}

/// Result of reading Source scan sidecar candidates. The rows have passed
/// segment pruning and exact local cursors only; query execution must still
/// apply residual predicates such as nested metadata and label aliases.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceScanCandidateRead {
    Rows {
        graph_epoch: u64,
        skipped_segment_count: usize,
        report: SegmentReadExecutionReport,
        rows: Vec<SourceScanRow>,
    },
    Fallback(ScanSegmentFallback),
}

/// Metadata returned after streaming checkpoint-published Source candidates.
///
/// Rows are intentionally consumed at the storage/executor boundary instead of
/// being retained in a database-sized intermediate collection. `Fallback` is
/// returned only before the first row can reach the consumer; failures after
/// streaming starts are reported as errors so callers never duplicate output.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SourceScanCandidateVisit {
    Rows {
        graph_epoch: u64,
        skipped_segment_count: usize,
        report: SegmentReadExecutionReport,
        candidate_count: usize,
    },
    Fallback(ScanSegmentFallback),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceScanCandidateLimits {
    io_depth: NonZeroUsize,
    max_coalesced_bytes: NonZeroU64,
    max_wave_bytes: NonZeroU64,
    max_live_candidate_bytes: Option<usize>,
}

impl SourceScanCandidateLimits {
    const fn unbounded(
        io_depth: NonZeroUsize,
        max_coalesced_bytes: NonZeroU64,
        max_wave_bytes: NonZeroU64,
    ) -> Self {
        Self {
            io_depth,
            max_coalesced_bytes,
            max_wave_bytes,
            max_live_candidate_bytes: None,
        }
    }

    pub(crate) const fn bounded(
        io_depth: NonZeroUsize,
        max_coalesced_bytes: NonZeroU64,
        max_wave_bytes: NonZeroU64,
        max_live_candidate_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            io_depth,
            max_coalesced_bytes,
            max_wave_bytes,
            max_live_candidate_bytes: Some(max_live_candidate_bytes.get()),
        }
    }
}

#[derive(Debug, Clone)]
struct ScanPruningCandidate {
    strategy: ScanPruningStrategy,
    node_ids: BTreeSet<NodeId>,
    exact_empty: bool,
}

impl ScanPruningCandidate {
    fn exact(strategy: ScanPruningStrategy, node_ids: BTreeSet<NodeId>) -> Self {
        Self {
            strategy,
            exact_empty: node_ids.is_empty(),
            node_ids,
        }
    }
}

#[derive(Debug, Clone)]
struct RelationshipScanPruningCandidate {
    strategy: ScanPruningStrategy,
    rel_ids: BTreeSet<RelId>,
    exact_empty: bool,
}

impl RelationshipScanPruningCandidate {
    fn exact(strategy: ScanPruningStrategy, rel_ids: BTreeSet<RelId>) -> Self {
        Self {
            strategy,
            exact_empty: rel_ids.is_empty(),
            rel_ids,
        }
    }
}

impl GraphMutationTransaction {
    pub(crate) fn is_read_only(&self) -> bool {
        self.ops.is_empty()
    }

    pub(crate) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub(crate) fn store(&self) -> &GraphStore {
        &self.store
    }

    pub(crate) fn catalog_and_store_mut(&mut self) -> (&mut Catalog, &mut GraphStore) {
        (&mut self.catalog, &mut self.store)
    }

    pub(crate) fn savepoint(&self) -> GraphMutationSavepoint {
        GraphMutationSavepoint {
            catalog: self.catalog.clone(),
            store: self.store.snapshot(),
            op_len: self.ops.len(),
            row_len: self.rows.len(),
        }
    }

    pub(crate) fn restore(&mut self, savepoint: GraphMutationSavepoint) {
        self.catalog = savepoint.catalog;
        self.store = savepoint.store;
        self.ops.truncate(savepoint.op_len);
        self.rows.truncate(savepoint.row_len);
    }

    pub(crate) fn lock_footprint_since(
        &self,
        savepoint: &GraphMutationSavepoint,
    ) -> Result<GraphMutationLockFootprint> {
        let mut footprint = GraphMutationLockFootprint::default();
        collect_graph_lock_footprint(
            &self.catalog,
            &savepoint.store,
            &self.store,
            &self.ops[savepoint.op_len..],
            &mut footprint,
        )?;
        Ok(footprint)
    }

    pub(crate) fn stage_mutation_with_limits(
        &mut self,
        mutation: GraphMutation,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.stage_mutation(mutation, limits, true)
    }

    pub(crate) fn stage_mutation_without_commit_rows(
        &mut self,
        mutation: GraphMutation,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.stage_mutation(mutation, limits, false)
    }

    fn stage_mutation(
        &mut self,
        mutation: GraphMutation,
        limits: MutationLimits,
        retain_commit_rows: bool,
    ) -> Result<MutationSummary> {
        let mut captured_ops = Vec::new();
        let summary = self.store.commit_mutations_internal(
            &mut self.catalog,
            vec![mutation],
            limits,
            MutationCommitOptions {
                captured_graph_ops: Some(&mut captured_ops),
                ..MutationCommitOptions::default()
            },
        )?;
        self.ops.extend(captured_ops);
        if retain_commit_rows {
            self.rows.extend(summary.rows.iter().cloned());
        }
        Ok(summary)
    }
}

fn collect_graph_lock_footprint(
    catalog: &Catalog,
    before: &GraphStore,
    store: &GraphStore,
    ops: &[WalOp],
    footprint: &mut GraphMutationLockFootprint,
) -> Result<()> {
    for op in ops {
        match op {
            WalOp::CreateNode { id, label, .. } => {
                footprint.allocates_node_ids = true;
                footprint.node_writes.insert(*id);
                footprint.node_label_names.insert(label.clone());
                footprint.exclusive_node_label_names.insert(label.clone());
            }
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type,
                ..
            } => {
                footprint.allocates_relationship_ids = true;
                footprint.relationship_writes.insert(*id);
                footprint.relationship_type_names.insert(rel_type.clone());
                footprint
                    .exclusive_relationship_type_names
                    .insert(rel_type.clone());
                for endpoint in [*source, *target] {
                    for label_id in store
                        .node_owned(endpoint)?
                        .into_iter()
                        .flat_map(|node| node.labels)
                    {
                        if let Some(label) = catalog.label_name(label_id) {
                            footprint.node_label_read_names.insert(label.to_string());
                        }
                    }
                }
                record_relationship_endpoint_locks(
                    footprint,
                    *source,
                    *target,
                    catalog.rel_type_id(rel_type),
                );
            }
            WalOp::SetNodeProperty { id, property, .. } => {
                footprint.node_writes.insert(*id);
                for label_id in store
                    .node_owned(*id)?
                    .into_iter()
                    .flat_map(|node| node.labels)
                {
                    if let Some(label) = catalog.label_name(label_id) {
                        footprint.node_label_names.insert(label.to_string());
                        if catalog.unique_constraints().any(|constraint| {
                            constraint.subject == crate::schema::ConstraintSubject::Node(label_id)
                                && constraint.property == *property
                        }) {
                            footprint
                                .exclusive_node_label_names
                                .insert(label.to_string());
                        }
                    }
                }
            }
            WalOp::SetRelationshipProperty { id, property, .. } => {
                footprint.relationship_writes.insert(*id);
                if let Some(relationship) = store.relationship_owned(*id)?
                    && let Some(rel_type) = catalog.rel_type_name(relationship.rel_type)
                {
                    footprint
                        .relationship_type_names
                        .insert(rel_type.to_string());
                    if catalog.relationship_unique_constraints().any(|constraint| {
                        constraint.subject
                            == crate::schema::ConstraintSubject::Relationship(relationship.rel_type)
                            && constraint.property == *property
                    }) {
                        footprint
                            .exclusive_relationship_type_names
                            .insert(rel_type.to_string());
                    }
                }
            }
            WalOp::DeleteNode { id } => {
                footprint.node_writes.insert(*id);
                footprint.node_delete_guard_writes.insert(*id);
                for label_id in before
                    .node_owned(*id)?
                    .into_iter()
                    .flat_map(|node| node.labels)
                {
                    if let Some(label) = catalog.label_name(label_id) {
                        footprint.node_label_names.insert(label.to_string());
                    }
                }
            }
            WalOp::DeleteRelationship { id } => {
                footprint.relationship_writes.insert(*id);
                let relationship = before.relationship_owned(*id)?.ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "deleted relationship {} is missing while deriving transaction locks",
                        id.0
                    ))
                })?;
                if let Some(rel_type) = catalog.rel_type_name(relationship.rel_type) {
                    footprint
                        .relationship_type_names
                        .insert(rel_type.to_string());
                }
                for endpoint in [relationship.source, relationship.target] {
                    for label_id in before
                        .node_owned(endpoint)?
                        .into_iter()
                        .flat_map(|node| node.labels)
                    {
                        if let Some(label) = catalog.label_name(label_id) {
                            footprint.node_label_read_names.insert(label.to_string());
                        }
                    }
                }
                record_relationship_endpoint_locks(
                    footprint,
                    relationship.source,
                    relationship.target,
                    Some(relationship.rel_type),
                );
            }
            WalOp::Batch(ops) => {
                collect_graph_lock_footprint(catalog, before, store, ops, footprint)?
            }
            WalOp::CreateNodeLabel { .. }
            | WalOp::CreateRelationshipType { .. }
            | WalOp::CreateNodeTable { .. }
            | WalOp::CreateRelationshipTable { .. }
            | WalOp::CreateProperty { .. }
            | WalOp::AlterTableState { .. }
            | WalOp::AlterPropertyState { .. }
            | WalOp::GcTableDescriptor { .. }
            | WalOp::GcPropertyDescriptor { .. }
            | WalOp::CreateIndex { .. }
            | WalOp::CreateCompositeIndex { .. }
            | WalOp::CreateRangeIndex { .. }
            | WalOp::CreateFullTextIndex { .. }
            | WalOp::CreateUniqueConstraint { .. }
            | WalOp::CreateNodePropertyExistsConstraint { .. }
            | WalOp::CreateRelationshipUniqueConstraint { .. }
            | WalOp::CreateRelationshipPropertyExistsConstraint { .. }
            | WalOp::ProjectGraph { .. }
            | WalOp::MarkInitialImportSource { .. }
            | WalOp::Relational { .. }
            | WalOp::RelationalSnapshot { .. }
            | WalOp::Append { .. } => footprint.requires_database_lock = true,
        }
    }
    Ok(())
}

fn record_relationship_endpoint_locks(
    footprint: &mut GraphMutationLockFootprint,
    source: NodeId,
    target: NodeId,
    rel_type: Option<RelTypeId>,
) {
    footprint.node_delete_guard_reads.insert(source);
    footprint.node_delete_guard_reads.insert(target);
    footprint
        .adjacency_writes
        .insert(GraphAdjacencyLockIdentity {
            node_id: source,
            rel_type,
            direction: AdjacencyDirection::Outgoing,
        });
    footprint
        .adjacency_writes
        .insert(GraphAdjacencyLockIdentity {
            node_id: target,
            rel_type,
            direction: AdjacencyDirection::Incoming,
        });
}

fn compact_transaction_graph_ops(ops: Vec<WalOp>) -> Vec<WalOp> {
    let mut compacted = Vec::<Option<WalOp>>::with_capacity(ops.len());
    let mut created_nodes = BTreeMap::<NodeId, usize>::new();
    let mut created_relationships = BTreeMap::<RelId, usize>::new();

    for op in ops {
        match op {
            WalOp::CreateNode {
                id,
                label,
                properties,
            } => {
                let index = compacted.len();
                compacted.push(Some(WalOp::CreateNode {
                    id,
                    label,
                    properties,
                }));
                created_nodes.insert(id, index);
            }
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => {
                let index = compacted.len();
                compacted.push(Some(WalOp::CreateRelationship {
                    id,
                    source,
                    target,
                    rel_type,
                    properties,
                }));
                created_relationships.insert(id, index);
            }
            WalOp::SetNodeProperty {
                id,
                property,
                value,
            } => {
                let folded = created_nodes.get(&id).is_some_and(|index| {
                    let Some(WalOp::CreateNode { properties, .. }) = compacted[*index].as_mut()
                    else {
                        return false;
                    };
                    properties.insert(property.clone(), value.clone());
                    true
                });
                if !folded {
                    compacted.push(Some(WalOp::SetNodeProperty {
                        id,
                        property,
                        value,
                    }));
                }
            }
            WalOp::SetRelationshipProperty {
                id,
                property,
                value,
            } => {
                let folded = created_relationships.get(&id).is_some_and(|index| {
                    let Some(WalOp::CreateRelationship { properties, .. }) =
                        compacted[*index].as_mut()
                    else {
                        return false;
                    };
                    properties.insert(property.clone(), value.clone());
                    true
                });
                if !folded {
                    compacted.push(Some(WalOp::SetRelationshipProperty {
                        id,
                        property,
                        value,
                    }));
                }
            }
            WalOp::DeleteRelationship { id } => {
                if let Some(index) = created_relationships.remove(&id) {
                    compacted[index] = None;
                } else {
                    compacted.push(Some(WalOp::DeleteRelationship { id }));
                }
            }
            WalOp::DeleteNode { id } => {
                if let Some(index) = created_nodes.remove(&id) {
                    compacted[index] = None;
                } else {
                    compacted.push(Some(WalOp::DeleteNode { id }));
                }
            }
            other => compacted.push(Some(other)),
        }
    }

    compacted.into_iter().flatten().collect()
}

impl GraphStore {
    pub fn in_memory() -> Self {
        Self::default()
    }

    pub(crate) fn ensure_usable(&self) -> Result<()> {
        if self.integrity_poisoned.load(AtomicOrdering::Acquire) {
            return Err(SkeinError::Storage(
                "database handle is poisoned after a runtime storage integrity failure; close and reopen the database before issuing more operations"
                    .to_string(),
            ));
        }
        if self.post_wal_apply_poisoned {
            return Err(SkeinError::Storage(
                "database handle is poisoned after a durable WAL batch failed during in-memory apply; close and reopen the database before issuing more operations"
                    .to_string(),
            ));
        }
        self.validate_authoritative_relational_index_open()?;
        Ok(())
    }

    pub fn post_wal_apply_poisoned(&self) -> bool {
        self.post_wal_apply_poisoned
    }

    pub(crate) fn projection_generation_store(
        &self,
    ) -> Result<skein_storage::ProjectionGenerationStore> {
        self.projection_generations.clone().ok_or_else(|| {
            SkeinError::Storage(
                "projection generation catalog requires a durable database".to_string(),
            )
        })
    }

    pub fn storage_handle_poisoned(&self) -> bool {
        self.post_wal_apply_poisoned || self.integrity_poisoned.load(AtomicOrdering::Acquire)
    }

    pub(crate) fn poison_on_storage_error<T>(&self, result: &Result<T>) {
        if matches!(result, Err(SkeinError::StorageIntegrity(_))) {
            self.integrity_poisoned.store(true, AtomicOrdering::Release);
        }
    }

    pub fn open(path: impl AsRef<Path>, catalog: &mut Catalog) -> Result<Self> {
        Self::open_with_durability(path, catalog, DurabilityPolicy::default())
    }

    pub fn open_with_durability(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            catalog,
            durability,
            DurableOpenMode::CreateIfMissing,
            WalReplayConfig::default(),
        )
    }

    pub fn open_with_durability_and_recovery(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
        recovery_mode: RecoveryMode,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            catalog,
            durability,
            DurableOpenMode::CreateIfMissing,
            WalReplayConfig {
                recovery_mode,
                ..WalReplayConfig::default()
            },
        )
    }

    pub fn open_with_durability_and_replay_config(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
        replay_config: WalReplayConfig,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            catalog,
            durability,
            DurableOpenMode::CreateIfMissing,
            replay_config,
        )
    }

    pub fn open_read_only_with_durability(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
        recovery_mode: RecoveryMode,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            catalog,
            durability,
            DurableOpenMode::ExistingOnly,
            WalReplayConfig {
                recovery_mode,
                ..WalReplayConfig::default()
            },
        )
    }

    pub fn open_read_only_with_durability_and_replay_config(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
        replay_config: WalReplayConfig,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            catalog,
            durability,
            DurableOpenMode::ExistingOnly,
            replay_config,
        )
    }

    fn open_with_options(
        path: impl AsRef<Path>,
        catalog: &mut Catalog,
        durability: DurabilityPolicy,
        mode: DurableOpenMode,
        replay_config: WalReplayConfig,
    ) -> Result<Self> {
        if replay_config.recovery_mode != RecoveryMode::Strict {
            return Err(SkeinError::Storage(
                "WAL repair is not available through database open; use DatabaseDoctor to plan and explicitly apply repair before opening in strict mode"
                    .to_string(),
            ));
        }
        let total_open_started = std::time::Instant::now();
        let durable_manifest_open_started = std::time::Instant::now();
        let durable = match mode {
            DurableOpenMode::CreateIfMissing => DurableStore::open(
                path.as_ref(),
                durability,
                replay_config.segment_cache_capacity_bytes,
                replay_config.max_graph_manifest_open_bytes,
                replay_config.max_bytes,
                replay_config.max_record_bytes,
                replay_config.max_batch_operations,
            )?,
            DurableOpenMode::ExistingOnly => DurableStore::open_existing_only(
                path.as_ref(),
                durability,
                replay_config.segment_cache_capacity_bytes,
                replay_config.max_graph_manifest_open_bytes,
                replay_config.max_bytes,
                replay_config.max_record_bytes,
                replay_config.max_batch_operations,
            )?,
        };
        let durable_manifest_open_micros = elapsed_micros(durable_manifest_open_started);
        let (mut store, _) = Self::finish_open(
            durable,
            catalog,
            replay_config,
            durable_manifest_open_micros,
        )?;
        let activation_started = std::time::Instant::now();
        store.activate_out_of_core_relational_rows()?;
        store
            .storage_recovery_report
            .open_timings
            .post_replay_open_micros = store
            .storage_recovery_report
            .open_timings
            .post_replay_open_micros
            .saturating_add(elapsed_micros(activation_started));
        store.storage_recovery_report.open_timings.total_open_micros =
            elapsed_micros(total_open_started);
        Ok(store)
    }

    fn finish_open(
        durable: DurableStore,
        catalog: &mut Catalog,
        replay_config: WalReplayConfig,
        durable_manifest_open_micros: u64,
    ) -> Result<(Self, Catalog)> {
        let projection_generation_root = durable.root_path.join("projection-generations");
        let projection_generations = if durable.read_only {
            if projection_generation_root.exists() {
                Some(
                    skein_storage::ProjectionGenerationStore::open_existing(
                        &projection_generation_root,
                    )
                    .map_err(|error| SkeinError::Storage(error.to_string()))?,
                )
            } else {
                None
            }
        } else {
            Some(
                skein_storage::ProjectionGenerationStore::open(&projection_generation_root)
                    .map_err(|error| SkeinError::Storage(error.to_string()))?,
            )
        };
        let mut store = Self {
            next_node_id: 0,
            next_rel_id: 0,
            commit_epoch: 0,
            nodes: CowSegmentedMap::default(),
            relationships: CowSegmentedMap::default(),
            basic_statistics: BasicGraphStatistics::default(),
            checkpoint_statistics: GraphStatistics::default(),
            advanced_statistics_dirty: AdvancedStatisticsDirtyState::default(),
            outgoing: CowSegmentedMap::default(),
            incoming: CowSegmentedMap::default(),
            property_index: CowSegmentedMap::default(),
            composite_property_index: CowSegmentedMap::default(),
            full_text_property_index: CowSegmentedMap::default(),
            relationship_property_index: CowSegmentedMap::default(),
            projected_graphs: CowSegment::default(),
            projected_graph_artifacts: CowSegment::default(),
            stable_id_mapping: CowSegment::default(),
            initial_import_source_fingerprint: None,
            search_projection_change_log_start_epoch: 0,
            search_projection_graph_changes: CowSegment::default(),
            search_projection_change_log_retained_bytes: 0,
            max_search_projection_change_log_entries: None,
            max_search_projection_change_log_bytes: None,
            search_projection_primary_key_capture_limits: Default::default(),
            source_scan_manifest: CowSegment::default(),
            storage_recovery_report: StorageRecoveryReport::default(),
            canonical_base: None,
            canonical_adjacency: None,
            persistent_property_projection: None,
            graph_index_read_metrics: Arc::new(GraphIndexReadMetrics::default()),
            canonical_base_out_of_core: false,
            node_tombstones: CowSegment::default(),
            relationship_tombstones: CowSegment::default(),
            residency_mode: replay_config.residency_mode,
            auto_materialize_checkpoint_bytes: replay_config.auto_materialize_checkpoint_bytes,
            max_out_of_core_delta_bytes: replay_config.max_out_of_core_delta_bytes,
            post_wal_apply_poisoned: false,
            integrity_poisoned: Arc::new(AtomicBool::new(false)),
            relational_state: RelationalState::default(),
            append_state: AppendState::default(),
            append_mutation_limits: AppendMutationLimits::default(),
            append_publication_config: AppendPublicationConfig::default(),
            append_generation_reader: None,
            relational_mutation_limits: RelationalMutationLimits::default(),
            relational_overflow_config: RelationalOverflowConfig::default(),
            columnar_shadow: ColumnarShadowState::default(),
            relational_index_shadow: RelationalIndexShadowState::new(
                replay_config.relational_index_mode,
            ),
            relational_row_pages: RelationalRowPageState::default(),
            projection_generations,
            runtime_governor: None,
            durable: Some(durable),
        };
        if replay_config
            .relational_index_mode
            .requires_authoritative_indexes()
        {
            store.relational_state.omit_materialized_index_postings();
        }
        let checkpoint_root_open_started = std::time::Instant::now();
        store.load_checkpoint(catalog, replay_config)?;
        store.mount_append_generation_for_recovery()?;
        if replay_config.graph_columnar_shadow_checkpoint {
            // Mounted between checkpoint load and WAL replay so replayed
            // mutations mark their derived shadow tables dirty.
            store.mount_columnar_shadow_for_recovery()?;
        }
        store.mount_relational_row_pages_for_recovery()?;
        store.mount_relational_index_shadow_for_recovery();
        let checkpoint_root_open_micros = elapsed_micros(checkpoint_root_open_started);
        let checkpoint_catalog = catalog.clone();
        let wal_replay_started = std::time::Instant::now();
        let mut storage_recovery_report = store.replay_wal(catalog, replay_config)?;
        let wal_replay_micros = elapsed_micros(wal_replay_started);
        let post_replay_open_started = std::time::Instant::now();
        store.validate_authoritative_relational_index_open()?;
        store.validate_relationship_endpoints()?;
        store.refresh_basic_statistics_epoch();
        store.load_projected_graph_artifacts()?;
        store.load_stable_id_mapping()?;
        store.load_source_scan_manifest()?;
        storage_recovery_report.open_timings = StorageOpenTimings {
            durable_manifest_open_micros,
            checkpoint_root_open_micros,
            wal_replay_micros,
            post_replay_open_micros: elapsed_micros(post_replay_open_started),
            total_open_micros: 0,
        };
        store.storage_recovery_report = storage_recovery_report;
        Ok((store, checkpoint_catalog))
    }

    fn open_for_derived_repair(
        path: &Path,
        replay_config: WalReplayConfig,
    ) -> Result<(Self, Catalog, Catalog)> {
        if replay_config.recovery_mode != RecoveryMode::Strict {
            return Err(SkeinError::Storage(
                "derived repair requires strict WAL replay".to_string(),
            ));
        }
        let total_open_started = std::time::Instant::now();
        let durable_manifest_open_started = std::time::Instant::now();
        let durable = DurableStore::open_for_derived_repair(
            path,
            DurabilityPolicy::default(),
            replay_config.segment_cache_capacity_bytes,
            replay_config.max_graph_manifest_open_bytes,
            replay_config.max_bytes,
            replay_config.max_record_bytes,
            replay_config.max_batch_operations,
        )?;
        let durable_manifest_open_micros = elapsed_micros(durable_manifest_open_started);
        let mut recovered_catalog = Catalog::default();
        let (mut store, checkpoint_catalog) = Self::finish_open(
            durable,
            &mut recovered_catalog,
            replay_config,
            durable_manifest_open_micros,
        )?;
        store.storage_recovery_report.open_timings.total_open_micros =
            elapsed_micros(total_open_started);
        Ok((store, recovered_catalog, checkpoint_catalog))
    }

    fn enable_derived_repair_writes(&mut self) -> Result<()> {
        let durable = self.durable.as_mut().ok_or_else(|| {
            SkeinError::Storage("derived repair requires durable storage".to_string())
        })?;
        durable.read_only = false;
        Ok(())
    }

    pub(crate) fn relational_state(&self) -> &RelationalState {
        &self.relational_state
    }

    pub fn storage_version(&self) -> &'static str {
        STORAGE_VERSION
    }

    pub fn is_out_of_core(&self) -> bool {
        self.canonical_base_out_of_core
    }

    pub fn commit_epoch(&self) -> u64 {
        self.commit_epoch
    }

    pub fn published_read_view(&self) -> PublishedReadView {
        let checkpoint = self
            .durable
            .as_ref()
            .filter(|durable| durable.checkpoint_encoded_len.is_some());
        PublishedReadView::new(
            self.commit_epoch,
            checkpoint.map(|durable| durable.checkpoint_commit_epoch),
            checkpoint.map(|durable| ManifestGeneration(durable.checkpoint_epoch)),
        )
    }

    pub(crate) fn begin_wal_sync_group(&mut self) -> Result<bool> {
        let Some(durable) = &mut self.durable else {
            return Ok(false);
        };
        durable.begin_wal_sync_group()
    }

    pub(crate) fn wal_sync_group_progress(&self) -> WalSyncGroupProgress {
        self.durable
            .as_ref()
            .map_or_else(WalSyncGroupProgress::default, |durable| {
                durable.wal_sync_group_progress()
            })
    }

    pub(crate) fn wal_sync_group_active(&self) -> bool {
        self.durable
            .as_ref()
            .is_some_and(DurableStore::wal_sync_group_active)
    }

    pub(crate) fn finish_wal_sync_group(&mut self) -> Result<WalSyncGroupFlush> {
        let Some(durable) = &mut self.durable else {
            return Ok(WalSyncGroupFlush::default());
        };
        match durable.finish_wal_sync_group() {
            Ok(flush) => Ok(flush),
            Err(error) => {
                self.post_wal_apply_poisoned = true;
                Err(error)
            }
        }
    }

    pub fn search_projection_change_log_start_epoch(&self) -> u64 {
        self.search_projection_change_log_start_epoch
    }

    pub fn search_projection_graph_changes_after(
        &self,
        commit_epoch: u64,
    ) -> Vec<SearchProjectionGraphChange> {
        self.search_projection_graph_changes
            .iter()
            .filter(|change| change.commit_epoch > commit_epoch)
            .cloned()
            .collect()
    }

    pub fn search_projection_changes_after(
        &self,
        commit_epoch: u64,
    ) -> Vec<skein_storage::SearchProjectionChange> {
        self.search_projection_graph_changes_after(commit_epoch)
    }

    pub fn search_projection_changefeed_status(&self) -> SearchProjectionChangefeedStatus {
        SearchProjectionChangefeedStatus {
            graph_commit_epoch: self.commit_epoch,
            resume_floor_commit_epoch: self.search_projection_change_log_start_epoch,
            oldest_retained_mutation_id: self
                .search_projection_graph_changes
                .first()
                .map(SearchProjectionGraphChange::mutation_id),
            newest_retained_mutation_id: self
                .search_projection_graph_changes
                .last()
                .map(SearchProjectionGraphChange::mutation_id),
            first_rebuild_required_mutation_id: self
                .search_projection_graph_changes
                .iter()
                .find(|change| change.relational_primary_key_changes.requires_rebuild())
                .map(SearchProjectionGraphChange::mutation_id),
            retained_mutation_count: self.search_projection_graph_changes.len(),
            retained_bytes: self.search_projection_change_log_retained_bytes,
            max_retained_bytes: self.max_search_projection_change_log_bytes,
            restart_recoverable: self.durable.is_some(),
        }
    }

    pub fn set_max_search_projection_change_log_entries(&mut self, max_entries: Option<usize>) {
        self.max_search_projection_change_log_entries = max_entries;
        self.trim_search_projection_graph_change_log();
    }

    pub fn set_max_search_projection_change_log_bytes(&mut self, max_bytes: Option<usize>) {
        self.max_search_projection_change_log_bytes = max_bytes;
        self.trim_search_projection_graph_change_log();
    }

    pub fn set_search_projection_primary_key_capture_limits(
        &mut self,
        limits: skein_storage::RelationalPrimaryKeyChangeCaptureLimits,
    ) {
        self.search_projection_primary_key_capture_limits = limits;
        for change in self.search_projection_graph_changes.iter_mut() {
            if change.relational_primary_key_changes.exceeds_limits(limits) {
                change.relational_primary_key_changes =
                    skein_storage::RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                        reason: skein_storage::RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded,
                    };
            }
        }
        self.search_projection_change_log_retained_bytes = self
            .search_projection_graph_changes
            .iter()
            .map(SearchProjectionGraphChange::estimated_retained_bytes)
            .fold(0usize, usize::saturating_add);
        self.trim_search_projection_graph_change_log();
    }

    pub fn set_telemetry_sink(&mut self, telemetry: Option<Arc<dyn TelemetrySink>>) {
        if let Some(durable) = &mut self.durable {
            durable.telemetry = telemetry.map(|sink| Arc::new(RootStorageTelemetry(sink)) as _);
        }
    }

    pub fn stable_id_mapping(&self) -> Result<StoreStableIdMapping> {
        self.durable.as_ref().map_or_else(
            || Ok((*self.stable_id_mapping).clone()),
            DurableStore::materialize_stable_id_mapping,
        )
    }

    pub fn initial_import_source_fingerprint(&self) -> Option<&str> {
        self.initial_import_source_fingerprint.as_deref()
    }

    pub fn replace_stable_id_mapping(&mut self, mapping: StoreStableIdMapping) -> Result<()> {
        self.replace_stable_id_mapping_for_epoch(mapping, self.commit_epoch)
    }

    fn replace_stable_id_mapping_for_epoch(
        &mut self,
        mapping: StoreStableIdMapping,
        covered_commit_epoch: u64,
    ) -> Result<()> {
        if self
            .durable
            .as_ref()
            .is_some_and(|durable| durable.read_only)
        {
            return Err(SkeinError::Storage(
                "stable id mapping persistence is not allowed in read-only mode".to_string(),
            ));
        }
        if let Some(durable) = &mut self.durable {
            durable.write_stable_id_mapping(&mapping, covered_commit_epoch)?;
            self.stable_id_mapping = CowSegment::default();
        } else {
            self.stable_id_mapping = mapping.into();
        }
        Ok(())
    }

    pub fn ensure_stable_id_mapping(
        &mut self,
        required_node_ids: &BTreeSet<NodeId>,
        required_relationship_ids: &BTreeSet<RelId>,
    ) -> Result<StoreStableIdMapping> {
        if self
            .durable
            .as_ref()
            .is_some_and(|durable| durable.read_only)
        {
            return Err(SkeinError::Storage(
                "stable id mapping persistence is not allowed in read-only mode".to_string(),
            ));
        }
        let existing = self.stable_id_mapping()?;
        let mapping = StoreStableIdMapping {
            node_stable_ids: required_node_ids
                .iter()
                .map(|id| {
                    (
                        *id,
                        existing
                            .node_stable_ids
                            .get(id)
                            .cloned()
                            .unwrap_or_else(|| generated_stable_id("node", id.0)),
                    )
                })
                .collect(),
            relationship_stable_ids: required_relationship_ids
                .iter()
                .map(|id| {
                    (
                        *id,
                        existing
                            .relationship_stable_ids
                            .get(id)
                            .cloned()
                            .unwrap_or_else(|| generated_stable_id("relationship", id.0)),
                    )
                })
                .collect(),
        };
        if mapping != existing {
            self.replace_stable_id_mapping_for_epoch(mapping.clone(), self.commit_epoch)?;
        }
        Ok(mapping)
    }

    pub fn snapshot(&self) -> Self {
        Self {
            next_node_id: self.next_node_id,
            next_rel_id: self.next_rel_id,
            commit_epoch: self.commit_epoch,
            nodes: self.nodes.clone(),
            relationships: self.relationships.clone(),
            basic_statistics: self.basic_statistics.clone(),
            checkpoint_statistics: self.checkpoint_statistics.clone(),
            advanced_statistics_dirty: self.advanced_statistics_dirty,
            outgoing: self.outgoing.clone(),
            incoming: self.incoming.clone(),
            property_index: self.property_index.clone(),
            composite_property_index: self.composite_property_index.clone(),
            full_text_property_index: self.full_text_property_index.clone(),
            relationship_property_index: self.relationship_property_index.clone(),
            projected_graphs: self.projected_graphs.clone(),
            projected_graph_artifacts: self.projected_graph_artifacts.clone(),
            stable_id_mapping: self.stable_id_mapping.clone(),
            initial_import_source_fingerprint: self.initial_import_source_fingerprint.clone(),
            search_projection_change_log_start_epoch: self.search_projection_change_log_start_epoch,
            search_projection_graph_changes: self.search_projection_graph_changes.clone(),
            search_projection_change_log_retained_bytes: self
                .search_projection_change_log_retained_bytes,
            max_search_projection_change_log_entries: self.max_search_projection_change_log_entries,
            max_search_projection_change_log_bytes: self.max_search_projection_change_log_bytes,
            search_projection_primary_key_capture_limits: self
                .search_projection_primary_key_capture_limits,
            source_scan_manifest: self.source_scan_manifest.clone(),
            storage_recovery_report: self.storage_recovery_report.clone(),
            canonical_base: self.canonical_base.clone(),
            canonical_adjacency: self.canonical_adjacency.clone(),
            persistent_property_projection: self.persistent_property_projection.clone(),
            graph_index_read_metrics: Arc::clone(&self.graph_index_read_metrics),
            canonical_base_out_of_core: self.canonical_base_out_of_core,
            node_tombstones: self.node_tombstones.clone(),
            relationship_tombstones: self.relationship_tombstones.clone(),
            residency_mode: self.residency_mode,
            auto_materialize_checkpoint_bytes: self.auto_materialize_checkpoint_bytes,
            max_out_of_core_delta_bytes: self.max_out_of_core_delta_bytes,
            post_wal_apply_poisoned: self.post_wal_apply_poisoned,
            integrity_poisoned: Arc::clone(&self.integrity_poisoned),
            relational_state: self.relational_state.clone(),
            append_state: self.append_state.clone(),
            append_mutation_limits: self.append_mutation_limits,
            append_publication_config: self.append_publication_config,
            append_generation_reader: self.append_generation_reader.clone(),
            relational_mutation_limits: self.relational_mutation_limits,
            relational_overflow_config: self.relational_overflow_config,
            columnar_shadow: self.columnar_shadow.clone(),
            relational_index_shadow: self
                .relational_index_shadow
                .snapshot_at_epoch(self.commit_epoch),
            relational_row_pages: self
                .relational_row_pages
                .snapshot_at_epoch(self.commit_epoch),
            projection_generations: None,
            runtime_governor: self.runtime_governor.clone(),
            durable: None,
        }
    }

    /// Threads the engine's runtime governor into the store so background
    /// shadow work can request admission (`WorkClass::Shadow`, background
    /// priority). The store never constructs a governor of its own.
    pub fn set_runtime_governor(&mut self, governor: skein_qos::RuntimeGovernor) {
        self.runtime_governor = Some(Arc::new(RuntimeGovernorBackgroundAdmission(governor)));
    }

    pub fn register_projected_graph(
        &mut self,
        name: &str,
        definition: ProjectedGraphDefinition,
    ) -> Result<()> {
        if let Some(durable) = &mut self.durable {
            durable.append_project_graph(name, &definition)?;
        }
        self.apply_project_graph_definition(name.to_string(), definition);
        self.finish_non_relational_commit();
        Ok(())
    }

    pub fn projected_graph_definition(&self, name: &str) -> Option<&ProjectedGraphDefinition> {
        self.projected_graphs.get(name)
    }

    pub fn projected_graph_artifact(
        &self,
        name: &str,
        definition: &ProjectedGraphDefinition,
    ) -> Option<ProjectedGraph> {
        let artifact = self.projected_graph_artifacts.get(name)?;
        (artifact.commit_epoch == self.commit_epoch && &artifact.definition == definition).then(
            || {
                ProjectedGraph::from_parts(
                    artifact.data.nodes.clone(),
                    artifact.data.csr_offsets.clone(),
                    artifact.data.csr_targets.clone(),
                    artifact.data.csc_offsets.clone(),
                    artifact.data.csc_sources.clone(),
                )
                .expect("validated projected graph artifact")
            },
        )
    }

    pub fn projected_graph_statuses(&self) -> Vec<ProjectedGraphStatus> {
        self.projected_graphs
            .iter()
            .map(|(name, definition)| {
                let artifact = self.projected_graph_artifacts.get(name);
                let reusable = artifact.is_some_and(|artifact| {
                    artifact.commit_epoch == self.commit_epoch && artifact.definition == *definition
                });
                ProjectedGraphStatus {
                    name: name.clone(),
                    node_labels: definition.node_labels.clone(),
                    rel_types: definition.rel_types.clone(),
                    projection_epoch: artifact.map(|artifact| artifact.projection_epoch),
                    commit_epoch: artifact.map(|artifact| artifact.commit_epoch),
                    node_count: artifact.map(|artifact| artifact.data.node_count()),
                    edge_count: artifact.map(|artifact| artifact.data.edge_count()),
                    reusable,
                }
            })
            .collect()
    }

    fn next_projection_epoch(&self) -> u64 {
        let checkpoint_epoch = self
            .durable
            .as_ref()
            .map(|durable| durable.checkpoint_epoch)
            .unwrap_or_default();
        let artifact_epoch = self
            .projected_graph_artifacts
            .values()
            .map(|artifact| artifact.projection_epoch)
            .max()
            .unwrap_or_default();
        checkpoint_epoch.max(artifact_epoch) + 1
    }
}

pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    sync_parent_directory(path)?;
    Ok(())
}

fn safe_reclaim_commit_epoch(
    checkpoint_commit_epoch: u64,
    oldest_reader_commit_epoch: Option<u64>,
) -> u64 {
    oldest_reader_commit_epoch
        .map(|epoch| epoch.saturating_sub(1))
        .unwrap_or(checkpoint_commit_epoch)
}

fn composite_property_index_key(
    node: &NodeRecord,
    properties: &[String],
) -> Option<Vec<(String, Value)>> {
    properties
        .iter()
        .map(|property| {
            node.properties
                .get(property)
                .cloned()
                .map(|value| (property.clone(), value))
        })
        .collect()
}

fn scalar_property_index_cardinality(
    index: &NodePropertyIndex,
    label_id: LabelId,
    property: &str,
) -> (u64, u64) {
    index
        .iter()
        .filter(|((candidate_label, candidate_property, _), _)| {
            *candidate_label == label_id && candidate_property == property
        })
        .fold((0_u64, 0_u64), |(size, unique), (_, node_ids)| {
            (
                size.saturating_add(node_ids.len() as u64),
                unique.saturating_add(1),
            )
        })
}

fn composite_property_index_unique_values(
    index: &CompositePropertyIndex,
    label_id: LabelId,
    properties: &[String],
) -> u64 {
    index
        .keys()
        .filter(|(candidate_label, key)| {
            *candidate_label == label_id
                && key
                    .iter()
                    .map(|(property, _)| property)
                    .eq(properties.iter())
        })
        .count() as u64
}

fn compute_index_statistics_samples(
    catalog: &Catalog,
    property_index: &NodePropertyIndex,
    composite_property_index: &CompositePropertyIndex,
) -> BTreeMap<IndexId, IndexStatisticsSample> {
    let mut samples = BTreeMap::new();
    for index in catalog
        .property_indexes()
        .filter(|index| index.kind != IndexKind::FullText)
    {
        let (index_size, unique_values) =
            scalar_property_index_cardinality(property_index, index.label_id, &index.property);
        samples.insert(
            index.id,
            IndexStatisticsSample::exact(index_size, unique_values),
        );
    }
    for index in catalog.composite_property_indexes() {
        let index_size = composite_property_index
            .iter()
            .filter(|((candidate_label, key), _)| {
                *candidate_label == index.label_id
                    && key
                        .iter()
                        .map(|(property, _)| property)
                        .eq(index.properties.iter())
            })
            .fold(0_u64, |size, (_, node_ids)| {
                size.saturating_add(node_ids.len() as u64)
            });
        let unique_values = composite_property_index_unique_values(
            composite_property_index,
            index.label_id,
            &index.properties,
        );
        samples.insert(
            index.id,
            IndexStatisticsSample::exact(index_size, unique_values),
        );
    }
    samples
}

fn retain_valid_index_statistics_samples(statistics: &mut GraphStatistics, catalog: &Catalog) {
    statistics
        .index_samples
        .retain(|id, sample| catalog.supports_index_statistics(*id) && sample.is_valid());
}

fn full_text_index_tokens(value: &str) -> BTreeSet<String> {
    let normalized = value.to_lowercase();
    let chars = normalized.chars().collect::<Vec<_>>();
    let mut tokens = BTreeSet::new();
    for start in 0..chars.len() {
        for width in 1..=3 {
            let end = start + width;
            if end > chars.len() {
                break;
            }
            let token = chars[start..end].iter().collect::<String>();
            if !token.chars().all(char::is_whitespace) {
                tokens.insert(token);
            }
        }
    }
    tokens
}

fn full_text_query_tokens(query: &str) -> Vec<String> {
    full_text_index_tokens(query).into_iter().collect()
}

fn ensure_table_descriptor(catalog: &mut Catalog, kind: TableKind, name: &str) -> TableId {
    match kind {
        TableKind::Node => {
            catalog.get_or_create_label(name);
        }
        TableKind::Relationship => {
            catalog.get_or_create_rel_type(name);
        }
    }
    catalog.get_or_create_table(kind, name)
}

fn validate_property_descriptor(
    catalog: &Catalog,
    store: &GraphStore,
    table_id: TableId,
    property: &str,
    value_type: PropertyType,
    nullable: bool,
) -> Result<()> {
    validate_property_descriptor_with_table_state(
        catalog, store, table_id, property, value_type, nullable, false,
    )
}

fn validate_property_descriptor_with_table_state(
    catalog: &Catalog,
    store: &GraphStore,
    table_id: TableId,
    property: &str,
    value_type: PropertyType,
    nullable: bool,
    force: bool,
) -> Result<()> {
    let Some(table) = catalog.table_descriptor(table_id) else {
        return Err(SkeinError::Storage(format!(
            "property schema references missing table {}",
            table_id.0
        )));
    };
    if !force && table.state != SchemaObjectState::Public {
        return Ok(());
    }
    match table.kind {
        TableKind::Node => {
            let Some(label_id) = catalog.label_id(&table.name) else {
                return Ok(());
            };
            for node in store.nodes.values() {
                if node.labels.contains(&label_id) {
                    validate_property_schema_value(
                        &table.name,
                        property,
                        value_type,
                        nullable,
                        node.properties.get(property),
                        &format!("node {}", node.id.0),
                    )?;
                }
            }
        }
        TableKind::Relationship => {
            let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
                return Ok(());
            };
            for relationship in store.relationships.values() {
                if relationship.rel_type == rel_type_id {
                    validate_property_schema_value(
                        &table.name,
                        property,
                        value_type,
                        nullable,
                        relationship.properties.get(property),
                        &format!("relationship {}", relationship.id.0),
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn reserve_schema_maintenance_budget(
    used_estimated_operations: &mut usize,
    max_estimated_operations: Option<usize>,
    estimated_operations: usize,
) -> bool {
    let Some(max_estimated_operations) = max_estimated_operations else {
        return true;
    };
    let next = used_estimated_operations.saturating_add(estimated_operations);
    if next > max_estimated_operations {
        return false;
    }
    *used_estimated_operations = next;
    true
}

fn validate_table_descriptor(
    catalog: &Catalog,
    store: &GraphStore,
    table_id: TableId,
) -> Result<()> {
    for property in catalog
        .property_descriptors()
        .filter(|property| property.table_id == table_id)
    {
        if property.state == SchemaObjectState::Gc {
            continue;
        }
        validate_property_descriptor_with_table_state(
            catalog,
            store,
            table_id,
            &property.name,
            property.value_type,
            property.nullable,
            true,
        )?;
    }
    Ok(())
}

fn apply_wal_op_to_snapshot(
    catalog: &Catalog,
    nodes: &mut CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &mut CowSegmentedMap<RelId, RelRecord>,
    op: &WalOp,
) {
    match op {
        WalOp::CreateNode {
            id,
            label,
            properties,
        } => {
            if let Some(label_id) = catalog.label_id(label) {
                nodes.insert(
                    *id,
                    NodeRecord {
                        id: *id,
                        labels: BTreeSet::from([label_id]),
                        properties: properties.clone(),
                    },
                );
            }
        }
        WalOp::SetNodeProperty {
            id,
            property,
            value,
        } => {
            if let Some(node) = nodes.get_mut(id) {
                node.properties.insert(property.clone(), value.clone());
            }
        }
        WalOp::SetRelationshipProperty {
            id,
            property,
            value,
        } => {
            if let Some(relationship) = relationships.get_mut(id) {
                relationship
                    .properties
                    .insert(property.clone(), value.clone());
            }
        }
        WalOp::DeleteNode { id } => {
            nodes.remove(id);
        }
        WalOp::CreateRelationship {
            id,
            source,
            target,
            rel_type,
            properties,
        } => {
            if let Some(rel_type_id) = catalog.rel_type_id(rel_type) {
                relationships.insert(
                    *id,
                    RelRecord {
                        id: *id,
                        source: *source,
                        target: *target,
                        rel_type: rel_type_id,
                        properties: properties.clone(),
                    },
                );
            }
        }
        WalOp::DeleteRelationship { id } => {
            relationships.remove(id);
        }
        WalOp::Batch(ops) => {
            for op in ops {
                apply_wal_op_to_snapshot(catalog, nodes, relationships, op);
            }
        }
        WalOp::CreateNodeLabel { .. }
        | WalOp::CreateRelationshipType { .. }
        | WalOp::CreateNodeTable { .. }
        | WalOp::CreateRelationshipTable { .. }
        | WalOp::CreateProperty { .. }
        | WalOp::AlterTableState { .. }
        | WalOp::AlterPropertyState { .. }
        | WalOp::GcTableDescriptor { .. }
        | WalOp::GcPropertyDescriptor { .. }
        | WalOp::CreateIndex { .. }
        | WalOp::CreateCompositeIndex { .. }
        | WalOp::CreateRangeIndex { .. }
        | WalOp::CreateFullTextIndex { .. }
        | WalOp::CreateUniqueConstraint { .. }
        | WalOp::CreateNodePropertyExistsConstraint { .. }
        | WalOp::CreateRelationshipUniqueConstraint { .. }
        | WalOp::CreateRelationshipPropertyExistsConstraint { .. }
        | WalOp::ProjectGraph { .. }
        | WalOp::MarkInitialImportSource { .. }
        | WalOp::Relational { .. }
        | WalOp::RelationalSnapshot { .. }
        | WalOp::Append { .. } => {}
    }
}

fn encode_projected_graph_artifacts(
    catalog: &Catalog,
    store: &GraphStore,
    projection_epoch: u64,
) -> String {
    let mut body = String::new();
    body.push_str("SKEIN_PROJECTED_GRAPHS_V1\n");
    body.push_str(&format!(
        "artifact_version\t{PROJECTED_GRAPH_ARTIFACT_VERSION}\n"
    ));
    body.push_str(&format!("projection_epoch\t{projection_epoch}\n"));
    body.push_str(&format!("commit_epoch\t{}\n", store.commit_epoch));
    for (name, definition) in store.projected_graphs.iter() {
        let graph = projected_graph_from_definition(catalog, store, definition);
        let data = ProjectedGraphArtifactData::new(
            graph.nodes().to_vec(),
            graph.csr_offsets().to_vec(),
            graph.csr_targets().to_vec(),
            graph.csc_offsets().to_vec(),
            graph.csc_sources().to_vec(),
        )
        .expect("fresh analytics projection is structurally valid");
        body.push_str(&format!(
            "graph\t{}\t{}\t{}\t{}\t{}\n",
            encode_string(name),
            encode_string_vec(&definition.node_labels),
            encode_string_vec(&definition.rel_types),
            data.node_count(),
            data.edge_count()
        ));
        body.push_str(&format!(
            "nodes\t{}\n",
            encode_u64_vec(data.nodes.iter().map(|node| node.0))
        ));
        body.push_str(&format!(
            "csr_offsets\t{}\n",
            encode_usize_vec(data.csr_offsets.iter().copied())
        ));
        body.push_str(&format!(
            "csr_targets\t{}\n",
            encode_usize_vec(data.csr_targets.iter().copied())
        ));
        body.push_str(&format!(
            "csc_offsets\t{}\n",
            encode_usize_vec(data.csc_offsets.iter().copied())
        ));
        body.push_str(&format!(
            "csc_sources\t{}\n",
            encode_usize_vec(data.csc_sources.iter().copied())
        ));
    }
    body
}

fn projected_graph_from_definition(
    catalog: &Catalog,
    store: &GraphStore,
    definition: &ProjectedGraphDefinition,
) -> ProjectedGraph {
    if definition.node_labels.is_empty() && definition.rel_types.is_empty() {
        return ProjectedGraph::from_store(store, None);
    }
    let label_ids = definition
        .node_labels
        .iter()
        .filter_map(|label| catalog.label_id(label))
        .collect::<Vec<_>>();
    if !definition.node_labels.is_empty() && label_ids.is_empty() {
        return ProjectedGraph::empty();
    }
    let rel_type_ids = definition
        .rel_types
        .iter()
        .filter_map(|rel_type| catalog.rel_type_id(rel_type))
        .collect::<Vec<_>>();
    if !definition.rel_types.is_empty() && rel_type_ids.is_empty() {
        if label_ids.is_empty() {
            return ProjectedGraph::from_store_without_edges(store);
        }
        return ProjectedGraph::from_store_labels_without_edges(store, &label_ids);
    }
    ProjectedGraph::from_store_labels_and_rel_types(store, &label_ids, &rel_type_ids)
}

fn decode_projected_graph_artifacts(
    body: &str,
) -> Result<(u64, BTreeMap<String, ProjectedGraphArtifact>)> {
    let mut lines = body.lines();
    match lines.next() {
        Some("SKEIN_PROJECTED_GRAPHS_V1") => {}
        _ => {
            return Err(SkeinError::Storage(
                "invalid projected graph artifact header".to_string(),
            ));
        }
    }
    let artifact_version = decode_projected_graph_u64_header(
        lines.next(),
        "artifact_version",
        "projected graph artifact version",
    )?;
    if artifact_version != PROJECTED_GRAPH_ARTIFACT_VERSION {
        return Err(SkeinError::Storage(format!(
            "unsupported projected graph artifact version: {artifact_version}"
        )));
    }
    let projection_epoch = decode_projected_graph_u64_header(
        lines.next(),
        "projection_epoch",
        "projected graph artifact projection epoch",
    )?;
    let commit_epoch = decode_projected_graph_u64_header(
        lines.next(),
        "commit_epoch",
        "projected graph artifact commit epoch",
    )?;

    let mut artifacts = BTreeMap::new();
    while let Some(line) = lines.next() {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["graph", raw_name, raw_node_labels, raw_rel_types, raw_node_count, raw_edge_count] => {
                let name = decode_string(raw_name)?;
                let definition = ProjectedGraphDefinition {
                    node_labels: decode_string_vec(raw_node_labels)?,
                    rel_types: decode_string_vec(raw_rel_types)?,
                };
                let node_count = parse_u64(raw_node_count, "projected graph artifact node count")?;
                let edge_count = parse_u64(raw_edge_count, "projected graph artifact edge count")?;
                let nodes = decode_projected_graph_nodes_line(lines.next())?;
                let csr_offsets = decode_projected_graph_usize_line(lines.next(), "csr_offsets")?;
                let csr_targets = decode_projected_graph_usize_line(lines.next(), "csr_targets")?;
                let csc_offsets = decode_projected_graph_usize_line(lines.next(), "csc_offsets")?;
                let csc_sources = decode_projected_graph_usize_line(lines.next(), "csc_sources")?;
                if nodes.len() as u64 != node_count {
                    return Err(SkeinError::Storage(format!(
                        "projected graph artifact node count mismatch for {name}"
                    )));
                }
                if csr_targets.len() as u64 != edge_count || csc_sources.len() as u64 != edge_count
                {
                    return Err(SkeinError::Storage(format!(
                        "projected graph artifact edge count mismatch for {name}"
                    )));
                }
                let data = ProjectedGraphArtifactData::new(
                    nodes,
                    csr_offsets,
                    csr_targets,
                    csc_offsets,
                    csc_sources,
                )
                .map_err(SkeinError::Storage)?;
                artifacts.insert(
                    name,
                    ProjectedGraphArtifact {
                        projection_epoch,
                        commit_epoch,
                        definition,
                        data,
                    },
                );
            }
            [""] => {}
            _ => {
                return Err(SkeinError::Storage(format!(
                    "invalid projected graph artifact line: {line}"
                )));
            }
        }
    }
    Ok((commit_epoch, artifacts))
}

fn decode_projected_graph_u64_header(
    line: Option<&str>,
    expected: &str,
    name: &str,
) -> Result<u64> {
    let Some(line) = line else {
        return Err(SkeinError::Storage(format!(
            "missing projected graph artifact {expected}"
        )));
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        [field, raw] if *field == expected => parse_u64(raw, name),
        _ => Err(SkeinError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}

fn decode_projected_graph_nodes_line(line: Option<&str>) -> Result<Vec<NodeId>> {
    let Some(line) = line else {
        return Err(SkeinError::Storage(
            "missing projected graph artifact nodes line".to_string(),
        ));
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        ["nodes", raw_values] => decode_u64_vec(raw_values, "projected graph artifact node id")
            .map(|nodes| nodes.into_iter().map(NodeId).collect()),
        _ => Err(SkeinError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}

fn decode_projected_graph_usize_line(line: Option<&str>, expected: &str) -> Result<Vec<usize>> {
    let Some(line) = line else {
        return Err(SkeinError::Storage(format!(
            "missing projected graph artifact {expected} line"
        )));
    };
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        [name, raw_values] if *name == expected => {
            decode_usize_vec(raw_values, "projected graph artifact index")
        }
        _ => Err(SkeinError::Storage(format!(
            "invalid projected graph artifact line: {line}"
        ))),
    }
}

fn validate_property_schemas(
    catalog: &Catalog,
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> Result<()> {
    for property in catalog.property_descriptors() {
        if property.state != SchemaObjectState::Public {
            continue;
        }
        let Some(table) = catalog.table_descriptor(property.table_id) else {
            continue;
        };
        if table.state != SchemaObjectState::Public {
            continue;
        }
        match table.kind {
            TableKind::Node => {
                let Some(label_id) = catalog.label_id(&table.name) else {
                    continue;
                };
                for node in nodes.values() {
                    if node.labels.contains(&label_id) {
                        validate_property_schema_value(
                            &table.name,
                            &property.name,
                            property.value_type,
                            property.nullable,
                            node.properties.get(&property.name),
                            &format!("node {}", node.id.0),
                        )?;
                    }
                }
            }
            TableKind::Relationship => {
                let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
                    continue;
                };
                for relationship in relationships.values() {
                    if relationship.rel_type == rel_type_id {
                        validate_property_schema_value(
                            &table.name,
                            &property.name,
                            property.value_type,
                            property.nullable,
                            relationship.properties.get(&property.name),
                            &format!("relationship {}", relationship.id.0),
                        )?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_node_record_constraints(catalog: &Catalog, node: &NodeRecord) -> Result<()> {
    for property in catalog.property_descriptors() {
        if property.state != SchemaObjectState::Public {
            continue;
        }
        let Some(table) = catalog.table_descriptor(property.table_id) else {
            continue;
        };
        if table.kind != TableKind::Node || table.state != SchemaObjectState::Public {
            continue;
        }
        let Some(label_id) = catalog.label_id(&table.name) else {
            continue;
        };
        if node.labels.contains(&label_id) {
            validate_property_schema_value(
                &table.name,
                &property.name,
                property.value_type,
                property.nullable,
                node.properties.get(&property.name),
                &format!("node {}", node.id.0),
            )?;
        }
    }
    for constraint in catalog.node_property_exists_constraints() {
        let crate::schema::ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        if node.labels.contains(&label_id)
            && !node
                .properties
                .get(&constraint.property)
                .is_some_and(|value| value != &Value::Null)
        {
            let label = catalog.label_name(label_id).unwrap_or("<unknown>");
            return Err(SkeinError::Storage(format!(
                "node property exists constraint violation on :{label}({}) for node {}",
                constraint.property, node.id.0
            )));
        }
    }
    Ok(())
}

fn validate_relationship_record_constraints(
    catalog: &Catalog,
    relationship: &RelRecord,
) -> Result<()> {
    for property in catalog.property_descriptors() {
        if property.state != SchemaObjectState::Public {
            continue;
        }
        let Some(table) = catalog.table_descriptor(property.table_id) else {
            continue;
        };
        if table.kind != TableKind::Relationship || table.state != SchemaObjectState::Public {
            continue;
        }
        let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
            continue;
        };
        if relationship.rel_type == rel_type_id {
            validate_property_schema_value(
                &table.name,
                &property.name,
                property.value_type,
                property.nullable,
                relationship.properties.get(&property.name),
                &format!("relationship {}", relationship.id.0),
            )?;
        }
    }
    for constraint in catalog.relationship_property_exists_constraints() {
        let crate::schema::ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        if relationship.rel_type == rel_type_id
            && !relationship
                .properties
                .get(&constraint.property)
                .is_some_and(|value| value != &Value::Null)
        {
            let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
            return Err(SkeinError::Storage(format!(
                "relationship property exists constraint violation on :{rel_type}({}) for relationship {}",
                constraint.property, relationship.id.0
            )));
        }
    }
    Ok(())
}

fn validate_changed_node_uniqueness(
    store: &GraphStore,
    catalog: &Catalog,
    changes: &BTreeMap<NodeId, Option<NodeRecord>>,
) -> Result<()> {
    for constraint in catalog.unique_constraints() {
        let crate::schema::ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        let mut changed_values = BTreeMap::<Value, NodeId>::new();
        for node in changes.values().flatten() {
            if !node.labels.contains(&label_id) {
                continue;
            }
            let Some(value) = node.properties.get(&constraint.property) else {
                continue;
            };
            if value == &Value::Null {
                continue;
            }
            if let Some(previous) = changed_values.insert(value.clone(), node.id) {
                let label = catalog.label_name(label_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "unique constraint violation on :{label}({}) for nodes {} and {}",
                    constraint.property, previous.0, node.id.0
                )));
            }
        }
        for (value, changed_id) in changed_values {
            let mut violation = None;
            store.visit_nodes_owned(Some(label_id), |node| {
                if changes.contains_key(&node.id) {
                    return GraphScanControl::Continue;
                }
                if node.properties.get(&constraint.property) == Some(&value) {
                    violation = Some(node.id);
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            if let Some(existing_id) = violation {
                let label = catalog.label_name(label_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "unique constraint violation on :{label}({}) for nodes {} and {}",
                    constraint.property, existing_id.0, changed_id.0
                )));
            }
        }
    }
    Ok(())
}

fn validate_changed_relationship_uniqueness(
    store: &GraphStore,
    catalog: &Catalog,
    changes: &BTreeMap<RelId, Option<RelRecord>>,
) -> Result<()> {
    for constraint in catalog.relationship_unique_constraints() {
        let crate::schema::ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        let mut changed_values = BTreeMap::<Value, RelId>::new();
        for relationship in changes.values().flatten() {
            if relationship.rel_type != rel_type_id {
                continue;
            }
            let Some(value) = relationship.properties.get(&constraint.property) else {
                continue;
            };
            if value == &Value::Null {
                continue;
            }
            if let Some(previous) = changed_values.insert(value.clone(), relationship.id) {
                let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "relationship unique constraint violation on :{rel_type}({}) for relationships {} and {}",
                    constraint.property, previous.0, relationship.id.0
                )));
            }
        }
        for (value, changed_id) in changed_values {
            let mut violation = None;
            store.visit_relationships_owned(Some(rel_type_id), |relationship| {
                if changes.contains_key(&relationship.id) {
                    return GraphScanControl::Continue;
                }
                if relationship.properties.get(&constraint.property) == Some(&value) {
                    violation = Some(relationship.id);
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            if let Some(existing_id) = violation {
                let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "relationship unique constraint violation on :{rel_type}({}) for relationships {} and {}",
                    constraint.property, existing_id.0, changed_id.0
                )));
            }
        }
    }
    Ok(())
}

fn validate_property_schema_value(
    table: &str,
    property: &str,
    value_type: PropertyType,
    nullable: bool,
    value: Option<&Value>,
    record: &str,
) -> Result<()> {
    let Some(value) = value else {
        if nullable {
            return Ok(());
        }
        return Err(property_schema_error(
            table,
            property,
            record,
            "property is not nullable",
        ));
    };
    if value == &Value::Null {
        if nullable {
            return Ok(());
        }
        return Err(property_schema_error(
            table,
            property,
            record,
            "property is not nullable",
        ));
    }
    let matches = matches!(
        (value_type, value),
        (PropertyType::Any, _)
            | (PropertyType::Bool, Value::Bool(_))
            | (PropertyType::Int, Value::Int(_))
            | (PropertyType::Float, Value::Float(_))
            | (PropertyType::String, Value::String(_))
            | (PropertyType::Text, Value::String(_))
            | (PropertyType::List, Value::List(_))
    );
    if matches {
        Ok(())
    } else {
        Err(property_schema_error(
            table,
            property,
            record,
            &format!("expected {}", encode_property_type(value_type)),
        ))
    }
}

fn property_schema_error(table: &str, property: &str, record: &str, reason: &str) -> SkeinError {
    SkeinError::Storage(format!(
        "property schema violation on {record} in {table}({property}): {reason}"
    ))
}

fn validate_unique_constraints(
    catalog: &Catalog,
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
) -> Result<()> {
    for constraint in catalog.unique_constraints() {
        let crate::schema::ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        validate_unique_property(catalog, nodes, label_id, &constraint.property)?;
    }
    Ok(())
}

fn validate_relationship_unique_constraints(
    catalog: &Catalog,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> Result<()> {
    for constraint in catalog.relationship_unique_constraints() {
        let crate::schema::ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        validate_unique_relationship_property(
            catalog,
            relationships,
            rel_type_id,
            &constraint.property,
        )?;
    }
    Ok(())
}

fn validate_node_property_exists_constraints(
    catalog: &Catalog,
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
) -> Result<()> {
    for constraint in catalog.node_property_exists_constraints() {
        let crate::schema::ConstraintSubject::Node(label_id) = constraint.subject else {
            continue;
        };
        validate_node_property_exists(catalog, nodes, label_id, &constraint.property)?;
    }
    Ok(())
}

fn validate_relationship_property_exists_constraints(
    catalog: &Catalog,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> Result<()> {
    for constraint in catalog.relationship_property_exists_constraints() {
        let crate::schema::ConstraintSubject::Relationship(rel_type_id) = constraint.subject else {
            continue;
        };
        validate_relationship_property_exists(
            catalog,
            relationships,
            rel_type_id,
            &constraint.property,
        )?;
    }
    Ok(())
}

fn validate_node_property_exists(
    catalog: &Catalog,
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    label_id: LabelId,
    property: &str,
) -> Result<()> {
    for node in nodes.values() {
        if !node.labels.contains(&label_id) {
            continue;
        }
        match node.properties.get(property) {
            Some(value) if value != &Value::Null => {}
            _ => {
                let label = catalog.label_name(label_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "node property exists constraint violation on :{label}({property}) for node {}",
                    node.id.0
                )));
            }
        }
    }
    Ok(())
}

fn validate_relationship_property_exists(
    catalog: &Catalog,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    rel_type_id: RelTypeId,
    property: &str,
) -> Result<()> {
    for relationship in relationships.values() {
        if relationship.rel_type != rel_type_id {
            continue;
        }
        match relationship.properties.get(property) {
            Some(value) if value != &Value::Null => {}
            _ => {
                let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "relationship property exists constraint violation on :{rel_type}({property}) for relationship {}",
                    relationship.id.0
                )));
            }
        }
    }
    Ok(())
}

fn validate_unique_property(
    catalog: &Catalog,
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    label_id: LabelId,
    property: &str,
) -> Result<()> {
    let mut seen = BTreeMap::<Value, NodeId>::new();
    for node in nodes.values() {
        if !node.labels.contains(&label_id) {
            continue;
        }
        let Some(value) = node.properties.get(property) else {
            continue;
        };
        if value == &Value::Null {
            continue;
        }
        if let Some(previous) = seen.insert(value.clone(), node.id) {
            let label = catalog.label_name(label_id).unwrap_or("<unknown>");
            return Err(SkeinError::Storage(format!(
                "unique constraint violation on :{label}({property}) for nodes {} and {}",
                previous.0, node.id.0
            )));
        }
    }
    Ok(())
}

fn validate_unique_property_streaming(
    store: &GraphStore,
    catalog: &Catalog,
    label_id: LabelId,
    property: &str,
) -> Result<()> {
    let mut validation_error = None;
    store.visit_nodes_owned(Some(label_id), |node| {
        let Some(value) = node.properties.get(property).cloned() else {
            return GraphScanControl::Continue;
        };
        if value == Value::Null {
            return GraphScanControl::Continue;
        }
        let mut duplicate = None;
        match store.visit_nodes_owned(Some(label_id), |candidate| {
            if candidate.id > node.id && candidate.properties.get(property) == Some(&value) {
                duplicate = Some(candidate.id);
                GraphScanControl::Stop
            } else {
                GraphScanControl::Continue
            }
        }) {
            Ok(_) => {}
            Err(error) => {
                validation_error = Some(error);
                return GraphScanControl::Stop;
            }
        }
        if let Some(duplicate) = duplicate {
            let label = catalog.label_name(label_id).unwrap_or("<unknown>");
            validation_error = Some(SkeinError::Storage(format!(
                "unique constraint violation on :{label}({property}) for nodes {} and {}",
                node.id.0, duplicate.0
            )));
            GraphScanControl::Stop
        } else {
            GraphScanControl::Continue
        }
    })?;
    validation_error.map_or(Ok(()), Err)
}

fn validate_unique_relationship_property(
    catalog: &Catalog,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    rel_type_id: RelTypeId,
    property: &str,
) -> Result<()> {
    let mut seen = BTreeMap::<Value, RelId>::new();
    for relationship in relationships.values() {
        if relationship.rel_type != rel_type_id {
            continue;
        }
        let Some(value) = relationship.properties.get(property) else {
            continue;
        };
        if value == &Value::Null {
            continue;
        }
        if let Some(previous) = seen.insert(value.clone(), relationship.id) {
            let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
            return Err(SkeinError::Storage(format!(
                "relationship unique constraint violation on :{rel_type}({property}) for relationships {} and {}",
                previous.0, relationship.id.0
            )));
        }
    }
    Ok(())
}

fn validate_unique_relationship_property_streaming(
    store: &GraphStore,
    catalog: &Catalog,
    rel_type_id: RelTypeId,
    property: &str,
) -> Result<()> {
    let mut validation_error = None;
    store.visit_relationships_owned(Some(rel_type_id), |relationship| {
        let Some(value) = relationship.properties.get(property).cloned() else {
            return GraphScanControl::Continue;
        };
        if value == Value::Null {
            return GraphScanControl::Continue;
        }
        let mut duplicate = None;
        match store.visit_relationships_owned(Some(rel_type_id), |candidate| {
            if candidate.id > relationship.id
                && candidate.properties.get(property) == Some(&value)
            {
                duplicate = Some(candidate.id);
                GraphScanControl::Stop
            } else {
                GraphScanControl::Continue
            }
        }) {
            Ok(_) => {}
            Err(error) => {
                validation_error = Some(error);
                return GraphScanControl::Stop;
            }
        }
        if let Some(duplicate) = duplicate {
            let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
            validation_error = Some(SkeinError::Storage(format!(
                "relationship unique constraint violation on :{rel_type}({property}) for relationships {} and {}",
                relationship.id.0, duplicate.0
            )));
            GraphScanControl::Stop
        } else {
            GraphScanControl::Continue
        }
    })?;
    validation_error.map_or(Ok(()), Err)
}

#[cfg(test)]
fn compute_statistics(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    computed_at_commit_epoch: u64,
) -> GraphStatistics {
    compute_statistics_with_basic(
        nodes,
        relationships,
        None,
        compute_basic_statistics(nodes, relationships, computed_at_commit_epoch),
    )
}

fn compute_statistics_for_catalog(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    catalog: &Catalog,
    basic_statistics: BasicGraphStatistics,
) -> GraphStatistics {
    compute_statistics_with_basic(nodes, relationships, Some(catalog), basic_statistics)
}

fn graph_statistics_from_basic(
    basic_statistics: BasicGraphStatistics,
    advanced_statistics_complete: bool,
) -> GraphStatistics {
    GraphStatistics {
        computed_at_commit_epoch: basic_statistics.computed_at_commit_epoch,
        advanced_statistics_complete,
        histogram_sample_limit: MAX_PROPERTY_HISTOGRAM_VALUES,
        node_count: basic_statistics.node_count,
        relationship_count: basic_statistics.relationship_count,
        label_counts: basic_statistics.label_counts,
        rel_type_counts: basic_statistics.rel_type_counts,
        ..GraphStatistics::default()
    }
}

fn compute_statistics_with_basic(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    catalog: Option<&Catalog>,
    basic_statistics: BasicGraphStatistics,
) -> GraphStatistics {
    let mut statistics = graph_statistics_from_basic(basic_statistics, true);
    let mut property_values = BTreeMap::<(LabelId, String), BTreeSet<Value>>::new();
    let mut rel_property_values = BTreeMap::<(RelTypeId, String), BTreeSet<Value>>::new();
    let mut excluded_property_groups = BTreeSet::<(LabelId, String)>::new();
    let mut excluded_rel_property_groups = BTreeSet::<(RelTypeId, String)>::new();
    let mut rel_type_sources = BTreeMap::<RelTypeId, BTreeSet<NodeId>>::new();
    let mut rel_type_targets = BTreeMap::<RelTypeId, BTreeSet<NodeId>>::new();
    let mut path_sources = BTreeMap::<(LabelId, RelTypeId, LabelId), BTreeSet<NodeId>>::new();
    let mut path_targets = BTreeMap::<(LabelId, RelTypeId, LabelId), BTreeSet<NodeId>>::new();
    let mut outgoing_by_source_type = BTreeMap::<(NodeId, RelTypeId), Vec<NodeId>>::new();

    for node in nodes.values() {
        for label_id in &node.labels {
            for (property, value) in &node.properties {
                let key = (*label_id, property.clone());
                collect_property_statistic_value(
                    &mut property_values,
                    &mut excluded_property_groups,
                    key,
                    value,
                    node_property_supports_optimizer_statistics(
                        catalog, *label_id, property, value,
                    ),
                );
            }
        }
    }
    for relationship in relationships.values() {
        rel_type_sources
            .entry(relationship.rel_type)
            .or_default()
            .insert(relationship.source);
        rel_type_targets
            .entry(relationship.rel_type)
            .or_default()
            .insert(relationship.target);
        outgoing_by_source_type
            .entry((relationship.source, relationship.rel_type))
            .or_default()
            .push(relationship.target);
        for (property, value) in &relationship.properties {
            let key = (relationship.rel_type, property.clone());
            collect_property_statistic_value(
                &mut rel_property_values,
                &mut excluded_rel_property_groups,
                key,
                value,
                relationship_property_supports_optimizer_statistics(
                    catalog,
                    relationship.rel_type,
                    property,
                    value,
                ),
            );
        }
        if let (Some(source), Some(target)) = (
            nodes.get(&relationship.source),
            nodes.get(&relationship.target),
        ) {
            for source_label in &source.labels {
                for target_label in &target.labels {
                    let path_key = (*source_label, relationship.rel_type, *target_label);
                    *statistics.path_counts.entry(path_key).or_default() += 1;
                    path_sources
                        .entry(path_key)
                        .or_default()
                        .insert(relationship.source);
                    path_targets
                        .entry(path_key)
                        .or_default()
                        .insert(relationship.target);
                }
            }
        }
    }
    statistics.rel_type_source_counts = rel_type_sources
        .into_iter()
        .map(|(rel_type, sources)| (rel_type, sources.len() as u64))
        .collect();
    statistics.rel_type_target_counts = rel_type_targets
        .into_iter()
        .map(|(rel_type, targets)| (rel_type, targets.len() as u64))
        .collect();
    statistics.path_source_distinct_counts = path_sources
        .into_iter()
        .map(|(path, sources)| (path, sources.len() as u64))
        .collect();
    statistics.path_target_distinct_counts = path_targets
        .into_iter()
        .map(|(path, targets)| (path, targets.len() as u64))
        .collect();
    for (key, values) in property_values {
        let histogram_sample_limit = adaptive_histogram_sample_limit(values.len());
        let is_sampled = values.len() > histogram_sample_limit;
        statistics
            .property_distinct_counts
            .insert(key.clone(), values.len() as u64);
        statistics
            .property_histograms
            .insert(key.clone(), sample_histogram_values(values));
        statistics
            .sampled_property_histograms
            .insert(key, is_sampled);
    }
    for (key, values) in rel_property_values {
        let histogram_sample_limit = adaptive_histogram_sample_limit(values.len());
        let is_sampled = values.len() > histogram_sample_limit;
        statistics
            .rel_property_distinct_counts
            .insert(key.clone(), values.len() as u64);
        statistics
            .rel_property_histograms
            .insert(key.clone(), sample_histogram_values(values));
        statistics
            .sampled_rel_property_histograms
            .insert(key, is_sampled);
    }
    let bounded_path_statistics = compute_bounded_path_statistics(
        nodes,
        &outgoing_by_source_type,
        MAX_BOUNDED_PATH_STAT_HOPS,
    );
    statistics.bounded_path_counts = bounded_path_statistics.counts;
    statistics.bounded_path_source_distinct_counts = bounded_path_statistics.source_distinct_counts;
    statistics.bounded_path_target_distinct_counts = bounded_path_statistics.target_distinct_counts;
    statistics
}

fn property_value_supports_optimizer_statistics(value: &Value) -> bool {
    match value {
        Value::Null
        | Value::Bool(_)
        | Value::Int(_)
        | Value::Float(_)
        | Value::String(_)
        | Value::Uuid(_) => true,
        Value::Binary(_) | Value::List(_) | Value::Map(_) => false,
    }
}

fn property_type_supports_optimizer_statistics(value_type: PropertyType) -> bool {
    !matches!(value_type, PropertyType::Text | PropertyType::List)
}

fn node_property_supports_optimizer_statistics(
    catalog: Option<&Catalog>,
    label_id: LabelId,
    property: &str,
    value: &Value,
) -> bool {
    property_supports_optimizer_statistics(
        catalog.and_then(|catalog| {
            let label = catalog.label_name(label_id)?;
            declared_property_type(catalog, TableKind::Node, label, property)
        }),
        value,
    )
}

fn relationship_property_supports_optimizer_statistics(
    catalog: Option<&Catalog>,
    rel_type_id: RelTypeId,
    property: &str,
    value: &Value,
) -> bool {
    property_supports_optimizer_statistics(
        catalog.and_then(|catalog| {
            let rel_type = catalog.rel_type_name(rel_type_id)?;
            declared_property_type(catalog, TableKind::Relationship, rel_type, property)
        }),
        value,
    )
}

fn declared_property_type(
    catalog: &Catalog,
    table_kind: TableKind,
    table: &str,
    property: &str,
) -> Option<PropertyType> {
    let table_id = catalog.table_id(table_kind, table)?;
    let property_id = catalog.property_descriptor_id(table_id, property)?;
    catalog
        .property_descriptor(property_id)
        .map(|descriptor| descriptor.value_type)
}

fn property_supports_optimizer_statistics(
    declared_type: Option<PropertyType>,
    value: &Value,
) -> bool {
    declared_type.is_none_or(property_type_supports_optimizer_statistics)
        && property_value_supports_optimizer_statistics(value)
}

fn collect_property_statistic_value<K: Ord>(
    values: &mut BTreeMap<K, BTreeSet<Value>>,
    excluded: &mut BTreeSet<K>,
    key: K,
    value: &Value,
    eligible: bool,
) {
    if !eligible {
        values.remove(&key);
        excluded.insert(key);
    } else if !excluded.contains(&key) {
        values.entry(key).or_default().insert(value.clone());
    }
}

fn retain_supported_property_statistics(
    statistics: &mut GraphStatistics,
    catalog: Option<&Catalog>,
) {
    retain_supported_property_statistics_group(
        &mut statistics.property_distinct_counts,
        &mut statistics.property_histograms,
        &mut statistics.sampled_property_histograms,
        |(label_id, property), value| {
            node_property_supports_optimizer_statistics(catalog, *label_id, property, value)
        },
    );
    retain_supported_property_statistics_group(
        &mut statistics.rel_property_distinct_counts,
        &mut statistics.rel_property_histograms,
        &mut statistics.sampled_rel_property_histograms,
        |(rel_type_id, property), value| {
            relationship_property_supports_optimizer_statistics(
                catalog,
                *rel_type_id,
                property,
                value,
            )
        },
    );
}

fn retain_supported_property_statistics_group<K: Ord + Clone>(
    distinct_counts: &mut BTreeMap<K, u64>,
    histograms: &mut BTreeMap<K, Vec<Value>>,
    sampled_histograms: &mut BTreeMap<K, bool>,
    mut supports: impl FnMut(&K, &Value) -> bool,
) {
    let complete_groups = histograms
        .iter()
        .filter(|(key, values)| {
            distinct_counts.contains_key(*key)
                && sampled_histograms.contains_key(*key)
                && !values.is_empty()
                && values.iter().all(|value| supports(key, value))
        })
        .map(|(key, _)| key.clone())
        .collect::<BTreeSet<_>>();
    distinct_counts.retain(|key, _| complete_groups.contains(key));
    histograms.retain(|key, _| complete_groups.contains(key));
    sampled_histograms.retain(|key, _| complete_groups.contains(key));
}

fn compute_node_property_distinct_counts_from_index(
    property_index: &NodePropertyIndex,
    catalog: &Catalog,
) -> BTreeMap<(LabelId, String), u64> {
    compute_supported_property_distinct_counts(
        property_index
            .keys()
            .map(|(label, property, value)| ((*label, property.clone()), value)),
        |(label, property), value| {
            node_property_supports_optimizer_statistics(Some(catalog), *label, property, value)
        },
    )
}

fn compute_relationship_property_distinct_counts_from_index(
    relationship_property_index: &RelationshipPropertyIndex,
    catalog: &Catalog,
) -> BTreeMap<(RelTypeId, String), u64> {
    compute_supported_property_distinct_counts(
        relationship_property_index
            .keys()
            .map(|(rel_type, property, value)| ((*rel_type, property.clone()), value)),
        |(rel_type, property), value| {
            relationship_property_supports_optimizer_statistics(
                Some(catalog),
                *rel_type,
                property,
                value,
            )
        },
    )
}

fn compute_supported_property_distinct_counts<'a, K: Ord>(
    entries: impl Iterator<Item = (K, &'a Value)>,
    mut supports: impl FnMut(&K, &Value) -> bool,
) -> BTreeMap<K, u64> {
    let mut counts = BTreeMap::<K, Option<u64>>::new();
    for (key, value) in entries {
        let eligible = supports(&key, value);
        let count = counts.entry(key).or_insert(Some(0));
        if eligible {
            if let Some(count) = count {
                *count = count.saturating_add(1);
            }
        } else {
            *count = None;
        }
    }
    counts
        .into_iter()
        .filter_map(|(key, count)| count.map(|count| (key, count)))
        .collect()
}

/// Recomputes the node property index the way the write path maintains it:
/// declared properties only. Recomputing every property would report the
/// undeclared ones as permanently missing, which is the design, not a defect.
fn recompute_node_property_index(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    catalog: &Catalog,
) -> NodePropertyIndex {
    let mut index = NodePropertyIndex::default();
    for node in nodes.values() {
        for label_id in &node.labels {
            for (property, value) in &node.properties {
                if !catalog.has_scalar_property_index(*label_id, property) {
                    continue;
                }
                index
                    .entry_or_default((*label_id, property.clone(), value.clone()))
                    .insert(node.id);
            }
        }
    }
    index
}

fn recompute_relationship_property_index(
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> RelationshipPropertyIndex {
    let mut index = RelationshipPropertyIndex::default();
    for relationship in relationships.values() {
        for (property, value) in &relationship.properties {
            index
                .entry_or_default((relationship.rel_type, property.clone(), value.clone()))
                .insert(relationship.id);
        }
    }
    index
}

fn node_property_index_reference_count(index: &NodePropertyIndex) -> usize {
    index.values().map(|node_ids| node_ids.len()).sum()
}

fn relationship_property_index_reference_count(index: &RelationshipPropertyIndex) -> usize {
    index.values().map(|rel_ids| rel_ids.len()).sum()
}

fn property_index_mismatch_summary(
    maintained: &NodePropertyIndex,
    recomputed: &NodePropertyIndex,
) -> (usize, usize, usize, Vec<(LabelId, String, Value)>) {
    let mut missing_key_count = 0usize;
    let mut extra_key_count = 0usize;
    let mut mismatched_key_count = 0usize;
    let mut mismatched_keys = Vec::new();
    for key in maintained
        .keys()
        .chain(recomputed.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
    {
        match (maintained.get(&key), recomputed.get(&key)) {
            (Some(left), Some(right)) if left == right => {}
            (Some(_), Some(_)) => mismatched_key_count += 1,
            (Some(_), None) => extra_key_count += 1,
            (None, Some(_)) => missing_key_count += 1,
            (None, None) => {}
        }
        if maintained.get(&key) != recomputed.get(&key)
            && mismatched_keys.len() < MAX_ADJACENCY_CONSISTENCY_SAMPLES
        {
            mismatched_keys.push(key);
        }
    }
    (
        missing_key_count,
        extra_key_count,
        mismatched_key_count,
        mismatched_keys,
    )
}

fn relationship_property_index_mismatch_summary(
    maintained: &RelationshipPropertyIndex,
    recomputed: &RelationshipPropertyIndex,
) -> (usize, usize, usize, Vec<(RelTypeId, String, Value)>) {
    let mut missing_key_count = 0usize;
    let mut extra_key_count = 0usize;
    let mut mismatched_key_count = 0usize;
    let mut mismatched_keys = Vec::new();
    for key in maintained
        .keys()
        .chain(recomputed.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
    {
        match (maintained.get(&key), recomputed.get(&key)) {
            (Some(left), Some(right)) if left == right => {}
            (Some(_), Some(_)) => mismatched_key_count += 1,
            (Some(_), None) => extra_key_count += 1,
            (None, Some(_)) => missing_key_count += 1,
            (None, None) => {}
        }
        if maintained.get(&key) != recomputed.get(&key)
            && mismatched_keys.len() < MAX_ADJACENCY_CONSISTENCY_SAMPLES
        {
            mismatched_keys.push(key);
        }
    }
    (
        missing_key_count,
        extra_key_count,
        mismatched_key_count,
        mismatched_keys,
    )
}

fn compute_basic_statistics(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    computed_at_commit_epoch: u64,
) -> BasicGraphStatistics {
    let mut statistics = BasicGraphStatistics {
        computed_at_commit_epoch,
        node_count: nodes.len() as u64,
        relationship_count: relationships.len() as u64,
        ..BasicGraphStatistics::default()
    };
    for node in nodes.values() {
        for label_id in &node.labels {
            *statistics.label_counts.entry(*label_id).or_default() += 1;
        }
    }
    for relationship in relationships.values() {
        *statistics
            .rel_type_counts
            .entry(relationship.rel_type)
            .or_default() += 1;
    }
    statistics
}

fn decrement_counter<K>(counts: &mut BTreeMap<K, u64>, key: &K)
where
    K: Ord,
{
    let Some(count) = counts.get_mut(key) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        counts.remove(key);
    }
}

#[derive(Debug, Default)]
struct BoundedPathStatistics {
    counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
    source_distinct_counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
    target_distinct_counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
}

fn compute_bounded_path_statistics(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    outgoing_by_source_type: &BTreeMap<(NodeId, RelTypeId), Vec<NodeId>>,
    max_hops: usize,
) -> BoundedPathStatistics {
    let mut accumulator = BoundedPathStatAccumulator::default();
    let context = BoundedPathStatContext {
        nodes,
        outgoing_by_source_type,
        max_hops,
    };
    let rel_types = outgoing_by_source_type
        .keys()
        .map(|(_, rel_type)| *rel_type)
        .collect::<BTreeSet<_>>();
    for source in nodes.values() {
        for source_label in &source.labels {
            for rel_type in &rel_types {
                context.collect(
                    source.id,
                    source.id,
                    *source_label,
                    *rel_type,
                    1,
                    &mut accumulator,
                );
            }
        }
    }
    BoundedPathStatistics {
        counts: accumulator.counts,
        source_distinct_counts: accumulator
            .sources
            .into_iter()
            .map(|(path, sources)| (path, sources.len() as u64))
            .collect(),
        target_distinct_counts: accumulator
            .targets
            .into_iter()
            .map(|(path, targets)| (path, targets.len() as u64))
            .collect(),
    }
}

struct BoundedPathStatContext<'a> {
    nodes: &'a CowSegmentedMap<NodeId, NodeRecord>,
    outgoing_by_source_type: &'a BTreeMap<(NodeId, RelTypeId), Vec<NodeId>>,
    max_hops: usize,
}

#[derive(Debug, Default)]
struct BoundedPathStatAccumulator {
    counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
    sources: BTreeMap<(LabelId, RelTypeId, LabelId, usize), BTreeSet<NodeId>>,
    targets: BTreeMap<(LabelId, RelTypeId, LabelId, usize), BTreeSet<NodeId>>,
}

impl BoundedPathStatContext<'_> {
    fn collect(
        &self,
        root_source: NodeId,
        current: NodeId,
        source_label: LabelId,
        rel_type: RelTypeId,
        hop: usize,
        accumulator: &mut BoundedPathStatAccumulator,
    ) {
        if hop > self.max_hops {
            return;
        }
        let Some(targets) = self.outgoing_by_source_type.get(&(current, rel_type)) else {
            return;
        };
        for target_id in targets {
            let Some(target) = self.nodes.get(target_id) else {
                continue;
            };
            for target_label in &target.labels {
                let path_key = (source_label, rel_type, *target_label, hop);
                *accumulator.counts.entry(path_key).or_default() += 1;
                accumulator
                    .sources
                    .entry(path_key)
                    .or_default()
                    .insert(root_source);
                accumulator
                    .targets
                    .entry(path_key)
                    .or_default()
                    .insert(*target_id);
            }
            self.collect(
                root_source,
                *target_id,
                source_label,
                rel_type,
                hop + 1,
                accumulator,
            );
        }
    }
}

fn adaptive_histogram_sample_limit(distinct_count: usize) -> usize {
    if distinct_count <= MID_PROPERTY_HISTOGRAM_DISTINCT_VALUES {
        MIN_PROPERTY_HISTOGRAM_VALUES
    } else if distinct_count <= MAX_PROPERTY_HISTOGRAM_DISTINCT_VALUES {
        MID_PROPERTY_HISTOGRAM_VALUES
    } else {
        MAX_PROPERTY_HISTOGRAM_VALUES
    }
}

fn sample_histogram_values(values: BTreeSet<Value>) -> Vec<Value> {
    let len = values.len();
    let sample_limit = adaptive_histogram_sample_limit(len);
    if len <= sample_limit {
        return values.into_iter().collect();
    }
    let sorted = values.into_iter().collect::<Vec<_>>();
    (0..sample_limit)
        .map(|sample_index| {
            let value_index = sample_index * (len - 1) / (sample_limit - 1);
            sorted[value_index].clone()
        })
        .collect()
}

fn property_filter_matches(
    filter: &PropertyFilter,
    id: u64,
    properties: &BTreeMap<String, Value>,
) -> bool {
    match filter {
        PropertyFilter::And(filters) => filters
            .iter()
            .all(|filter| property_filter_matches(filter, id, properties)),
        PropertyFilter::Or(filters) => filters
            .iter()
            .any(|filter| property_filter_matches(filter, id, properties)),
        PropertyFilter::Not(filter) => !property_filter_matches(filter, id, properties),
        PropertyFilter::IdEq { value } => &Value::Int(id as i64) == value,
        PropertyFilter::IdNotEq { value } => &Value::Int(id as i64) != value,
        PropertyFilter::IdRange { lower, upper } => {
            range_bounds_match(&Value::Int(id as i64), lower.as_ref(), upper.as_ref())
        }
        PropertyFilter::IdIn { values } => {
            values.iter().any(|value| value == &Value::Int(id as i64))
        }
        PropertyFilter::Eq { property, value } => properties
            .get(property)
            .map(|actual| actual == value)
            .unwrap_or(false),
        PropertyFilter::NotEq { property, value } => properties
            .get(property)
            .map(|actual| actual != value)
            .unwrap_or(false),
        PropertyFilter::IsNull { property } => properties
            .get(property)
            .map(|actual| actual == &Value::Null)
            .unwrap_or(true),
        PropertyFilter::IsNotNull { property } => properties
            .get(property)
            .map(|actual| actual != &Value::Null)
            .unwrap_or(false),
        PropertyFilter::In { property, values } => properties
            .get(property)
            .map(|actual| values.iter().any(|value| value == actual))
            .unwrap_or(false),
        PropertyFilter::ListContains { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::List(values) => Some(values.iter().any(|actual| actual == value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::ListContainsLower { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::List(values) => Some(values.iter().any(|actual| match actual {
                    Value::String(actual) => actual.to_lowercase().contains(value),
                    _ => false,
                })),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::Contains { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.contains(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::StartsWith { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.starts_with(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::EndsWith { property, value } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(actual.ends_with(value)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::RegexMatch { property, pattern } => properties
            .get(property)
            .and_then(|actual| match actual {
                Value::String(actual) => Some(pattern.is_match(actual)),
                _ => None,
            })
            .unwrap_or(false),
        PropertyFilter::DefaultIfNullOrEq {
            property,
            empty,
            default,
            value,
            negated,
        } => {
            let actual = properties.get(property).unwrap_or(&Value::Null);
            let normalized = if actual == &Value::Null || actual == empty {
                default
            } else {
                actual
            };
            let matches = normalized == value;
            if *negated {
                !matches
            } else {
                matches
            }
        }
        PropertyFilter::Range {
            property,
            lower,
            upper,
        } => properties
            .get(property)
            .map(|actual| range_bounds_match(actual, lower.as_ref(), upper.as_ref()))
            .unwrap_or(false),
    }
}

fn properties_contain_all(
    properties: &BTreeMap<String, Value>,
    required: &BTreeMap<String, Value>,
) -> bool {
    required
        .iter()
        .all(|(property, value)| properties.get(property) == Some(value))
}

fn adjacency_layout_for_degree(degree: usize) -> AdjacencyLayout {
    if degree >= DENSE_ADJACENCY_DEGREE_THRESHOLD {
        AdjacencyLayout::Dense
    } else {
        AdjacencyLayout::Sparse
    }
}

fn adjacency_consolidation_plan(
    candidates: &[AdjacencyConsolidationCandidate],
) -> AdjacencyConsolidationPlan {
    candidates.iter().fold(
        AdjacencyConsolidationPlan::default(),
        |mut plan, candidate| {
            plan.group_count = plan.group_count.saturating_add(1);
            plan.delta_entry_count = plan
                .delta_entry_count
                .saturating_add(candidate.delta_entry_count);
            plan.estimated_entries = plan
                .estimated_entries
                .saturating_add(candidate.estimated_entries);
            plan
        },
    )
}

fn maintained_adjacency_groups(
    outgoing: &CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
    incoming: &CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
) -> AdjacencyGroups {
    let mut groups = AdjacencyGroups::new();
    for ((node_id, rel_type), rel_ids) in outgoing.iter() {
        groups.insert(
            AdjacencyGroupKey {
                node_id: *node_id,
                rel_type: *rel_type,
                direction: AdjacencyDirection::Outgoing,
            },
            rel_ids
                .iter_copied()
                .map(|entry| entry.relationship_id)
                .collect(),
        );
    }
    for ((node_id, rel_type), rel_ids) in incoming.iter() {
        groups.insert(
            AdjacencyGroupKey {
                node_id: *node_id,
                rel_type: *rel_type,
                direction: AdjacencyDirection::Incoming,
            },
            rel_ids
                .iter_copied()
                .map(|entry| entry.relationship_id)
                .collect(),
        );
    }
    groups
}

fn recompute_adjacency_groups(
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> AdjacencyGroups {
    let mut groups = AdjacencyGroups::new();
    for relationship in relationships.values() {
        groups
            .entry(AdjacencyGroupKey {
                node_id: relationship.source,
                rel_type: relationship.rel_type,
                direction: AdjacencyDirection::Outgoing,
            })
            .or_default()
            .insert(relationship.id);
        groups
            .entry(AdjacencyGroupKey {
                node_id: relationship.target,
                rel_type: relationship.rel_type,
                direction: AdjacencyDirection::Incoming,
            })
            .or_default()
            .insert(relationship.id);
    }
    groups
}

fn compute_degree_statistics_from_adjacency(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    outgoing: &CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
    incoming: &CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
) -> BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry> {
    compute_degree_statistics_from_groups(nodes, maintained_adjacency_groups(outgoing, incoming))
}

fn compute_degree_statistics_from_relationships(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
) -> BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry> {
    compute_degree_statistics_from_groups(nodes, recompute_adjacency_groups(relationships))
}

fn compute_degree_statistics_from_groups(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    groups: AdjacencyGroups,
) -> BTreeMap<DegreeStatisticsKey, DegreeStatisticsEntry> {
    let rel_types = groups
        .keys()
        .map(|key| key.rel_type)
        .collect::<BTreeSet<_>>();
    let label_counts = label_counts_for_degree_statistics(nodes);
    let mut statistics = BTreeMap::<DegreeStatisticsKey, DegreeStatisticsEntry>::new();
    for (label_id, node_count) in &label_counts {
        for rel_type in &rel_types {
            for direction in [AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming] {
                statistics.insert(
                    DegreeStatisticsKey {
                        label_id: *label_id,
                        rel_type: *rel_type,
                        direction,
                    },
                    DegreeStatisticsEntry {
                        node_count: *node_count,
                        non_zero_node_count: 0,
                        relationship_count: 0,
                        max_degree: 0,
                        dense_node_count: 0,
                    },
                );
            }
        }
    }
    for (group, rel_ids) in groups {
        let Some(node) = nodes.get(&group.node_id) else {
            continue;
        };
        let degree = rel_ids.len() as u64;
        for label_id in &node.labels {
            let entry = statistics
                .entry(DegreeStatisticsKey {
                    label_id: *label_id,
                    rel_type: group.rel_type,
                    direction: group.direction,
                })
                .or_insert(DegreeStatisticsEntry {
                    node_count: label_counts.get(label_id).copied().unwrap_or_default(),
                    non_zero_node_count: 0,
                    relationship_count: 0,
                    max_degree: 0,
                    dense_node_count: 0,
                });
            entry.non_zero_node_count += 1;
            entry.relationship_count += degree;
            entry.max_degree = entry.max_degree.max(degree);
            if rel_ids.len() >= DENSE_ADJACENCY_DEGREE_THRESHOLD {
                entry.dense_node_count += 1;
            }
        }
    }
    statistics
}

fn label_counts_for_degree_statistics(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
) -> BTreeMap<LabelId, u64> {
    let mut label_counts = BTreeMap::new();
    for node in nodes.values() {
        for label_id in &node.labels {
            *label_counts.entry(*label_id).or_default() += 1;
        }
    }
    label_counts
}

fn sample_relationship_ids(rel_ids: &BTreeSet<RelId>) -> Vec<RelId> {
    rel_ids
        .iter()
        .copied()
        .take(MAX_ADJACENCY_CONSISTENCY_SAMPLES)
        .collect()
}

fn adjacency_direction_sort_key(direction: AdjacencyDirection) -> u8 {
    match direction {
        AdjacencyDirection::Outgoing => 0,
        AdjacencyDirection::Incoming => 1,
    }
}

fn generated_stable_id(kind: &str, physical_id: u64) -> Value {
    Value::Map(BTreeMap::from([
        (
            "source".to_string(),
            Value::String("skein-stable-id-v1".to_string()),
        ),
        ("kind".to_string(), Value::String(kind.to_string())),
        (
            "physical_id".to_string(),
            Value::String(physical_id.to_string()),
        ),
    ]))
}

pub(crate) fn evaluate_node_set_value(
    properties: &BTreeMap<String, Value>,
    assignment: &NodeSetAssignment,
) -> Result<Value> {
    match &assignment.value {
        NodeSetValue::Value(value) => Ok(value.clone()),
        NodeSetValue::Coalesce { default } => Ok(match properties.get(&assignment.property) {
            None | Some(Value::Null) => default.clone(),
            Some(value) => value.clone(),
        }),
        NodeSetValue::AddInt { amount } => {
            let current = match properties.get(&assignment.property) {
                None | Some(Value::Null) => 0,
                Some(Value::Int(value)) => *value,
                Some(value) => {
                    return Err(SkeinError::Execution(format!(
                        "property increment requires an integer or null value, got {value:?}"
                    )));
                }
            };
            Ok(Value::Int(current.checked_add(*amount).ok_or_else(
                || SkeinError::Execution("property increment overflowed i64".to_string()),
            )?))
        }
        NodeSetValue::DecrementFloorZero => {
            let current = match properties.get(&assignment.property) {
                None | Some(Value::Null) => 0,
                Some(Value::Int(value)) => *value,
                Some(value) => {
                    return Err(SkeinError::Execution(format!(
                        "property decrement requires an integer or null value, got {value:?}"
                    )));
                }
            };
            Ok(Value::Int(if current > 0 { current - 1 } else { 0 }))
        }
        NodeSetValue::PreserveNewerExisting { incoming, preserve } => {
            let current = properties
                .get(&assignment.property)
                .cloned()
                .unwrap_or(Value::Null);
            if incoming == &Value::Null {
                return Ok(current);
            }
            if *preserve
                && current != Value::Null
                && value_gt_for_preserve_newer_existing(&current, incoming)
            {
                return Ok(current);
            }
            Ok(incoming.clone())
        }
    }
}

fn value_gt_for_preserve_newer_existing(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => left > right,
        (Value::Float(left), Value::Float(right)) => left > right,
        (Value::Int(left), Value::Float(right)) => (*left as f64) > *right,
        (Value::Float(left), Value::Int(right)) => *left > (*right as f64),
        (Value::String(left), Value::String(right)) => left > right,
        _ => false,
    }
}

fn apply_node_assignments_to_properties(
    properties: &mut BTreeMap<String, Value>,
    assignments: &[NodeSetAssignment],
) -> Result<()> {
    for assignment in assignments {
        let value = evaluate_node_set_value(properties, assignment)?;
        properties.insert(assignment.property.clone(), value);
    }
    Ok(())
}

fn optional_label_id(catalog: &Catalog, label: &str) -> Option<LabelId> {
    if label.is_empty() {
        None
    } else {
        catalog.label_id(label)
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_set_relationship_properties_mutation(
    store: &GraphStore,
    catalog: &Catalog,
    ops: &mut Vec<WalOp>,
    rows: &mut Vec<BTreeMap<String, Value>>,
    pending_nodes: &[PendingNode],
    pending_relationships: &mut [PendingRelationship],
    update: RelationshipPropertiesUpdate,
    limits: MutationLimits,
) -> Result<()> {
    let (Some(source_label_id), Some(target_label_id), Some(rel_type_id)) = (
        catalog.label_id(&update.source_label),
        catalog.label_id(&update.target_label),
        catalog.rel_type_id(&update.rel_type),
    ) else {
        return Ok(());
    };
    let source_ids = store
        .matching_node_ids_with_pending_bounded(
            Some(source_label_id),
            update.filter.as_ref(),
            pending_nodes,
            remaining_mutation_affected_rows(rows.len(), limits)?,
            "max_mutation_affected_rows",
        )?
        .into_iter()
        .collect::<BTreeSet<_>>();
    for relationship in store.relationship_records_owned() {
        let relationship = relationship?;
        if relationship.rel_type != rel_type_id || !source_ids.contains(&relationship.source) {
            continue;
        }
        if update
            .rel_filter
            .as_ref()
            .map(|filter| {
                !property_filter_matches(filter, relationship.id.0, &relationship.properties)
            })
            .unwrap_or(false)
        {
            continue;
        }
        let target_matches = store
            .node_owned(relationship.target)?
            .map(|target| {
                target.labels.contains(&target_label_id)
                    && update
                        .target_filter
                        .as_ref()
                        .map(|filter| {
                            property_filter_matches(filter, target.id.0, &target.properties)
                        })
                        .unwrap_or(true)
            })
            .unwrap_or(false);
        if !target_matches {
            continue;
        }
        ensure_additional_mutation_limits(
            ops.len(),
            rows.len(),
            update.assignments.len(),
            1,
            limits,
        )?;
        for assignment in &update.assignments {
            ops.push(WalOp::SetRelationshipProperty {
                id: relationship.id,
                property: assignment.property.clone(),
                value: assignment.value.clone(),
            });
        }
        rows.push(BTreeMap::from([(
            "rel_id".to_string(),
            Value::Int(relationship.id.0 as i64),
        )]));
    }
    for (relationship_id, source, target, pending_rel_type_id, properties) in pending_relationships
    {
        if *pending_rel_type_id != rel_type_id || !source_ids.contains(source) {
            continue;
        }
        if update
            .rel_filter
            .as_ref()
            .map(|filter| !property_filter_matches(filter, relationship_id.0, properties))
            .unwrap_or(false)
        {
            continue;
        }
        if !node_matches_label_and_filter(
            store,
            pending_nodes,
            *target,
            target_label_id,
            update.target_filter.as_ref(),
        )? {
            continue;
        }
        ensure_additional_mutation_limits(ops.len(), rows.len(), 0, 1, limits)?;
        for assignment in &update.assignments {
            properties.insert(assignment.property.clone(), assignment.value.clone());
            apply_pending_relationship_property(
                ops,
                *relationship_id,
                &assignment.property,
                assignment.value.clone(),
            );
        }
        rows.push(BTreeMap::from([(
            "rel_id".to_string(),
            Value::Int(relationship_id.0 as i64),
        )]));
    }
    Ok(())
}

fn node_matches_label_and_filter(
    store: &GraphStore,
    pending_nodes: &[PendingNode],
    id: NodeId,
    label_id: LabelId,
    filter: Option<&PropertyFilter>,
) -> Result<bool> {
    if let Some(node) = store.node_owned(id)? {
        return Ok(node.labels.contains(&label_id)
            && filter
                .map(|filter| property_filter_matches(filter, id.0, &node.properties))
                .unwrap_or(true));
    }
    Ok(pending_nodes
        .iter()
        .find(|(pending_id, _, _)| *pending_id == id)
        .map(|(_, pending_label_id, properties)| {
            *pending_label_id == label_id
                && filter
                    .map(|filter| property_filter_matches(filter, id.0, properties))
                    .unwrap_or(true)
        })
        .unwrap_or(false))
}

fn relationships_with_pending_matching_bounded(
    store: &GraphStore,
    pending_nodes: &[PendingNode],
    pending_relationships: &[PendingRelationship],
    request: RelationshipMatchRequest<'_>,
    max_relationships: usize,
) -> Result<Vec<RelationshipCandidate>> {
    let mut relationships = Vec::new();
    for relationship in store.relationship_records_owned() {
        let relationship = relationship?;
        if relationship.rel_type != request.rel_type_id
            || !properties_contain_all(&relationship.properties, request.rel_properties)
            || !node_matches_optional_label_and_filter_checked(
                store,
                pending_nodes,
                relationship.source,
                request.source_label_id,
                request.source_filter,
            )?
            || !node_matches_optional_label_and_filter_checked(
                store,
                pending_nodes,
                relationship.target,
                request.target_label_id,
                request.target_filter,
            )?
        {
            continue;
        }
        relationships.push(RelationshipCandidate {
            source: relationship.source,
            target: relationship.target,
            properties: relationship.properties.clone(),
        });
        if relationships.len() > max_relationships {
            return Err(SkeinError::Execution(format!(
                "mutation would exceed max_mutation_affected_rows {max_relationships}"
            )));
        }
    }

    for (_, source, target, pending_rel_type_id, properties) in pending_relationships {
        if *pending_rel_type_id != request.rel_type_id
            || !properties_contain_all(properties, request.rel_properties)
            || !node_matches_optional_label_and_filter_checked(
                store,
                pending_nodes,
                *source,
                request.source_label_id,
                request.source_filter,
            )?
            || !node_matches_optional_label_and_filter_checked(
                store,
                pending_nodes,
                *target,
                request.target_label_id,
                request.target_filter,
            )?
        {
            continue;
        }
        relationships.push(RelationshipCandidate {
            source: *source,
            target: *target,
            properties: properties.clone(),
        });
        if relationships.len() > max_relationships {
            return Err(SkeinError::Execution(format!(
                "mutation would exceed max_mutation_affected_rows {max_relationships}"
            )));
        }
    }
    Ok(relationships)
}

fn node_matches_optional_label_and_filter_checked(
    store: &GraphStore,
    pending_nodes: &[PendingNode],
    id: NodeId,
    label_id: Option<LabelId>,
    filter: Option<&PropertyFilter>,
) -> Result<bool> {
    if let Some(node) = store.node_owned(id)? {
        return Ok(
            label_id.is_none_or(|label_id| node.labels.contains(&label_id))
                && filter
                    .is_none_or(|filter| property_filter_matches(filter, id.0, &node.properties)),
        );
    }
    Ok(pending_nodes
        .iter()
        .find(|(pending_id, _, _)| *pending_id == id)
        .is_some_and(|(_, pending_label_id, properties)| {
            label_id.is_none_or(|label_id| *pending_label_id == label_id)
                && filter.is_none_or(|filter| property_filter_matches(filter, id.0, properties))
        }))
}

fn apply_pending_relationship_property(ops: &mut [WalOp], id: RelId, property: &str, value: Value) {
    for op in ops {
        if let WalOp::CreateRelationship {
            id: create_id,
            properties,
            ..
        } = op
            && *create_id == id
        {
            properties.insert(property.to_string(), value);
            break;
        }
    }
}

fn pending_relationship_ids_for_nodes(
    pending_relationships: &[PendingRelationship],
    node_ids: &[NodeId],
) -> Vec<RelId> {
    let node_ids = node_ids.iter().copied().collect::<BTreeSet<_>>();
    pending_relationships
        .iter()
        .filter_map(|(relationship_id, source, target, _, _)| {
            (node_ids.contains(source) || node_ids.contains(target)).then_some(*relationship_id)
        })
        .collect()
}

fn remove_pending_node(ops: &mut Vec<WalOp>, pending_nodes: &mut Vec<PendingNode>, id: NodeId) {
    pending_nodes.retain(|(node_id, _, _)| *node_id != id);
    ops.retain(|op| match op {
        WalOp::CreateNode { id: node_id, .. } | WalOp::SetNodeProperty { id: node_id, .. } => {
            *node_id != id
        }
        _ => true,
    });
}

fn remove_pending_relationship(
    ops: &mut Vec<WalOp>,
    pending_relationships: &mut Vec<PendingRelationship>,
    id: RelId,
) {
    pending_relationships.retain(|(relationship_id, _, _, _, _)| *relationship_id != id);
    ops.retain(|op| match op {
        WalOp::CreateRelationship {
            id: relationship_id,
            ..
        }
        | WalOp::SetRelationshipProperty {
            id: relationship_id,
            ..
        } => *relationship_id != id,
        _ => true,
    });
}

fn range_bounds_match(
    value: &Value,
    lower: Option<&(Value, bool)>,
    upper: Option<&(Value, bool)>,
) -> bool {
    if let Some((bound, inclusive)) = lower {
        let Some(ordering) = comparable_value_ordering(value, bound) else {
            return false;
        };
        if ordering == std::cmp::Ordering::Less
            || (ordering == std::cmp::Ordering::Equal && !inclusive)
        {
            return false;
        }
    }
    if let Some((bound, inclusive)) = upper {
        let Some(ordering) = comparable_value_ordering(value, bound) else {
            return false;
        };
        if ordering == std::cmp::Ordering::Greater
            || (ordering == std::cmp::Ordering::Equal && !inclusive)
        {
            return false;
        }
    }
    true
}

fn comparable_value_ordering(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (Value::Float(left), Value::Float(right)) => Some(left.total_cmp(right)),
        (Value::Int(left), Value::Float(right)) => Some((*left as f64).total_cmp(right)),
        (Value::Float(left), Value::Int(right)) => Some(left.total_cmp(&(*right as f64))),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn merge_relationship_row(
    source: NodeId,
    relationship: RelId,
    target: NodeId,
    created: bool,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
        ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
        ("created".to_string(), Value::Bool(created)),
    ])
}

fn split_checkpoint_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "checkpoint missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "checkpoint checksum")?;
    Ok((body, checksum))
}

fn relational_checkpoint_metadata(body: &str) -> Result<Option<DurableArtifactMetadata>> {
    let mut encoded_len = None;
    let mut encoded_checksum = None;
    let mut encoded_sha256 = None;
    for line in body.lines() {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["relational_checkpoint_encoded_len", raw] if encoded_len.is_none() => {
                encoded_len = Some(parse_u64(raw, "relational checkpoint encoded length")?);
            }
            ["relational_checkpoint_encoded_checksum", raw] if encoded_checksum.is_none() => {
                encoded_checksum = Some(parse_u64(raw, "relational checkpoint encoded checksum")?);
            }
            ["relational_checkpoint_encoded_sha256", raw] if encoded_sha256.is_none() => {
                encoded_sha256 = Some(raw.parse().map_err(|error| {
                    SkeinError::Storage(format!(
                        "invalid relational checkpoint encoded SHA-256: {error}"
                    ))
                })?);
            }
            ["relational_checkpoint_encoded_len", _]
            | ["relational_checkpoint_encoded_checksum", _]
            | ["relational_checkpoint_encoded_sha256", _] => {
                return Err(SkeinError::Storage(
                    "checkpoint contains duplicate relational artifact metadata".to_string(),
                ));
            }
            _ => {}
        }
    }
    if !artifact_metadata_presence_consistent(encoded_len, encoded_checksum, encoded_sha256) {
        return Err(SkeinError::Storage(
            "checkpoint relational artifact metadata is incomplete".to_string(),
        ));
    }
    Ok(encoded_len.map(|encoded_len| DurableArtifactMetadata {
        encoded_len,
        encoded_checksum: encoded_checksum.expect("validated relational checksum"),
        encoded_sha256: encoded_sha256.expect("validated relational SHA-256"),
    }))
}

fn split_manifest_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "manifest missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "manifest checksum")?;
    Ok((body, checksum))
}

fn split_projected_graph_artifact_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "projected graph artifact missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "projected graph artifact checksum")?;
    Ok((body, checksum))
}

pub(crate) fn encode_durable_text(text: &str, compression: DurableCompression) -> Result<Vec<u8>> {
    match compression {
        DurableCompression::Zstd => encode_zstd_durable_text(text),
    }
}

fn encode_zstd_durable_text(text: &str) -> Result<Vec<u8>> {
    let compressed = zstd::stream::encode_all(text.as_bytes(), DEFAULT_COMPRESSION_LEVEL)
        .map_err(|error| SkeinError::Storage(format!("zstd compression failed: {error}")))?;
    let compressed_checksum = checksum_bytes(&compressed);
    let uncompressed_checksum = checksum_bytes(text.as_bytes());
    let header = format!(
        "{DURABLE_COMPRESSION_HEADER}\ncodec\tzstd\nuncompressed_checksum\t{uncompressed_checksum}\ncompressed_checksum\t{compressed_checksum}\nuncompressed_len\t{}\ncompressed_len\t{}\n\n",
        text.len(),
        compressed.len()
    );
    let mut encoded = header.into_bytes();
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

fn read_durable_text(path: &Path, name: &str) -> Result<String> {
    let bytes = fs::read(path)?;
    read_durable_text_bytes(&bytes, name)
}

pub(crate) fn read_durable_text_bytes(bytes: &[u8], name: &str) -> Result<String> {
    read_durable_text_bytes_with_limit(bytes, name, None)
}

fn read_durable_text_bytes_with_limit(
    bytes: &[u8],
    name: &str,
    max_decoded_bytes: Option<u64>,
) -> Result<String> {
    if !bytes.starts_with(DURABLE_COMPRESSION_HEADER.as_bytes()) {
        return Err(SkeinError::Storage(format!(
            "{name} is missing the V1 compressed envelope"
        )));
    }
    decode_compressed_durable_text(bytes, name, max_decoded_bytes)
}

fn decode_compressed_durable_text(
    bytes: &[u8],
    name: &str,
    max_decoded_bytes: Option<u64>,
) -> Result<String> {
    let Some(header_end) = bytes.windows(2).position(|window| window == b"\n\n") else {
        return Err(SkeinError::Storage(format!(
            "{name} compressed envelope missing header terminator"
        )));
    };
    let header = std::str::from_utf8(&bytes[..header_end]).map_err(|error| {
        SkeinError::Storage(format!(
            "{name} compressed envelope header is invalid: {error}"
        ))
    })?;
    let payload = &bytes[header_end + 2..];
    let mut codec = None;
    let mut compressed_checksum = None;
    let mut uncompressed_checksum = None;
    let mut compressed_len = None;
    let mut uncompressed_len = None;
    let mut seen_fields = BTreeSet::new();
    for line in header.lines() {
        if line == DURABLE_COMPRESSION_HEADER {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        if !seen_fields.insert(fields[0]) {
            return Err(SkeinError::Storage(format!(
                "{name} compressed envelope has duplicate field: {}",
                fields[0]
            )));
        }
        match fields.as_slice() {
            ["codec", value] => codec = Some(*value),
            ["compressed_checksum", value] => {
                compressed_checksum = Some(parse_u64(value, "compressed checksum")?);
            }
            ["uncompressed_checksum", value] => {
                uncompressed_checksum = Some(parse_u64(value, "uncompressed checksum")?);
            }
            ["compressed_len", value] => {
                compressed_len = Some(parse_usize(value, "compressed length")?);
            }
            ["uncompressed_len", value] => {
                uncompressed_len = Some(parse_usize(value, "uncompressed length")?);
            }
            _ => {
                return Err(SkeinError::Storage(format!(
                    "{name} compressed envelope has invalid header line: {line}"
                )));
            }
        }
    }
    if codec != Some("zstd") {
        return Err(SkeinError::Storage(format!(
            "{name} compressed envelope uses unsupported codec"
        )));
    }
    let expected_compressed_len = compressed_len.ok_or_else(|| {
        SkeinError::Storage(format!("{name} compressed envelope missing compressed_len"))
    })?;
    if payload.len() != expected_compressed_len {
        return Err(SkeinError::Storage(format!(
            "{name} compressed length mismatch: expected {expected_compressed_len}, got {}",
            payload.len()
        )));
    }
    let expected_compressed_checksum = compressed_checksum.ok_or_else(|| {
        SkeinError::Storage(format!(
            "{name} compressed envelope missing compressed_checksum"
        ))
    })?;
    let actual_compressed_checksum = checksum_bytes(payload);
    if actual_compressed_checksum != expected_compressed_checksum {
        return Err(SkeinError::Storage(format!(
            "{name} compressed checksum mismatch: expected {expected_compressed_checksum}, got {actual_compressed_checksum}"
        )));
    }
    let expected_uncompressed_len = uncompressed_len.ok_or_else(|| {
        SkeinError::Storage(format!(
            "{name} compressed envelope missing uncompressed_len"
        ))
    })?;
    if max_decoded_bytes.is_some_and(|limit| expected_uncompressed_len as u64 > limit) {
        return Err(SkeinError::Storage(format!(
            "{name} decoded byte limit exceeded: max_decoded_bytes={}",
            max_decoded_bytes.unwrap_or_default()
        )));
    }
    let decode_limit = max_decoded_bytes
        .unwrap_or(expected_uncompressed_len as u64)
        .min(usize::MAX as u64);
    let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(payload)).map_err(|error| {
        SkeinError::Storage(format!("{name} zstd decompression failed: {error}"))
    })?;
    let initial_capacity = expected_uncompressed_len.min(8 * 1024 * 1024);
    let mut decoded = Vec::with_capacity(initial_capacity);
    decoder
        .by_ref()
        .take(decode_limit.saturating_add(1))
        .read_to_end(&mut decoded)
        .map_err(|error| {
            SkeinError::Storage(format!("{name} zstd decompression failed: {error}"))
        })?;
    if decoded.len() as u64 > decode_limit {
        return Err(SkeinError::Storage(format!(
            "{name} decoded byte limit exceeded: max_decoded_bytes={decode_limit}"
        )));
    }
    if decoded.len() != expected_uncompressed_len {
        return Err(SkeinError::Storage(format!(
            "{name} uncompressed length mismatch: expected {expected_uncompressed_len}, got {}",
            decoded.len()
        )));
    }
    let expected_uncompressed_checksum = uncompressed_checksum.ok_or_else(|| {
        SkeinError::Storage(format!(
            "{name} compressed envelope missing uncompressed_checksum"
        ))
    })?;
    let actual_uncompressed_checksum = checksum_bytes(&decoded);
    if actual_uncompressed_checksum != expected_uncompressed_checksum {
        return Err(SkeinError::Storage(format!(
            "{name} uncompressed checksum mismatch: expected {expected_uncompressed_checksum}, got {actual_uncompressed_checksum}"
        )));
    }
    String::from_utf8(decoded).map_err(|error| {
        SkeinError::Storage(format!(
            "{name} decompressed payload is not valid UTF-8: {error}"
        ))
    })
}

fn parse_label_set(input: &str) -> Result<BTreeSet<LabelId>> {
    if input.is_empty() {
        return Ok(BTreeSet::new());
    }
    input
        .split(',')
        .map(|raw| parse_u32(raw, "label id").map(LabelId))
        .collect()
}

fn encode_string_vec(values: &[String]) -> String {
    values
        .iter()
        .map(|value| encode_string(value))
        .collect::<Vec<_>>()
        .join(":")
}

fn decode_string_vec(input: &str) -> Result<Vec<String>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input.split(':').map(decode_string).collect()
}

fn encode_search_projection_relational_primary_key_changes(
    capture: &skein_storage::RelationalPrimaryKeyChangeCapture,
) -> Result<(String, String)> {
    match capture {
        skein_storage::RelationalPrimaryKeyChangeCapture::Captured { tables, .. } => {
            let mut encoded_tables = Vec::with_capacity(tables.len());
            for table in tables {
                let encoded_keys = table
                    .primary_keys
                    .iter()
                    .map(|key| {
                        skein_storage::encode_relational_primary_key(key)
                            .map(|encoded| encode_bytes(&encoded))
                            .map_err(|error| SkeinError::Storage(error.to_string()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                encoded_tables.push(format!(
                    "{}={}",
                    encode_string(&table.table),
                    encoded_keys.join(":")
                ));
            }
            Ok(("exact".to_string(), encoded_tables.join(";")))
        }
        skein_storage::RelationalPrimaryKeyChangeCapture::RequiresRebuild { reason } => Ok((
            match reason {
                skein_storage::RelationalPrimaryKeyChangeRebuildReason::SchemaRewrite => {
                    "rebuild_schema_rewrite"
                }
                skein_storage::RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded => {
                    "rebuild_capture_limit"
                }
                skein_storage::RelationalPrimaryKeyChangeRebuildReason::UnsupportedKeyEncoding => {
                    "rebuild_key_encoding"
                }
                skein_storage::RelationalPrimaryKeyChangeRebuildReason::WalEncodingLimitExceeded => {
                    "rebuild_wal_encoding_limit"
                }
                skein_storage::RelationalPrimaryKeyChangeRebuildReason::MissingWalCapture => {
                    "rebuild_missing_wal_capture"
                }
                skein_storage::RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement => {
                    "rebuild_snapshot_replacement"
                }
                skein_storage::RelationalPrimaryKeyChangeRebuildReason::MultipleRelationalTransactions => {
                    "rebuild_multiple_relational_transactions"
                }
            }
            .to_string(),
            String::new(),
        )),
    }
}

fn decode_search_projection_relational_primary_key_changes(
    raw_kind: &str,
    raw_changes: &str,
) -> Result<skein_storage::RelationalPrimaryKeyChangeCapture> {
    use skein_storage::{
        RelationalPrimaryKeyChangeCapture, RelationalPrimaryKeyChangeRebuildReason,
        RelationalTablePrimaryKeyChanges,
    };

    let rebuild_reason = match raw_kind {
        "exact" => None,
        "rebuild_schema_rewrite" => Some(RelationalPrimaryKeyChangeRebuildReason::SchemaRewrite),
        "rebuild_capture_limit" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded)
        }
        "rebuild_key_encoding" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::UnsupportedKeyEncoding)
        }
        "rebuild_wal_encoding_limit" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::WalEncodingLimitExceeded)
        }
        "rebuild_missing_wal_capture" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::MissingWalCapture)
        }
        "rebuild_snapshot_replacement" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement)
        }
        "rebuild_multiple_relational_transactions" => {
            Some(RelationalPrimaryKeyChangeRebuildReason::MultipleRelationalTransactions)
        }
        _ => {
            return Err(SkeinError::Storage(format!(
                "invalid search projection relational change kind: {raw_kind}"
            )))
        }
    };
    if let Some(reason) = rebuild_reason {
        if !raw_changes.is_empty() {
            return Err(SkeinError::Storage(format!(
                "search projection rebuild marker {raw_kind} contains unexpected key payload"
            )));
        }
        return Ok(RelationalPrimaryKeyChangeCapture::RequiresRebuild { reason });
    }

    const TABLE_FIXED_BYTES: usize = 4;
    const KEY_FIXED_BYTES: usize = 4;
    let mut tables = Vec::new();
    let mut encoded_bytes = 0usize;
    if !raw_changes.is_empty() {
        for raw_table in raw_changes.split(';') {
            let Some((raw_name, raw_keys)) = raw_table.split_once('=') else {
                return Err(SkeinError::Storage(format!(
                    "invalid search projection relational table change: {raw_table}"
                )));
            };
            let table = decode_string(raw_name)?;
            if raw_keys.is_empty() {
                return Err(SkeinError::Storage(format!(
                    "search projection relational table {table} contains no primary keys"
                )));
            }
            encoded_bytes = encoded_bytes
                .checked_add(TABLE_FIXED_BYTES)
                .and_then(|bytes| bytes.checked_add(table.len()))
                .ok_or_else(|| {
                    SkeinError::Storage(
                        "search projection relational change byte count overflow".to_string(),
                    )
                })?;
            let mut primary_keys = Vec::new();
            for raw_key in raw_keys.split(':') {
                let key_bytes = decode_bytes(raw_key)?;
                let key = skein_storage::decode_relational_primary_key(&key_bytes)
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
                encoded_bytes = encoded_bytes
                    .checked_add(KEY_FIXED_BYTES)
                    .and_then(|bytes| bytes.checked_add(key_bytes.len()))
                    .ok_or_else(|| {
                        SkeinError::Storage(
                            "search projection relational change byte count overflow".to_string(),
                        )
                    })?;
                primary_keys.push(key);
            }
            tables.push(RelationalTablePrimaryKeyChanges {
                table,
                primary_keys,
            });
        }
    }
    Ok(RelationalPrimaryKeyChangeCapture::Captured {
        tables,
        encoded_bytes,
    })
}

fn encode_value_vec(values: &[Value]) -> String {
    values
        .iter()
        .map(|value| encode_string(&encode_value(value)))
        .collect::<Vec<_>>()
        .join(":")
}

fn decode_value_vec(input: &str) -> Result<Vec<Value>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(':')
        .map(|value| decode_string(value).and_then(|value| decode_value(&value)))
        .collect()
}

fn encode_u64_vec(values: impl IntoIterator<Item = u64>) -> String {
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_u64_vec(input: &str, name: &str) -> Result<Vec<u64>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .map(|value| parse_u64(value, name))
        .collect()
}

fn validate_search_projection_checkpoint_changes(
    start_epoch: u64,
    checkpoint_commit_epoch: u64,
    changes: &[SearchProjectionGraphChange],
) -> Result<()> {
    if start_epoch > checkpoint_commit_epoch {
        return Err(SkeinError::Storage(format!(
            "search projection change log start epoch {start_epoch} exceeds checkpoint commit epoch {checkpoint_commit_epoch}"
        )));
    }
    let mut previous_epoch = start_epoch;
    for change in changes {
        if change.commit_epoch <= previous_epoch {
            return Err(SkeinError::Storage(format!(
                "search projection change commit epoch {} is not greater than previous epoch {previous_epoch}",
                change.commit_epoch
            )));
        }
        if change.commit_epoch > checkpoint_commit_epoch {
            return Err(SkeinError::Storage(format!(
                "search projection change commit epoch {} exceeds checkpoint commit epoch {checkpoint_commit_epoch}",
                change.commit_epoch
            )));
        }
        if !change
            .upsert_node_ids
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(SkeinError::Storage(format!(
                "search projection change at commit epoch {} has unordered or duplicate upsert node ids",
                change.commit_epoch
            )));
        }
        if !change
            .delete_document_ids
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(SkeinError::Storage(format!(
                "search projection change at commit epoch {} has unordered or duplicate delete document ids",
                change.commit_epoch
            )));
        }
        if let skein_storage::RelationalPrimaryKeyChangeCapture::Captured { tables, .. } =
            &change.relational_primary_key_changes
        {
            if !tables.windows(2).all(|pair| pair[0].table < pair[1].table) {
                return Err(SkeinError::Storage(format!(
                    "search projection change at commit epoch {} has unordered or duplicate relational tables",
                    change.commit_epoch
                )));
            }
            for table in tables {
                if table.primary_keys.is_empty()
                    || !table.primary_keys.windows(2).all(|pair| pair[0] < pair[1])
                {
                    return Err(SkeinError::Storage(format!(
                        "search projection change at commit epoch {} has empty, unordered, or duplicate primary keys for table {}",
                        change.commit_epoch, table.table
                    )));
                }
            }
        }
        previous_epoch = change.commit_epoch;
    }
    Ok(())
}

fn encode_usize_vec(values: impl IntoIterator<Item = usize>) -> String {
    values
        .into_iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_usize_vec(input: &str, name: &str) -> Result<Vec<usize>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    input
        .split(',')
        .map(|value| {
            value
                .parse()
                .map_err(|_| SkeinError::Storage(format!("invalid {name}: {value}")))
        })
        .collect()
}

pub(crate) fn encode_properties(properties: &BTreeMap<String, Value>) -> String {
    properties
        .iter()
        .map(|(key, value)| format!("{}={}", encode_string(key), encode_value(value)))
        .collect::<Vec<_>>()
        .join(";")
}

pub(crate) fn decode_properties(input: &str) -> Result<BTreeMap<String, Value>> {
    let mut properties = BTreeMap::new();
    if input.is_empty() {
        return Ok(properties);
    }
    for pair in input.split(';') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(SkeinError::Storage(format!(
                "invalid property pair: {pair}"
            )));
        };
        properties.insert(decode_string(key)?, decode_value(value)?);
    }
    Ok(properties)
}

pub(crate) fn encode_value(value: &Value) -> String {
    match value {
        Value::Null => "n".to_string(),
        Value::Bool(false) => "b0".to_string(),
        Value::Bool(true) => "b1".to_string(),
        Value::Int(value) => format!("i{value}"),
        Value::Float(value) => format!("f{}", value.to_bits()),
        Value::String(value) => format!("s{}", encode_string(value)),
        Value::Uuid(value) => format!("u{value}"),
        Value::Binary(value) => format!(
            "x{}",
            value
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
        Value::List(values) => format!(
            "l{}",
            values
                .iter()
                .map(|value| encode_string(&encode_value(value)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Map(values) => format!(
            "m{}",
            values
                .iter()
                .map(|(key, value)| format!(
                    "{}={}",
                    encode_string(key),
                    encode_string(&encode_value(value))
                ))
                .collect::<Vec<_>>()
                .join(";")
        ),
    }
}

pub(crate) fn decode_value(input: &str) -> Result<Value> {
    if input.is_empty() {
        return Err(SkeinError::Storage("empty encoded value".to_string()));
    }
    let (kind, rest) = input.split_at(1);
    match kind {
        "n" if rest.is_empty() => Ok(Value::Null),
        "b" => match rest {
            "0" => Ok(Value::Bool(false)),
            "1" => Ok(Value::Bool(true)),
            _ => Err(SkeinError::Storage(format!("invalid bool value: {input}"))),
        },
        "i" => parse_i64(rest, "integer value").map(Value::Int),
        "f" => parse_u64(rest, "float value")
            .map(f64::from_bits)
            .map(Value::Float),
        "s" => decode_string(rest).map(Value::String),
        "u" => skein_core::Uuid::parse_str(rest)
            .map(Value::Uuid)
            .map_err(|error| SkeinError::Storage(format!("invalid UUID value: {error}"))),
        "x" => decode_hex_value(rest),
        "l" => decode_list_value(rest),
        "m" => decode_map_value(rest),
        _ => Err(SkeinError::Storage(format!(
            "invalid encoded value: {input}"
        ))),
    }
}

fn decode_hex_value(input: &str) -> Result<Value> {
    if !input.len().is_multiple_of(2) {
        return Err(SkeinError::Storage(
            "binary value has an odd number of hex digits".to_string(),
        ));
    }
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let high = decode_hex_digit(digits[0])?;
            let low = decode_hex_digit(digits[1])?;
            Ok((high << 4) | low)
        })
        .collect::<Result<Vec<_>>>()
        .map(Value::Binary)
}

fn decode_hex_digit(digit: u8) -> Result<u8> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(SkeinError::Storage(format!(
            "binary value contains invalid hex digit {:?}",
            char::from(digit)
        ))),
    }
}

fn decode_list_value(input: &str) -> Result<Value> {
    if input.is_empty() {
        return Ok(Value::List(Vec::new()));
    }
    input
        .split(',')
        .map(|item| decode_string(item).and_then(|value| decode_value(&value)))
        .collect::<Result<Vec<_>>>()
        .map(Value::List)
}

fn decode_map_value(input: &str) -> Result<Value> {
    let mut values = BTreeMap::new();
    if input.is_empty() {
        return Ok(Value::Map(values));
    }
    for item in input.split(';') {
        let Some((key, value)) = item.split_once('=') else {
            return Err(SkeinError::Storage(format!(
                "invalid encoded map item: {item}"
            )));
        };
        values.insert(
            decode_string(key)?,
            decode_string(value).and_then(|value| decode_value(&value))?,
        );
    }
    Ok(Value::Map(values))
}

fn encode_table_kind(kind: TableKind) -> &'static str {
    match kind {
        TableKind::Node => "node",
        TableKind::Relationship => "relationship",
    }
}

fn decode_table_kind(input: &str) -> Result<TableKind> {
    match input {
        "node" => Ok(TableKind::Node),
        "relationship" => Ok(TableKind::Relationship),
        _ => Err(SkeinError::Storage(format!("invalid table kind: {input}"))),
    }
}

fn encode_property_type(value_type: PropertyType) -> &'static str {
    match value_type {
        PropertyType::Any => "any",
        PropertyType::Bool => "bool",
        PropertyType::Int => "int",
        PropertyType::Float => "float",
        PropertyType::String => "string",
        PropertyType::Text => "text",
        PropertyType::List => "list",
    }
}

fn decode_property_type(input: &str) -> Result<PropertyType> {
    match input {
        "any" => Ok(PropertyType::Any),
        "bool" => Ok(PropertyType::Bool),
        "int" => Ok(PropertyType::Int),
        "float" => Ok(PropertyType::Float),
        "string" => Ok(PropertyType::String),
        "text" => Ok(PropertyType::Text),
        "list" => Ok(PropertyType::List),
        _ => Err(SkeinError::Storage(format!(
            "invalid property type: {input}"
        ))),
    }
}

fn encode_index_kind(kind: IndexKind) -> &'static str {
    match kind {
        IndexKind::Equality => "equality",
        IndexKind::Range => "range",
        IndexKind::FullText => "fulltext",
    }
}

fn decode_index_kind(input: &str) -> Result<IndexKind> {
    match input {
        "equality" => Ok(IndexKind::Equality),
        "range" => Ok(IndexKind::Range),
        "fulltext" => Ok(IndexKind::FullText),
        _ => Err(SkeinError::Storage(format!("invalid index kind: {input}"))),
    }
}

fn encode_nullable(nullable: bool) -> &'static str {
    if nullable {
        "nullable"
    } else {
        "not_null"
    }
}

fn encode_bool(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

fn decode_bool(input: &str, name: &str) -> Result<bool> {
    match input {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(SkeinError::Storage(format!("invalid {name}: {input}"))),
    }
}

fn decode_nullable(input: &str) -> Result<bool> {
    match input {
        "nullable" => Ok(true),
        "not_null" => Ok(false),
        _ => Err(SkeinError::Storage(format!(
            "invalid nullable flag: {input}"
        ))),
    }
}

fn encode_schema_object_state(state: SchemaObjectState) -> &'static str {
    match state {
        SchemaObjectState::DeleteOnly => "delete_only",
        SchemaObjectState::WriteOnly => "write_only",
        SchemaObjectState::Backfill => "backfill",
        SchemaObjectState::Validating => "validating",
        SchemaObjectState::Public => "public",
        SchemaObjectState::Gc => "gc",
    }
}

fn decode_schema_object_state(input: &str) -> Result<SchemaObjectState> {
    match input {
        "delete_only" => Ok(SchemaObjectState::DeleteOnly),
        "write_only" => Ok(SchemaObjectState::WriteOnly),
        "backfill" => Ok(SchemaObjectState::Backfill),
        "validating" => Ok(SchemaObjectState::Validating),
        "public" => Ok(SchemaObjectState::Public),
        "gc" => Ok(SchemaObjectState::Gc),
        _ => Err(SkeinError::Storage(format!(
            "invalid schema object state: {input}"
        ))),
    }
}

pub(crate) fn encode_string(input: &str) -> String {
    encode_bytes(input.as_bytes())
}

fn encode_bytes(input: &[u8]) -> String {
    input.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn decode_string(input: &str) -> Result<String> {
    let bytes = decode_bytes(input)?;
    String::from_utf8(bytes).map_err(|error| SkeinError::Storage(error.to_string()))
}

fn decode_bytes(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(SkeinError::Storage(format!(
            "invalid hex string length: {}",
            input.len()
        )));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for offset in (0..input.len()).step_by(2) {
        let byte = u8::from_str_radix(&input[offset..offset + 2], 16)
            .map_err(|_| SkeinError::Storage(format!("invalid hex string: {input}")))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

pub(crate) fn checksum_bytes(bytes: &[u8]) -> u64 {
    checksum_u64(bytes)
}

fn verify_integrity(
    bytes: &[u8],
    expected_len: u64,
    expected_checksum: u64,
    expected_sha256: Sha256Digest,
    artifact: &str,
) -> Result<()> {
    let actual_len = bytes.len() as u64;
    if actual_len != expected_len {
        return Err(SkeinError::Storage(format!(
            "{artifact} encoded length mismatch: expected {expected_len}, got {actual_len}"
        )));
    }
    let actual = integrity_digest(bytes);
    if actual.crc32c.as_u64() != expected_checksum {
        return Err(SkeinError::Storage(format!(
            "{artifact} CRC32C mismatch: expected {expected_checksum}, got {}",
            actual.crc32c
        )));
    }
    if actual.sha256 != expected_sha256 {
        return Err(SkeinError::Storage(format!(
            "{artifact} SHA-256 mismatch: expected {expected_sha256}, got {}",
            actual.sha256
        )));
    }
    Ok(())
}

fn verify_file_integrity(
    path: &Path,
    expected_len: u64,
    expected_checksum: u64,
    expected_sha256: Sha256Digest,
    artifact: &str,
) -> Result<()> {
    let (actual_len, actual_checksum, actual_sha256) = file_checksum(path)?;
    if actual_len != expected_len {
        return Err(SkeinError::Storage(format!(
            "{artifact} encoded length mismatch: expected {expected_len}, got {actual_len}"
        )));
    }
    if actual_checksum != expected_checksum {
        return Err(SkeinError::Storage(format!(
            "{artifact} CRC32C mismatch: expected {expected_checksum}, got {actual_checksum}"
        )));
    }
    if actual_sha256 != expected_sha256 {
        return Err(SkeinError::Storage(format!(
            "{artifact} SHA-256 mismatch: expected {expected_sha256}, got {actual_sha256}"
        )));
    }
    Ok(())
}

fn elapsed_micros(started: std::time::Instant) -> u64 {
    started.elapsed().as_micros().min(u64::MAX as u128) as u64
}

pub(crate) fn parse_u64(input: &str, name: &str) -> Result<u64> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn parse_u32(input: &str, name: &str) -> Result<u32> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn parse_usize(input: &str, name: &str) -> Result<usize> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn parse_statistics_path_key(
    source: &str,
    rel_type: &str,
    target: &str,
) -> Result<(LabelId, RelTypeId, LabelId)> {
    Ok((
        LabelId(parse_u32(source, "statistics source label id")?),
        RelTypeId(parse_u32(rel_type, "statistics relationship type id")?),
        LabelId(parse_u32(target, "statistics target label id")?),
    ))
}

fn parse_statistics_bounded_path_key(
    source: &str,
    rel_type: &str,
    target: &str,
    hops: &str,
) -> Result<(LabelId, RelTypeId, LabelId, usize)> {
    let (source, rel_type, target) = parse_statistics_path_key(source, rel_type, target)?;
    Ok((
        source,
        rel_type,
        target,
        parse_usize(hops, "statistics bounded path hop count")?,
    ))
}

pub(crate) fn parse_i64(input: &str, name: &str) -> Result<i64> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn encode_optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn encode_optional_sha256(value: Option<Sha256Digest>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

fn parse_optional_u64(input: &str, name: &str) -> Result<Option<u64>> {
    if input == "none" {
        Ok(None)
    } else {
        parse_u64(input, name).map(Some)
    }
}

fn parse_optional_sha256(input: &str, name: &str) -> Result<Option<Sha256Digest>> {
    if input == "none" {
        Ok(None)
    } else {
        input
            .parse()
            .map(Some)
            .map_err(|error| SkeinError::Storage(format!("invalid {name}: {error}")))
    }
}

fn validate_storage_version(version: &str) -> Result<()> {
    if version == STORAGE_VERSION {
        return Ok(());
    }
    Err(SkeinError::Storage(format!(
        "unsupported storage version: {version}; expected {STORAGE_VERSION}"
    )))
}

fn estimated_node_record_bytes(node: &NodeRecord) -> u64 {
    32u64
        .saturating_add((node.labels.len() as u64).saturating_mul(4))
        .saturating_add(estimated_properties_bytes(&node.properties))
}

fn estimated_relationship_record_bytes(relationship: &RelRecord) -> u64 {
    40u64.saturating_add(estimated_properties_bytes(&relationship.properties))
}

fn estimated_properties_bytes(properties: &BTreeMap<String, Value>) -> u64 {
    properties.iter().fold(0u64, |bytes, (key, value)| {
        bytes
            .saturating_add(key.len() as u64)
            .saturating_add(estimated_value_bytes(value))
            .saturating_add(16)
    })
}

fn estimated_value_bytes(value: &Value) -> u64 {
    match value {
        Value::Null => 1,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => 8,
        Value::String(value) => value.len() as u64,
        Value::Binary(value) => value.len() as u64,
        Value::Uuid(_) => 16,
        Value::List(values) => values.iter().fold(16u64, |bytes, value| {
            bytes.saturating_add(estimated_value_bytes(value))
        }),
        Value::Map(values) => estimated_properties_bytes(values),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_adjacency_artifact_generation_file, canonical_manifest_generation_file,
        checksum_bytes, compute_statistics, encode_durable_text, estimated_properties_bytes,
        property_projection_artifact_generation_file, property_spill_artifact_generation_file,
        read_durable_text, restore_storage_backup, retain_supported_property_statistics,
        set_checkpoint_failpoint, set_generation_reclamation_remove_failpoint,
        set_wal_apply_failpoint, source_scan, AdjacencyConsolidationPlan, AdjacencyDirection,
        AdjacencyGroupStats, AdjacencyLayout, CheckpointPublishStage, ConnectedNodesCreate,
        CowSegmentedMap, DatabaseDoctor, DegreeStatisticsEntry, DegreeStatisticsKey,
        DurableCompression, DurableManifest, GraphScanControl, GraphStore, NodeId, NodeRecord,
        NodeSetAssignment, NodeSetValue, OrderedAdjacencyEntry, PersistentGraphIndexClass,
        ProjectedGraphDefinition, PropertyFilter, RelId, RelRecord, RelTypeId,
        RelationshipDeleteRequest, ScanPruningStrategy, ScanPruningTargetKind,
        SearchProjectionGraphChange, SkeinError, SourceScanCandidateLimits,
        SourceScanCandidateRead, SourceScanCandidateVisit, SourceScanRow, WalDoctorOptions,
        COW_MAP_TARGET_SEGMENT_BYTES, DENSE_ADJACENCY_DEGREE_THRESHOLD, DURABLE_COMPRESSION_HEADER,
        MANIFEST_FILE,
    };
    use crate::schema::{Catalog, GraphStatistics, LabelId, PropertyType, TableKind};
    use crate::value::Value;
    use skein_integrity::integrity_digest;
    use skein_storage::{
        DurabilityPolicy, GraphMutation, MutationLimits, RelationalColumnSchema,
        RelationalHydrationBudget, RelationalInsertMode, RelationalKey, RelationalRow,
        RelationalScalarType, RelationalTableSchema, RelationalTransaction, RelationalValue,
        RelationalWrite, RelationshipPropertyUpdate, ScanPredicate, ScanSegmentAccessPlan,
        ScanSegmentFallback, ScanSegmentManifest, StorageResidencyMode, WalReplayConfig,
    };
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::num::{NonZeroU64, NonZeroUsize};

    fn property_projection_block_corrupt_offset(path: &std::path::Path, kind_tag: u8) -> u64 {
        let encoded = fs::read(path).unwrap();
        encoded
            .windows(8)
            .enumerate()
            .find_map(|(offset, header)| {
                (header == b"SKNIDX01" && encoded.get(offset.saturating_add(24)) == Some(&kind_tag))
                    .then_some(offset.saturating_add(25) as u64)
            })
            .expect("selected property projection block exists")
    }

    #[test]
    fn snapshot_shares_segments_until_the_live_store_mutates_them() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("id", Value::String("memory-a".to_string()))]),
            )
            .unwrap();

        let snapshot = store.snapshot();
        assert!(store.nodes.shares_storage_with(&snapshot.nodes));
        assert!(store
            .relationships
            .shares_storage_with(&snapshot.relationships));

        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("id", Value::String("memory-b".to_string()))]),
            )
            .unwrap();

        assert_eq!(snapshot.nodes.len(), 1);
        assert_eq!(store.nodes.len(), 2);
        assert!(!store.nodes.shares_storage_with(&snapshot.nodes));
        assert!(store
            .relationships
            .shares_storage_with(&snapshot.relationships));
    }

    #[test]
    fn wal_append_handle_is_reused_and_rotated_with_its_generation() {
        let path = unique_test_dir("wal_append_handle_cache");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let durable = store.durable.as_ref().unwrap();
        assert!(durable.wal_append_file.is_none());
        assert_eq!(durable.wal_append_open_count, 0);

        for id in 1..=2 {
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
                .unwrap();
        }
        let durable = store.durable.as_ref().unwrap();
        assert!(durable.wal_append_file.is_some());
        assert_eq!(durable.wal_append_open_count, 1);
        let checkpoint_source = store.checkpoint_source();
        assert!(checkpoint_source
            .durable
            .as_ref()
            .unwrap()
            .wal_append_file
            .is_none());
        drop(checkpoint_source);

        store.checkpoint(&catalog).unwrap();
        assert!(store.durable.as_ref().unwrap().wal_append_file.is_none());
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
            .unwrap();
        let durable = store.durable.as_ref().unwrap();
        assert!(durable.wal_append_file.is_some());
        assert_eq!(durable.wal_append_open_count, 2);
        drop(store);

        let mut reopened = GraphStore::open(&path, &mut catalog).unwrap();
        assert_eq!(reopened.durable.as_ref().unwrap().wal_append_open_count, 0);
        reopened
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(4))]))
            .unwrap();
        assert_eq!(reopened.durable.as_ref().unwrap().wal_append_open_count, 1);
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn graph_and_relational_state_share_wal_epoch_and_checkpoint_publication() {
        let path = unique_test_dir("unified_relational_commit");
        let backup = unique_test_dir("unified_relational_backup");
        let restored = unique_test_dir("unified_relational_restored");
        let table = RelationalTableSchema {
            name: "messages".to_string(),
            columns: vec![
                RelationalColumnSchema {
                    name: "id".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: None,
                },
                RelationalColumnSchema {
                    name: "body".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
        };

        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).expect("open durable store");
            store
                .commit_mutations_and_relational(
                    &mut catalog,
                    vec![GraphMutation::CreateNode {
                        label: "Marker".to_string(),
                        properties: properties([("id", Value::String("graph-1".to_string()))]),
                    }],
                    RelationalTransaction {
                        writes: vec![RelationalWrite::CreateTable(table)],
                    },
                    MutationLimits::default(),
                )
                .expect("commit graph and relational schema");
            assert_eq!(store.commit_epoch(), 1);
            assert_eq!(store.nodes.len(), 1);
            assert!(store.relational_state().table_schema("messages").is_some());
        }

        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).expect("replay unified WAL");
            assert_eq!(store.commit_epoch(), 1);
            assert_eq!(store.nodes.len(), 1);
            assert!(store.relational_state().table_schema("messages").is_some());

            store
                .commit_mutations_and_relational(
                    &mut catalog,
                    vec![GraphMutation::CreateNode {
                        label: "Marker".to_string(),
                        properties: properties([("id", Value::String("graph-2".to_string()))]),
                    }],
                    RelationalTransaction {
                        writes: vec![RelationalWrite::Insert {
                            table: "messages".to_string(),
                            rows: vec![RelationalRow::new(vec![
                                RelationalValue::Text("message-1".to_string()),
                                RelationalValue::Text("payload".repeat(1_024)),
                            ])],
                            mode: RelationalInsertMode::Error,
                        }],
                    },
                    MutationLimits::default(),
                )
                .expect("commit graph and relational row");
            assert_eq!(store.commit_epoch(), 2);
            store.checkpoint(&catalog).expect("publish checkpoint");
            assert!(path.join("relational.1.skein").exists());
            assert_eq!(
                store
                    .relational_state()
                    .file_backed_overflow_segment_count(),
                1
            );
            store
                .backup_to(&catalog, &backup)
                .expect("back up relational checkpoint");
            assert!(backup.join("relational.2.skein").exists());
        }

        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).expect("load unified checkpoint");
            assert_eq!(store.commit_epoch(), 2);
            assert_eq!(store.nodes.len(), 2);
            assert_eq!(store.relational_state().row_count("messages"), 1);
            assert_eq!(
                store
                    .relational_state()
                    .file_backed_overflow_segment_count(),
                1
            );
            let hydrated = store
                .relational_state()
                .hydrate_row(
                    "messages",
                    &RelationalKey(vec![RelationalValue::Text("message-1".to_string())]),
                    &mut RelationalHydrationBudget::default(),
                )
                .expect("hydrate file-backed row")
                .expect("message row");
            assert_eq!(
                hydrated.values()[1],
                RelationalValue::Text("payload".repeat(1_024))
            );
        }

        restore_storage_backup(&backup, &restored).expect("restore unified backup");
        {
            let mut catalog = Catalog::default();
            let store =
                GraphStore::open(&restored, &mut catalog).expect("open restored checkpoint");
            assert_eq!(store.commit_epoch(), 2);
            assert_eq!(store.nodes.len(), 2);
            assert_eq!(store.relational_state().row_count("messages"), 1);
            assert_eq!(
                store
                    .relational_state()
                    .file_backed_overflow_segment_count(),
                1
            );
        }

        fs::remove_dir_all(path).expect("remove test store");
        fs::remove_dir_all(backup).expect("remove backup");
        fs::remove_dir_all(restored).expect("remove restored store");
    }

    #[test]
    fn mixed_commit_recovers_both_states_after_post_wal_apply_failure() {
        let path = unique_test_dir("unified_relational_apply_failure");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).expect("open durable store");
            set_wal_apply_failpoint(Some(0));
            let error = store
                .commit_mutations_and_relational(
                    &mut catalog,
                    vec![GraphMutation::CreateNode {
                        label: "Marker".to_string(),
                        properties: properties([("id", Value::String("graph-1".to_string()))]),
                    }],
                    RelationalTransaction {
                        writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                            name: "messages".to_string(),
                            columns: vec![RelationalColumnSchema {
                                name: "id".to_string(),
                                scalar_type: RelationalScalarType::Text,
                                nullable: false,
                                default: None,
                            }],
                            primary_key: vec!["id".to_string()],
                            unique_constraints: Vec::new(),
                            foreign_keys: Vec::new(),
                            indexes: Vec::new(),
                        })],
                    },
                    MutationLimits::default(),
                )
                .expect_err("injected apply failure must poison the handle");
            set_wal_apply_failpoint(None);
            assert!(error.to_string().contains("injected failure"));
            assert!(store.post_wal_apply_poisoned());
            assert_eq!(store.nodes.len(), 0);
            assert!(store.relational_state().is_empty());
        }

        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).expect("recover canonical WAL batch");
            assert_eq!(store.commit_epoch(), 1);
            assert_eq!(store.nodes.len(), 1);
            assert!(store.relational_state().table_schema("messages").is_some());
        }

        fs::remove_dir_all(path).expect("remove test store");
    }

    #[test]
    fn relational_checkpoint_corruption_fails_scrub_and_reopen() {
        let path = unique_test_dir("relational_checkpoint_corruption");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).expect("open durable store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                            name: "messages".to_string(),
                            columns: vec![RelationalColumnSchema {
                                name: "id".to_string(),
                                scalar_type: RelationalScalarType::Text,
                                nullable: false,
                                default: None,
                            }],
                            primary_key: vec!["id".to_string()],
                            unique_constraints: Vec::new(),
                            foreign_keys: Vec::new(),
                            indexes: Vec::new(),
                        })],
                    },
                )
                .expect("create relational table");
            store.checkpoint(&catalog).expect("publish checkpoint");

            let relational_path = path.join("relational.1.skein");
            let mut bytes = fs::read(&relational_path).expect("read relational checkpoint");
            *bytes.last_mut().expect("relational checkpoint payload") ^= 0xff;
            fs::write(&relational_path, bytes).expect("corrupt relational checkpoint");

            let error = store
                .scrub_storage()
                .expect_err("scrub must reject relational corruption");
            assert!(error.to_string().contains("relational checkpoint"));
        }

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog)
            .expect_err("reopen must reject relational corruption");
        assert!(error.to_string().contains("relational checkpoint"));
        fs::remove_dir_all(path).expect("remove test store");
    }

    #[test]
    fn active_snapshot_detaches_only_the_mutated_adjacency_posting_list() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source_a = store
            .create_node(&mut catalog, "Source", BTreeMap::new())
            .unwrap();
        let source_b = store
            .create_node(&mut catalog, "Source", BTreeMap::new())
            .unwrap();
        let target_a = store
            .create_node(&mut catalog, "Target", BTreeMap::new())
            .unwrap();
        let target_b = store
            .create_node(&mut catalog, "Target", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source_a,
                target_a,
                "LINKS_TO",
                BTreeMap::new(),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source_b,
                target_a,
                "LINKS_TO",
                BTreeMap::new(),
            )
            .unwrap();
        let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
        let snapshot = store.snapshot();

        assert!(store
            .outgoing
            .get(&(source_a, rel_type))
            .unwrap()
            .shares_pivot_with(snapshot.outgoing.get(&(source_a, rel_type)).unwrap()));
        assert!(store
            .outgoing
            .get(&(source_b, rel_type))
            .unwrap()
            .shares_pivot_with(snapshot.outgoing.get(&(source_b, rel_type)).unwrap()));

        store
            .create_relationship(
                &mut catalog,
                source_a,
                target_b,
                "LINKS_TO",
                BTreeMap::new(),
            )
            .unwrap();

        assert!(!store
            .outgoing
            .get(&(source_a, rel_type))
            .unwrap()
            .shares_pivot_with(snapshot.outgoing.get(&(source_a, rel_type)).unwrap()));
        assert!(store
            .outgoing
            .get(&(source_b, rel_type))
            .unwrap()
            .shares_pivot_with(snapshot.outgoing.get(&(source_b, rel_type)).unwrap()));
        assert_eq!(store.outgoing_relationships(source_a, rel_type).count(), 2);
        assert_eq!(
            snapshot.outgoing_relationships(source_a, rel_type).count(),
            1
        );
    }

    #[test]
    fn active_snapshot_buffers_dense_adjacency_mutation_in_a_mini_delta() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Source", BTreeMap::new())
            .unwrap();
        let mut targets = Vec::new();
        for _ in 0..=DENSE_ADJACENCY_DEGREE_THRESHOLD {
            targets.push(
                store
                    .create_node(&mut catalog, "Target", BTreeMap::new())
                    .unwrap(),
            );
        }
        for target in targets.iter().take(DENSE_ADJACENCY_DEGREE_THRESHOLD) {
            store
                .create_relationship(&mut catalog, source, *target, "LINKS_TO", BTreeMap::new())
                .unwrap();
        }
        let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
        let snapshot = store.snapshot();

        store
            .create_relationship(
                &mut catalog,
                source,
                targets[DENSE_ADJACENCY_DEGREE_THRESHOLD],
                "LINKS_TO",
                BTreeMap::new(),
            )
            .unwrap();

        let live_posting = store.outgoing.get(&(source, rel_type)).unwrap();
        let snapshot_posting = snapshot.outgoing.get(&(source, rel_type)).unwrap();
        assert!(live_posting.shares_pivot_with(snapshot_posting));
        assert_eq!(live_posting.mini_delta_len(), 1);
        assert_eq!(snapshot_posting.mini_delta_len(), 0);
        assert_eq!(
            store.outgoing_relationships(source, rel_type).count(),
            DENSE_ADJACENCY_DEGREE_THRESHOLD + 1
        );
        assert_eq!(
            snapshot.outgoing_relationships(source, rel_type).count(),
            DENSE_ADJACENCY_DEGREE_THRESHOLD
        );
    }

    #[test]
    fn bounded_adjacency_consolidation_preserves_snapshot_and_graph_epoch() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Source", BTreeMap::new())
            .unwrap();
        let target_count =
            DENSE_ADJACENCY_DEGREE_THRESHOLD + skein_storage::ADJACENCY_DELTA_CONSOLIDATION_ENTRIES;
        let targets = (0..target_count)
            .map(|_| {
                store
                    .create_node(&mut catalog, "Target", BTreeMap::new())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        for target in targets.iter().take(DENSE_ADJACENCY_DEGREE_THRESHOLD) {
            store
                .create_relationship(&mut catalog, source, *target, "LINKS_TO", BTreeMap::new())
                .unwrap();
        }
        let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
        let snapshot = store.snapshot();
        for target in targets.iter().skip(DENSE_ADJACENCY_DEGREE_THRESHOLD) {
            store
                .create_relationship(&mut catalog, source, *target, "LINKS_TO", BTreeMap::new())
                .unwrap();
        }
        let commit_epoch = store.commit_epoch();
        let plan = store.adjacency_consolidation_plan();
        assert_eq!(plan.group_count, 1);
        assert_eq!(
            plan.delta_entry_count,
            skein_storage::ADJACENCY_DELTA_CONSOLIDATION_ENTRIES
        );

        let deferred =
            store.consolidate_bounded_adjacency_deltas(plan.estimated_entries.saturating_sub(1));
        assert_eq!(deferred.consolidated_group_count, 0);
        assert_eq!(deferred.remaining, plan);

        let report = store.consolidate_bounded_adjacency_deltas(plan.estimated_entries);
        assert_eq!(report.planned, plan);
        assert_eq!(report.consolidated_group_count, 1);
        assert_eq!(
            report.consolidated_delta_entry_count,
            plan.delta_entry_count
        );
        assert_eq!(
            report.consolidated_estimated_entries,
            plan.estimated_entries
        );
        assert_eq!(report.remaining, AdjacencyConsolidationPlan::default());
        assert_eq!(store.commit_epoch(), commit_epoch);
        assert_eq!(
            store.outgoing_relationships(source, rel_type).count(),
            target_count
        );
        assert_eq!(
            snapshot.outgoing_relationships(source, rel_type).count(),
            DENSE_ADJACENCY_DEGREE_THRESHOLD
        );
    }

    #[test]
    fn active_snapshot_detaches_only_the_mutated_property_posting_list() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_property_index(&mut catalog, "Memory", "topic")
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("topic", Value::String("storage".to_string()))]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("topic", Value::String("retrieval".to_string()))]),
            )
            .unwrap();
        let label_id = catalog.label_id("Memory").unwrap();
        let storage_key = (
            label_id,
            "topic".to_string(),
            Value::String("storage".to_string()),
        );
        let retrieval_key = (
            label_id,
            "topic".to_string(),
            Value::String("retrieval".to_string()),
        );
        let snapshot = store.snapshot();

        assert!(store
            .property_index
            .get(&storage_key)
            .unwrap()
            .shares_storage_with(snapshot.property_index.get(&storage_key).unwrap()));
        assert!(store
            .property_index
            .get(&retrieval_key)
            .unwrap()
            .shares_storage_with(snapshot.property_index.get(&retrieval_key).unwrap()));

        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("topic", Value::String("storage".to_string()))]),
            )
            .unwrap();

        assert!(!store
            .property_index
            .get(&storage_key)
            .unwrap()
            .shares_storage_with(snapshot.property_index.get(&storage_key).unwrap()));
        assert!(store
            .property_index
            .get(&retrieval_key)
            .unwrap()
            .shares_storage_with(snapshot.property_index.get(&retrieval_key).unwrap()));
        assert_eq!(
            store
                .seek_nodes_by_property(label_id, "topic", &Value::String("storage".to_string()),)
                .count(),
            2
        );
        assert_eq!(
            snapshot
                .seek_nodes_by_property(label_id, "topic", &Value::String("storage".to_string()),)
                .count(),
            1
        );
    }

    #[test]
    fn active_snapshot_detaches_only_the_mutated_map_page() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for id in 0..1_100 {
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
                .unwrap();
        }
        let snapshot = store.snapshot();
        let segment_count = store.nodes.segment_count();
        assert!(segment_count >= 3);
        assert_eq!(
            store.nodes.shared_segment_count_with(&snapshot.nodes),
            segment_count
        );

        store.apply_set_node_property(
            &catalog,
            NodeId(0),
            "title".to_string(),
            Value::String("updated".to_string()),
        );

        assert_eq!(store.nodes.segment_count(), segment_count);
        assert_eq!(
            store.nodes.shared_segment_count_with(&snapshot.nodes),
            segment_count - 1
        );
        assert_eq!(
            snapshot
                .nodes
                .get(&NodeId(0))
                .unwrap()
                .properties
                .get("title"),
            None
        );
        assert_eq!(
            store.nodes.get(&NodeId(0)).unwrap().properties.get("title"),
            Some(&Value::String("updated".to_string()))
        );
    }

    #[test]
    fn large_records_split_cow_pages_by_estimated_bytes() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for id in 0..8 {
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
                .unwrap();
        }
        let snapshot = store.snapshot();
        assert_eq!(store.nodes.segment_count(), 1);

        store.apply_set_node_property(
            &catalog,
            NodeId(0),
            "body".to_string(),
            Value::String("x".repeat(COW_MAP_TARGET_SEGMENT_BYTES * 2)),
        );

        assert_eq!(snapshot.nodes.segment_count(), 1);
        assert!(store.nodes.segment_count() >= 2);
        assert_eq!(store.nodes.shared_segment_count_with(&snapshot.nodes), 0);
        assert!(!snapshot
            .nodes
            .get(&NodeId(0))
            .unwrap()
            .properties
            .contains_key("body"));
    }

    #[test]
    fn replays_relationships_from_wal_and_rebuilds_adjacency() {
        let path = unique_test_dir("rel_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(
                    &mut catalog,
                    source,
                    target,
                    "RELATES_TO",
                    properties([("weight", Value::Int(7))]),
                )
                .unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let rel_type = catalog.rel_type_id("RELATES_TO").unwrap();
            let rels = store
                .outgoing_relationships(NodeId(0), rel_type)
                .collect::<Vec<_>>();
            assert_eq!(rels.len(), 1);
            assert_eq!(rels[0].target, NodeId(1));
            assert_eq!(rels[0].properties.get("weight"), Some(&Value::Int(7)));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn backup_restore_publishes_one_verified_generation() {
        let path = unique_test_dir("backup_source");
        let backup = unique_test_dir("backup_image");
        let restored = unique_test_dir("backup_restored");
        let content = "backup-property-spill-".repeat(8_192);
        let report = {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("id", Value::String("memory-1".to_string())),
                        ("content", Value::String(content.clone())),
                    ]),
                )
                .unwrap();
            let target = store
                .create_node(
                    &mut catalog,
                    "Entity",
                    properties([("id", Value::String("entity-1".to_string()))]),
                )
                .unwrap();
            store
                .create_relationship(
                    &mut catalog,
                    source,
                    target,
                    "MENTIONS",
                    properties([("weight", Value::Int(7))]),
                )
                .unwrap();
            store.backup_to(&catalog, &backup).unwrap()
        };
        assert!(report.generation > 0);
        assert_eq!(report.file_count, 26);
        assert!(backup
            .join(skein_storage::append_generation_manifest_file(
                report.generation
            ))
            .exists());
        assert!(backup
            .join(skein_storage::canonical_segment_descriptor_page_file(
                report.generation
            ))
            .exists());
        assert!(backup
            .join(skein_storage::canonical_segment_descriptor_root_file(
                report.generation
            ))
            .exists());
        assert!(backup
            .join(skein_storage::canonical_adjacency_descriptor_page_file(
                report.generation
            ))
            .exists());
        assert!(backup
            .join(skein_storage::canonical_adjacency_descriptor_root_file(
                report.generation
            ))
            .exists());
        assert!(backup
            .join(skein_storage::property_projection_descriptor_page_file(
                report.generation
            ))
            .exists());
        assert!(backup
            .join(skein_storage::property_projection_descriptor_root_file(
                report.generation
            ))
            .exists());
        assert!(backup
            .join(skein_storage::property_spill_descriptor_page_file(
                report.generation
            ))
            .exists());
        assert!(backup
            .join(skein_storage::property_spill_descriptor_root_file(
                report.generation
            ))
            .exists());

        let restore = restore_storage_backup(&backup, &restored).unwrap();
        assert_eq!(restore.generation, report.generation);
        assert_eq!(restore.manifest_checksum, report.manifest_checksum);
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&restored, &mut catalog).unwrap();
        assert_eq!(store.scan_nodes(None).count(), 2);
        assert_eq!(store.scan_relationships(None).count(), 1);
        assert_eq!(
            store.node(NodeId(0)).unwrap().properties["content"],
            Value::String(content)
        );
        assert_eq!(
            store.relationship(RelId(0)).unwrap().properties["weight"],
            Value::Int(7)
        );

        std::fs::remove_dir_all(path).unwrap();
        std::fs::remove_dir_all(backup).unwrap();
        drop(store);
        std::fs::remove_dir_all(restored).unwrap();
    }

    #[test]
    fn restore_rejects_corrupt_backup_before_destination_publication() {
        let path = unique_test_dir("backup_corrupt_source");
        let backup = unique_test_dir("backup_corrupt_image");
        let restored = unique_test_dir("backup_corrupt_restored");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("id", Value::String("memory-1".to_string()))]),
                )
                .unwrap();
            store.backup_to(&catalog, &backup).unwrap();
        }
        let checkpoint = active_checkpoint_path(&backup);
        let mut bytes = std::fs::read(&checkpoint).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(checkpoint, bytes).unwrap();

        let error = restore_storage_backup(&backup, &restored).unwrap_err();
        assert!(error.to_string().contains("verification failed"));
        assert!(!restored.exists());

        std::fs::remove_dir_all(path).unwrap();
        std::fs::remove_dir_all(backup).unwrap();
    }

    #[test]
    fn backup_and_restore_never_replace_existing_destinations() {
        let path = unique_test_dir("backup_existing_source");
        let backup = unique_test_dir("backup_existing_image");
        let restored = unique_test_dir("backup_existing_restored");
        std::fs::create_dir_all(&backup).unwrap();
        std::fs::create_dir_all(&restored).unwrap();
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let backup_error = store.backup_to(&catalog, &backup).unwrap_err();
        assert!(
            backup_error.to_string().contains("already exists"),
            "unexpected backup error: {backup_error}"
        );

        std::fs::remove_dir_all(&backup).unwrap();
        store.backup_to(&catalog, &backup).unwrap();
        let restore_error = restore_storage_backup(&backup, &restored).unwrap_err();
        assert!(restore_error.to_string().contains("already exists"));

        drop(store);
        std::fs::remove_dir_all(path).unwrap();
        std::fs::remove_dir_all(backup).unwrap();
        std::fs::remove_dir_all(restored).unwrap();
    }

    #[test]
    fn checkpoint_segment_scan_rejects_a_manifest_behind_the_graph_snapshot() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let manifest = ScanSegmentManifest::new(0, Vec::new()).unwrap();

        assert!(matches!(
            store.plan_checkpoint_segment_scan(Some(&manifest), &ScanPredicate::True),
            ScanSegmentAccessPlan::Read(_)
        ));
        store
            .create_node(
                &mut catalog,
                "Source",
                properties([("id", Value::String("source-1".to_string()))]),
            )
            .unwrap();

        assert!(matches!(
            store.plan_checkpoint_segment_scan(Some(&manifest), &ScanPredicate::True),
            ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::SnapshotEpochMismatch {
                reader_epoch: 1,
                manifest_epoch: 0,
            })
        ));
        assert!(matches!(
            store.plan_checkpoint_segment_scan(None, &ScanPredicate::True),
            ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::NoManifest)
        ));
    }

    #[test]
    fn checkpoint_publishes_source_scan_and_wal_mutation_invalidates_it() {
        let path = unique_test_dir("source_scan_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Source",
                    properties([
                        ("id", Value::String("source-a".to_string())),
                        ("space_id", Value::String("alpha".to_string())),
                    ]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
            assert!(matches!(
                store.plan_published_source_scan(&ScanPredicate::Eq {
                    property: "space_id".to_string(),
                    value: Value::String("alpha".to_string()),
                }),
                ScanSegmentAccessPlan::Read(_)
            ));
            let SourceScanCandidateRead::Rows { rows, report, .. } = store
                .read_published_source_scan_candidates(
                    &ScanPredicate::Eq {
                        property: "space_id".to_string(),
                        value: Value::String("alpha".to_string()),
                    },
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroU64::new(1024).unwrap(),
                    NonZeroU64::new(1024).unwrap(),
                )
                .unwrap()
            else {
                panic!("expected source scan payload read");
            };
            assert_eq!(report.range_count, 1);
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].properties["id"],
                Value::String("source-a".to_string())
            );
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("id", Value::String("memory-a".to_string()))]),
                )
                .unwrap();
            assert!(matches!(
                store.plan_published_source_scan(&ScanPredicate::True),
                ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::SnapshotEpochMismatch { .. })
            ));
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            assert!(matches!(
                store.plan_published_source_scan(&ScanPredicate::True),
                ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::SnapshotEpochMismatch { .. })
            ));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoint_publishes_digest_bound_canonical_segments() {
        let path = unique_test_dir("canonical_segment_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("id", Value::String("memory-1".to_string()))]),
                )
                .unwrap();
            let target = store
                .create_node(
                    &mut catalog,
                    "Entity",
                    properties([("id", Value::String("entity-1".to_string()))]),
                )
                .unwrap();
            store
                .create_relationship(
                    &mut catalog,
                    source,
                    target,
                    "MENTIONS",
                    properties([("weight", Value::Int(5))]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
            let manifest = store.canonical_segment_manifest().unwrap();
            assert_eq!(manifest.node_count, 2);
            assert_eq!(manifest.relationship_count, 1);
            assert_eq!(
                store.canonical_node_from_segments(source).unwrap(),
                store.node(source).cloned()
            );
            assert_eq!(
                store
                    .canonical_relationship_from_segments(RelId(0))
                    .unwrap(),
                store.relationship(RelId(0)).cloned()
            );
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            assert_eq!(store.canonical_segment_manifest().unwrap().node_count, 2);
            assert_eq!(
                store.canonical_node_from_segments(NodeId(1)).unwrap(),
                store.node(NodeId(1)).cloned()
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn graph_manifest_open_budget_is_aggregate_across_published_roots() {
        let path = unique_test_dir("graph_manifest_open_budget");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Entity", BTreeMap::new())
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }

        let canonical_manifest_bytes =
            fs::metadata(path.join(canonical_manifest_generation_file(1)))
                .unwrap()
                .len();
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let residency = store.storage_residency_report();
            assert_eq!(
                residency.graph_manifest_open_budget_bytes,
                skein_storage::DEFAULT_MAX_GRAPH_MANIFEST_OPEN_BYTES
            );
            assert!(residency.graph_manifest_encoded_bytes > canonical_manifest_bytes);
        }
        let replay_config = WalReplayConfig {
            max_graph_manifest_open_bytes: canonical_manifest_bytes,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let error = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("aggregate encoded graph manifest bytes during open"),
            "unexpected graph manifest admission error: {error}"
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn zero_graph_manifest_open_budget_is_rejected_before_initialization() {
        let path = unique_test_dir("zero_graph_manifest_open_budget");
        let replay_config = WalReplayConfig {
            max_graph_manifest_open_bytes: 0,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let error = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("max_graph_manifest_open_bytes must be non-zero"));
        assert!(!path.exists());
    }

    #[test]
    fn checkpoint_uses_compact_adjacency_root_and_enforces_manifest_budget_before_selection() {
        let path = unique_test_dir("checkpoint_graph_manifest_budget");
        let replay_config = WalReplayConfig {
            max_graph_manifest_open_bytes: 64 * 1024,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        store
            .commit_mutations(
                &mut catalog,
                vec![GraphMutation::MergeConnectedNodes(ConnectedNodesCreate {
                    source_label: "Memory".to_string(),
                    source_properties: properties([("id", Value::String("source-0".to_string()))]),
                    rel_type: "MENTIONS".to_string(),
                    rel_properties: BTreeMap::new(),
                    target_label: "Entity".to_string(),
                    target_properties: properties([("id", Value::String("target-0".to_string()))]),
                })],
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 1);

        let mutations = (1..=512)
            .map(|ordinal| {
                GraphMutation::MergeConnectedNodes(ConnectedNodesCreate {
                    source_label: "Memory".to_string(),
                    source_properties: properties([(
                        "id",
                        Value::String(format!("source-{ordinal}")),
                    )]),
                    rel_type: "MENTIONS".to_string(),
                    rel_properties: BTreeMap::new(),
                    target_label: "Entity".to_string(),
                    target_properties: properties([(
                        "id",
                        Value::String(format!("target-{ordinal}")),
                    )]),
                })
            })
            .collect();
        store.commit_mutations(&mut catalog, mutations).unwrap();
        store.checkpoint(&catalog).unwrap();
        assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 2);
        assert!(
            store
                .durable
                .as_ref()
                .unwrap()
                .graph_manifest_encoded_bytes()
                <= replay_config.max_graph_manifest_open_bytes
        );

        store
            .commit_mutations(
                &mut catalog,
                vec![GraphMutation::MergeConnectedNodes(ConnectedNodesCreate {
                    source_label: "Memory".to_string(),
                    source_properties: properties([(
                        "id",
                        Value::String("source-513".to_string()),
                    )]),
                    rel_type: "MENTIONS".to_string(),
                    rel_properties: BTreeMap::new(),
                    target_label: "Entity".to_string(),
                    target_properties: properties([(
                        "id",
                        Value::String("target-513".to_string()),
                    )]),
                })],
            )
            .unwrap();
        store
            .durable
            .as_mut()
            .unwrap()
            .set_graph_manifest_open_budget_bytes(1);
        let error = store.checkpoint(&catalog).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("aggregate encoded graph manifest bytes during open"),
            "unexpected graph manifest checkpoint admission error: {error}"
        );
        assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 2);
        let selected_manifest = DurableManifest::load(&path.join(MANIFEST_FILE)).unwrap();
        assert_eq!(selected_manifest.checkpoint_epoch, 2);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn graph_manifest_growth_is_rejected_before_unbounded_read() {
        let path = unique_test_dir("graph_manifest_growth");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }

        let manifest_path = path.join(canonical_manifest_generation_file(1));
        let original_len = fs::metadata(&manifest_path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&manifest_path)
            .unwrap()
            .set_len(original_len.saturating_add(1024 * 1024))
            .unwrap();

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(
            error.to_string().contains("exceeding its admitted bound"),
            "unexpected graph manifest growth error: {error}"
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_checkpoint_uses_generation_bound_dense_adjacency_and_fails_closed() {
        use std::io::{Seek, SeekFrom};

        let path = unique_test_dir("canonical_adjacency_checkpoint");
        let replay_config = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..WalReplayConfig::default()
        };
        let source;
        let first_relationship;
        let corrupt_offset;
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            source = store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap();
            first_relationship = RelId(0);
            for index in 0..70 {
                let target = store
                    .create_node(
                        &mut catalog,
                        "Entity",
                        properties([("ordinal", Value::Int(index))]),
                    )
                    .unwrap();
                store
                    .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
                    .unwrap();
            }
            store.checkpoint(&catalog).unwrap();
            let adjacency = store
                .durable
                .as_ref()
                .and_then(|durable| durable.canonical_adjacency.as_ref())
                .unwrap();
            let cache_before_scrub = store.segment_cache_snapshot().unwrap();
            let scrub = adjacency.deep_scrub().unwrap();
            assert_eq!(store.segment_cache_snapshot().unwrap(), cache_before_scrub);
            assert!(!path.join("adjacency.1.manifest.skein").exists());
            let descriptor_reader = skein_storage::GraphDescriptorTreeRootReader::open(
                skein_storage::GraphDescriptorTreePaths::new(
                    path.join(skein_storage::canonical_adjacency_descriptor_page_file(1)),
                    path.join(skein_storage::canonical_adjacency_descriptor_root_file(1)),
                ),
                skein_storage::GraphDescriptorTreeBuildConfig::default(),
            )
            .unwrap();
            assert_eq!(
                descriptor_reader.root().descriptor_count,
                scrub.descriptors_checked
            );
            assert_eq!(
                descriptor_reader.root().source_commit_epoch,
                store.commit_epoch
            );
            assert_eq!(descriptor_reader.report().page_payload_bytes_read, 0);
            let mention_type = catalog.rel_type_id("MENTIONS").unwrap();
            corrupt_offset = 24 + 48;

            let mut relationship_ids = Vec::new();
            store
                .visit_adjacent_relationships_owned(
                    source,
                    Some(mention_type),
                    AdjacencyDirection::Outgoing,
                    |relationship| {
                        relationship_ids.push(relationship.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(relationship_ids.len(), 70);
            let mut ordered_prefix = Vec::new();
            let control = store
                .try_visit_ordered_adjacent_relationships_owned(
                    source,
                    Some(mention_type),
                    AdjacencyDirection::Outgoing,
                    1024 * 1024,
                    |relationship| {
                        ordered_prefix.push(relationship.id);
                        Ok(if ordered_prefix.len() == 5 {
                            GraphScanControl::Stop
                        } else {
                            GraphScanControl::Continue
                        })
                    },
                )
                .unwrap();
            assert_eq!(control, GraphScanControl::Stop);
            assert_eq!(ordered_prefix, (0..5).map(RelId).collect::<Vec<_>>());
            let cold = store.storage_residency_report().graph_index_reads;
            assert!(cold.adjacency_dense_blocks_read > 0);
            assert!(cold.adjacency_descriptor_pages_visited > 0);
            assert!(cold.adjacency_descriptor_page_bytes_decoded > 0);
            assert!(cold.adjacency_descriptor_storage_bytes_read > 0);
            assert!(cold.adjacency_descriptor_cache_misses > 0);

            let target = store
                .create_node(&mut catalog, "Entity", BTreeMap::new())
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
                .unwrap();
            relationship_ids.clear();
            store
                .visit_adjacent_relationships_owned(
                    source,
                    Some(mention_type),
                    AdjacencyDirection::Outgoing,
                    |relationship| {
                        relationship_ids.push(relationship.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(relationship_ids.len(), 71);
            assert!(relationship_ids.contains(&first_relationship));
            let warm = store
                .storage_residency_report()
                .graph_index_reads
                .delta_since(cold);
            assert!(warm.adjacency_descriptor_cache_hits > 0);
            assert!(warm.adjacency_descriptor_page_bytes_decoded > 0);
            assert_eq!(warm.adjacency_descriptor_storage_bytes_read, 0);
        }
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path.join(canonical_adjacency_artifact_generation_file(1)))
                .unwrap();
            file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
            file.write_all(&[0xff]).unwrap();
            file.sync_all().unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            let mention_type = catalog.rel_type_id("MENTIONS").unwrap();
            let error = store
                .visit_adjacent_relationships_owned(
                    source,
                    Some(mention_type),
                    AdjacencyDirection::Outgoing,
                    |_| GraphScanControl::Continue,
                )
                .unwrap_err();
            assert!(
                error.to_string().contains("content digest verification"),
                "unexpected adjacency corruption error: {error}"
            );
            let poisoned = store
                .visit_adjacent_relationships_owned(
                    source,
                    Some(mention_type),
                    AdjacencyDirection::Outgoing,
                    |_| GraphScanControl::Continue,
                )
                .unwrap_err();
            assert!(poisoned.to_string().contains("poisoned"));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_checkpoint_requires_adjacency_generation_binding() {
        let path = unique_test_dir("canonical_adjacency_required");
        let replay_config = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..WalReplayConfig::default()
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Entity", BTreeMap::new())
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }

        let manifest_path = path.join(MANIFEST_FILE);
        let manifest = fs::read_to_string(&manifest_path).unwrap();
        let mut body = manifest
            .lines()
            .filter(|line| !line.starts_with("checksum\t"))
            .map(|line| {
                if line.starts_with("canonical_adjacency_") {
                    let (field, _) = line.split_once('\t').unwrap();
                    format!("{field}\tnone")
                } else {
                    line.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        body.push('\n');
        fs::write(
            &manifest_path,
            format!("{body}checksum\t{}\n", checksum_bytes(body.as_bytes())),
        )
        .unwrap();
        fs::remove_file(path.join(canonical_adjacency_artifact_generation_file(1))).unwrap();
        fs::remove_file(path.join(skein_storage::canonical_adjacency_descriptor_page_file(1)))
            .unwrap();
        fs::remove_file(path.join(skein_storage::canonical_adjacency_descriptor_root_file(1)))
            .unwrap();

        let mut catalog = Catalog::default();
        let error = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("canonical segments require canonical adjacency"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_property_spills_round_trip_and_fail_closed() {
        use std::io::{Seek, SeekFrom};

        let path = unique_test_dir("property_spill_checkpoint");
        let replay_config = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..WalReplayConfig::default()
        };
        let content = "property-spill-value-".repeat(8_192);
        let node_id;
        let corrupt_offset;
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            node_id = store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("content", Value::String(content.clone()))]),
                )
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Entity", BTreeMap::new())
                .unwrap();
            store
                .create_relationship(
                    &mut catalog,
                    node_id,
                    target,
                    "MENTIONS",
                    properties([("context", Value::String(content.clone()))]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
            store.scrub_storage().unwrap();
            let spill_manifest = store.property_spill_manifest().unwrap();
            assert_eq!(spill_manifest.value_count, 2);
            assert!(spill_manifest.value_bytes > 64 * 1024);
            assert_eq!(spill_manifest.source_commit_epoch, store.commit_epoch());
            let descriptor_root = skein_storage::GraphDescriptorTreeRootReader::open_bound(
                skein_storage::GraphDescriptorTreePaths::new(
                    path.join(skein_storage::property_spill_descriptor_page_file(1)),
                    path.join(skein_storage::property_spill_descriptor_root_file(1)),
                ),
                spill_manifest.descriptor_generation_artifacts(),
                skein_storage::GraphDescriptorTreeBuildConfig::default(),
            )
            .unwrap();
            assert_eq!(
                descriptor_root.root().descriptor_count,
                spill_manifest.block_count
            );
            corrupt_offset = 40;
            let canonical_manifest = store.canonical_segment_manifest().unwrap();
            assert!(canonical_manifest.artifact_len < 16 * 1024);
            assert_eq!(canonical_manifest.node_segment_count, 1);
            assert_eq!(
                store.node_owned(node_id).unwrap().unwrap().properties["content"],
                Value::String(content.clone())
            );
            let mention_type = catalog.rel_type_id("MENTIONS").unwrap();
            let mut contexts = Vec::new();
            store
                .visit_adjacent_relationships_owned(
                    node_id,
                    Some(mention_type),
                    AdjacencyDirection::Outgoing,
                    |relationship| {
                        contexts.push(relationship.properties["context"].clone());
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(contexts, vec![Value::String(content.clone())]);
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            assert_eq!(
                store.node_owned(node_id).unwrap().unwrap().properties["content"],
                Value::String(content)
            );
        }
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path.join(property_spill_artifact_generation_file(1)))
                .unwrap();
            file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
            file.write_all(&[0xff]).unwrap();
            file.sync_all().unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let error = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("content digest verification"),
                "unexpected property spill corruption error: {error}"
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_property_spill_requires_the_selected_descriptor_root() {
        let path = unique_test_dir("property_spill_descriptor_root");
        let replay_config = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..WalReplayConfig::default()
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            store
                .create_node(&mut catalog, "Memory", BTreeMap::new())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        fs::remove_file(path.join(skein_storage::property_spill_descriptor_root_file(1))).unwrap();
        let mut catalog = Catalog::default();
        let error = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("No such file")
                || error.to_string().contains("property spill descriptor"),
            "unexpected missing property spill descriptor error: {error}"
        );
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_property_projections_merge_wal_delta_and_fail_closed() {
        use skein_storage::PersistentPropertyProjectionKind;
        use std::io::{Seek, SeekFrom};

        let path = unique_test_dir("property_projection_checkpoint");
        let replay_config = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..WalReplayConfig::default()
        };
        let first_id;
        let second_id;
        let delta_id;
        let corrupt_offset;
        let composite_properties = vec!["key".to_string(), "rank".to_string()];
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            store
                .create_range_property_index(&mut catalog, "Memory", "rank")
                .unwrap();
            store
                .create_property_index(&mut catalog, "Memory", "key")
                .unwrap();
            store
                .create_full_text_property_index(&mut catalog, "Memory", "content")
                .unwrap();
            store
                .create_composite_property_index(&mut catalog, "Memory", &composite_properties)
                .unwrap();
            first_id = store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("key", Value::String("first".to_string())),
                        ("rank", Value::Int(10)),
                        ("content", Value::String("alpha beta".to_string())),
                    ]),
                )
                .unwrap();
            second_id = store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("key", Value::String("second".to_string())),
                        ("rank", Value::Int(20)),
                        ("content", Value::String("beta gamma".to_string())),
                    ]),
                )
                .unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("key", Value::String("third".to_string())),
                        ("rank", Value::Int(30)),
                        ("content", Value::String("delta".to_string())),
                    ]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();

            let label_id = catalog.label_id("Memory").unwrap();
            let manifest = store.persistent_property_projection_manifest().unwrap();
            assert!(manifest.supports(label_id, "key", PersistentPropertyProjectionKind::Equality));
            assert!(manifest.supports(label_id, "rank", PersistentPropertyProjectionKind::Range));
            assert!(manifest.supports(
                label_id,
                "content",
                PersistentPropertyProjectionKind::FullText
            ));
            assert!(manifest.supports_composite_equality(label_id, &composite_properties));
            let graph_index_reads_before = store.storage_residency_report().graph_index_reads;
            let read_snapshot = store.snapshot();
            corrupt_offset = property_projection_block_corrupt_offset(
                &path.join(property_projection_artifact_generation_file(1)),
                4,
            );

            let lower = (Value::Int(15), true);
            let upper = (Value::Int(25), true);
            let mut range_ids = Vec::new();
            store
                .visit_nodes_by_property_range_owned(
                    label_id,
                    "rank",
                    Some(&lower),
                    Some(&upper),
                    |node| {
                        range_ids.push(node.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(range_ids, vec![second_id]);
            let mut equality_ids = Vec::new();
            read_snapshot
                .visit_nodes_by_property_owned(
                    label_id,
                    "key",
                    &[Value::String("second".to_string())],
                    |node| {
                        equality_ids.push(node.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(equality_ids, vec![second_id]);
            let mut full_text_ids = Vec::new();
            store
                .visit_nodes_by_full_text_property_owned(label_id, "content", "beta", |node| {
                    full_text_ids.push(node.id);
                    GraphScanControl::Continue
                })
                .unwrap();
            assert_eq!(full_text_ids, vec![first_id, second_id]);
            let mut composite_ids = Vec::new();
            store
                .visit_nodes_by_composite_property_owned(
                    label_id,
                    &[
                        ("key".to_string(), Value::String("second".to_string())),
                        ("rank".to_string(), Value::Int(20)),
                    ],
                    |node| {
                        composite_ids.push(node.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(composite_ids, vec![second_id]);
            let graph_index_reads = store
                .storage_residency_report()
                .graph_index_reads
                .delta_since(graph_index_reads_before);
            assert_eq!(
                graph_index_reads.operation_count(PersistentGraphIndexClass::NodeEquality),
                1
            );
            assert_eq!(
                graph_index_reads.operation_count(PersistentGraphIndexClass::NodeRange),
                1
            );
            assert_eq!(
                graph_index_reads.operation_count(PersistentGraphIndexClass::NodeFullText),
                1
            );
            assert_eq!(
                graph_index_reads.operation_count(PersistentGraphIndexClass::NodeCompositeEquality),
                1
            );
            assert!(graph_index_reads.property_blocks_read >= 4);
            assert!(graph_index_reads.property_bytes_read > 0);

            let second_filter = PropertyFilter::IdEq {
                value: Value::Int(second_id.0 as i64),
            };
            store
                .set_node_property(
                    &mut catalog,
                    "Memory",
                    Some(&second_filter),
                    "key",
                    Value::String("updated".to_string()),
                )
                .unwrap();
            store
                .set_node_property(
                    &mut catalog,
                    "Memory",
                    Some(&second_filter),
                    "rank",
                    Value::Int(40),
                )
                .unwrap();
            store
                .set_node_property(
                    &mut catalog,
                    "Memory",
                    Some(&second_filter),
                    "content",
                    Value::String("omega".to_string()),
                )
                .unwrap();
            let first_filter = PropertyFilter::IdEq {
                value: Value::Int(first_id.0 as i64),
            };
            store
                .delete_nodes(&mut catalog, "Memory", Some(&first_filter), false)
                .unwrap();
            delta_id = store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("key", Value::String("second".to_string())),
                        ("rank", Value::Int(18)),
                        ("content", Value::String("beta epsilon".to_string())),
                    ]),
                )
                .unwrap();

            range_ids.clear();
            store
                .visit_nodes_by_property_range_owned(
                    label_id,
                    "rank",
                    Some(&lower),
                    Some(&upper),
                    |node| {
                        range_ids.push(node.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(range_ids, vec![delta_id]);
            equality_ids.clear();
            store
                .visit_nodes_by_property_owned(
                    label_id,
                    "key",
                    &[Value::String("second".to_string())],
                    |node| {
                        equality_ids.push(node.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(equality_ids, vec![delta_id]);
            full_text_ids.clear();
            store
                .visit_nodes_by_full_text_property_owned(label_id, "content", "beta", |node| {
                    full_text_ids.push(node.id);
                    GraphScanControl::Continue
                })
                .unwrap();
            assert_eq!(full_text_ids, vec![delta_id]);
            composite_ids.clear();
            store
                .visit_nodes_by_composite_property_owned(
                    label_id,
                    &[
                        ("key".to_string(), Value::String("second".to_string())),
                        ("rank".to_string(), Value::Int(18)),
                    ],
                    |node| {
                        composite_ids.push(node.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(composite_ids, vec![delta_id]);
            composite_ids.clear();
            store
                .visit_nodes_by_composite_property_owned(
                    label_id,
                    &[
                        ("key".to_string(), Value::String("updated".to_string())),
                        ("rank".to_string(), Value::Int(40)),
                    ],
                    |node| {
                        composite_ids.push(node.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(composite_ids, vec![second_id]);
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            let label_id = catalog.label_id("Memory").unwrap();
            let lower = (Value::Int(15), true);
            let upper = (Value::Int(25), true);
            let mut range_ids = Vec::new();
            store
                .visit_nodes_by_property_range_owned(
                    label_id,
                    "rank",
                    Some(&lower),
                    Some(&upper),
                    |node| {
                        range_ids.push(node.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(range_ids, vec![delta_id]);
            let mut equality_ids = Vec::new();
            store
                .visit_nodes_by_property_owned(
                    label_id,
                    "key",
                    &[Value::String("second".to_string())],
                    |node| {
                        equality_ids.push(node.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(equality_ids, vec![delta_id]);
            let mut composite_ids = Vec::new();
            store
                .visit_nodes_by_composite_property_owned(
                    label_id,
                    &[
                        ("key".to_string(), Value::String("second".to_string())),
                        ("rank".to_string(), Value::Int(18)),
                    ],
                    |node| {
                        composite_ids.push(node.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(composite_ids, vec![delta_id]);
        }
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path.join(property_projection_artifact_generation_file(1)))
                .unwrap();
            file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
            file.write_all(&[0xff]).unwrap();
            file.sync_all().unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            let label_id = catalog.label_id("Memory").unwrap();
            let error = store
                .visit_nodes_by_composite_property_owned(
                    label_id,
                    &[
                        ("key".to_string(), Value::String("second".to_string())),
                        ("rank".to_string(), Value::Int(18)),
                    ],
                    |_| GraphScanControl::Continue,
                )
                .unwrap_err();
            assert!(
                error.to_string().contains("content digest verification"),
                "unexpected property projection corruption error: {error}"
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_relationship_property_projection_prunes_and_merges_wal_delta() {
        use skein_storage::PersistentPropertyProjectionKind;
        use std::io::{Seek, SeekFrom};

        let path = unique_test_dir("relationship_property_projection_checkpoint");
        let replay_config = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..WalReplayConfig::default()
        };
        let source;
        let second_relationship;
        let delta_relationship;
        let corrupt_offset;
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            source = store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("id", Value::String("source".to_string()))]),
                )
                .unwrap();
            let targets = (0..3)
                .map(|id| {
                    store
                        .create_node(&mut catalog, "Entity", properties([("id", Value::Int(id))]))
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let relationship_ids = [10_i64, 20, 30]
                .into_iter()
                .zip(&targets)
                .map(|(rank, target)| {
                    store
                        .create_relationship(
                            &mut catalog,
                            source,
                            *target,
                            "LINKS_TO",
                            properties([("rank", Value::Int(rank))]),
                        )
                        .unwrap()
                })
                .collect::<Vec<_>>();
            second_relationship = relationship_ids[1];
            store.checkpoint(&catalog).unwrap();

            let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
            let manifest = store.persistent_property_projection_manifest().unwrap();
            assert!(manifest.supports_relationship(
                rel_type,
                "rank",
                PersistentPropertyProjectionKind::RelationshipEquality,
            ));
            assert!(manifest.supports_relationship(
                rel_type,
                "rank",
                PersistentPropertyProjectionKind::RelationshipRange,
            ));
            let graph_index_reads_before = store.storage_residency_report().graph_index_reads;
            corrupt_offset = property_projection_block_corrupt_offset(
                &path.join(property_projection_artifact_generation_file(1)),
                5,
            );

            let mut ids = Vec::new();
            let (_, report) = store
                .visit_adjacent_relationships_with_filter_owned(
                    source,
                    Some(rel_type),
                    AdjacencyDirection::Outgoing,
                    &PropertyFilter::Eq {
                        property: "rank".to_string(),
                        value: Value::Int(20),
                    },
                    |relationship| {
                        ids.push(relationship.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(ids, vec![second_relationship]);
            assert!(report.is_some_and(|report| {
                report.pruned
                    && matches!(
                        report.strategy,
                        ScanPruningStrategy::PropertyEq { ref property }
                            if property == "rank"
                    )
            }));
            let mut outgoing_ids = Vec::new();
            store
                .visit_adjacent_relationships_owned(
                    source,
                    Some(rel_type),
                    AdjacencyDirection::Outgoing,
                    |relationship| {
                        outgoing_ids.push(relationship.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(outgoing_ids.len(), 3);
            let mut incoming_ids = Vec::new();
            store
                .visit_adjacent_relationships_owned(
                    targets[1],
                    Some(rel_type),
                    AdjacencyDirection::Incoming,
                    |relationship| {
                        incoming_ids.push(relationship.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(incoming_ids, vec![second_relationship]);

            store
                .set_relationship_property(
                    &mut catalog,
                    RelationshipPropertyUpdate {
                        source_label: "Memory".to_string(),
                        filter: Some(PropertyFilter::IdEq {
                            value: Value::Int(source.0 as i64),
                        }),
                        rel_type: "LINKS_TO".to_string(),
                        target_label: "Entity".to_string(),
                        target_filter: None,
                        rel_filter: Some(PropertyFilter::IdEq {
                            value: Value::Int(second_relationship.0 as i64),
                        }),
                        property: "rank".to_string(),
                        value: Value::Int(40),
                    },
                )
                .unwrap();
            delta_relationship = store
                .create_relationship(
                    &mut catalog,
                    source,
                    targets[1],
                    "LINKS_TO",
                    properties([("rank", Value::Int(20))]),
                )
                .unwrap();

            ids.clear();
            store
                .visit_adjacent_relationships_with_filter_owned(
                    source,
                    Some(rel_type),
                    AdjacencyDirection::Outgoing,
                    &PropertyFilter::Eq {
                        property: "rank".to_string(),
                        value: Value::Int(20),
                    },
                    |relationship| {
                        ids.push(relationship.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(ids, vec![delta_relationship]);
            ids.clear();
            store
                .visit_adjacent_relationships_with_filter_owned(
                    source,
                    Some(rel_type),
                    AdjacencyDirection::Outgoing,
                    &PropertyFilter::Range {
                        property: "rank".to_string(),
                        lower: Some((Value::Int(35), true)),
                        upper: Some((Value::Int(45), true)),
                    },
                    |relationship| {
                        ids.push(relationship.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(ids, vec![second_relationship]);
            let graph_index_reads = store
                .storage_residency_report()
                .graph_index_reads
                .delta_since(graph_index_reads_before);
            assert_eq!(
                graph_index_reads.operation_count(PersistentGraphIndexClass::RelationshipEquality),
                2
            );
            assert_eq!(
                graph_index_reads.operation_count(PersistentGraphIndexClass::RelationshipRange),
                1
            );
            assert_eq!(
                graph_index_reads.operation_count(PersistentGraphIndexClass::ForwardAdjacency),
                1
            );
            assert_eq!(
                graph_index_reads.operation_count(PersistentGraphIndexClass::ReverseAdjacency),
                1
            );
            assert!(graph_index_reads.property_blocks_read >= 2);
            assert!(graph_index_reads.property_bytes_read > 0);
            assert!(graph_index_reads.adjacency_blocks_read >= 2);
            assert!(graph_index_reads.adjacency_bytes_read > 0);
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
            let mut ids = Vec::new();
            store
                .visit_adjacent_relationships_with_filter_owned(
                    source,
                    Some(rel_type),
                    AdjacencyDirection::Outgoing,
                    &PropertyFilter::Eq {
                        property: "rank".to_string(),
                        value: Value::Int(20),
                    },
                    |relationship| {
                        ids.push(relationship.id);
                        GraphScanControl::Continue
                    },
                )
                .unwrap();
            assert_eq!(ids, vec![delta_relationship]);
        }
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path.join(property_projection_artifact_generation_file(1)))
                .unwrap();
            file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
            file.write_all(&[0xff]).unwrap();
            file.sync_all().unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay_config,
            )
            .unwrap();
            let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
            let error = store
                .visit_adjacent_relationships_with_filter_owned(
                    source,
                    Some(rel_type),
                    AdjacencyDirection::Outgoing,
                    &PropertyFilter::Eq {
                        property: "rank".to_string(),
                        value: Value::Int(20),
                    },
                    |_| GraphScanControl::Continue,
                )
                .unwrap_err();
            assert!(
                error.to_string().contains("content digest verification"),
                "unexpected relationship property projection corruption error: {error}"
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn persistent_graph_indexes_match_canonical_fallbacks_and_report_each_class() {
        let path = unique_test_dir("persistent_graph_index_differential");
        let replay_config = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        store
            .create_property_index(&mut catalog, "Memory", "key")
            .unwrap();
        store
            .create_range_property_index(&mut catalog, "Memory", "rank")
            .unwrap();
        store
            .create_full_text_property_index(&mut catalog, "Memory", "content")
            .unwrap();
        store
            .create_composite_property_index(
                &mut catalog,
                "Memory",
                &["key".to_string(), "rank".to_string()],
            )
            .unwrap();
        let source = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("key", Value::String("first".to_string())),
                    ("rank", Value::Int(10)),
                    ("content", Value::String("alpha beta".to_string())),
                ]),
            )
            .unwrap();
        let second = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("key", Value::String("second".to_string())),
                    ("rank", Value::Int(20)),
                    ("content", Value::String("beta gamma".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("key", Value::String("third".to_string())),
                    ("rank", Value::Int(30)),
                    ("content", Value::String("delta".to_string())),
                ]),
            )
            .unwrap();
        let targets = (0..3)
            .map(|ordinal| {
                store
                    .create_node(
                        &mut catalog,
                        "Entity",
                        properties([("ordinal", Value::Int(ordinal))]),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let relationships = [10_i64, 20, 30]
            .into_iter()
            .zip(&targets)
            .map(|(weight, target)| {
                store
                    .create_relationship(
                        &mut catalog,
                        source,
                        *target,
                        "LINKS_TO",
                        properties([("weight", Value::Int(weight))]),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        store.checkpoint(&catalog).unwrap();

        let mut oracle = store.snapshot();
        oracle.persistent_property_projection = None;
        oracle.canonical_adjacency = None;
        let label_id = catalog.label_id("Memory").unwrap();
        let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
        let lower = (Value::Int(15), true);
        let upper = (Value::Int(25), true);
        let before = store.storage_residency_report().graph_index_reads;

        let mut indexed_equality = Vec::new();
        store
            .visit_nodes_by_property_owned(
                label_id,
                "key",
                &[Value::String("second".to_string())],
                |node| {
                    indexed_equality.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        let mut oracle_equality = Vec::new();
        oracle
            .visit_nodes_by_property_owned(
                label_id,
                "key",
                &[Value::String("second".to_string())],
                |node| {
                    oracle_equality.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(indexed_equality, oracle_equality);
        assert_eq!(indexed_equality, vec![second]);

        let mut indexed_range = Vec::new();
        store
            .visit_nodes_by_property_range_owned(
                label_id,
                "rank",
                Some(&lower),
                Some(&upper),
                |node| {
                    indexed_range.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        let mut oracle_range = Vec::new();
        oracle
            .visit_nodes_by_property_range_owned(
                label_id,
                "rank",
                Some(&lower),
                Some(&upper),
                |node| {
                    oracle_range.push(node.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(indexed_range, oracle_range);

        let mut indexed_full_text = Vec::new();
        store
            .visit_nodes_by_full_text_property_owned(label_id, "content", "beta", |node| {
                indexed_full_text.push(node.id);
                GraphScanControl::Continue
            })
            .unwrap();
        let mut oracle_full_text = Vec::new();
        oracle
            .visit_nodes_by_full_text_property_owned(label_id, "content", "beta", |node| {
                oracle_full_text.push(node.id);
                GraphScanControl::Continue
            })
            .unwrap();
        assert_eq!(indexed_full_text, oracle_full_text);

        let predicates = [
            ("key".to_string(), Value::String("second".to_string())),
            ("rank".to_string(), Value::Int(20)),
        ];
        let mut indexed_composite = Vec::new();
        store
            .visit_nodes_by_composite_property_owned(label_id, &predicates, |node| {
                indexed_composite.push(node.id);
                GraphScanControl::Continue
            })
            .unwrap();
        let mut oracle_composite = Vec::new();
        oracle
            .visit_nodes_by_composite_property_owned(label_id, &predicates, |node| {
                oracle_composite.push(node.id);
                GraphScanControl::Continue
            })
            .unwrap();
        assert_eq!(indexed_composite, oracle_composite);

        let equality_filter = PropertyFilter::Eq {
            property: "weight".to_string(),
            value: Value::Int(20),
        };
        let range_filter = PropertyFilter::Range {
            property: "weight".to_string(),
            lower: Some(lower.clone()),
            upper: Some(upper.clone()),
        };
        let mut indexed_relationship_equality = Vec::new();
        store
            .visit_adjacent_relationships_with_filter_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                &equality_filter,
                |relationship| {
                    indexed_relationship_equality.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        let mut oracle_relationship_equality = Vec::new();
        oracle
            .visit_adjacent_relationships_with_filter_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                &equality_filter,
                |relationship| {
                    oracle_relationship_equality.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(indexed_relationship_equality, oracle_relationship_equality);
        assert_eq!(indexed_relationship_equality, vec![relationships[1]]);

        let mut indexed_relationship_range = Vec::new();
        store
            .visit_adjacent_relationships_with_filter_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                &range_filter,
                |relationship| {
                    indexed_relationship_range.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        let mut oracle_relationship_range = Vec::new();
        oracle
            .visit_adjacent_relationships_with_filter_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                &range_filter,
                |relationship| {
                    oracle_relationship_range.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(indexed_relationship_range, oracle_relationship_range);

        let mut indexed_forward = Vec::new();
        store
            .visit_adjacent_relationships_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                |relationship| {
                    indexed_forward.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        let mut oracle_forward = Vec::new();
        oracle
            .visit_adjacent_relationships_owned(
                source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                |relationship| {
                    oracle_forward.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(indexed_forward, oracle_forward);

        let mut indexed_reverse = Vec::new();
        store
            .visit_adjacent_relationships_owned(
                targets[1],
                Some(rel_type),
                AdjacencyDirection::Incoming,
                |relationship| {
                    indexed_reverse.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        let mut oracle_reverse = Vec::new();
        oracle
            .visit_adjacent_relationships_owned(
                targets[1],
                Some(rel_type),
                AdjacencyDirection::Incoming,
                |relationship| {
                    oracle_reverse.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(indexed_reverse, oracle_reverse);

        let reads = store
            .storage_residency_report()
            .graph_index_reads
            .delta_since(before);
        for class in PersistentGraphIndexClass::ALL {
            assert_eq!(reads.operation_count(class), 1, "{}", class.as_str());
            assert!(reads.blocks_read(class) > 0, "{}", class.as_str());
            assert!(reads.bytes_read(class) > 0, "{}", class.as_str());
        }
        assert!(reads.property_blocks_read >= 6);
        assert!(reads.property_bytes_read > 0);
        assert!(reads.adjacency_blocks_read >= 2);
        assert!(reads.adjacency_bytes_read > 0);

        drop(oracle);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn relationship_projection_estimate_fallback_reports_descriptor_io() {
        let path = unique_test_dir("relationship_projection_estimate_fallback");
        let replay_config = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay_config,
        )
        .unwrap();
        let narrow_source = store
            .create_node(&mut catalog, "Entity", BTreeMap::new())
            .unwrap();
        let wide_source = store
            .create_node(&mut catalog, "Entity", BTreeMap::new())
            .unwrap();
        let targets = (0..5)
            .map(|_| {
                store
                    .create_node(&mut catalog, "Entity", BTreeMap::new())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let expected = store
            .create_relationship(
                &mut catalog,
                narrow_source,
                targets[0],
                "LINKS_TO",
                properties([("weight", Value::Int(20))]),
            )
            .unwrap();
        for target in &targets[1..] {
            store
                .create_relationship(
                    &mut catalog,
                    wide_source,
                    *target,
                    "LINKS_TO",
                    properties([("weight", Value::Int(20))]),
                )
                .unwrap();
        }
        store.checkpoint(&catalog).unwrap();

        let rel_type = catalog.rel_type_id("LINKS_TO").unwrap();
        let before = store.storage_residency_report().graph_index_reads;
        let mut relationships = Vec::new();
        store
            .visit_adjacent_relationships_with_filter_owned(
                narrow_source,
                Some(rel_type),
                AdjacencyDirection::Outgoing,
                &PropertyFilter::Eq {
                    property: "weight".to_string(),
                    value: Value::Int(20),
                },
                |relationship| {
                    relationships.push(relationship.id);
                    GraphScanControl::Continue
                },
            )
            .unwrap();
        assert_eq!(relationships, vec![expected]);

        let reads = store
            .storage_residency_report()
            .graph_index_reads
            .delta_since(before);
        assert_eq!(
            reads.operation_count(PersistentGraphIndexClass::RelationshipEquality),
            1
        );
        assert_eq!(
            reads.blocks_read(PersistentGraphIndexClass::RelationshipEquality),
            0
        );
        assert!(reads.property_descriptor_pages_visited > 0);
        assert!(reads.property_descriptor_storage_bytes_read > 0);
        assert_eq!(reads.property_blocks_read, 0);
        assert_eq!(
            reads.operation_count(PersistentGraphIndexClass::ForwardAdjacency),
            1
        );
        assert!(reads.blocks_read(PersistentGraphIndexClass::ForwardAdjacency) > 0);

        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn corrupted_source_scan_artifact_never_blocks_canonical_graph_recovery() {
        let path = unique_test_dir("source_scan_corruption");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Source",
                    properties([("id", Value::String("source-a".to_string()))]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        std::fs::write(path.join(source_scan::SOURCE_SCAN_PAYLOAD_FILE), b"corrupt").unwrap();
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        assert!(store.node(NodeId(0)).is_some());
        assert!(matches!(
            store.plan_published_source_scan(&ScanPredicate::True),
            ScanSegmentAccessPlan::Fallback(ScanSegmentFallback::NoManifest)
        ));
        assert!(!path.join(source_scan::SOURCE_SCAN_PAYLOAD_FILE).exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn source_scan_reader_caches_independently_verified_segment_ranges() {
        let path = unique_test_dir("source_scan_coalesced_ranges");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        for id in 0..129 {
            store
                .create_node(
                    &mut catalog,
                    "Source",
                    properties([("id", Value::String(format!("source-{id}")))]),
                )
                .unwrap();
        }
        store.checkpoint(&catalog).unwrap();
        let SourceScanCandidateRead::Rows { rows, report, .. } = store
            .read_published_source_scan_candidates(
                &ScanPredicate::True,
                NonZeroUsize::new(2).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
            )
            .unwrap()
        else {
            panic!("expected source scan payload read");
        };
        assert_eq!(rows.len(), 129);
        assert_eq!(report.range_count, 2);
        assert_eq!(report.wave_count, 1);
        let first_cache = store.segment_cache_snapshot().unwrap();
        assert_eq!(first_cache.entry_count, 2);
        assert_eq!(first_cache.miss_count, 2);

        let SourceScanCandidateRead::Rows { rows, .. } = store
            .read_published_source_scan_candidates(
                &ScanPredicate::True,
                NonZeroUsize::new(2).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
            )
            .unwrap()
        else {
            panic!("expected cached source scan payload read");
        };
        assert_eq!(rows.len(), 129);
        let second_cache = store.segment_cache_snapshot().unwrap();
        assert_eq!(second_cache.hit_count, 2);
        assert_eq!(second_cache.resident_bytes, first_cache.resident_bytes);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn source_scan_candidate_budget_tracks_one_live_segment() {
        let path = unique_test_dir("source_scan_live_segment_budget");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        for id in 0..=source_scan::SOURCE_SCAN_TARGET_ROWS {
            store
                .create_node(
                    &mut catalog,
                    "Source",
                    properties([("id", Value::String(format!("source-{id}")))]),
                )
                .unwrap();
        }
        store.checkpoint(&catalog).unwrap();
        let SourceScanCandidateRead::Rows { rows, .. } = store
            .read_published_source_scan_candidates(
                &ScanPredicate::True,
                NonZeroUsize::new(2).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
            )
            .unwrap()
        else {
            panic!("expected source scan payload read");
        };
        let row_bytes = |row: &SourceScanRow| {
            std::mem::size_of::<SourceScanRow>().saturating_add(
                usize::try_from(estimated_properties_bytes(&row.properties)).unwrap_or(usize::MAX),
            )
        };
        let first_segment_bytes = rows
            .iter()
            .take(source_scan::SOURCE_SCAN_TARGET_ROWS)
            .fold(0usize, |total, row| total.saturating_add(row_bytes(row)));
        let all_candidate_bytes = rows
            .iter()
            .fold(0usize, |total, row| total.saturating_add(row_bytes(row)));
        assert!(all_candidate_bytes > first_segment_bytes);

        let mut visited = 0usize;
        let visit = store
            .visit_published_source_scan_candidates_bounded(
                &ScanPredicate::True,
                SourceScanCandidateLimits::bounded(
                    NonZeroUsize::new(2).unwrap(),
                    NonZeroU64::new(1024 * 1024).unwrap(),
                    NonZeroU64::new(1024 * 1024).unwrap(),
                    NonZeroUsize::new(first_segment_bytes).unwrap(),
                ),
                None,
                &mut |_| {
                    visited = visited.saturating_add(1);
                    Ok(GraphScanControl::Continue)
                },
            )
            .unwrap();
        let SourceScanCandidateVisit::Rows {
            candidate_count, ..
        } = visit
        else {
            panic!("expected bounded source scan visit");
        };
        assert_eq!(visited, source_scan::SOURCE_SCAN_TARGET_ROWS + 1);
        assert_eq!(candidate_count, visited);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn source_scan_corruption_after_stream_admission_fails_closed() {
        let path = unique_test_dir("source_scan_runtime_corruption");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Source",
                properties([("id", Value::String("source-a".to_string()))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();

        let payload_path = path.join(source_scan::SOURCE_SCAN_PAYLOAD_FILE);
        let mut payload = fs::read(&payload_path).unwrap();
        let corrupt_at = payload.len() / 2;
        payload[corrupt_at] ^= 0xff;
        fs::write(&payload_path, payload).unwrap();

        let error = store
            .read_published_source_scan_candidates(
                &ScanPredicate::True,
                NonZeroUsize::new(2).unwrap(),
                NonZeroU64::new(1024).unwrap(),
                NonZeroU64::new(1024).unwrap(),
            )
            .expect_err("an admitted streaming read must fail closed on corruption");
        assert!(matches!(error, SkeinError::StorageIntegrity(_)));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn ordered_adjacency_entries_sort_by_neighbor_then_relationship_id() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();
        let target_one = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
            .unwrap();
        let target_two = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
            .unwrap();
        let target_three = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(3))]))
            .unwrap();

        let rel_three = store
            .create_relationship(
                &mut catalog,
                source,
                target_three,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        let rel_one = store
            .create_relationship(
                &mut catalog,
                source,
                target_one,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        let rel_two = store
            .create_relationship(
                &mut catalog,
                source,
                target_two,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

        let outgoing =
            store.ordered_adjacency_entries(source, rel_type, AdjacencyDirection::Outgoing);
        let incoming =
            store.ordered_adjacency_entries(target_one, rel_type, AdjacencyDirection::Incoming);

        assert_eq!(
            outgoing,
            vec![
                OrderedAdjacencyEntry {
                    relationship_id: rel_one,
                    neighbor_id: target_one,
                },
                OrderedAdjacencyEntry {
                    relationship_id: rel_two,
                    neighbor_id: target_two,
                },
                OrderedAdjacencyEntry {
                    relationship_id: rel_three,
                    neighbor_id: target_three,
                },
            ]
        );
        assert_eq!(
            incoming,
            vec![OrderedAdjacencyEntry {
                relationship_id: rel_one,
                neighbor_id: source,
            }]
        );
    }

    #[test]
    fn ordered_adjacency_visitor_streams_without_degree_sized_sort_keys() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        for _ in 0..3 {
            let target = store
                .create_node(&mut catalog, "Entity", BTreeMap::new())
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
                .unwrap();
        }
        let mention_type = catalog.rel_type_id("MENTIONS").unwrap();
        let mut visited = Vec::new();
        let control = store
            .try_visit_ordered_adjacent_relationships_owned(
                source,
                Some(mention_type),
                AdjacencyDirection::Outgoing,
                1,
                |relationship| {
                    visited.push(relationship.id);
                    Ok(if visited.len() == 2 {
                        GraphScanControl::Stop
                    } else {
                        GraphScanControl::Continue
                    })
                },
            )
            .unwrap();
        assert_eq!(control, GraphScanControl::Stop);
        assert_eq!(visited, vec![RelId(0), RelId(1)]);
    }

    #[test]
    fn ordered_adjacency_entries_for_node_sort_across_relationship_types() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();
        let target_one = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
            .unwrap();
        let target_two = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
            .unwrap();

        let rel_late_neighbor = store
            .create_relationship(
                &mut catalog,
                source,
                target_two,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        let rel_early_neighbor = store
            .create_relationship(
                &mut catalog,
                source,
                target_one,
                "RELATES_TO",
                BTreeMap::new(),
            )
            .unwrap();

        let outgoing =
            store.ordered_adjacency_entries_for_node(source, AdjacencyDirection::Outgoing);
        let incoming =
            store.ordered_adjacency_entries_for_node(target_one, AdjacencyDirection::Incoming);

        assert_eq!(
            outgoing,
            vec![
                OrderedAdjacencyEntry {
                    relationship_id: rel_early_neighbor,
                    neighbor_id: target_one,
                },
                OrderedAdjacencyEntry {
                    relationship_id: rel_late_neighbor,
                    neighbor_id: target_two,
                },
            ]
        );
        assert_eq!(
            incoming,
            vec![OrderedAdjacencyEntry {
                relationship_id: rel_early_neighbor,
                neighbor_id: source,
            }]
        );
    }

    #[test]
    fn adjacency_group_stats_classify_sparse_and_dense_groups() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();

        for target_index in 0..DENSE_ADJACENCY_DEGREE_THRESHOLD {
            let target = store
                .create_node(
                    &mut catalog,
                    "Entity",
                    properties([("id", Value::Int(target_index as i64))]),
                )
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
                .unwrap();
        }
        let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

        let dense_stats =
            store.adjacency_group_stats(source, rel_type, AdjacencyDirection::Outgoing);
        let sparse_stats =
            store.adjacency_group_stats(NodeId(1), rel_type, AdjacencyDirection::Incoming);

        assert_eq!(dense_stats.degree, DENSE_ADJACENCY_DEGREE_THRESHOLD);
        assert_eq!(dense_stats.layout, AdjacencyLayout::Dense);
        assert_eq!(sparse_stats.degree, 1);
        assert_eq!(sparse_stats.layout, AdjacencyLayout::Sparse);
    }

    #[test]
    fn adjacency_group_stats_for_node_reports_each_relationship_type() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();
        let mentions_target = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
            .unwrap();
        let relates_target = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                mentions_target,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                relates_target,
                "RELATES_TO",
                BTreeMap::new(),
            )
            .unwrap();
        let mentions = catalog.rel_type_id("MENTIONS").unwrap();
        let relates_to = catalog.rel_type_id("RELATES_TO").unwrap();

        let stats = store.adjacency_group_stats_for_node(source, AdjacencyDirection::Outgoing);

        assert_eq!(
            stats,
            vec![
                AdjacencyGroupStats {
                    node_id: source,
                    rel_type: mentions,
                    direction: AdjacencyDirection::Outgoing,
                    degree: 1,
                    layout: AdjacencyLayout::Sparse,
                },
                AdjacencyGroupStats {
                    node_id: source,
                    rel_type: relates_to,
                    direction: AdjacencyDirection::Outgoing,
                    degree: 1,
                    layout: AdjacencyLayout::Sparse,
                },
            ]
        );
    }

    #[test]
    fn adjacency_consistency_report_matches_full_recompute_after_relationship_mutations() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();
        let target_one = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
            .unwrap();
        let target_two = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target_one,
                "MENTIONS",
                properties([("confidence", Value::Float(0.8))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target_two,
                "MENTIONS",
                properties([("confidence", Value::Float(0.9))]),
            )
            .unwrap();

        let report = store.adjacency_consistency_report();
        assert!(report.ready);
        assert_eq!(report.relationship_count, 2);
        assert_eq!(report.maintained_group_count, 3);
        assert_eq!(report.recomputed_group_count, 3);
        assert_eq!(report.missing_group_count, 0);
        assert_eq!(report.extra_group_count, 0);
        assert_eq!(report.mismatched_group_count, 0);
        assert_eq!(report.dangling_relationship_count, 0);
        assert!(report.mismatches.is_empty());

        store
            .delete_relationships(
                &mut catalog,
                RelationshipDeleteRequest {
                    source_label: "Memory".to_string(),
                    filter: Some(PropertyFilter::Eq {
                        property: "id".to_string(),
                        value: Value::Int(0),
                    }),
                    rel_type: "MENTIONS".to_string(),
                    target_label: "Entity".to_string(),
                    target_filter: Some(PropertyFilter::Eq {
                        property: "id".to_string(),
                        value: Value::Int(1),
                    }),
                    rel_filter: None,
                },
            )
            .unwrap();
        let report = store.adjacency_consistency_report();
        assert!(report.ready);
        assert_eq!(report.relationship_count, 1);
        assert_eq!(report.maintained_group_count, 2);
        assert_eq!(report.recomputed_group_count, 2);
        assert!(report.mismatches.is_empty());
    }

    #[test]
    fn adjacency_consistency_report_matches_full_recompute_after_detach_delete() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
            .unwrap();
        store
            .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
            .unwrap();

        store
            .delete_nodes(
                &mut catalog,
                "Memory",
                Some(&PropertyFilter::Eq {
                    property: "id".to_string(),
                    value: Value::Int(0),
                }),
                true,
            )
            .unwrap();
        let report = store.adjacency_consistency_report();
        assert!(report.ready);
        assert_eq!(report.relationship_count, 0);
        assert_eq!(report.maintained_group_count, 0);
        assert_eq!(report.recomputed_group_count, 0);
        assert_eq!(report.dangling_relationship_count, 0);
        assert!(report.mismatches.is_empty());
    }

    #[test]
    fn checkpoints_relationships_and_truncates_wal() {
        let path = unique_test_dir("rel_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "RELATES_TO", BTreeMap::new())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        assert_eq!(read_test_wal(&path).unwrap(), "");
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let rel_type = catalog.rel_type_id("RELATES_TO").unwrap();
            let rels = store
                .outgoing_relationships(NodeId(0), rel_type)
                .collect::<Vec<_>>();
            assert_eq!(rels.len(), 1);
            assert_eq!(rels[0].source, NodeId(0));
            assert_eq!(rels[0].target, NodeId(1));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoints_search_projection_changes_for_restart_safe_catch_up() {
        let path = unique_test_dir("search_projection_change_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("id", Value::String("deleted-memory".to_string()))]),
                )
                .unwrap();
            store
                .delete_nodes(
                    &mut catalog,
                    "Memory",
                    Some(&PropertyFilter::Eq {
                        property: "id".to_string(),
                        value: Value::String("deleted-memory".to_string()),
                    }),
                    true,
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }

        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        assert_eq!(store.commit_epoch(), 2);
        assert_eq!(store.search_projection_change_log_start_epoch(), 0);
        assert_eq!(
            store.search_projection_graph_changes_after(0),
            vec![
                SearchProjectionGraphChange {
                    commit_epoch: 1,
                    upsert_node_ids: vec![0],
                    delete_document_ids: Vec::new(),
                    relational_primary_key_changes:
                        skein_storage::RelationalPrimaryKeyChangeCapture::Captured {
                            tables: Vec::new(),
                            encoded_bytes: 0,
                        },
                },
                SearchProjectionGraphChange {
                    commit_epoch: 2,
                    upsert_node_ids: Vec::new(),
                    delete_document_ids: vec!["memory:deleted-memory".to_string()],
                    relational_primary_key_changes:
                        skein_storage::RelationalPrimaryKeyChangeCapture::Captured {
                            tables: Vec::new(),
                            encoded_bytes: 0,
                        },
                },
            ]
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoint_requires_projection_change_boundary() {
        let path = unique_test_dir("search_projection_checkpoint_boundary");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("id", Value::String("memory-1".to_string()))]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        let checkpoint_path = active_checkpoint_path(&path);
        rewrite_checksummed_file(
            &checkpoint_path,
            "search_projection_change_log_start_epoch\t0\n",
            "",
            "checkpoint",
        );
        rewrite_checksummed_file(
            &checkpoint_path,
            "search_projection_change\t1\t0\t\n",
            "",
            "checkpoint",
        );

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error
            .to_string()
            .contains("checkpoint search projection changes are missing their start epoch"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_checkpoint_relationship_with_missing_endpoint() {
        let path = unique_test_dir("rel_checkpoint_missing_endpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "RELATES_TO", BTreeMap::new())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }

        let properties_one = crate::store::encode_properties(&properties([("id", Value::Int(1))]));
        let properties_two = crate::store::encode_properties(&properties([("id", Value::Int(2))]));
        let corrupt_records = format!(
            "node\t0\t0\t{properties_one}\nnode\t1\t0\t{properties_two}\nrel\t0\t0\t99\t0\t"
        );
        rewrite_checksummed_file(
            &active_checkpoint_path(&path),
            "canonical_records\ttrue",
            &corrupt_records,
            "checkpoint",
        );

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error
            .to_string()
            .contains("relationship 0 references missing target node 99"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoint_publishes_manifest_with_epoch_boundary() {
        let path = unique_test_dir("manifest_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }

        let checkpoint_bytes = std::fs::read(active_checkpoint_path(&path)).unwrap();
        assert!(checkpoint_bytes.starts_with(DURABLE_COMPRESSION_HEADER.as_bytes()));
        let header_end = checkpoint_bytes
            .windows(2)
            .position(|window| window == b"\n\n")
            .unwrap();
        let header = std::str::from_utf8(&checkpoint_bytes[..header_end]).unwrap();
        assert!(header.contains("codec\tzstd\n"));
        let checkpoint = read_durable_text(&active_checkpoint_path(&path), "checkpoint").unwrap();
        assert!(checkpoint.contains("commit_epoch\t2\n"));
        let manifest = std::fs::read_to_string(path.join("manifest.skein")).unwrap();
        assert!(manifest.contains("SKEIN_MANIFEST_V1\n"));
        assert!(manifest.contains("checkpoint_generation\t1\n"));
        assert!(manifest.contains("wal_generation\t1\n"));
        assert!(manifest.contains("checkpoint_epoch\t1\n"));
        assert!(manifest.contains("checkpoint_commit_epoch\t2\n"));
        assert!(manifest.contains("oldest_reader_commit_epoch\tnone\n"));
        assert!(manifest.contains("safe_reclaim_commit_epoch\t2\n"));
        assert!(manifest.contains("wal_replay_start_lsn\t3\n"));
        assert!(manifest.contains("next_lsn\t3\n"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn prepared_checkpoint_rejects_stale_source_without_rotating_wal() {
        let path = unique_test_dir("prepared_checkpoint_stale_source");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();

        let source = store.checkpoint_source();
        let prepared = source.prepare_checkpoint(&catalog).unwrap().unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
        let error = store
            .publish_prepared_checkpoint(prepared, None)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("checkpoint source changed before publication"));
        assert!(!path.join("checkpoint.1.skein").exists());
        assert!(!path.join("wal.1.skein").exists());
        assert!(!path
            .join(skein_storage::canonical_adjacency_descriptor_page_file(1))
            .exists());
        assert!(!path
            .join(skein_storage::canonical_adjacency_descriptor_root_file(1))
            .exists());
        assert!(!path.join(".checkpoint.1.prepare").exists());
        drop(source);
        drop(store);

        let mut recovered_catalog = Catalog::default();
        let recovered = GraphStore::open(&path, &mut recovered_catalog).unwrap();
        assert_eq!(recovered.commit_epoch(), 2);
        assert_eq!(
            recovered.storage_recovery_report().checkpoint_commit_epoch,
            None
        );
        let memory = recovered_catalog.label_id("Memory").unwrap();
        assert_eq!(recovered.scan_nodes(Some(memory)).count(), 2);
        drop(recovered);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn writable_open_reclaims_abandoned_future_checkpoint_generation() {
        let path = unique_test_dir("abandoned_prepared_checkpoint");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        let source = store.checkpoint_source();
        let prepared = source.prepare_checkpoint(&catalog).unwrap().unwrap();
        assert!(path.join("checkpoint.1.skein").exists());
        assert!(path.join("wal.1.skein").exists());
        assert!(path.join(".checkpoint.1.prepare").exists());
        drop(prepared);
        drop(source);
        drop(store);

        let mut recovered_catalog = Catalog::default();
        let recovered = GraphStore::open(&path, &mut recovered_catalog).unwrap();
        assert_eq!(recovered.commit_epoch(), 1);
        assert!(!path.join("checkpoint.1.skein").exists());
        assert!(!path.join("wal.1.skein").exists());
        assert!(!path.join(".checkpoint.1.prepare").exists());
        drop(recovered);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoint_publish_failpoints_recover_one_complete_generation() {
        for (stage, expected_checkpoint_epoch) in [
            (CheckpointPublishStage::CheckpointPersisted, None),
            (CheckpointPublishStage::WalPrepared, None),
            (CheckpointPublishStage::ManifestPublished, Some(1)),
        ] {
            let path = unique_test_dir(&format!("checkpoint_failpoint_{stage:?}"));
            {
                let mut catalog = Catalog::default();
                let mut store = GraphStore::open(&path, &mut catalog).unwrap();
                store
                    .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                    .unwrap();
                set_checkpoint_failpoint(Some(stage));
                let error = store.checkpoint(&catalog).unwrap_err();
                set_checkpoint_failpoint(None);
                assert!(error.to_string().contains("injected checkpoint failure"));
            }

            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            assert_eq!(store.commit_epoch(), 1);
            assert_eq!(
                store.storage_recovery_report().checkpoint_epoch,
                expected_checkpoint_epoch
            );
            let memory = catalog.label_id("Memory").unwrap();
            assert_eq!(store.scan_nodes(Some(memory)).count(), 1);
            match expected_checkpoint_epoch {
                Some(generation) => {
                    let durable = store.durable.as_ref().unwrap();
                    assert_eq!(
                        durable
                            .relational_row_generation_artifacts
                            .unwrap()
                            .generation,
                        generation
                    );
                    assert_eq!(
                        durable
                            .relational_overflow_generation_artifacts
                            .unwrap()
                            .generation,
                        generation
                    );
                    assert!(path
                        .join(skein_storage::canonical_segment_descriptor_page_file(
                            generation
                        ))
                        .exists());
                    assert!(path
                        .join(skein_storage::canonical_segment_descriptor_root_file(
                            generation
                        ))
                        .exists());
                    assert!(path
                        .join(skein_storage::canonical_adjacency_descriptor_page_file(
                            generation
                        ))
                        .exists());
                    assert!(path
                        .join(skein_storage::canonical_adjacency_descriptor_root_file(
                            generation
                        ))
                        .exists());
                }
                None => {
                    assert!(!path
                        .join(skein_storage::relational_row_page_manifest_generation_file(
                            1
                        ))
                        .exists());
                    assert!(!path
                        .join(skein_storage::relational_overflow_manifest_generation_file(
                            1
                        ))
                        .exists());
                    assert!(!path
                        .join(skein_storage::canonical_segment_descriptor_page_file(1))
                        .exists());
                    assert!(!path
                        .join(skein_storage::canonical_segment_descriptor_root_file(1))
                        .exists());
                    assert!(!path
                        .join(skein_storage::canonical_adjacency_descriptor_page_file(1))
                        .exists());
                    assert!(!path
                        .join(skein_storage::canonical_adjacency_descriptor_root_file(1))
                        .exists());
                }
            }
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn checkpoint_reclamation_failure_is_reported_and_retried_after_publication() {
        let path = unique_test_dir("checkpoint_reclamation_retry");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        for id in 1..=2 {
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }

        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
            .unwrap();
        set_generation_reclamation_remove_failpoint(Some("checkpoint.1.skein".to_string()));
        let checkpoint_result = store.checkpoint(&catalog);
        set_generation_reclamation_remove_failpoint(None);
        checkpoint_result.unwrap();

        assert_eq!(store.commit_epoch(), 3);
        assert_eq!(store.durable.as_ref().unwrap().checkpoint_epoch, 3);
        assert_eq!(store.scan_nodes(None).count(), 3);
        assert!(path.join("checkpoint.1.skein").exists());
        let pressure = store.storage_pressure_snapshot(None);
        assert!(pressure.generation_reclamation_retry_required);
        assert_eq!(pressure.generation_reclamation_pending_files, 1);
        assert!(pressure.generation_reclamation_pending_bytes > 0);
        assert!(pressure
            .reason_codes
            .contains(&skein_storage::StoragePressureReasonCode::GenerationReclamationDebt));

        drop(store);
        let mut recovered_catalog = Catalog::default();
        let mut recovered = GraphStore::open(&path, &mut recovered_catalog).unwrap();
        assert_eq!(recovered.commit_epoch(), 3);
        assert_eq!(
            recovered.storage_recovery_report().checkpoint_epoch,
            Some(3)
        );
        assert_eq!(recovered.scan_nodes(None).count(), 3);

        recovered.checkpoint(&recovered_catalog).unwrap();
        assert!(!path.join("checkpoint.1.skein").exists());
        let pressure = recovered.storage_pressure_snapshot(None);
        assert!(!pressure.generation_reclamation_retry_required);
        assert_eq!(pressure.generation_reclamation_pending_files, 0);
        assert_eq!(pressure.generation_reclamation_pending_bytes, 0);
        assert!(!pressure
            .reason_codes
            .contains(&skein_storage::StoragePressureReasonCode::GenerationReclamationDebt));

        drop(recovered);
        let mut reopened_catalog = Catalog::default();
        let reopened = GraphStore::open(&path, &mut reopened_catalog).unwrap();
        assert_eq!(reopened.commit_epoch(), 3);
        assert_eq!(reopened.storage_recovery_report().checkpoint_epoch, Some(4));
        assert_eq!(reopened.scan_nodes(None).count(), 3);
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn checkpoint_retains_one_previous_generation_before_reclaim() {
        let path = unique_test_dir("checkpoint_generation_reclaim");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        for id in 1..=3 {
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(id))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }

        assert!(!path.join("checkpoint.1.skein").exists());
        assert!(!path.join("wal.1.skein").exists());
        assert!(!path.join("canonical.1.skein").exists());
        assert!(!path.join("canonical.1.manifest.skein").exists());
        assert!(!path
            .join(skein_storage::canonical_segment_descriptor_page_file(1))
            .exists());
        assert!(!path
            .join(skein_storage::canonical_segment_descriptor_root_file(1))
            .exists());
        assert!(!path.join("adjacency.1.skein").exists());
        assert!(!path.join("adjacency.1.manifest.skein").exists());
        assert!(!path
            .join(skein_storage::canonical_adjacency_descriptor_page_file(1))
            .exists());
        assert!(!path
            .join(skein_storage::canonical_adjacency_descriptor_root_file(1))
            .exists());
        assert!(!path.join("properties.1.skein").exists());
        assert!(!path.join("properties.1.manifest.skein").exists());
        assert!(!path.join("property-index.1.skein").exists());
        assert!(!path.join("property-index.1.manifest.skein").exists());
        assert!(!path
            .join(skein_storage::relational_row_page_manifest_generation_file(
                1
            ))
            .exists());
        assert!(!path
            .join(skein_storage::relational_overflow_manifest_generation_file(
                1
            ))
            .exists());
        assert!(path.join("checkpoint.2.skein").exists());
        assert!(path.join("wal.2.skein").exists());
        assert!(path.join("canonical.2.skein").exists());
        assert!(path.join("canonical.2.manifest.skein").exists());
        assert!(path
            .join(skein_storage::canonical_segment_descriptor_page_file(2))
            .exists());
        assert!(path
            .join(skein_storage::canonical_segment_descriptor_root_file(2))
            .exists());
        assert!(path.join("adjacency.2.skein").exists());
        assert!(!path.join("adjacency.2.manifest.skein").exists());
        assert!(path
            .join(skein_storage::canonical_adjacency_descriptor_page_file(2))
            .exists());
        assert!(path
            .join(skein_storage::canonical_adjacency_descriptor_root_file(2))
            .exists());
        assert!(path.join("properties.2.skein").exists());
        assert!(path.join("properties.2.manifest.skein").exists());
        assert!(path.join("property-index.2.skein").exists());
        assert!(path.join("property-index.2.manifest.skein").exists());
        assert!(path
            .join(skein_storage::relational_row_page_manifest_generation_file(
                2
            ))
            .exists());
        assert!(path
            .join(skein_storage::relational_overflow_manifest_generation_file(
                2
            ))
            .exists());
        assert!(path.join("checkpoint.3.skein").exists());
        assert!(path.join("wal.3.skein").exists());
        assert!(path.join("canonical.3.skein").exists());
        assert!(path.join("canonical.3.manifest.skein").exists());
        assert!(path
            .join(skein_storage::canonical_segment_descriptor_page_file(3))
            .exists());
        assert!(path
            .join(skein_storage::canonical_segment_descriptor_root_file(3))
            .exists());
        assert!(path.join("adjacency.3.skein").exists());
        assert!(!path.join("adjacency.3.manifest.skein").exists());
        assert!(path
            .join(skein_storage::canonical_adjacency_descriptor_page_file(3))
            .exists());
        assert!(path
            .join(skein_storage::canonical_adjacency_descriptor_root_file(3))
            .exists());
        assert!(path.join("properties.3.skein").exists());
        assert!(path.join("properties.3.manifest.skein").exists());
        assert!(path.join("property-index.3.skein").exists());
        assert!(path.join("property-index.3.manifest.skein").exists());
        assert!(path
            .join(skein_storage::relational_row_page_manifest_generation_file(
                3
            ))
            .exists());
        assert!(path
            .join(skein_storage::relational_overflow_manifest_generation_file(
                3
            ))
            .exists());
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn reopen_after_checkpoint_uses_manifest_lsn_without_overwriting_wal() {
        let path = unique_test_dir("manifest_reopen_lsn");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
        }

        let wal = read_test_wal(&path).unwrap();
        assert!(wal.starts_with("2\tcreate_node\t"));
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let nodes = store.scan_nodes(Some(label)).collect::<Vec<_>>();
        assert_eq!(nodes.len(), 2);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_corrupt_manifest() {
        let path = unique_test_dir("corrupt_manifest");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        let manifest_path = path.join("manifest.skein");
        let manifest = std::fs::read_to_string(&manifest_path).unwrap();
        std::fs::write(
            &manifest_path,
            manifest.replace("checkpoint_epoch\t1\n", "checkpoint_epoch\t2\n"),
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error.to_string().contains("manifest checksum mismatch"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_unsupported_manifest_storage_version() {
        let path = unique_test_dir("manifest_storage_version");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        rewrite_checksummed_file(
            &path.join("manifest.skein"),
            "version\tskein-storage-v1\n",
            "version\tskein-storage-v0\n",
            "manifest",
        );

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported storage version: skein-storage-v0"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn single_storage_format_rejects_unpublished_manifest() {
        let path = unique_test_dir("unpublished_manifest_format");
        {
            let mut catalog = Catalog::default();
            GraphStore::open(&path, &mut catalog).unwrap();
        }
        rewrite_checksummed_file(
            &path.join("manifest.skein"),
            "SKEIN_MANIFEST_V1\n",
            "INVALID_MANIFEST_HEADER\n",
            "manifest",
        );

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error.to_string().contains("missing the V1 format header"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn single_storage_format_rejects_incomplete_v1_manifest() {
        let path = unique_test_dir("incomplete_v1_manifest");
        {
            let mut catalog = Catalog::default();
            GraphStore::open(&path, &mut catalog).unwrap();
        }
        rewrite_checksummed_file(
            &path.join("manifest.skein"),
            "checkpoint_generation\tnone\n",
            "",
            "manifest",
        );

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error
            .to_string()
            .contains("manifest is missing required field: checkpoint_generation"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn single_storage_format_rejects_manifestless_artifacts() {
        let path = unique_test_dir("artifacts_without_manifest");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("checkpoint.skein"), b"unpublished format").unwrap();

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error
            .to_string()
            .contains("storage artifacts but no durable manifest"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_unsupported_checkpoint_storage_version() {
        let path = unique_test_dir("checkpoint_storage_version");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        rewrite_checksummed_file(
            &active_checkpoint_path(&path),
            "version\tskein-storage-v1\n",
            "version\tskein-storage-v0\n",
            "checkpoint",
        );

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported storage version: skein-storage-v0"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn replays_property_index_from_wal() {
        let path = unique_test_dir("property_index_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_property_index(&mut catalog, "Memory", "id")
                .unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("id", Value::Int(1)),
                        ("title", Value::String("Graph foundations".to_string())),
                    ]),
                )
                .unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let label = catalog.label_id("Memory").unwrap();
            let nodes = store
                .seek_nodes_by_property(label, "id", &Value::Int(1))
                .collect::<Vec<_>>();
            assert_eq!(nodes.len(), 1);
            assert_eq!(
                nodes[0].properties.get("title"),
                Some(&Value::String("Graph foundations".to_string()))
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rebuilds_property_index_from_checkpoint() {
        let path = unique_test_dir("property_index_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("id", Value::Int(1)),
                        ("title", Value::String("Graph foundations".to_string())),
                    ]),
                )
                .unwrap();
            // Declared after the write, so the checkpoint has to carry the
            // backfilled content and not just the descriptor.
            store
                .create_property_index(&mut catalog, "Memory", "title")
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let label = catalog.label_id("Memory").unwrap();
            let nodes = store
                .seek_nodes_by_property(
                    label,
                    "title",
                    &Value::String("Graph foundations".to_string()),
                )
                .collect::<Vec<_>>();
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].properties.get("id"), Some(&Value::Int(1)));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn scan_pruning_uses_property_eq_index_for_unique_key() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:1".to_string())),
                    ("title", Value::String("Graph foundations".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:2".to_string())),
                    ("title", Value::String("Storage notes".to_string())),
                ]),
            )
            .unwrap();

        store
            .create_property_index(&mut catalog, "Memory", "stable_id")
            .unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::Eq {
                property: "stable_id".to_string(),
                value: Value::String("memory:2".to_string()),
            }),
        );

        assert_eq!(scan.nodes.len(), 1);
        assert_eq!(
            scan.nodes[0].properties.get("title"),
            Some(&Value::String("Storage notes".to_string()))
        );
        assert_eq!(
            scan.report.strategy,
            ScanPruningStrategy::PropertyEq {
                property: "stable_id".to_string()
            }
        );
        assert_eq!(scan.report.target_kind, ScanPruningTargetKind::Node);
        assert_eq!(scan.report.label_id, Some(label));
        assert_eq!(scan.report.rel_type_id, None);
        assert!(scan.report.pruned);
        assert_eq!(scan.report.candidate_count_before_filter, 1);
        assert_eq!(scan.report.candidate_count_before_pruning, 2);
        assert_eq!(scan.report.pruned_candidate_count, 1);
        assert_eq!(scan.report.filtered_out_count, 0);
    }

    #[test]
    fn scan_pruning_treats_in_values_as_enum_set() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for state in ["active", "deleted", "forgotten"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("state", Value::String(state.to_string()))]),
                )
                .unwrap();
        }

        store
            .create_property_index(&mut catalog, "Memory", "state")
            .unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::In {
                property: "state".to_string(),
                values: vec![
                    Value::String("deleted".to_string()),
                    Value::String("forgotten".to_string()),
                ],
            }),
        );
        let states = scan
            .nodes
            .iter()
            .map(|node| node.properties.get("state").unwrap().clone())
            .collect::<BTreeSet<_>>();

        assert_eq!(
            states,
            BTreeSet::from([
                Value::String("deleted".to_string()),
                Value::String("forgotten".to_string()),
            ])
        );
        assert_eq!(
            scan.report.strategy,
            ScanPruningStrategy::PropertyIn {
                property: "state".to_string()
            }
        );
        assert_eq!(scan.report.candidate_count_before_filter, 2);
        assert_eq!(scan.report.candidate_count_before_pruning, 3);
        assert_eq!(scan.report.pruned_candidate_count, 1);
    }

    #[test]
    fn scan_pruning_uses_property_exists_for_not_null_filter() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("confidence", Value::Float(0.9))]),
            )
            .unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([]))
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("confidence", Value::Null)]),
            )
            .unwrap();

        store
            .create_property_index(&mut catalog, "Memory", "confidence")
            .unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::IsNotNull {
                property: "confidence".to_string(),
            }),
        );

        assert_eq!(scan.nodes.len(), 1);
        assert_eq!(
            scan.report.strategy,
            ScanPruningStrategy::PropertyExists {
                property: "confidence".to_string()
            }
        );
        assert!(scan.report.pruned);
        assert!(!scan.report.exact_empty);
        assert_eq!(scan.report.candidate_count_before_filter, 1);
        assert_eq!(scan.report.candidate_count_before_pruning, 3);
        assert_eq!(scan.report.pruned_candidate_count, 2);
        assert_eq!(scan.report.filtered_out_count, 0);
    }

    #[test]
    fn scan_pruning_uses_property_missing_or_null_for_null_filter() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("latest_at", Value::Int(10))]),
            )
            .unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([]))
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("latest_at", Value::Null)]),
            )
            .unwrap();

        store
            .create_property_index(&mut catalog, "Memory", "latest_at")
            .unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::IsNull {
                property: "latest_at".to_string(),
            }),
        );

        assert_eq!(scan.nodes.len(), 2);
        assert_eq!(
            scan.report.strategy,
            ScanPruningStrategy::PropertyMissingOrNull {
                property: "latest_at".to_string()
            }
        );
        assert!(scan.report.pruned);
        assert!(!scan.report.exact_empty);
        assert_eq!(scan.report.candidate_count_before_filter, 2);
        assert_eq!(scan.report.candidate_count_before_pruning, 3);
        assert_eq!(scan.report.pruned_candidate_count, 1);
        assert_eq!(scan.report.filtered_out_count, 0);
    }

    #[test]
    fn scan_pruning_uses_default_if_null_eq_for_normalized_default_filter() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(&mut catalog, "Thread", properties([]))
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Thread",
                properties([("space_id", Value::String(String::new()))]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Thread",
                properties([("space_id", Value::String("default".to_string()))]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Thread",
                properties([("space_id", Value::String("team".to_string()))]),
            )
            .unwrap();

        store
            .create_property_index(&mut catalog, "Thread", "space_id")
            .unwrap();
        let label = catalog.label_id("Thread").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::DefaultIfNullOrEq {
                property: "space_id".to_string(),
                empty: Value::String(String::new()),
                default: Value::String("default".to_string()),
                value: Value::String("default".to_string()),
                negated: false,
            }),
        );

        assert_eq!(scan.nodes.len(), 3);
        assert_eq!(
            scan.report.strategy,
            ScanPruningStrategy::PropertyDefaultIfNullEq {
                property: "space_id".to_string()
            }
        );
        assert!(scan.report.pruned);
        assert_eq!(scan.report.candidate_count_before_filter, 3);
        assert_eq!(scan.report.candidate_count_before_pruning, 4);
        assert_eq!(scan.report.pruned_candidate_count, 1);
        assert_eq!(scan.report.filtered_out_count, 0);
    }

    #[test]
    fn scan_pruning_uses_default_if_null_not_eq_for_normalized_default_filter() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(&mut catalog, "Thread", properties([]))
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Thread",
                properties([("space_id", Value::String(String::new()))]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Thread",
                properties([("space_id", Value::String("default".to_string()))]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Thread",
                properties([("space_id", Value::String("team".to_string()))]),
            )
            .unwrap();

        store
            .create_property_index(&mut catalog, "Thread", "space_id")
            .unwrap();
        let label = catalog.label_id("Thread").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::DefaultIfNullOrEq {
                property: "space_id".to_string(),
                empty: Value::String(String::new()),
                default: Value::String("default".to_string()),
                value: Value::String("default".to_string()),
                negated: true,
            }),
        );

        assert_eq!(scan.nodes.len(), 1);
        assert_eq!(
            scan.report.strategy,
            ScanPruningStrategy::PropertyDefaultIfNullNotEq {
                property: "space_id".to_string()
            }
        );
        assert!(scan.report.pruned);
        assert_eq!(scan.report.candidate_count_before_filter, 1);
        assert_eq!(scan.report.candidate_count_before_pruning, 4);
        assert_eq!(scan.report.pruned_candidate_count, 3);
        assert_eq!(scan.report.filtered_out_count, 0);
    }

    #[test]
    fn scan_pruning_uses_smallest_candidate_in_and_filter() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:1".to_string())),
                    ("state", Value::String("active".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:2".to_string())),
                    ("state", Value::String("active".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String("memory:3".to_string())),
                    ("state", Value::String("forgotten".to_string())),
                ]),
            )
            .unwrap();

        store
            .create_property_index(&mut catalog, "Memory", "state")
            .unwrap();
        store
            .create_property_index(&mut catalog, "Memory", "stable_id")
            .unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::And(vec![
                PropertyFilter::In {
                    property: "state".to_string(),
                    values: vec![
                        Value::String("active".to_string()),
                        Value::String("forgotten".to_string()),
                    ],
                },
                PropertyFilter::Eq {
                    property: "stable_id".to_string(),
                    value: Value::String("memory:2".to_string()),
                },
            ])),
        );

        assert_eq!(scan.nodes.len(), 1);
        assert_eq!(
            scan.report.strategy,
            ScanPruningStrategy::PropertyEq {
                property: "stable_id".to_string()
            }
        );
        assert_eq!(scan.report.candidate_count_before_filter, 1);
        assert_eq!(scan.report.candidate_count_before_pruning, 3);
        assert_eq!(scan.report.pruned_candidate_count, 2);
    }

    #[test]
    fn scan_pruning_unions_prunable_or_branches() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        for state in ["active", "deleted", "forgotten"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([("state", Value::String(state.to_string()))]),
                )
                .unwrap();
        }

        store
            .create_property_index(&mut catalog, "Memory", "state")
            .unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::Or(vec![
                PropertyFilter::Eq {
                    property: "state".to_string(),
                    value: Value::String("active".to_string()),
                },
                PropertyFilter::Eq {
                    property: "state".to_string(),
                    value: Value::String("forgotten".to_string()),
                },
            ])),
        );

        assert_eq!(scan.nodes.len(), 2);
        assert_eq!(scan.report.strategy, ScanPruningStrategy::OrUnion);
        assert_eq!(scan.report.candidate_count_before_filter, 2);
        assert_eq!(scan.report.candidate_count_before_pruning, 3);
        assert_eq!(scan.report.pruned_candidate_count, 1);
        assert!(scan.report.pruned);
    }

    #[test]
    fn scan_pruning_reports_exact_empty_for_empty_in_filter() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("state", Value::String("active".to_string()))]),
            )
            .unwrap();

        store
            .create_property_index(&mut catalog, "Memory", "state")
            .unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::In {
                property: "state".to_string(),
                values: vec![],
            }),
        );

        assert!(scan.nodes.is_empty());
        assert_eq!(scan.report.strategy, ScanPruningStrategy::Empty);
        assert!(scan.report.exact_empty);
        assert_eq!(scan.report.candidate_count_before_filter, 0);
        assert_eq!(scan.report.candidate_count_before_pruning, 1);
        assert_eq!(scan.report.pruned_candidate_count, 1);
    }

    #[test]
    fn scan_pruning_uses_property_range_for_numeric_and_iso_date_strings() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("importance", Value::Float(0.2)),
                    ("updated_at", Value::String("2026-07-01".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("importance", Value::Float(0.7)),
                    ("updated_at", Value::String("2026-07-15".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("importance", Value::Float(0.9)),
                    ("updated_at", Value::String("2026-08-01".to_string())),
                ]),
            )
            .unwrap();

        store
            .create_property_index(&mut catalog, "Memory", "importance")
            .unwrap();
        store
            .create_property_index(&mut catalog, "Memory", "updated_at")
            .unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let numeric_scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::Range {
                property: "importance".to_string(),
                lower: Some((Value::Float(0.5), true)),
                upper: Some((Value::Float(0.8), true)),
            }),
        );
        assert_eq!(numeric_scan.nodes.len(), 1);
        assert_eq!(
            numeric_scan.report.strategy,
            ScanPruningStrategy::PropertyRange {
                property: "importance".to_string()
            }
        );
        assert_eq!(numeric_scan.report.candidate_count_before_filter, 1);
        assert_eq!(numeric_scan.report.candidate_count_before_pruning, 3);
        assert_eq!(numeric_scan.report.pruned_candidate_count, 2);

        let date_scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::Range {
                property: "updated_at".to_string(),
                lower: Some((Value::String("2026-07-01".to_string()), true)),
                upper: Some((Value::String("2026-07-31".to_string()), true)),
            }),
        );
        assert_eq!(date_scan.nodes.len(), 2);
        assert_eq!(
            date_scan.report.strategy,
            ScanPruningStrategy::PropertyRange {
                property: "updated_at".to_string()
            }
        );
        assert_eq!(date_scan.report.candidate_count_before_filter, 2);
        assert_eq!(date_scan.report.candidate_count_before_pruning, 3);
        assert_eq!(date_scan.report.pruned_candidate_count, 1);
    }

    #[test]
    fn relationship_scan_pruning_uses_property_equality_and_in_list() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
        for state in ["active", "deleted", "archived"] {
            store
                .create_relationship(
                    &mut catalog,
                    source,
                    target,
                    "RELATES_TO",
                    properties([("lifecycle_state", Value::String(state.to_string()))]),
                )
                .unwrap();
        }
        let rel_type = catalog.rel_type_id("RELATES_TO").unwrap();

        let eq_scan = store.scan_relationships_with_filter_pruning(
            Some(rel_type),
            Some(&PropertyFilter::Eq {
                property: "lifecycle_state".to_string(),
                value: Value::String("active".to_string()),
            }),
        );
        assert_eq!(eq_scan.relationships.len(), 1);
        assert_eq!(
            eq_scan.report.strategy,
            ScanPruningStrategy::PropertyEq {
                property: "lifecycle_state".to_string()
            }
        );
        assert_eq!(
            eq_scan.report.target_kind,
            ScanPruningTargetKind::Relationship
        );
        assert_eq!(eq_scan.report.label_id, None);
        assert_eq!(eq_scan.report.rel_type_id, Some(rel_type));
        assert_eq!(eq_scan.report.candidate_count_before_pruning, 3);
        assert_eq!(eq_scan.report.candidate_count_before_filter, 1);
        assert_eq!(eq_scan.report.pruned_candidate_count, 2);

        let in_scan = store.scan_relationships_with_filter_pruning(
            Some(rel_type),
            Some(&PropertyFilter::In {
                property: "lifecycle_state".to_string(),
                values: vec![
                    Value::String("active".to_string()),
                    Value::String("archived".to_string()),
                ],
            }),
        );
        assert_eq!(in_scan.relationships.len(), 2);
        assert_eq!(
            in_scan.report.strategy,
            ScanPruningStrategy::PropertyIn {
                property: "lifecycle_state".to_string()
            }
        );
        assert_eq!(in_scan.report.candidate_count_before_filter, 2);
        assert_eq!(in_scan.report.pruned_candidate_count, 1);
    }

    #[test]
    fn relationship_scan_pruning_uses_property_range_and_tracks_updates() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
            .unwrap();
        let target = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
            .unwrap();
        let old = store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "MENTIONS",
                properties([("confidence", Value::Float(0.2))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "MENTIONS",
                properties([("confidence", Value::Float(0.7))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "MENTIONS",
                properties([("confidence", Value::Float(0.9))]),
            )
            .unwrap();
        let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

        let range_scan = store.scan_relationships_with_filter_pruning(
            Some(rel_type),
            Some(&PropertyFilter::Range {
                property: "confidence".to_string(),
                lower: Some((Value::Float(0.5), true)),
                upper: Some((Value::Float(0.8), true)),
            }),
        );
        assert_eq!(range_scan.relationships.len(), 1);
        assert_eq!(
            range_scan.report.strategy,
            ScanPruningStrategy::PropertyRange {
                property: "confidence".to_string()
            }
        );
        assert_eq!(range_scan.report.candidate_count_before_pruning, 3);
        assert_eq!(range_scan.report.candidate_count_before_filter, 1);
        assert_eq!(range_scan.report.pruned_candidate_count, 2);

        store.apply_set_relationship_property(old, "confidence".to_string(), Value::Float(0.75));
        let updated_scan = store.scan_relationships_with_filter_pruning(
            Some(rel_type),
            Some(&PropertyFilter::Range {
                property: "confidence".to_string(),
                lower: Some((Value::Float(0.5), true)),
                upper: Some((Value::Float(0.8), true)),
            }),
        );
        assert_eq!(updated_scan.relationships.len(), 2);
        assert_eq!(updated_scan.report.candidate_count_before_filter, 2);

        store.apply_delete_relationship(old);
        let deleted_scan = store.scan_relationships_with_filter_pruning(
            Some(rel_type),
            Some(&PropertyFilter::Eq {
                property: "confidence".to_string(),
                value: Value::Float(0.75),
            }),
        );
        assert!(deleted_scan.relationships.is_empty());
        assert_eq!(deleted_scan.report.candidate_count_before_filter, 0);
        assert_eq!(deleted_scan.report.pruned_candidate_count, 2);
    }

    #[test]
    fn scan_pruning_falls_back_for_unindexed_string_contains() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("title", Value::String("Graph foundations".to_string()))]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("title", Value::String("Storage notes".to_string()))]),
            )
            .unwrap();

        store
            .create_property_index(&mut catalog, "Memory", "title")
            .unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let scan = store.scan_nodes_with_filter_pruning(
            &catalog,
            Some(label),
            Some(&PropertyFilter::Contains {
                property: "title".to_string(),
                value: "Graph".to_string(),
            }),
        );

        assert_eq!(scan.nodes.len(), 1);
        assert_eq!(scan.report.strategy, ScanPruningStrategy::FullLabelScan);
        assert!(!scan.report.pruned);
        assert_eq!(scan.report.candidate_count_before_filter, 2);
        assert_eq!(scan.report.candidate_count_before_pruning, 2);
        assert_eq!(scan.report.pruned_candidate_count, 0);
        assert_eq!(scan.report.filtered_out_count, 1);
    }

    #[test]
    fn list_values_round_trip_through_checkpoint() {
        let path = unique_test_dir("list_value_checkpoint");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([(
                        "tags",
                        Value::List(vec![
                            Value::String("graph".to_string()),
                            Value::String("storage".to_string()),
                        ]),
                    )]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let node = store.scan_nodes(None).next().unwrap();
            assert_eq!(
                node.properties.get("tags"),
                Some(&Value::List(vec![
                    Value::String("graph".to_string()),
                    Value::String("storage".to_string()),
                ]))
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn loads_projected_graph_artifact_from_checkpoint() {
        let path = unique_test_dir("projected_graph_artifact_cache");
        let definition = ProjectedGraphDefinition {
            node_labels: vec!["Memory".to_string()],
            rel_types: vec!["LINKS".to_string()],
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "LINKS", BTreeMap::new())
                .unwrap();
            store
                .register_projected_graph("MemoryGraph", definition.clone())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        let artifact_bytes = std::fs::read(path.join("projected_graphs.skein")).unwrap();
        assert!(artifact_bytes.starts_with(DURABLE_COMPRESSION_HEADER.as_bytes()));
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            let artifact = store
                .projected_graph_artifact("MemoryGraph", &definition)
                .unwrap();
            assert_eq!(artifact.node_count(), 2);
            assert_eq!(artifact.edge_count(), 1);
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn ignores_projected_graph_artifact_after_wal_replay_advances_epoch() {
        let path = unique_test_dir("projected_graph_artifact_stale");
        let definition = ProjectedGraphDefinition {
            node_labels: vec!["Memory".to_string()],
            rel_types: vec!["LINKS".to_string()],
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let source = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            let target = store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "LINKS", BTreeMap::new())
                .unwrap();
            store
                .register_projected_graph("MemoryGraph", definition.clone())
                .unwrap();
            store.checkpoint(&catalog).unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(3))]))
                .unwrap();
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open(&path, &mut catalog).unwrap();
            assert!(store
                .projected_graph_artifact("MemoryGraph", &definition)
                .is_none());
            let status = store
                .projected_graph_statuses()
                .into_iter()
                .find(|status| status.name == "MemoryGraph")
                .unwrap();
            assert!(!status.reusable);
            assert_eq!(status.projection_epoch, None);
            assert_eq!(status.commit_epoch, None);
            assert_eq!(status.node_count, None);
            assert_eq!(status.edge_count, None);
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn doctor_repairs_torn_wal_tail_after_strict_open_rejects_it() {
        let path = unique_test_dir("torn_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
        }
        std::fs::OpenOptions::new()
            .append(true)
            .open(active_wal_path(&path))
            .unwrap()
            .write_all(b"torn-entry-without-checksum")
            .unwrap();

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error
            .to_string()
            .contains("strict WAL recovery rejected torn tail"));
        let plan =
            DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
        assert_eq!(
            plan.discarded_wal_tail_bytes,
            b"torn-entry-without-checksum".len() as u64
        );
        DatabaseDoctor::apply_wal_tail_repair(
            &path,
            &plan,
            plan.acknowledge_potential_data_loss(),
            WalDoctorOptions::default(),
        )
        .unwrap();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let label = catalog.label_id("Memory").unwrap();
        let nodes = store.scan_nodes(Some(label)).collect::<Vec<_>>();
        assert_eq!(nodes.len(), 1);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn doctor_discards_torn_batch_wal_without_partial_path_recovery() {
        let path = unique_test_dir("torn_batch_wal");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_connected_nodes(
                    &mut catalog,
                    ConnectedNodesCreate {
                        source_label: "Memory".to_string(),
                        source_properties: properties([("id", Value::Int(1))]),
                        rel_type: "MENTIONS".to_string(),
                        rel_properties: BTreeMap::new(),
                        target_label: "Entity".to_string(),
                        target_properties: properties([("id", Value::Int(10))]),
                    },
                )
                .unwrap();
        }
        let wal_path = active_wal_path(&path);
        truncate_test_wal_tail(&wal_path, 8);

        let plan =
            DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
        DatabaseDoctor::apply_wal_tail_repair(
            &path,
            &plan,
            plan.acknowledge_potential_data_loss(),
            WalDoctorOptions::default(),
        )
        .unwrap();
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        assert!(store.scan_nodes(None).next().is_none());
        assert!(catalog.rel_type_id("MENTIONS").is_none());
        std::fs::remove_dir_all(path).unwrap();
    }

    /// Cuts `cut` bytes off the WAL tail, leaving the final record's
    /// fragment chain physically incomplete (a binary torn tail).
    fn truncate_test_wal_tail(wal_path: &std::path::Path, cut: u64) {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(wal_path)
            .unwrap();
        let len = file.metadata().unwrap().len();
        file.set_len(len - cut).unwrap();
        file.sync_all().unwrap();
    }

    #[test]
    fn rejects_and_quarantines_checksum_corruption_before_valid_wal_suffix() {
        let path = unique_test_dir("wal_middle_corruption");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
        }
        let wal_path = active_wal_path(&path);
        // Flip one payload byte of the first record: mid-log corruption
        // ahead of a valid suffix must fail closed.
        let mut wal = std::fs::read(&wal_path).unwrap();
        let first_payload_offset = super::WAL_BINARY_FILE_HEADER_BYTES
            + super::wal_codec::frame::WAL_FRAGMENT_HEADER_BYTES;
        wal[first_payload_offset] ^= 0xff;
        std::fs::write(&wal_path, &wal).unwrap();

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error.to_string().contains("WAL corruption at byte offset"));
        assert!(error.to_string().contains("checksum mismatch"));
        assert_eq!(
            std::fs::read_dir(path.join("quarantine")).unwrap().count(),
            1
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_and_quarantines_checksum_corruption_at_wal_tail() {
        let path = unique_test_dir("wal_tail_checksum_corruption");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
        }
        let wal_path = active_wal_path(&path);
        // Flip the stored checksum of the final complete fragment chain:
        // a complete chain failing its checksum is corruption, never a
        // repairable torn tail.
        let mut corrupt_wal = std::fs::read(&wal_path).unwrap();
        corrupt_wal[super::WAL_BINARY_FILE_HEADER_BYTES] ^= 0xff;
        std::fs::write(&wal_path, &corrupt_wal).unwrap();

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error.to_string().contains("WAL corruption at byte offset"));
        assert!(error.to_string().contains("checksum mismatch"));
        assert_eq!(std::fs::read(&wal_path).unwrap(), corrupt_wal);
        assert_eq!(
            std::fs::read_dir(path.join("quarantine")).unwrap().count(),
            1
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rejects_and_quarantines_non_contiguous_wal_lsn() {
        let path = unique_test_dir("wal_lsn_gap");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                .unwrap();
            store
                .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
                .unwrap();
        }
        let wal_path = active_wal_path(&path);
        // Rebuild the WAL with well-formed framing but a gap in the LSN
        // sequence: the second record claims LSN 3 instead of 2.
        let mut cursor = match super::WalRecordCursor::open(&wal_path, None).unwrap() {
            super::WalOpenOutcome::Cursor(cursor) => cursor,
            _ => panic!("test WAL is missing its header"),
        };
        let generation = cursor.generation();
        let start_lsn = cursor.start_lsn();
        let mut entries = Vec::new();
        loop {
            match cursor.next().unwrap() {
                super::WalCursorEvent::Entry { entry, .. } => entries.push(entry),
                super::WalCursorEvent::Eof => break,
                _ => panic!("test WAL is damaged"),
            }
        }
        entries.last_mut().unwrap().lsn += 1;
        let mut rewritten = super::encode_binary_wal_header(generation, start_lsn);
        for (index, entry) in entries.iter().enumerate() {
            let payload = super::encode_binary_wal_record(entry, index as u64 + 1).unwrap();
            let position = rewritten.len() as u64 - super::WAL_BINARY_FILE_HEADER_BYTES as u64;
            rewritten.extend_from_slice(&super::frame_binary_wal_record(
                generation, &payload, position,
            ));
        }
        std::fs::write(&wal_path, rewritten).unwrap();

        let mut catalog = Catalog::default();
        let error = GraphStore::open(&path, &mut catalog).unwrap_err();
        assert!(error.to_string().contains("WAL LSN sequence mismatch"));
        assert_eq!(
            std::fs::read_dir(path.join("quarantine")).unwrap().count(),
            1
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn mutation_rejects_over_depth_value_before_wal_append() {
        fn value_at_depth(depth: usize) -> Value {
            assert!(depth > 0);
            (1..depth).fold(Value::Null, |value, _| Value::List(vec![value]))
        }

        let path = unique_test_dir("wal_value_depth_admission");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let accepted_id = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("payload", value_at_depth(32))]),
            )
            .unwrap();
        let commit_epoch = store.commit_epoch();
        let wal_path = active_wal_path(&path);
        let wal_len = std::fs::metadata(&wal_path).unwrap().len();

        let error = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("payload", value_at_depth(33))]),
            )
            .unwrap_err();
        assert!(matches!(error, SkeinError::Semantic(_)));
        assert!(error.to_string().contains("nesting exceeds 32"));
        assert_eq!(store.commit_epoch(), commit_epoch);
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), wal_len);

        store.checkpoint(&catalog).unwrap();
        drop(store);
        let mut reopened_catalog = Catalog::default();
        let reopened = GraphStore::open(&path, &mut reopened_catalog).unwrap();
        assert!(reopened.node_owned(accepted_id).unwrap().is_some());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn manifest_fails_closed_for_missing_canonical_artifacts() {
        for missing in ["checkpoint", "wal", "manifest"] {
            let path = unique_test_dir(&format!("missing_{missing}"));
            {
                let mut catalog = Catalog::default();
                let mut store = GraphStore::open(&path, &mut catalog).unwrap();
                store
                    .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
                    .unwrap();
                store.checkpoint(&catalog).unwrap();
            }
            match missing {
                "checkpoint" => std::fs::remove_file(active_checkpoint_path(&path)).unwrap(),
                "wal" => std::fs::remove_file(active_wal_path(&path)).unwrap(),
                "manifest" => std::fs::remove_file(path.join("manifest.skein")).unwrap(),
                _ => unreachable!(),
            }

            let mut catalog = Catalog::default();
            let error = GraphStore::open(&path, &mut catalog).unwrap_err();
            assert!(
                error.to_string().contains("missing")
                    || error.to_string().contains("no durable manifest")
            );
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn doctor_recovers_complete_mem_shaped_batches_before_torn_tail() {
        let path = unique_test_dir("mem_shaped_batch_recovery");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            let old_memory = store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
                        ("id", Value::String("mem-old".to_string())),
                        ("is_latest", Value::Bool(true)),
                        ("updated_at", Value::Int(10)),
                    ]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();

            store
                .set_node_properties_by_ids(
                    &mut catalog,
                    &[old_memory],
                    &[
                        NodeSetAssignment {
                            property: "is_latest".to_string(),
                            value: NodeSetValue::Value(Value::Bool(false)),
                        },
                        NodeSetAssignment {
                            property: "updated_at".to_string(),
                            value: NodeSetValue::Value(Value::Int(20)),
                        },
                    ],
                )
                .unwrap();
            store
                .create_connected_nodes(
                    &mut catalog,
                    ConnectedNodesCreate {
                        source_label: "Memory".to_string(),
                        source_properties: properties([
                            ("id", Value::String("mem-new".to_string())),
                            ("is_latest", Value::Bool(true)),
                        ]),
                        rel_type: "EVOLVES".to_string(),
                        rel_properties: properties([
                            ("content_relation", Value::String("replaces".to_string())),
                            ("confidence", Value::Float(0.9)),
                        ]),
                        target_label: "Memory".to_string(),
                        target_properties: properties([
                            ("id", Value::String("mem-snapshot-old".to_string())),
                            ("is_latest", Value::Bool(false)),
                        ]),
                    },
                )
                .unwrap();
            store
                .create_connected_nodes(
                    &mut catalog,
                    ConnectedNodesCreate {
                        source_label: "Memory".to_string(),
                        source_properties: properties([(
                            "id",
                            Value::String("mem-torn-new".to_string()),
                        )]),
                        rel_type: "EVOLVES".to_string(),
                        rel_properties: properties([(
                            "content_relation",
                            Value::String("torn".to_string()),
                        )]),
                        target_label: "Memory".to_string(),
                        target_properties: properties([(
                            "id",
                            Value::String("mem-torn-old".to_string()),
                        )]),
                    },
                )
                .unwrap();
        }

        let wal_path = active_wal_path(&path);
        truncate_test_wal_tail(&wal_path, 8);

        let plan =
            DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
        let repair = DatabaseDoctor::apply_wal_tail_repair(
            &path,
            &plan,
            plan.acknowledge_potential_data_loss(),
            WalDoctorOptions::default(),
        )
        .unwrap();
        assert_eq!(repair.next_lsn_after_repair, 4);
        assert!(repair.discarded_wal_tail_bytes > 0);

        let mut catalog = Catalog::default();
        let store = GraphStore::open(&path, &mut catalog).unwrap();
        let report = store.storage_recovery_report();
        assert!(report.durable);
        assert_eq!(report.checkpoint_commit_epoch, Some(1));
        assert_eq!(report.wal_replay_start_lsn, Some(2));
        assert_eq!(report.next_lsn_after_replay, Some(4));
        assert_eq!(report.replayed_wal_entries, 2);
        assert!(!report.torn_tail_ignored);
        assert!(report.torn_tail_reason.is_none());
        assert_eq!(report.recovered_commit_epoch, 3);

        let memory_label = catalog.label_id("Memory").unwrap();
        let memories = store.scan_nodes(Some(memory_label)).collect::<Vec<_>>();
        let ids = memories
            .iter()
            .filter_map(|node| node.properties.get("id").cloned())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            ids,
            BTreeSet::from([
                Value::String("mem-old".to_string()),
                Value::String("mem-new".to_string()),
                Value::String("mem-snapshot-old".to_string()),
            ])
        );
        let old = memories
            .iter()
            .find(|node| node.properties.get("id") == Some(&Value::String("mem-old".to_string())))
            .unwrap();
        assert_eq!(old.properties.get("is_latest"), Some(&Value::Bool(false)));
        assert_eq!(old.properties.get("updated_at"), Some(&Value::Int(20)));

        let evolves = catalog.rel_type_id("EVOLVES").unwrap();
        let relationships = store.scan_relationships(Some(evolves)).collect::<Vec<_>>();
        assert_eq!(relationships.len(), 1);
        assert_eq!(
            relationships[0].properties.get("content_relation"),
            Some(&Value::String("replaces".to_string()))
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn adaptive_histograms_use_medium_sample_for_medium_cardinality() {
        let nodes = histogram_nodes("score", 2_000);
        let statistics = compute_statistics(&nodes, &CowSegmentedMap::default(), 1);
        let histogram = statistics
            .property_histograms
            .get(&(LabelId(0), "score".to_string()))
            .unwrap();

        assert_eq!(statistics.histogram_sample_limit, 512);
        assert_eq!(histogram.len(), 256);
        assert_eq!(histogram.first(), Some(&Value::Int(0)));
        assert_eq!(histogram.last(), Some(&Value::Int(1_999)));
        assert_eq!(
            statistics
                .property_distinct_counts
                .get(&(LabelId(0), "score".to_string())),
            Some(&2_000)
        );
        assert_eq!(
            statistics
                .sampled_property_histograms
                .get(&(LabelId(0), "score".to_string())),
            Some(&true)
        );
    }

    #[test]
    fn adaptive_histograms_use_max_sample_for_large_cardinality() {
        let nodes = histogram_nodes("score", 5_000);
        let statistics = compute_statistics(&nodes, &CowSegmentedMap::default(), 1);
        let histogram = statistics
            .property_histograms
            .get(&(LabelId(0), "score".to_string()))
            .unwrap();

        assert_eq!(histogram.len(), 512);
        assert_eq!(histogram.first(), Some(&Value::Int(0)));
        assert_eq!(histogram.last(), Some(&Value::Int(4_999)));
    }

    #[test]
    fn incremental_basic_statistics_match_full_recompute_after_mutations() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([("id", Value::String("stat-source".to_string()))]),
            )
            .unwrap();
        let target = store
            .create_node(
                &mut catalog,
                "Entity",
                properties([("id", Value::String("stat-target".to_string()))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "MENTIONS",
                properties([("confidence", Value::Float(0.9))]),
            )
            .unwrap();

        let report = store.basic_statistics_consistency_report();
        assert!(report.ready);
        assert!(report.mismatched_fields.is_empty());
        assert_eq!(report.incremental, report.recomputed);
        assert_eq!(report.incremental.node_count, 2);
        assert_eq!(report.incremental.relationship_count, 1);
        assert_eq!(
            report
                .incremental
                .label_counts
                .get(&catalog.label_id("Memory").unwrap()),
            Some(&1)
        );
        assert_eq!(
            report
                .incremental
                .label_counts
                .get(&catalog.label_id("Entity").unwrap()),
            Some(&1)
        );
        assert_eq!(
            report
                .incremental
                .rel_type_counts
                .get(&catalog.rel_type_id("MENTIONS").unwrap()),
            Some(&1)
        );

        store
            .delete_nodes(
                &mut catalog,
                "Memory",
                Some(&PropertyFilter::Eq {
                    property: "id".to_string(),
                    value: Value::String("stat-source".to_string()),
                }),
                true,
            )
            .unwrap();
        let report = store.basic_statistics_consistency_report();
        assert!(report.ready);
        assert!(report.mismatched_fields.is_empty());
        assert_eq!(report.incremental, report.recomputed);
        assert_eq!(report.incremental.node_count, 1);
        assert_eq!(report.incremental.relationship_count, 0);
        assert!(!report
            .incremental
            .label_counts
            .contains_key(&catalog.label_id("Memory").unwrap()));
        assert!(!report
            .incremental
            .rel_type_counts
            .contains_key(&catalog.rel_type_id("MENTIONS").unwrap()));
    }

    #[test]
    fn degree_statistics_consistency_report_matches_full_recompute_after_mutations() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();
        let target_one = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(1))]))
            .unwrap();
        let target_two = store
            .create_node(&mut catalog, "Entity", properties([("id", Value::Int(2))]))
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target_one,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target_two,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
        let memory_label = catalog.label_id("Memory").unwrap();
        let entity_label = catalog.label_id("Entity").unwrap();
        let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

        let report = store.degree_statistics_consistency_report();
        assert!(report.ready);
        assert!(report.mismatched_keys.is_empty());
        assert_eq!(report.maintained, report.recomputed);
        assert_eq!(
            report
                .maintained
                .get(&DegreeStatisticsKey {
                    label_id: memory_label,
                    rel_type,
                    direction: AdjacencyDirection::Outgoing,
                })
                .copied(),
            Some(DegreeStatisticsEntry {
                node_count: 1,
                non_zero_node_count: 1,
                relationship_count: 2,
                max_degree: 2,
                dense_node_count: 0,
            })
        );
        assert_eq!(
            report
                .maintained
                .get(&DegreeStatisticsKey {
                    label_id: entity_label,
                    rel_type,
                    direction: AdjacencyDirection::Incoming,
                })
                .copied(),
            Some(DegreeStatisticsEntry {
                node_count: 2,
                non_zero_node_count: 2,
                relationship_count: 2,
                max_degree: 1,
                dense_node_count: 0,
            })
        );

        store
            .delete_relationships(
                &mut catalog,
                RelationshipDeleteRequest {
                    source_label: "Memory".to_string(),
                    filter: None,
                    rel_type: "MENTIONS".to_string(),
                    target_label: "Entity".to_string(),
                    target_filter: Some(PropertyFilter::Eq {
                        property: "id".to_string(),
                        value: Value::Int(1),
                    }),
                    rel_filter: None,
                },
            )
            .unwrap();
        let report = store.degree_statistics_consistency_report();
        assert!(report.ready);
        assert_eq!(report.maintained, report.recomputed);
        assert_eq!(
            report
                .maintained
                .get(&DegreeStatisticsKey {
                    label_id: memory_label,
                    rel_type,
                    direction: AdjacencyDirection::Outgoing,
                })
                .map(|entry| entry.relationship_count),
            Some(1)
        );
    }

    #[test]
    fn degree_statistics_consistency_report_counts_dense_groups() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(0))]))
            .unwrap();
        for id in 0..DENSE_ADJACENCY_DEGREE_THRESHOLD {
            let target = store
                .create_node(
                    &mut catalog,
                    "Entity",
                    properties([("id", Value::Int(id as i64))]),
                )
                .unwrap();
            store
                .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
                .unwrap();
        }
        let report = store.degree_statistics_consistency_report();
        let memory_label = catalog.label_id("Memory").unwrap();
        let rel_type = catalog.rel_type_id("MENTIONS").unwrap();

        assert!(report.ready);
        assert_eq!(
            report
                .maintained
                .get(&DegreeStatisticsKey {
                    label_id: memory_label,
                    rel_type,
                    direction: AdjacencyDirection::Outgoing,
                })
                .map(|entry| (entry.max_degree, entry.dense_node_count)),
            Some((DENSE_ADJACENCY_DEGREE_THRESHOLD as u64, 1))
        );
    }

    #[test]
    fn distinct_value_statistics_consistency_report_matches_index_after_mutations() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        store
            .create_property_index(&mut catalog, "Memory", "unit_type")
            .unwrap();
        store
            .create_property_index(&mut catalog, "Memory", "importance")
            .unwrap();
        let source = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("id", Value::String("memory:1".to_string())),
                    ("unit_type", Value::String("note".to_string())),
                    ("importance", Value::Float(0.2)),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("id", Value::String("memory:2".to_string())),
                    ("unit_type", Value::String("task".to_string())),
                    ("importance", Value::Float(0.7)),
                ]),
            )
            .unwrap();
        let target = store
            .create_node(
                &mut catalog,
                "Entity",
                properties([("id", Value::String("entity:1".to_string()))]),
            )
            .unwrap();
        let weak = store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "MENTIONS",
                properties([("confidence", Value::Float(0.2))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "MENTIONS",
                properties([("confidence", Value::Float(0.7))]),
            )
            .unwrap();

        let report = store.distinct_value_statistics_consistency_report(&catalog);
        let memory_label = catalog.label_id("Memory").unwrap();
        let rel_type = catalog.rel_type_id("MENTIONS").unwrap();
        assert!(report.ready);
        assert_eq!(
            report
                .maintained_property_distinct_counts
                .get(&(memory_label, "unit_type".to_string())),
            Some(&2)
        );
        assert_eq!(
            report
                .maintained_property_distinct_counts
                .get(&(memory_label, "importance".to_string())),
            Some(&2)
        );
        assert_eq!(
            report
                .maintained_rel_property_distinct_counts
                .get(&(rel_type, "confidence".to_string())),
            Some(&2)
        );

        store
            .set_node_property(
                &mut catalog,
                "Memory",
                Some(&PropertyFilter::Eq {
                    property: "id".to_string(),
                    value: Value::String("memory:2".to_string()),
                }),
                "unit_type",
                Value::String("note".to_string()),
            )
            .unwrap();
        store.apply_set_relationship_property(weak, "confidence".to_string(), Value::Float(0.7));

        let report = store.distinct_value_statistics_consistency_report(&catalog);
        assert!(report.ready);
        assert!(report.mismatched_property_keys.is_empty());
        assert!(report.mismatched_rel_property_keys.is_empty());
        assert_eq!(
            report.maintained_property_distinct_counts,
            report.recomputed_property_distinct_counts
        );
        assert_eq!(
            report.maintained_rel_property_distinct_counts,
            report.recomputed_rel_property_distinct_counts
        );
        assert_eq!(
            report
                .maintained_property_distinct_counts
                .get(&(memory_label, "unit_type".to_string())),
            Some(&1)
        );
        assert_eq!(
            report
                .maintained_rel_property_distinct_counts
                .get(&(rel_type, "confidence".to_string())),
            Some(&1)
        );

        store.apply_delete_relationship(weak);
        let report = store.distinct_value_statistics_consistency_report(&catalog);
        assert!(report.ready);
        assert_eq!(
            report
                .maintained_rel_property_distinct_counts
                .get(&(rel_type, "confidence".to_string())),
            Some(&1)
        );
    }

    #[test]
    fn property_index_consistency_report_matches_full_scan_after_mutations() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
        let source = store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("id", Value::String("memory:1".to_string())),
                    ("unit_type", Value::String("note".to_string())),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("id", Value::String("memory:2".to_string())),
                    ("unit_type", Value::String("task".to_string())),
                ]),
            )
            .unwrap();
        let target = store
            .create_node(
                &mut catalog,
                "Entity",
                properties([("id", Value::String("entity:1".to_string()))]),
            )
            .unwrap();
        let weak = store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "MENTIONS",
                properties([("confidence", Value::Float(0.2))]),
            )
            .unwrap();
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                "MENTIONS",
                properties([("confidence", Value::Float(0.7))]),
            )
            .unwrap();

        let report = store.property_index_consistency_report(&catalog);
        assert!(report.ready);
        assert_eq!(
            report.node_index_entry_count,
            report.recomputed_node_index_entry_count
        );
        assert_eq!(
            report.node_index_reference_count,
            report.recomputed_node_index_reference_count
        );
        assert_eq!(
            report.relationship_index_entry_count,
            report.recomputed_relationship_index_entry_count
        );
        assert_eq!(
            report.relationship_index_reference_count,
            report.recomputed_relationship_index_reference_count
        );
        assert_eq!(report.missing_node_key_count, 0);
        assert_eq!(report.extra_node_key_count, 0);
        assert_eq!(report.mismatched_node_key_count, 0);
        assert_eq!(report.missing_relationship_key_count, 0);
        assert_eq!(report.extra_relationship_key_count, 0);
        assert_eq!(report.mismatched_relationship_key_count, 0);
        assert!(report.mismatched_node_keys.is_empty());
        assert!(report.mismatched_relationship_keys.is_empty());

        store
            .set_node_property(
                &mut catalog,
                "Memory",
                Some(&PropertyFilter::Eq {
                    property: "id".to_string(),
                    value: Value::String("memory:2".to_string()),
                }),
                "unit_type",
                Value::String("note".to_string()),
            )
            .unwrap();
        store.apply_set_relationship_property(weak, "confidence".to_string(), Value::Float(0.7));
        store.apply_delete_relationship(weak);

        let report = store.property_index_consistency_report(&catalog);
        assert!(report.ready);
        assert_eq!(
            report.node_index_entry_count,
            report.recomputed_node_index_entry_count
        );
        assert_eq!(
            report.node_index_reference_count,
            report.recomputed_node_index_reference_count
        );
        assert_eq!(
            report.relationship_index_entry_count,
            report.recomputed_relationship_index_entry_count
        );
        assert_eq!(
            report.relationship_index_reference_count,
            report.recomputed_relationship_index_reference_count
        );
        assert!(report.mismatched_node_keys.is_empty());
        assert!(report.mismatched_relationship_keys.is_empty());
    }

    #[test]
    fn statistics_track_relationship_property_distinct_counts() {
        let relationships = (0..10)
            .map(|id| {
                (
                    RelId(id),
                    RelRecord {
                        id: RelId(id),
                        source: NodeId(id),
                        target: NodeId(id + 100),
                        rel_type: RelTypeId(0),
                        properties: properties([("weight", Value::Int((id % 4) as i64))]),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>()
            .into();

        let statistics = compute_statistics(&CowSegmentedMap::default(), &relationships, 1);

        assert_eq!(
            statistics
                .rel_property_distinct_counts
                .get(&(RelTypeId(0), "weight".to_string())),
            Some(&4)
        );
        assert_eq!(
            statistics
                .rel_property_histograms
                .get(&(RelTypeId(0), "weight".to_string()))
                .and_then(|values| values.first()),
            Some(&Value::Int(0))
        );
        assert_eq!(
            statistics
                .rel_property_histograms
                .get(&(RelTypeId(0), "weight".to_string()))
                .and_then(|values| values.last()),
            Some(&Value::Int(3))
        );
        assert_eq!(
            statistics
                .sampled_rel_property_histograms
                .get(&(RelTypeId(0), "weight".to_string())),
            Some(&false)
        );
    }

    #[test]
    fn recovered_property_statistics_drop_ineligible_and_incomplete_groups() {
        let mut catalog = Catalog::default();
        let label_id = catalog.get_or_create_label("Metric");
        let rel_type_id = catalog.get_or_create_rel_type("MEASURES");
        let node_table = catalog.get_or_create_table(TableKind::Node, "Metric");
        let relationship_table = catalog.get_or_create_table(TableKind::Relationship, "MEASURES");
        catalog.get_or_create_property(node_table, "body", PropertyType::Text, true);
        catalog.get_or_create_property(relationship_table, "note", PropertyType::Text, true);
        let compact_node = (label_id, "score".to_string());
        let text_node = (label_id, "body".to_string());
        let incomplete_node = (label_id, "missing_marker".to_string());
        let empty_node = (label_id, "empty_histogram".to_string());
        let compact_relationship = (rel_type_id, "weight".to_string());
        let text_relationship = (rel_type_id, "note".to_string());
        let mut statistics = GraphStatistics {
            property_distinct_counts: BTreeMap::from([
                (compact_node.clone(), 2),
                (text_node.clone(), 1),
                (incomplete_node.clone(), 1),
                (empty_node.clone(), 0),
            ]),
            property_histograms: BTreeMap::from([
                (compact_node.clone(), vec![Value::Int(1), Value::Int(2)]),
                (
                    text_node.clone(),
                    vec![Value::String("payload".to_string())],
                ),
                (incomplete_node, vec![Value::Int(3)]),
                (empty_node.clone(), Vec::new()),
            ]),
            sampled_property_histograms: BTreeMap::from([
                (compact_node.clone(), false),
                (text_node, false),
                (empty_node, false),
            ]),
            rel_property_distinct_counts: BTreeMap::from([
                (compact_relationship.clone(), 1),
                (text_relationship.clone(), 1),
            ]),
            rel_property_histograms: BTreeMap::from([
                (compact_relationship.clone(), vec![Value::Float(0.5)]),
                (
                    text_relationship.clone(),
                    vec![Value::String("payload".to_string())],
                ),
            ]),
            sampled_rel_property_histograms: BTreeMap::from([
                (compact_relationship.clone(), false),
                (text_relationship, false),
            ]),
            ..GraphStatistics::default()
        };

        retain_supported_property_statistics(&mut statistics, Some(&catalog));

        assert_eq!(
            statistics.property_distinct_counts,
            BTreeMap::from([(compact_node.clone(), 2)])
        );
        assert_eq!(
            statistics
                .property_histograms
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec![compact_node.clone()]
        );
        assert_eq!(
            statistics
                .sampled_property_histograms
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec![compact_node]
        );
        assert_eq!(
            statistics.rel_property_distinct_counts,
            BTreeMap::from([(compact_relationship.clone(), 1)])
        );
        assert_eq!(
            statistics
                .rel_property_histograms
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec![compact_relationship]
        );
    }

    #[test]
    fn statistics_track_path_source_and_target_coverage() {
        let nodes = [
            (0, LabelId(0)),
            (1, LabelId(0)),
            (10, LabelId(1)),
            (11, LabelId(1)),
            (12, LabelId(1)),
            (20, LabelId(1)),
            (21, LabelId(1)),
        ]
        .into_iter()
        .map(|(id, label)| {
            (
                NodeId(id),
                NodeRecord {
                    id: NodeId(id),
                    labels: BTreeSet::from([label]),
                    properties: BTreeMap::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
        .into();
        let relationships = [
            (0, 0, 10),
            (1, 0, 11),
            (2, 1, 11),
            (3, 1, 12),
            (4, 1, 12),
            (5, 10, 20),
            (6, 11, 20),
            (7, 12, 21),
        ]
        .into_iter()
        .map(|(id, source, target)| {
            (
                RelId(id),
                RelRecord {
                    id: RelId(id),
                    source: NodeId(source),
                    target: NodeId(target),
                    rel_type: RelTypeId(0),
                    properties: BTreeMap::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
        .into();

        let statistics = compute_statistics(&nodes, &relationships, 1);
        let path = (LabelId(0), RelTypeId(0), LabelId(1));

        assert_eq!(statistics.path_counts.get(&path), Some(&5));
        assert_eq!(statistics.path_source_distinct_counts.get(&path), Some(&2));
        assert_eq!(statistics.path_target_distinct_counts.get(&path), Some(&3));

        let two_hop_path = (LabelId(0), RelTypeId(0), LabelId(1), 2);
        assert_eq!(statistics.bounded_path_counts.get(&two_hop_path), Some(&5));
        assert_eq!(
            statistics
                .bounded_path_source_distinct_counts
                .get(&two_hop_path),
            Some(&2)
        );
        assert_eq!(
            statistics
                .bounded_path_target_distinct_counts
                .get(&two_hop_path),
            Some(&2)
        );
    }

    #[test]
    fn adaptive_histograms_track_relationship_sampling() {
        let relationships = (0..2_000)
            .map(|id| {
                (
                    RelId(id),
                    RelRecord {
                        id: RelId(id),
                        source: NodeId(id),
                        target: NodeId(id + 100),
                        rel_type: RelTypeId(0),
                        properties: properties([("score", Value::Int(id as i64))]),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>()
            .into();

        let statistics = compute_statistics(&CowSegmentedMap::default(), &relationships, 1);
        let histogram = statistics
            .rel_property_histograms
            .get(&(RelTypeId(0), "score".to_string()))
            .unwrap();

        assert_eq!(histogram.len(), 256);
        assert_eq!(histogram.first(), Some(&Value::Int(0)));
        assert_eq!(histogram.last(), Some(&Value::Int(1_999)));
        assert_eq!(
            statistics
                .sampled_rel_property_histograms
                .get(&(RelTypeId(0), "score".to_string())),
            Some(&true)
        );
    }

    fn properties<const N: usize>(entries: [(&str, Value); N]) -> BTreeMap<String, Value> {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    fn histogram_nodes(property: &str, count: u64) -> CowSegmentedMap<NodeId, NodeRecord> {
        (0..count)
            .map(|id| {
                (
                    NodeId(id),
                    NodeRecord {
                        id: NodeId(id),
                        labels: BTreeSet::from([LabelId(0)]),
                        properties: properties([(property, Value::Int(id as i64))]),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>()
            .into()
    }

    fn unique_test_dir(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_store_{name}_{nanos}"))
    }

    fn active_wal_path(path: impl AsRef<std::path::Path>) -> std::path::PathBuf {
        active_generation_path(path.as_ref(), "wal_generation", "wal")
    }

    fn active_checkpoint_path(path: impl AsRef<std::path::Path>) -> std::path::PathBuf {
        active_generation_path(path.as_ref(), "checkpoint_generation", "checkpoint")
    }

    fn read_test_wal(path: impl AsRef<std::path::Path>) -> std::io::Result<String> {
        super::render_wal_records_for_test(&active_wal_path(path))
    }

    fn active_generation_path(
        root: &std::path::Path,
        manifest_field: &str,
        prefix: &str,
    ) -> std::path::PathBuf {
        let manifest = std::fs::read_to_string(root.join("manifest.skein")).unwrap();
        assert!(manifest.contains("SKEIN_MANIFEST_V1\n"));
        let generation = manifest.lines().find_map(|line| {
            let (field, value) = line.split_once('\t')?;
            (field == manifest_field && value != "none").then_some(value)
        });
        root.join(format!(
            "{prefix}.{}.skein",
            generation.expect("active generation must exist")
        ))
    }

    fn rewrite_checksummed_file(path: &std::path::Path, from: &str, to: &str, kind: &str) {
        let was_compressed = std::fs::read(path)
            .unwrap()
            .starts_with(DURABLE_COMPRESSION_HEADER.as_bytes());
        let text = if kind == "manifest" {
            std::fs::read_to_string(path).unwrap()
        } else {
            read_durable_text(path, kind).unwrap()
        };
        let (body, _) = text.rsplit_once("checksum\t").unwrap();
        let body = body.replace(from, to);
        let checksum = checksum_bytes(body.as_bytes());
        let rewritten = format!("{body}checksum\t{checksum}\n");
        if was_compressed {
            std::fs::write(
                path,
                encode_durable_text(&rewritten, DurableCompression::default()).unwrap(),
            )
            .unwrap();
        } else {
            std::fs::write(path, rewritten.as_bytes()).unwrap();
        }
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("checkpoint."))
        {
            refresh_manifest_checkpoint_metadata(path);
        }
        let rewritten = if kind == "manifest" {
            std::fs::read_to_string(path).unwrap()
        } else {
            read_durable_text(path, kind).unwrap()
        };
        assert!(
            rewritten.contains(to),
            "{kind} rewrite did not update storage version"
        );
    }

    fn refresh_manifest_checkpoint_metadata(checkpoint_path: &std::path::Path) {
        let root = checkpoint_path.parent().unwrap();
        let manifest_path = root.join("manifest.skein");
        let manifest = std::fs::read_to_string(&manifest_path).unwrap();
        let (body, _) = manifest.rsplit_once("checksum\t").unwrap();
        let checkpoint = std::fs::read(checkpoint_path).unwrap();
        let encoded_len = checkpoint.len() as u64;
        let integrity = integrity_digest(&checkpoint);
        let encoded_checksum = integrity.crc32c.as_u64();
        let encoded_sha256 = integrity.sha256;
        let mut rewritten_body = String::new();
        for line in body.lines() {
            if line.starts_with("checkpoint_encoded_len\t") {
                rewritten_body.push_str(&format!("checkpoint_encoded_len\t{encoded_len}\n"));
            } else if line.starts_with("checkpoint_encoded_checksum\t") {
                rewritten_body.push_str(&format!(
                    "checkpoint_encoded_checksum\t{encoded_checksum}\n"
                ));
            } else if line.starts_with("checkpoint_encoded_sha256\t") {
                rewritten_body.push_str(&format!("checkpoint_encoded_sha256\t{encoded_sha256}\n"));
            } else {
                rewritten_body.push_str(line);
                rewritten_body.push('\n');
            }
        }
        let checksum = checksum_bytes(rewritten_body.as_bytes());
        std::fs::write(
            manifest_path,
            format!("{rewritten_body}checksum\t{checksum}\n"),
        )
        .unwrap();
    }
}
