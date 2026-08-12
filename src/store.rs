use crate::analytics::ProjectedGraph;
use crate::error::{Result, SkeinError};
use crate::schema::{
    BasicGraphStatistics, Catalog, ConstraintId, GraphStatistics, IndexId, IndexKind, LabelId,
    PropertyId, PropertyType, RelTypeId, SchemaObjectState, TableDescriptor, TableId, TableKind,
};
use crate::search::{
    search_projection_document_id_for_label_and_properties, search_projection_document_id_for_node,
};
use crate::telemetry::TelemetrySink;
use crate::value::Value;
use skein_core::RuntimeTaskContext;
use skein_integrity::{checksum_u64, integrity_digest, Sha256Digest};
#[path = "store/artifact_files.rs"]
mod artifact_files;
#[path = "store/backup.rs"]
mod backup;
#[path = "store/cow.rs"]
mod cow;
#[path = "store/derived_repair.rs"]
mod derived_repair;
#[path = "store/doctor.rs"]
mod doctor;
#[path = "store/durable.rs"]
mod durable;
#[path = "store/read_view.rs"]
mod read_view;
#[path = "store/source_scan.rs"]
mod source_scan;
#[path = "store/statistics_refresh.rs"]
mod statistics_refresh;
#[path = "store/wal_codec.rs"]
mod wal_codec;
use artifact_files::{
    canonical_adjacency_artifact_generation_file, canonical_adjacency_manifest_generation_file,
    canonical_artifact_generation_file, canonical_manifest_generation_file,
    checkpoint_generation_file, cleanup_abandoned_checkpoint_preparations, has_storage_artifacts,
    parse_canonical_adjacency_manifest_generation_file, parse_canonical_manifest_generation_file,
    parse_generation_file, parse_property_projection_manifest_generation_file,
    parse_property_spill_manifest_generation_file, property_projection_artifact_generation_file,
    property_projection_manifest_generation_file, property_spill_artifact_generation_file,
    property_spill_manifest_generation_file, relational_checkpoint_generation_file,
    storage_generation_for_file, store_id_for_path, wal_generation_file,
};
pub use backup::restore_storage_backup;
use backup::{
    copy_backup_file, copy_file_with_checksum, file_checksum, remove_source_scan_artifacts,
    validate_backup_files, validate_new_backup_destination,
};
#[cfg(test)]
use cow::COW_MAP_TARGET_SEGMENT_BYTES;
use cow::{CowSegment, CowSegmentedMap};
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
    load_published_property_projection, BackupFileEntry, BackupManifest, CheckpointImage,
    CheckpointManifestArtifacts, DerivedArtifactBuildConfig, DurableArtifactMetadata,
    DurableManifest, DurableOpenMode, DurableStore,
};
pub use read_view::PublishedReadView;
use skein_storage::{
    available_storage_space, decode_relational_checkpoint, decode_relational_checkpoint_file,
    decode_relational_wal_batch, encode_relational_checkpoint, encode_relational_wal_batch,
    sync_parent_directory, AdjacencyPostingList, CanonicalEndpointDirection, CanonicalNodeIterator,
    CanonicalRelationshipIterator, CanonicalSegmentError, RelationalDecodeLimits,
    RelationalMutationLimits, RelationalOverflowConfig, RelationalState, RelationalTransaction,
};
pub use skein_storage::{
    AdjacencyDirection, AdjacencyGroupConsistencyMismatch, AdjacencyGroupKey, AdjacencyGroupStats,
    AdjacencyLayout, CanonicalAdjacencyBuildReport, CanonicalAdjacencyConfig,
    CanonicalAdjacencyEntry, CanonicalAdjacencyManifest, CanonicalAdjacencyReadReport,
    CanonicalAdjacencyReader, CanonicalAdjacencyWriter, CanonicalScanControl,
    CanonicalSegmentConfig, CanonicalSegmentManifest, CanonicalSegmentReader,
    CanonicalSegmentWriter, ConnectedNodesCreate, DurabilityPolicy, DurableCompression,
    FileSegmentRangeReader, GraphMutation, ManifestGeneration, MatchedRelationshipCopyMerge,
    MatchedRelationshipCreate, MatchedRelationshipMerge, MatchedRelationshipRetargetMerge,
    MatchedRelationshipSourceRetargetMerge, MutationLimits, NodeId, NodeRecord, NodeSetAssignment,
    NodeSetValue, OrderedAdjacencyEntry, PersistentPropertyProjectionConfig,
    PersistentPropertyProjectionDefinition, PersistentPropertyProjectionError,
    PersistentPropertyProjectionKind, PersistentPropertyProjectionManifest,
    PersistentPropertyProjectionReader, PersistentPropertyProjectionWriter,
    ProjectedGraphDefinition, ProjectedGraphStatus, PropertyFilter,
    PropertyIndexProjectionRebuildAction, PropertySpillConfig, PropertySpillManifest,
    PropertySpillReader, RecoveryMode, RelId, RelRecord, RelationshipDeleteRequest,
    RelationshipOnCreatePropertyValue, RelationshipPropertiesUpdate, RelationshipPropertyUpdate,
    RelationshipSetAssignment, RelationshipTargetNodeDelete, ScanPredicate, ScanPruningReport,
    ScanPruningStrategy, ScanPruningTargetKind, ScanSegmentAccessPlan, ScanSegmentFallback,
    ScanSegmentManifest, SchemaMaintenanceAction, SchemaMaintenancePlanItem,
    SearchProjectionChangefeedReadiness, SearchProjectionChangefeedStatus,
    SearchProjectionGraphChange, SearchProjectionMutationId, SegmentCache, SegmentCacheSnapshot,
    SegmentRangeReader, SegmentReadError, SegmentReadExecutionError, SegmentReadExecutionReport,
    SegmentReadExecutor, SegmentReadPayload, SegmentReadRange, SegmentReadSchedule,
    SegmentReadScheduler, SegmentReadWave, StorageBackupReport, StorageDebtController,
    StoragePressureReasonCode, StoragePressureSignals, StoragePressureSnapshot,
    StoragePressureState, StorageReclamationWatermark, StorageRecoveryReport, StorageResidencyMode,
    StorageRestoreReport, StorageScrubReport, StoreId, StoreStableIdMapping, WalReplayConfig,
    STORAGE_PRESSURE_DELAY_RATIO_PER_MILLION, STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION,
};
pub(crate) use skein_storage::{WalSyncGroupFlush, WalSyncGroupProgress};
pub use source_scan::SourceScanRow;
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
    encode_wal_header, quarantine_corrupt_wal, reject_corrupt_wal_record, sniff_wal_format,
    WalCursorEvent, WalEntry, WalFileFormat, WalOp, WalOpenOutcome, WalRecordCursor,
};

const STORAGE_VERSION: &str = "skein-storage-v1";
const MANIFEST_FILE: &str = "manifest.skein";
const PROJECTED_GRAPHS_FILE: &str = "projected_graphs.skein";
const STABLE_ID_MAPPING_FILE: &str = "stable_ids.skein";
const RELATIONAL_CHECKPOINT_FILE_PREFIX: &str = "relational";
const PROJECTED_GRAPH_ARTIFACT_VERSION: u64 = 1;
const CHECKPOINT_HEADER_V1: &str = "SKEIN_CHECKPOINT_V1";
const MANIFEST_HEADER_V1: &str = "SKEIN_MANIFEST_V1";
const WAL_HEADER_V1: &str = "SKEIN_WAL_V1";
const BACKUP_MANIFEST_FILE: &str = "backup.skein";
const BACKUP_HEADER_V1: &str = "SKEIN_BACKUP_V1";
const CANONICAL_MANIFEST_MAX_BYTES: u64 = 256 * 1024 * 1024;
const CANONICAL_ADJACENCY_MANIFEST_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const PROPERTY_SPILL_MANIFEST_MAX_BYTES: u64 = 256 * 1024 * 1024;
const PROPERTY_PROJECTION_MANIFEST_MAX_BYTES: u64 = 1024 * 1024 * 1024;
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

/// Test support: renders a WAL file (either format) as its canonical V1
/// text record lines, one encoded record per line, header excluded. A torn
/// tail ends the rendering; corruption renders a terminal marker line so
/// identity comparisons on damaged files stay deterministic.
#[cfg(test)]
pub(crate) fn decode_wal_records_as_v1_text(path: &Path) -> std::io::Result<String> {
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
                out.push_str(&entry.encode());
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

/// Test support: rewrites a cleanly decodable WAL file into the V1 text
/// encoding with the same generation header and records. Exercises the
/// text-to-binary upgrade path that real databases cross at checkpoint.
#[cfg(test)]
pub(crate) fn rewrite_wal_as_v1_text(path: &Path) -> Result<()> {
    let mut cursor = match WalRecordCursor::open(path, None)? {
        WalOpenOutcome::Cursor(cursor) => cursor,
        _ => {
            return Err(SkeinError::Storage(
                "cannot rewrite a WAL without a valid header".to_string(),
            ));
        }
    };
    let mut text = encode_wal_header(cursor.generation(), cursor.start_lsn());
    text.push('\n');
    loop {
        match cursor.next()? {
            WalCursorEvent::Entry { entry, .. } => {
                text.push_str(&entry.encode());
                text.push('\n');
            }
            WalCursorEvent::Eof => break,
            WalCursorEvent::TornTail { .. } | WalCursorEvent::Corrupt { .. } => {
                return Err(SkeinError::Storage(
                    "cannot rewrite a damaged WAL as V1 text".to_string(),
                ));
            }
        }
    }
    // Drop the cursor's handle on this exact path before truncating it, and
    // sync on the write handle itself: Windows FlushFileBuffers denies a
    // read-only handle, which POSIX fsync happily accepts.
    drop(cursor);
    let mut file = File::create(path)?;
    std::io::Write::write_all(&mut file, text.as_bytes())?;
    file.sync_all()?;
    Ok(())
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationSummary {
    pub rows: Vec<BTreeMap<String, Value>>,
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

#[derive(Debug, Clone, PartialEq)]
struct ProjectedGraphArtifact {
    projection_epoch: u64,
    commit_epoch: u64,
    definition: ProjectedGraphDefinition,
    graph: ProjectedGraph,
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

#[derive(Debug, Default)]
pub struct GraphStore {
    next_node_id: u64,
    next_rel_id: u64,
    commit_epoch: u64,
    nodes: CowSegmentedMap<NodeId, NodeRecord>,
    relationships: CowSegmentedMap<RelId, RelRecord>,
    basic_statistics: BasicGraphStatistics,
    checkpoint_statistics: GraphStatistics,
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
    max_search_projection_change_log_entries: Option<usize>,
    source_scan_manifest: CowSegment<Option<ScanSegmentManifest>>,
    storage_recovery_report: StorageRecoveryReport,
    canonical_base: Option<CanonicalSegmentReader>,
    canonical_adjacency: Option<CanonicalAdjacencyReader>,
    persistent_property_projection: Option<PersistentPropertyProjectionReader>,
    canonical_base_out_of_core: bool,
    node_tombstones: CowSegment<BTreeSet<NodeId>>,
    relationship_tombstones: CowSegment<BTreeSet<RelId>>,
    residency_mode: StorageResidencyMode,
    auto_materialize_checkpoint_bytes: u64,
    max_out_of_core_delta_bytes: Option<u64>,
    post_wal_apply_poisoned: bool,
    integrity_poisoned: Arc<AtomicBool>,
    relational_state: RelationalState,
    relational_mutation_limits: RelationalMutationLimits,
    relational_overflow_config: RelationalOverflowConfig,
    durable: Option<DurableStore>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphScanControl {
    Continue,
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageResidencyReport {
    pub out_of_core: bool,
    pub canonical_generation: Option<u64>,
    pub canonical_artifact_bytes: u64,
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
    pub segment_cache_capacity_bytes: u64,
    pub segment_cache_resident_bytes: u64,
    pub segment_cache_pinned_bytes: u64,
    pub segment_cache_hit_count: u64,
    pub segment_cache_miss_count: u64,
    pub segment_cache_eviction_count: u64,
    pub segment_cache_admission_rejection_count: u64,
    pub segment_cache_digest_mismatch_count: u64,
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
            None,
            limits,
            false,
            Some(&mut captured_ops),
        )?;
        self.ops.extend(captured_ops);
        if retain_commit_rows {
            self.rows.extend(summary.rows.iter().cloned());
        }
        Ok(summary)
    }
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
        Ok(())
    }

    pub fn post_wal_apply_poisoned(&self) -> bool {
        self.post_wal_apply_poisoned
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
        let durable = match mode {
            DurableOpenMode::CreateIfMissing => DurableStore::open(
                path.as_ref(),
                durability,
                replay_config.segment_cache_capacity_bytes,
                replay_config.max_bytes,
                replay_config.max_record_bytes,
                replay_config.max_batch_operations,
            )?,
            DurableOpenMode::ExistingOnly => DurableStore::open_existing_only(
                path.as_ref(),
                durability,
                replay_config.segment_cache_capacity_bytes,
                replay_config.max_bytes,
                replay_config.max_record_bytes,
                replay_config.max_batch_operations,
            )?,
        };
        Self::finish_open(durable, catalog, replay_config).map(|(store, _)| store)
    }

    fn finish_open(
        durable: DurableStore,
        catalog: &mut Catalog,
        replay_config: WalReplayConfig,
    ) -> Result<(Self, Catalog)> {
        let mut store = Self {
            next_node_id: 0,
            next_rel_id: 0,
            commit_epoch: 0,
            nodes: CowSegmentedMap::default(),
            relationships: CowSegmentedMap::default(),
            basic_statistics: BasicGraphStatistics::default(),
            checkpoint_statistics: GraphStatistics::default(),
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
            max_search_projection_change_log_entries: None,
            source_scan_manifest: CowSegment::default(),
            storage_recovery_report: StorageRecoveryReport::default(),
            canonical_base: None,
            canonical_adjacency: None,
            persistent_property_projection: None,
            canonical_base_out_of_core: false,
            node_tombstones: CowSegment::default(),
            relationship_tombstones: CowSegment::default(),
            residency_mode: replay_config.residency_mode,
            auto_materialize_checkpoint_bytes: replay_config.auto_materialize_checkpoint_bytes,
            max_out_of_core_delta_bytes: replay_config.max_out_of_core_delta_bytes,
            post_wal_apply_poisoned: false,
            integrity_poisoned: Arc::new(AtomicBool::new(false)),
            relational_state: RelationalState::default(),
            relational_mutation_limits: RelationalMutationLimits::default(),
            relational_overflow_config: RelationalOverflowConfig::default(),
            durable: Some(durable),
        };
        store.load_checkpoint(catalog, replay_config)?;
        let checkpoint_catalog = catalog.clone();
        store.storage_recovery_report = store.replay_wal(catalog, replay_config)?;
        store.validate_relationship_endpoints()?;
        store.refresh_basic_statistics_epoch();
        store.load_projected_graph_artifacts()?;
        store.load_stable_id_mapping()?;
        store.load_source_scan_manifest()?;
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
        let durable = DurableStore::open_for_derived_repair(
            path,
            DurabilityPolicy::default(),
            replay_config.segment_cache_capacity_bytes,
            replay_config.max_bytes,
            replay_config.max_record_bytes,
            replay_config.max_batch_operations,
        )?;
        let mut recovered_catalog = Catalog::default();
        let (store, checkpoint_catalog) =
            Self::finish_open(durable, &mut recovered_catalog, replay_config)?;
        Ok((store, recovered_catalog, checkpoint_catalog))
    }

    fn enable_derived_repair_writes(&mut self) -> Result<()> {
        let durable = self.durable.as_mut().ok_or_else(|| {
            SkeinError::Storage("derived repair requires durable storage".to_string())
        })?;
        durable.read_only = false;
        Ok(())
    }

    pub fn create_node(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        properties: BTreeMap<String, Value>,
    ) -> Result<NodeId> {
        let label_id = catalog.get_or_create_label(label);
        let id = NodeId(self.next_node_id);
        let ops = [WalOp::CreateNode {
            id,
            label: label.to_string(),
            properties: properties.clone(),
        }];
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_create_node(id, label, &properties)?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        self.apply_create_node(catalog, id, label_id, properties);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_node_label(&mut self, catalog: &mut Catalog, label: &str) -> Result<LabelId> {
        if let Some(id) = catalog.label_id(label) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateNodeLabel {
                label: label.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_label(label);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_relationship_type(
        &mut self,
        catalog: &mut Catalog,
        rel_type: &str,
    ) -> Result<RelTypeId> {
        if let Some(id) = catalog.rel_type_id(rel_type) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateRelationshipType {
                rel_type: rel_type.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_rel_type(rel_type);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_node_table(&mut self, catalog: &mut Catalog, name: &str) -> Result<TableId> {
        if let Some(id) = catalog.table_id(TableKind::Node, name) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateNodeTable {
                name: name.to_string(),
            }])?;
        }
        catalog.get_or_create_label(name);
        let id = catalog.get_or_create_table(TableKind::Node, name);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_relationship_table(
        &mut self,
        catalog: &mut Catalog,
        name: &str,
    ) -> Result<TableId> {
        if let Some(id) = catalog.table_id(TableKind::Relationship, name) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateRelationshipTable {
                name: name.to_string(),
            }])?;
        }
        catalog.get_or_create_rel_type(name);
        let id = catalog.get_or_create_table(TableKind::Relationship, name);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_property_descriptor(
        &mut self,
        catalog: &mut Catalog,
        table_kind: TableKind,
        table: &str,
        property: &str,
        value_type: PropertyType,
        nullable: bool,
    ) -> Result<PropertyId> {
        let table_id = ensure_table_descriptor(catalog, table_kind, table);
        if let Some(id) = catalog.property_descriptor_id(table_id, property) {
            return Ok(id);
        }
        validate_property_descriptor(catalog, self, table_id, property, value_type, nullable)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateProperty {
                table_kind,
                table: table.to_string(),
                property: property.to_string(),
                value_type,
                nullable,
            }])?;
        }
        let id = catalog.get_or_create_property(table_id, property, value_type, nullable);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn alter_table_state(
        &mut self,
        catalog: &mut Catalog,
        table_kind: TableKind,
        table: &str,
        state: SchemaObjectState,
    ) -> Result<(TableId, bool)> {
        let Some(id) = catalog.table_id(table_kind, table) else {
            return Err(SkeinError::Storage(format!(
                "schema table '{table}' does not exist"
            )));
        };
        let Some(descriptor) = catalog.table_descriptor(id) else {
            return Err(SkeinError::Storage(format!(
                "schema table '{table}' does not exist"
            )));
        };
        if descriptor.state == state {
            return Ok((id, false));
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::AlterTableState {
                table_kind,
                table: table.to_string(),
                state,
            }])?;
        }
        catalog.set_table_state(id, state);
        self.commit_epoch += 1;
        Ok((id, true))
    }

    pub fn alter_property_state(
        &mut self,
        catalog: &mut Catalog,
        table_kind: TableKind,
        table: &str,
        property: &str,
        state: SchemaObjectState,
    ) -> Result<(PropertyId, bool)> {
        let Some(table_id) = catalog.table_id(table_kind, table) else {
            return Err(SkeinError::Storage(format!(
                "schema table '{table}' does not exist"
            )));
        };
        let Some(id) = catalog.property_descriptor_id(table_id, property) else {
            return Err(SkeinError::Storage(format!(
                "schema property '{table}.{property}' does not exist"
            )));
        };
        let Some(descriptor) = catalog.property_descriptor(id) else {
            return Err(SkeinError::Storage(format!(
                "schema property '{table}.{property}' does not exist"
            )));
        };
        if descriptor.state == state {
            return Ok((id, false));
        }
        if state == SchemaObjectState::Public {
            validate_property_descriptor(
                catalog,
                self,
                table_id,
                property,
                descriptor.value_type,
                descriptor.nullable,
            )?;
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::AlterPropertyState {
                table_kind,
                table: table.to_string(),
                property: property.to_string(),
                state,
            }])?;
        }
        catalog.set_property_state(id, state);
        self.commit_epoch += 1;
        Ok((id, true))
    }

    pub fn run_schema_maintenance(
        &mut self,
        catalog: &mut Catalog,
    ) -> Result<Vec<SchemaMaintenanceAction>> {
        self.run_schema_maintenance_with_budget(catalog, None)
    }

    pub fn run_bounded_schema_maintenance(
        &mut self,
        catalog: &mut Catalog,
        max_estimated_operations: usize,
    ) -> Result<Vec<SchemaMaintenanceAction>> {
        self.run_schema_maintenance_with_budget(catalog, Some(max_estimated_operations))
    }

    fn run_schema_maintenance_with_budget(
        &mut self,
        catalog: &mut Catalog,
        max_estimated_operations: Option<usize>,
    ) -> Result<Vec<SchemaMaintenanceAction>> {
        let mut ops = Vec::new();
        let mut actions = Vec::new();
        let mut used_estimated_operations = 0usize;

        let gc_table_ids = catalog
            .table_descriptors()
            .filter(|table| table.state == SchemaObjectState::Gc)
            .map(|table| table.id)
            .collect::<BTreeSet<_>>();

        for property in catalog.property_descriptors().cloned().collect::<Vec<_>>() {
            if gc_table_ids.contains(&property.table_id) {
                continue;
            }
            let Some(table) = catalog.table_descriptor(property.table_id).cloned() else {
                continue;
            };
            match property.state {
                SchemaObjectState::Backfill => {
                    let estimated_operations =
                        self.schema_table_record_count(catalog, &table).max(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    validate_property_descriptor(
                        catalog,
                        self,
                        property.table_id,
                        &property.name,
                        property.value_type,
                        property.nullable,
                    )?;
                    ops.push(WalOp::AlterPropertyState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        property: property.name.clone(),
                        state: SchemaObjectState::Validating,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Validating => {
                    let estimated_operations =
                        self.schema_table_record_count(catalog, &table).max(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    validate_property_descriptor(
                        catalog,
                        self,
                        property.table_id,
                        &property.name,
                        property.value_type,
                        property.nullable,
                    )?;
                    ops.push(WalOp::AlterPropertyState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        property: property.name.clone(),
                        state: SchemaObjectState::Public,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Gc => {
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        1,
                    ) {
                        continue;
                    }
                    ops.push(WalOp::GcPropertyDescriptor {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        property: property.name.clone(),
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: None,
                        action: "gc".to_string(),
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        for table in catalog.table_descriptors().cloned().collect::<Vec<_>>() {
            match table.state {
                SchemaObjectState::Backfill => {
                    let estimated_operations =
                        self.schema_table_record_count(catalog, &table).max(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    ops.push(WalOp::AlterTableState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        state: SchemaObjectState::Validating,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Validating => {
                    let active_property_count = catalog
                        .property_descriptors()
                        .filter(|property| {
                            property.table_id == table.id && property.state != SchemaObjectState::Gc
                        })
                        .count()
                        .max(1);
                    let estimated_operations = self
                        .schema_table_record_count(catalog, &table)
                        .max(1)
                        .saturating_mul(active_property_count);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    validate_table_descriptor(catalog, self, table.id)?;
                    ops.push(WalOp::AlterTableState {
                        table_kind: table.kind,
                        table: table.name.clone(),
                        state: SchemaObjectState::Public,
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                    });
                }
                SchemaObjectState::Gc => {
                    let properties = catalog
                        .property_descriptors()
                        .filter(|property| property.table_id == table.id)
                        .cloned()
                        .collect::<Vec<_>>();
                    let estimated_operations = properties.len().saturating_add(1);
                    if !reserve_schema_maintenance_budget(
                        &mut used_estimated_operations,
                        max_estimated_operations,
                        estimated_operations,
                    ) {
                        continue;
                    }
                    for property in properties {
                        ops.push(WalOp::GcPropertyDescriptor {
                            table_kind: table.kind,
                            table: table.name.clone(),
                            property: property.name.clone(),
                        });
                        actions.push(SchemaMaintenanceAction {
                            object_type: "property".to_string(),
                            object: format!("{}.{}", table.name, property.name),
                            from_state: property.state,
                            to_state: None,
                            action: "gc".to_string(),
                        });
                    }
                    ops.push(WalOp::GcTableDescriptor {
                        table_kind: table.kind,
                        table: table.name.clone(),
                    });
                    actions.push(SchemaMaintenanceAction {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: None,
                        action: "gc".to_string(),
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        if ops.is_empty() {
            return Ok(actions);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_schema_maintenance_op(catalog, op);
        }
        self.commit_epoch += 1;
        Ok(actions)
    }

    pub fn plan_schema_maintenance(&self, catalog: &Catalog) -> Vec<SchemaMaintenancePlanItem> {
        let mut plan = Vec::new();

        let gc_table_ids = catalog
            .table_descriptors()
            .filter(|table| table.state == SchemaObjectState::Gc)
            .map(|table| table.id)
            .collect::<BTreeSet<_>>();

        for property in catalog.property_descriptors().cloned() {
            if gc_table_ids.contains(&property.table_id) {
                continue;
            }
            let Some(table) = catalog.table_descriptor(property.table_id) else {
                continue;
            };
            let estimated_operations = self.schema_table_record_count(catalog, table).max(1);
            match property.state {
                SchemaObjectState::Backfill => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                        estimated_operations,
                    });
                }
                SchemaObjectState::Validating => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                        estimated_operations,
                    });
                }
                SchemaObjectState::Gc => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "property".to_string(),
                        object: format!("{}.{}", table.name, property.name),
                        from_state: property.state,
                        to_state: None,
                        action: "gc".to_string(),
                        estimated_operations: 1,
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        for table in catalog.table_descriptors().cloned() {
            let record_count = self.schema_table_record_count(catalog, &table).max(1);
            match table.state {
                SchemaObjectState::Backfill => {
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Validating),
                        action: "advance".to_string(),
                        estimated_operations: record_count,
                    });
                }
                SchemaObjectState::Validating => {
                    let active_property_count = catalog
                        .property_descriptors()
                        .filter(|property| {
                            property.table_id == table.id && property.state != SchemaObjectState::Gc
                        })
                        .count()
                        .max(1);
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: Some(SchemaObjectState::Public),
                        action: "advance".to_string(),
                        estimated_operations: record_count.saturating_mul(active_property_count),
                    });
                }
                SchemaObjectState::Gc => {
                    for property in catalog
                        .property_descriptors()
                        .filter(|property| property.table_id == table.id)
                    {
                        plan.push(SchemaMaintenancePlanItem {
                            object_type: "property".to_string(),
                            object: format!("{}.{}", table.name, property.name),
                            from_state: property.state,
                            to_state: None,
                            action: "gc".to_string(),
                            estimated_operations: 1,
                        });
                    }
                    plan.push(SchemaMaintenancePlanItem {
                        object_type: "table".to_string(),
                        object: table.name.clone(),
                        from_state: table.state,
                        to_state: None,
                        action: "gc".to_string(),
                        estimated_operations: 1,
                    });
                }
                SchemaObjectState::DeleteOnly
                | SchemaObjectState::WriteOnly
                | SchemaObjectState::Public => {}
            }
        }

        plan
    }

    fn schema_table_record_count(&self, catalog: &Catalog, table: &TableDescriptor) -> usize {
        match table.kind {
            TableKind::Node => {
                let Some(label_id) = catalog.label_id(&table.name) else {
                    return 0;
                };
                self.index_label_record_count(label_id)
            }
            TableKind::Relationship => {
                let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
                    return 0;
                };
                self.relationships
                    .values()
                    .filter(|relationship| relationship.rel_type == rel_type_id)
                    .count()
            }
        }
    }

    fn index_label_record_count(&self, label_id: LabelId) -> usize {
        self.nodes
            .values()
            .filter(|node| node.labels.contains(&label_id))
            .count()
    }

    pub fn create_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<IndexId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.property_index_id(label_id, property) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateIndex {
                label: label.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_property_index(label_id, property);
        self.backfill_property_index(label_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_composite_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        properties: &[String],
    ) -> Result<IndexId> {
        if properties.len() < 2 {
            return Err(SkeinError::Storage(
                "composite index requires at least two properties".to_string(),
            ));
        }
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.composite_property_index_id(label_id, properties) {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateCompositeIndex {
                label: label.to_string(),
                properties: properties.to_vec(),
            }])?;
        }
        let id = catalog.get_or_create_composite_property_index(label_id, properties);
        self.rebuild_composite_property_index_for_descriptor(label_id, properties);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_range_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<IndexId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.property_index_id_with_kind(label_id, property, IndexKind::Range)
        {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateRangeIndex {
                label: label.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id =
            catalog.get_or_create_property_index_with_kind(label_id, property, IndexKind::Range);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_full_text_property_index(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<IndexId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) =
            catalog.property_index_id_with_kind(label_id, property, IndexKind::FullText)
        {
            return Ok(id);
        }
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateFullTextIndex {
                label: label.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id =
            catalog.get_or_create_property_index_with_kind(label_id, property, IndexKind::FullText);
        self.rebuild_full_text_property_index_for_descriptor(label_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn rebuild_bounded_property_index_projections(
        &mut self,
        catalog: &Catalog,
        max_estimated_operations: usize,
    ) -> Vec<PropertyIndexProjectionRebuildAction> {
        let mut actions = Vec::new();
        let mut used_estimated_operations = 0usize;

        for index in catalog
            .composite_property_indexes()
            .cloned()
            .collect::<Vec<_>>()
        {
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            if !reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            ) {
                continue;
            }
            let indexed_entries =
                self.rebuild_composite_property_index_projection(index.label_id, &index.properties);
            actions.push(PropertyIndexProjectionRebuildAction {
                index_kind: "composite".to_string(),
                label: catalog
                    .label_name(index.label_id)
                    .unwrap_or("<unknown>")
                    .to_string(),
                properties: index.properties,
                estimated_operations,
                indexed_entries,
            });
        }

        for index in catalog.property_indexes().cloned().collect::<Vec<_>>() {
            if index.kind != IndexKind::FullText {
                continue;
            }
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            if !reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            ) {
                continue;
            }
            let indexed_entries =
                self.rebuild_full_text_property_index_projection(index.label_id, &index.property);
            actions.push(PropertyIndexProjectionRebuildAction {
                index_kind: "full_text".to_string(),
                label: catalog
                    .label_name(index.label_id)
                    .unwrap_or("<unknown>")
                    .to_string(),
                properties: vec![index.property],
                estimated_operations,
                indexed_entries,
            });
        }

        actions
    }

    pub fn bounded_property_index_projection_estimated_operations(
        &self,
        catalog: &Catalog,
        max_estimated_operations: usize,
    ) -> usize {
        let mut used_estimated_operations = 0usize;

        for index in catalog.composite_property_indexes() {
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            let _ = reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            );
        }

        for index in catalog.property_indexes() {
            if index.kind != IndexKind::FullText {
                continue;
            }
            let estimated_operations = self.index_label_record_count(index.label_id).max(1);
            let _ = reserve_schema_maintenance_budget(
                &mut used_estimated_operations,
                Some(max_estimated_operations),
                estimated_operations,
            );
        }

        used_estimated_operations
    }

    pub fn property_index_projection_estimated_operations(&self, catalog: &Catalog) -> usize {
        let composite_operations = catalog
            .composite_property_indexes()
            .map(|index| self.index_label_record_count(index.label_id).max(1))
            .fold(0usize, usize::saturating_add);
        let full_text_operations = catalog
            .property_indexes()
            .filter(|index| index.kind == IndexKind::FullText)
            .map(|index| self.index_label_record_count(index.label_id).max(1))
            .fold(0usize, usize::saturating_add);
        composite_operations.saturating_add(full_text_operations)
    }

    pub fn create_unique_constraint(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.unique_constraint_id(label_id, property) {
            return Ok(id);
        }
        self.validate_unique_constraint(catalog, label_id, property)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateUniqueConstraint {
                label: label.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_unique_constraint(label_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_node_property_exists_constraint(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = catalog.node_property_exists_constraint_id(label_id, property) {
            return Ok(id);
        }
        self.validate_node_property_exists_constraint(catalog, label_id, property)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateNodePropertyExistsConstraint {
                label: label.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_node_property_exists_constraint(label_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_relationship_property_exists_constraint(
        &mut self,
        catalog: &mut Catalog,
        rel_type: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let rel_type_id = catalog.get_or_create_rel_type(rel_type);
        if let Some(id) = catalog.relationship_property_exists_constraint_id(rel_type_id, property)
        {
            return Ok(id);
        }
        self.validate_relationship_property_exists_constraint(catalog, rel_type_id, property)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateRelationshipPropertyExistsConstraint {
                rel_type: rel_type.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id =
            catalog.get_or_create_relationship_property_exists_constraint(rel_type_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn create_relationship_unique_constraint(
        &mut self,
        catalog: &mut Catalog,
        rel_type: &str,
        property: &str,
    ) -> Result<ConstraintId> {
        let rel_type_id = catalog.get_or_create_rel_type(rel_type);
        if let Some(id) = catalog.relationship_unique_constraint_id(rel_type_id, property) {
            return Ok(id);
        }
        self.validate_relationship_unique_constraint(catalog, rel_type_id, property)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(vec![WalOp::CreateRelationshipUniqueConstraint {
                rel_type: rel_type.to_string(),
                property: property.to_string(),
            }])?;
        }
        let id = catalog.get_or_create_relationship_unique_constraint(rel_type_id, property);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn merge_node(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
        on_match_assignments: &[NodeSetAssignment],
        post_merge_assignments: &[NodeSetAssignment],
    ) -> Result<(NodeId, bool)> {
        let label_id = catalog.get_or_create_label(label);
        if let Some(id) = self.find_node_by_label_and_properties(label_id, &match_properties)? {
            if !on_match_assignments.is_empty() || !post_merge_assignments.is_empty() {
                let mut assignments =
                    Vec::with_capacity(on_match_assignments.len() + post_merge_assignments.len());
                assignments.extend_from_slice(on_match_assignments);
                assignments.extend_from_slice(post_merge_assignments);
                let ops = self.node_set_property_ops(&[id], &assignments)?;
                self.validate_constraints_for_ops(catalog, &ops)?;
                if let Some(durable) = &mut self.durable {
                    durable.append_batch(ops.clone())?;
                }
                self.record_search_projection_graph_changes_for_ops(
                    catalog,
                    self.commit_epoch + 1,
                    &ops,
                );
                for op in ops {
                    self.apply_wal_op(catalog, op)?;
                }
                self.commit_epoch += 1;
            }
            return Ok((id, false));
        }
        let mut properties = match_properties;
        for (property, value) in on_create_properties {
            properties.insert(property, value);
        }
        apply_node_assignments_to_properties(&mut properties, post_merge_assignments)?;
        let id = NodeId(self.next_node_id);
        let ops = [WalOp::CreateNode {
            id,
            label: label.to_string(),
            properties: properties.clone(),
        }];
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_create_node(id, label, &properties)?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        self.apply_create_node(catalog, id, label_id, properties);
        self.commit_epoch += 1;
        Ok((id, true))
    }

    pub fn create_relationship(
        &mut self,
        catalog: &mut Catalog,
        source: NodeId,
        target: NodeId,
        rel_type: &str,
        properties: BTreeMap<String, Value>,
    ) -> Result<RelId> {
        if self.node_owned(source)?.is_none() {
            return Err(SkeinError::Storage(format!(
                "source node {} does not exist",
                source.0
            )));
        }
        if self.node_owned(target)?.is_none() {
            return Err(SkeinError::Storage(format!(
                "target node {} does not exist",
                target.0
            )));
        }
        let rel_type_id = catalog.get_or_create_rel_type(rel_type);
        let id = RelId(self.next_rel_id);
        let ops = [WalOp::CreateRelationship {
            id,
            source,
            target,
            rel_type: rel_type.to_string(),
            properties: properties.clone(),
        }];
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_create_relationship(id, source, target, rel_type, &properties)?;
        }
        self.apply_create_relationship(id, source, target, rel_type_id, properties);
        self.commit_epoch += 1;
        Ok(id)
    }

    pub fn import_graph_snapshot_rows(
        &mut self,
        catalog: &mut Catalog,
        nodes: Vec<GraphSnapshotNodeImport>,
        relationships: Vec<GraphSnapshotRelationshipImport>,
    ) -> Result<()> {
        if self.basic_statistics.node_count != 0 || self.basic_statistics.relationship_count != 0 {
            return Err(SkeinError::Storage(
                "Skein Lightning initial import requires an empty target graph".to_string(),
            ));
        }
        let mut node_ids = BTreeSet::new();
        for (id, label, _) in &nodes {
            if label.is_empty() {
                return Err(SkeinError::Storage(
                    "Skein Lightning initial import node label is empty".to_string(),
                ));
            }
            if !node_ids.insert(*id) {
                return Err(SkeinError::Storage(format!(
                    "Skein Lightning initial import duplicate node id {}",
                    id.0
                )));
            }
        }
        let mut relationship_ids = BTreeSet::new();
        for (id, source, target, rel_type, _) in &relationships {
            if rel_type.is_empty() {
                return Err(SkeinError::Storage(
                    "Skein Lightning initial import relationship type is empty".to_string(),
                ));
            }
            if !relationship_ids.insert(*id) {
                return Err(SkeinError::Storage(format!(
                    "Skein Lightning initial import duplicate relationship id {}",
                    id.0
                )));
            }
            if !node_ids.contains(source) {
                return Err(SkeinError::Storage(format!(
                    "Skein Lightning initial import relationship {} references missing source node {}",
                    id.0, source.0
                )));
            }
            if !node_ids.contains(target) {
                return Err(SkeinError::Storage(format!(
                    "Skein Lightning initial import relationship {} references missing target node {}",
                    id.0, target.0
                )));
            }
        }
        if nodes.is_empty() && relationships.is_empty() {
            return Ok(());
        }

        let mut working_catalog = catalog.clone();
        for (_, label, _) in &nodes {
            working_catalog.get_or_create_label(label);
        }
        for (_, _, _, rel_type, _) in &relationships {
            working_catalog.get_or_create_rel_type(rel_type);
        }
        let mut ops = Vec::with_capacity(nodes.len() + relationships.len());
        ops.extend(
            nodes
                .iter()
                .map(|(id, label, properties)| WalOp::CreateNode {
                    id: *id,
                    label: label.clone(),
                    properties: properties.clone(),
                }),
        );
        ops.extend(
            relationships
                .iter()
                .map(
                    |(id, source, target, rel_type, properties)| WalOp::CreateRelationship {
                        id: *id,
                        source: *source,
                        target: *target,
                        rel_type: rel_type.clone(),
                        properties: properties.clone(),
                    },
                ),
        );
        self.validate_constraints_for_ops(&working_catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(
            &working_catalog,
            self.commit_epoch + 1,
            &ops,
        );
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok(())
    }

    pub(crate) fn import_skein_snapshot_rows_with_source_fingerprint(
        &mut self,
        catalog: &mut Catalog,
        import: SkeinSnapshotRowsImport,
    ) -> Result<()> {
        let SkeinSnapshotRowsImport {
            stable_id_mapping,
            source_fingerprint,
            nodes,
            relationships,
            relational_state,
            target_has_only_engine_bootstrap,
        } = import;
        if self.initial_import_source_fingerprint.is_some()
            || !self.nodes.is_empty()
            || !self.relationships.is_empty()
            || (!self.relational_state.is_empty() && !target_has_only_engine_bootstrap)
            || !catalog.is_empty()
        {
            return Err(SkeinError::Storage(
                "skein lightning initial import requires an empty target database".to_string(),
            ));
        }

        let mut node_ids = BTreeSet::new();
        for (id, label, _) in &nodes {
            if label.is_empty() {
                return Err(SkeinError::Storage(
                    "skein lightning initial import node label is empty".to_string(),
                ));
            }
            if !node_ids.insert(*id) {
                return Err(SkeinError::Storage(format!(
                    "skein lightning initial import duplicate node id {}",
                    id.0
                )));
            }
        }
        let mut relationship_ids = BTreeSet::new();
        for (id, source, target, rel_type, _) in &relationships {
            if rel_type.is_empty() {
                return Err(SkeinError::Storage(
                    "skein lightning initial import relationship type is empty".to_string(),
                ));
            }
            if !relationship_ids.insert(*id) {
                return Err(SkeinError::Storage(format!(
                    "skein lightning initial import duplicate relationship id {}",
                    id.0
                )));
            }
            if !node_ids.contains(source) || !node_ids.contains(target) {
                return Err(SkeinError::Storage(format!(
                    "skein lightning initial import relationship {} references a missing endpoint",
                    id.0
                )));
            }
        }

        let mut working_catalog = catalog.clone();
        for (_, label, _) in &nodes {
            working_catalog.get_or_create_label(label);
        }
        for (_, _, _, rel_type, _) in &relationships {
            working_catalog.get_or_create_rel_type(rel_type);
        }
        let relational_record =
            encode_relational_checkpoint(self.commit_epoch.saturating_add(1), &relational_state)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let mut ops = Vec::with_capacity(nodes.len() + relationships.len() + 2);
        ops.push(WalOp::MarkInitialImportSource { source_fingerprint });
        ops.push(WalOp::RelationalSnapshot {
            record: Arc::from(relational_record),
        });
        ops.extend(
            nodes
                .iter()
                .map(|(id, label, properties)| WalOp::CreateNode {
                    id: *id,
                    label: label.clone(),
                    properties: properties.clone(),
                }),
        );
        ops.extend(
            relationships
                .iter()
                .map(
                    |(id, source, target, rel_type, properties)| WalOp::CreateRelationship {
                        id: *id,
                        source: *source,
                        target: *target,
                        rel_type: rel_type.clone(),
                        properties: properties.clone(),
                    },
                ),
        );
        self.validate_constraints_for_ops(&working_catalog, &ops)?;

        // The mapping is durable before the WAL batch; recovery never observes imported
        // graph rows without the stable identities required to address them.
        self.replace_stable_id_mapping(stable_id_mapping)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(
            &working_catalog,
            self.commit_epoch + 1,
            &ops,
        );
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok(())
    }

    pub fn create_relationships_between_matches(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipCreate,
    ) -> Result<Vec<(NodeId, RelId, NodeId)>> {
        let source_label_id = if request.source_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.source_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let target_label_id = if request.target_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.target_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let sources =
            self.matching_node_ids(catalog, source_label_id, request.source_filter.as_ref())?;
        let targets =
            self.matching_node_ids(catalog, target_label_id, request.target_filter.as_ref())?;
        if sources.is_empty() || targets.is_empty() {
            return Ok(Vec::new());
        }

        catalog.get_or_create_rel_type(&request.rel_type);
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::with_capacity(sources.len() * targets.len());
        let mut ops = Vec::with_capacity(sources.len() * targets.len());
        for source in sources {
            for target in &targets {
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.rel_type.clone(),
                    properties: request.rel_properties.clone(),
                });
                rows.push((source, rel, *target));
            }
        }
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok(rows)
    }

    pub fn merge_relationships_between_matches(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let source_label_id = if request.source_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.source_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let target_label_id = if request.target_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.target_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let sources =
            self.matching_node_ids(catalog, source_label_id, request.source_filter.as_ref())?;
        let targets =
            self.matching_node_ids(catalog, target_label_id, request.target_filter.as_ref())?;
        if sources.is_empty() || targets.is_empty() {
            return Ok(Vec::new());
        }

        let rel_type_id = catalog.get_or_create_rel_type(&request.rel_type);
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::with_capacity(sources.len() * targets.len());
        let mut ops = Vec::new();
        for source in sources {
            for target in &targets {
                if let Some(rel) = self.find_relationship_by_property_subset(
                    source,
                    *target,
                    rel_type_id,
                    &request.rel_match_properties,
                )? {
                    rows.push((source, rel, *target, false));
                    continue;
                }
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                let mut properties = request.rel_match_properties.clone();
                for (property, value) in &request.on_create_properties {
                    properties.insert(property.clone(), value.clone());
                }
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.rel_type.clone(),
                    properties,
                });
                rows.push((source, rel, *target, true));
            }
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            if let Some(durable) = &mut self.durable {
                durable.append_batch(ops.clone())?;
            }
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op)?;
            }
            self.commit_epoch += 1;
        }
        Ok(rows)
    }

    pub fn merge_relationships_from_matched_relationships(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipCopyMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let source_label_id = if request.source_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.source_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let target_label_id = if request.target_label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(&request.target_label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let Some(old_rel_type_id) = catalog.rel_type_id(&request.old_rel_type) else {
            return Ok(Vec::new());
        };
        let new_rel_type_id = catalog.get_or_create_rel_type(&request.new_rel_type);
        let old_relationships = self
            .scan_relationships(Some(old_rel_type_id))
            .filter(|relationship| {
                properties_contain_all(&relationship.properties, &request.old_rel_filter)
                    && self
                        .nodes
                        .get(&relationship.source)
                        .map(|node| {
                            source_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .source_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
                    && self
                        .nodes
                        .get(&relationship.target)
                        .map(|node| {
                            target_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .target_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
            })
            .cloned()
            .collect::<Vec<_>>();

        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::with_capacity(old_relationships.len());
        let mut ops = Vec::new();
        for old_relationship in old_relationships {
            if let Some(rel) = self.find_relationship_by_property_subset(
                old_relationship.source,
                old_relationship.target,
                new_rel_type_id,
                &request.new_rel_match_properties,
            )? {
                rows.push((old_relationship.source, rel, old_relationship.target, false));
                continue;
            }
            let rel = RelId(next_rel_id);
            next_rel_id += 1;
            let mut properties = request.new_rel_match_properties.clone();
            for (property, value) in &request.on_create_properties {
                let value = match value {
                    RelationshipOnCreatePropertyValue::Value(value) => value.clone(),
                    RelationshipOnCreatePropertyValue::MatchedRelationshipProperty { property } => {
                        old_relationship
                            .properties
                            .get(property)
                            .cloned()
                            .unwrap_or(Value::Null)
                    }
                };
                properties.insert(property.clone(), value);
            }
            ops.push(WalOp::CreateRelationship {
                id: rel,
                source: old_relationship.source,
                target: old_relationship.target,
                rel_type: request.new_rel_type.clone(),
                properties,
            });
            rows.push((old_relationship.source, rel, old_relationship.target, true));
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            if let Some(durable) = &mut self.durable {
                durable.append_batch(ops.clone())?;
            }
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op)?;
            }
            self.commit_epoch += 1;
        }
        Ok(rows)
    }

    pub fn merge_relationships_to_matched_target(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipRetargetMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let source_label_id = optional_label_id(catalog, &request.source_label);
        if !request.source_label.is_empty() && source_label_id.is_none() {
            return Ok(Vec::new());
        }
        let Some(old_target_label_id) = catalog.label_id(&request.old_target_label) else {
            return Ok(Vec::new());
        };
        let Some(new_target_label_id) = catalog.label_id(&request.new_target_label) else {
            return Ok(Vec::new());
        };
        let Some(old_rel_type_id) = catalog.rel_type_id(&request.old_rel_type) else {
            return Ok(Vec::new());
        };
        let new_rel_type_id = catalog.get_or_create_rel_type(&request.new_rel_type);
        let source_ids = self
            .scan_relationships(Some(old_rel_type_id))
            .filter(|relationship| {
                properties_contain_all(&relationship.properties, &request.old_rel_filter)
                    && self
                        .nodes
                        .get(&relationship.source)
                        .map(|node| {
                            source_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .source_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
                    && self
                        .nodes
                        .get(&relationship.target)
                        .map(|node| {
                            node.labels.contains(&old_target_label_id)
                                && request
                                    .old_target_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
            })
            .map(|relationship| relationship.source)
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let target_ids = self.matching_node_ids(
            catalog,
            Some(new_target_label_id),
            request.new_target_filter.as_ref(),
        )?;
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::new();
        let mut ops = Vec::new();
        for source in source_ids {
            for target in &target_ids {
                if let Some(rel) = self.find_relationship_by_property_subset(
                    source,
                    *target,
                    new_rel_type_id,
                    &request.new_rel_match_properties,
                )? {
                    rows.push((source, rel, *target, false));
                    continue;
                }
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                let mut properties = request.new_rel_match_properties.clone();
                properties.extend(request.on_create_properties.clone());
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.new_rel_type.clone(),
                    properties,
                });
                rows.push((source, rel, *target, true));
            }
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            if let Some(durable) = &mut self.durable {
                durable.append_batch(ops.clone())?;
            }
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op)?;
            }
            self.commit_epoch += 1;
        }
        Ok(rows)
    }

    pub fn merge_relationships_from_matched_target(
        &mut self,
        catalog: &mut Catalog,
        request: MatchedRelationshipSourceRetargetMerge,
    ) -> Result<Vec<(NodeId, RelId, NodeId, bool)>> {
        let old_source_label_id = optional_label_id(catalog, &request.old_source_label);
        if !request.old_source_label.is_empty() && old_source_label_id.is_none() {
            return Ok(Vec::new());
        }
        let Some(old_target_label_id) = catalog.label_id(&request.old_target_label) else {
            return Ok(Vec::new());
        };
        let new_source_label_id = optional_label_id(catalog, &request.new_source_label);
        if !request.new_source_label.is_empty() && new_source_label_id.is_none() {
            return Ok(Vec::new());
        }
        let Some(old_rel_type_id) = catalog.rel_type_id(&request.old_rel_type) else {
            return Ok(Vec::new());
        };
        let new_rel_type_id = catalog.get_or_create_rel_type(&request.new_rel_type);
        let target_ids = self
            .scan_relationships(Some(old_rel_type_id))
            .filter(|relationship| {
                properties_contain_all(&relationship.properties, &request.old_rel_filter)
                    && self
                        .nodes
                        .get(&relationship.source)
                        .map(|node| {
                            old_source_label_id
                                .map(|label_id| node.labels.contains(&label_id))
                                .unwrap_or(true)
                                && request
                                    .old_source_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
                    && self
                        .nodes
                        .get(&relationship.target)
                        .map(|node| {
                            node.labels.contains(&old_target_label_id)
                                && request
                                    .old_target_filter
                                    .as_ref()
                                    .map(|filter| {
                                        property_filter_matches(filter, node.id.0, &node.properties)
                                    })
                                    .unwrap_or(true)
                        })
                        .unwrap_or(false)
            })
            .map(|relationship| relationship.target)
            .collect::<BTreeSet<_>>();
        if target_ids.is_empty() {
            return Ok(Vec::new());
        }
        let source_ids = self.matching_node_ids(
            catalog,
            new_source_label_id,
            request.new_source_filter.as_ref(),
        )?;
        let mut next_rel_id = self.next_rel_id;
        let mut rows = Vec::new();
        let mut ops = Vec::new();
        for source in source_ids {
            for target in &target_ids {
                if let Some(rel) = self.find_relationship_by_property_subset(
                    source,
                    *target,
                    new_rel_type_id,
                    &request.new_rel_match_properties,
                )? {
                    rows.push((source, rel, *target, false));
                    continue;
                }
                let rel = RelId(next_rel_id);
                next_rel_id += 1;
                let mut properties = request.new_rel_match_properties.clone();
                properties.extend(request.on_create_properties.clone());
                ops.push(WalOp::CreateRelationship {
                    id: rel,
                    source,
                    target: *target,
                    rel_type: request.new_rel_type.clone(),
                    properties,
                });
                rows.push((source, rel, *target, true));
            }
        }
        if !ops.is_empty() {
            self.validate_constraints_for_ops(catalog, &ops)?;
            if let Some(durable) = &mut self.durable {
                durable.append_batch(ops.clone())?;
            }
            self.record_search_projection_graph_changes_for_ops(
                catalog,
                self.commit_epoch + 1,
                &ops,
            );
            for op in ops {
                self.apply_wal_op(catalog, op)?;
            }
            self.commit_epoch += 1;
        }
        Ok(rows)
    }

    pub fn set_node_property(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        property: &str,
        value: Value,
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(catalog, label_id, filter)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = ids
            .iter()
            .map(|id| WalOp::SetNodeProperty {
                id: *id,
                property: property.to_string(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub fn add_int_node_property(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        property: &str,
        amount: i64,
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(catalog, label_id, filter)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = ids
            .iter()
            .map(|id| {
                let node = self.node_owned(*id)?.ok_or_else(|| {
                    SkeinError::Storage(format!("node {} disappeared during property update", id.0))
                })?;
                let current = match node.properties.get(property) {
                    None | Some(Value::Null) => 0,
                    Some(Value::Int(value)) => *value,
                    Some(value) => {
                        return Err(SkeinError::Execution(format!(
                            "property increment requires an integer or null value, got {value:?}"
                        )));
                    }
                };
                let value = current.checked_add(amount).ok_or_else(|| {
                    SkeinError::Execution("property increment overflowed i64".to_string())
                })?;
                Ok(WalOp::SetNodeProperty {
                    id: *id,
                    property: property.to_string(),
                    value: Value::Int(value),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            if let WalOp::SetNodeProperty {
                id,
                property,
                value,
            } = op
            {
                self.apply_set_node_property(catalog, id, property, value);
            }
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub fn set_node_properties(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        assignments: &[NodeSetAssignment],
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(catalog, label_id, filter)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = self.node_set_property_ops(&ids, assignments)?;
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub fn set_node_properties_by_ids(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        assignments: &[NodeSetAssignment],
    ) -> Result<Vec<NodeId>> {
        self.set_node_properties_by_ids_with_limits(
            catalog,
            ids,
            assignments,
            MutationLimits::default(),
        )
    }

    pub fn set_node_properties_by_ids_with_limits(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        assignments: &[NodeSetAssignment],
        limits: MutationLimits,
    ) -> Result<Vec<NodeId>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let operation_count = ids.len().checked_mul(assignments.len()).ok_or_else(|| {
            SkeinError::Execution("mutation operation count overflow".to_string())
        })?;
        ensure_additional_mutation_limits(0, 0, operation_count, ids.len(), limits)?;
        let ops = self.node_set_property_ops(ids, assignments)?;
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok(ids.to_vec())
    }

    fn node_set_property_ops(
        &self,
        ids: &[NodeId],
        assignments: &[NodeSetAssignment],
    ) -> Result<Vec<WalOp>> {
        let mut ops = Vec::with_capacity(ids.len().saturating_mul(assignments.len()));
        for id in ids {
            let node = self.node_owned(*id)?.ok_or_else(|| {
                SkeinError::Storage(format!("node {} disappeared during property update", id.0))
            })?;
            for assignment in assignments {
                let value = evaluate_node_set_value(&node.properties, assignment)?;
                ops.push(WalOp::SetNodeProperty {
                    id: *id,
                    property: assignment.property.clone(),
                    value,
                });
            }
        }
        Ok(ops)
    }

    pub fn delete_nodes(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        filter: Option<&PropertyFilter>,
        detach: bool,
    ) -> Result<Vec<NodeId>> {
        let label_id = if label.is_empty() {
            None
        } else {
            let Some(label_id) = catalog.label_id(label) else {
                return Ok(Vec::new());
            };
            Some(label_id)
        };
        let ids = self.matching_node_ids(catalog, label_id, filter)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = self.delete_node_ops(&ids, detach)?;
        self.ensure_out_of_core_delta_admission(&ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub(crate) fn delete_node_ids(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        detach: bool,
    ) -> Result<Vec<NodeId>> {
        self.delete_node_ids_with_limits(catalog, ids, detach, MutationLimits::default())
    }

    pub(crate) fn delete_node_ids_with_limits(
        &mut self,
        catalog: &mut Catalog,
        ids: &[NodeId],
        detach: bool,
        limits: MutationLimits,
    ) -> Result<Vec<NodeId>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        ensure_additional_mutation_limits(0, 0, 0, ids.len(), limits)?;
        let ops = self.delete_node_ops_bounded(ids, detach, limits.max_operations.get())?;
        self.ensure_out_of_core_delta_admission(&ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok(ids.to_vec())
    }

    pub fn delete_relationships(
        &mut self,
        catalog: &mut Catalog,
        request: RelationshipDeleteRequest,
    ) -> Result<Vec<RelId>> {
        let Some(source_label_id) = catalog.label_id(&request.source_label) else {
            return Ok(Vec::new());
        };
        let Some(target_label_id) = catalog.label_id(&request.target_label) else {
            return Ok(Vec::new());
        };
        let Some(rel_type_id) = catalog.rel_type_id(&request.rel_type) else {
            return Ok(Vec::new());
        };
        let source_ids = self
            .matching_node_ids(catalog, Some(source_label_id), request.filter.as_ref())?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let target_ids = request
            .target_filter
            .as_ref()
            .map(|filter| {
                self.matching_node_ids(catalog, Some(target_label_id), Some(filter))
                    .map(|ids| ids.into_iter().collect::<BTreeSet<_>>())
            })
            .transpose()?;
        if target_ids.as_ref().is_some_and(BTreeSet::is_empty) {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for relationship in self.relationship_records_owned() {
            let relationship = relationship?;
            if relationship.rel_type != rel_type_id
                || !source_ids.contains(&relationship.source)
                || request.rel_filter.as_ref().is_some_and(|filter| {
                    !property_filter_matches(filter, relationship.id.0, &relationship.properties)
                })
            {
                continue;
            }
            let target_matches = self.node_owned(relationship.target)?.is_some_and(|target| {
                target.labels.contains(&target_label_id)
                    && target_ids
                        .as_ref()
                        .is_none_or(|ids| ids.contains(&relationship.target))
            });
            if target_matches {
                ids.push(relationship.id);
            }
        }
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = ids
            .iter()
            .copied()
            .map(|id| WalOp::DeleteRelationship { id })
            .collect::<Vec<_>>();
        self.ensure_out_of_core_delta_admission(&ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub fn delete_relationship_target_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: RelationshipTargetNodeDelete,
    ) -> Result<Vec<NodeId>> {
        let ids = self.relationship_target_node_ids(catalog, &request)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops = self.delete_node_ops(&ids, request.detach)?;
        self.ensure_out_of_core_delta_admission(&ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    fn relationship_target_node_ids(
        &self,
        catalog: &Catalog,
        request: &RelationshipTargetNodeDelete,
    ) -> Result<Vec<NodeId>> {
        self.relationship_target_node_ids_bounded(catalog, request, usize::MAX)
    }

    fn relationship_target_node_ids_bounded(
        &self,
        catalog: &Catalog,
        request: &RelationshipTargetNodeDelete,
        max_ids: usize,
    ) -> Result<Vec<NodeId>> {
        let Some(source_label_id) = optional_label_id(catalog, &request.source_label) else {
            return Ok(Vec::new());
        };
        let Some(target_label_id) = optional_label_id(catalog, &request.target_label) else {
            return Ok(Vec::new());
        };
        let Some(rel_type_id) = catalog.rel_type_id(&request.rel_type) else {
            return Ok(Vec::new());
        };
        let source_ids = self
            .matching_node_ids_bounded(
                Some(source_label_id),
                request.source_filter.as_ref(),
                max_ids,
                "max_mutation_affected_rows",
            )?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let target_ids = request
            .target_filter
            .as_ref()
            .map(|filter| {
                self.matching_node_ids_bounded(
                    Some(target_label_id),
                    Some(filter),
                    max_ids,
                    "max_mutation_affected_rows",
                )
                .map(|ids| ids.into_iter().collect::<BTreeSet<_>>())
            })
            .transpose()?;
        if target_ids.as_ref().is_some_and(BTreeSet::is_empty) {
            return Ok(Vec::new());
        }
        let mut ids = BTreeSet::new();
        let mut callback_error = None;
        self.visit_relationships_owned(Some(rel_type_id), |relationship| {
            if !source_ids.contains(&relationship.source)
                || request.rel_filter.as_ref().is_some_and(|filter| {
                    !property_filter_matches(filter, relationship.id.0, &relationship.properties)
                })
                || target_ids
                    .as_ref()
                    .is_some_and(|ids| !ids.contains(&relationship.target))
            {
                return GraphScanControl::Continue;
            }
            match self.node_owned(relationship.target) {
                Ok(Some(target)) if target.labels.contains(&target_label_id) => {
                    ids.insert(relationship.target);
                    if ids.len() > max_ids {
                        callback_error = Some(SkeinError::Execution(format!(
                            "mutation would exceed max_mutation_affected_rows {max_ids}"
                        )));
                        return GraphScanControl::Stop;
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    callback_error = Some(error);
                    return GraphScanControl::Stop;
                }
            }
            GraphScanControl::Continue
        })?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        Ok(ids.into_iter().collect())
    }

    fn relationship_target_node_ids_with_pending_bounded(
        &self,
        catalog: &Catalog,
        request: &RelationshipTargetNodeDelete,
        pending_nodes: &[PendingNode],
        pending_relationships: &[PendingRelationship],
        max_ids: usize,
    ) -> Result<Vec<NodeId>> {
        let Some(source_label_id) = optional_label_id(catalog, &request.source_label) else {
            return Ok(Vec::new());
        };
        let Some(target_label_id) = optional_label_id(catalog, &request.target_label) else {
            return Ok(Vec::new());
        };
        let Some(rel_type_id) = catalog.rel_type_id(&request.rel_type) else {
            return Ok(Vec::new());
        };
        let source_ids = self
            .matching_node_ids_with_pending_bounded(
                Some(source_label_id),
                request.source_filter.as_ref(),
                pending_nodes,
                max_ids,
                "max_mutation_affected_rows",
            )?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let target_ids = request
            .target_filter
            .as_ref()
            .map(|filter| {
                self.matching_node_ids_with_pending_bounded(
                    Some(target_label_id),
                    Some(filter),
                    pending_nodes,
                    max_ids,
                    "max_mutation_affected_rows",
                )
                .map(|ids| ids.into_iter().collect::<BTreeSet<_>>())
            })
            .transpose()?;
        if target_ids.as_ref().is_some_and(BTreeSet::is_empty) {
            return Ok(Vec::new());
        }
        let mut ids = self.relationship_target_node_ids_bounded(catalog, request, max_ids)?;
        for (relationship_id, source, target, pending_rel_type_id, properties) in
            pending_relationships
        {
            if *pending_rel_type_id != rel_type_id
                || !source_ids.contains(source)
                || request.rel_filter.as_ref().is_some_and(|filter| {
                    !property_filter_matches(filter, relationship_id.0, properties)
                })
                || !node_matches_label_and_filter(
                    self,
                    pending_nodes,
                    *target,
                    target_label_id,
                    request.target_filter.as_ref(),
                )?
                || target_ids.as_ref().is_some_and(|ids| !ids.contains(target))
            {
                continue;
            }
            ids.push(*target);
            if ids.len() > max_ids {
                return Err(SkeinError::Execution(format!(
                    "mutation would exceed max_mutation_affected_rows {max_ids}"
                )));
            }
        }
        Ok(ids
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect())
    }

    pub fn set_relationship_property(
        &mut self,
        catalog: &mut Catalog,
        update: RelationshipPropertyUpdate,
    ) -> Result<Vec<RelId>> {
        self.set_relationship_properties(
            catalog,
            RelationshipPropertiesUpdate {
                source_label: update.source_label,
                filter: update.filter,
                rel_type: update.rel_type,
                target_label: update.target_label,
                target_filter: update.target_filter,
                rel_filter: update.rel_filter,
                assignments: vec![RelationshipSetAssignment {
                    property: update.property,
                    value: update.value,
                }],
            },
        )
    }

    pub fn set_relationship_properties(
        &mut self,
        catalog: &mut Catalog,
        update: RelationshipPropertiesUpdate,
    ) -> Result<Vec<RelId>> {
        let Some(source_label_id) = catalog.label_id(&update.source_label) else {
            return Ok(Vec::new());
        };
        let Some(target_label_id) = catalog.label_id(&update.target_label) else {
            return Ok(Vec::new());
        };
        let Some(rel_type_id) = catalog.rel_type_id(&update.rel_type) else {
            return Ok(Vec::new());
        };
        let source_ids = self
            .matching_node_ids(catalog, Some(source_label_id), update.filter.as_ref())?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for relationship in self.relationship_records_owned() {
            let relationship = relationship?;
            if relationship.rel_type != rel_type_id
                || !source_ids.contains(&relationship.source)
                || update.rel_filter.as_ref().is_some_and(|filter| {
                    !property_filter_matches(filter, relationship.id.0, &relationship.properties)
                })
            {
                continue;
            }
            if self
                .node_owned(relationship.target)?
                .is_some_and(|target| target.labels.contains(&target_label_id))
            {
                ids.push(relationship.id);
            }
        }
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let ops =
            ids.iter()
                .flat_map(|id| {
                    update.assignments.iter().map(move |assignment| {
                        WalOp::SetRelationshipProperty {
                            id: *id,
                            property: assignment.property.clone(),
                            value: assignment.value.clone(),
                        }
                    })
                })
                .collect::<Vec<_>>();
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok(ids)
    }

    pub fn create_connected_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: ConnectedNodesCreate,
    ) -> Result<(NodeId, RelId, NodeId)> {
        let source_label_id = catalog.get_or_create_label(&request.source_label);
        let target_label_id = catalog.get_or_create_label(&request.target_label);
        let rel_type_id = catalog.get_or_create_rel_type(&request.rel_type);
        let source = NodeId(self.next_node_id);
        let target = NodeId(self.next_node_id + 1);
        let relationship = RelId(self.next_rel_id);
        let ops = vec![
            WalOp::CreateNode {
                id: source,
                label: request.source_label.clone(),
                properties: request.source_properties.clone(),
            },
            WalOp::CreateNode {
                id: target,
                label: request.target_label.clone(),
                properties: request.target_properties.clone(),
            },
            WalOp::CreateRelationship {
                id: relationship,
                source,
                target,
                rel_type: request.rel_type.clone(),
                properties: request.rel_properties.clone(),
            },
        ];
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops)?;
        }
        self.apply_create_node(catalog, source, source_label_id, request.source_properties);
        self.apply_create_node(catalog, target, target_label_id, request.target_properties);
        self.apply_create_relationship(
            relationship,
            source,
            target,
            rel_type_id,
            request.rel_properties,
        );
        self.commit_epoch += 1;
        Ok((source, relationship, target))
    }

    pub fn merge_connected_nodes(
        &mut self,
        catalog: &mut Catalog,
        request: ConnectedNodesCreate,
    ) -> Result<(NodeId, RelId, NodeId, bool)> {
        let source_label_id = catalog.get_or_create_label(&request.source_label);
        let target_label_id = catalog.get_or_create_label(&request.target_label);
        let rel_type_id = catalog.get_or_create_rel_type(&request.rel_type);
        let source =
            self.find_node_by_label_and_properties(source_label_id, &request.source_properties)?;
        let target =
            self.find_node_by_label_and_properties(target_label_id, &request.target_properties)?;
        if let (Some(source), Some(target)) = (source, target)
            && let Some(relationship) = self.find_relationship_by_properties(
                source,
                target,
                rel_type_id,
                &request.rel_properties,
            )?
        {
            return Ok((source, relationship, target, false));
        }

        let source = source.unwrap_or(NodeId(self.next_node_id));
        let target = target.unwrap_or_else(|| {
            if source.0 == self.next_node_id {
                NodeId(self.next_node_id + 1)
            } else {
                NodeId(self.next_node_id)
            }
        });
        let relationship = RelId(self.next_rel_id);
        let ops = self.merge_connected_node_ops(&request, source, target, relationship)?;
        self.validate_constraints_for_ops(catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            durable.append_batch(ops.clone())?;
        }
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            self.apply_wal_op(catalog, op)?;
        }
        self.commit_epoch += 1;
        Ok((source, relationship, target, true))
    }

    pub fn commit_mutations(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
    ) -> Result<MutationSummary> {
        self.commit_mutations_with_limits(catalog, mutations, MutationLimits::default())
    }

    pub(crate) fn begin_mutation_transaction(&self, catalog: &Catalog) -> GraphMutationTransaction {
        GraphMutationTransaction {
            base_commit_epoch: self.commit_epoch,
            catalog: catalog.clone(),
            store: self.snapshot(),
            ops: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub(crate) fn commit_mutation_transaction_and_relational(
        &mut self,
        catalog: &mut Catalog,
        transaction: GraphMutationTransaction,
        relational_transaction: RelationalTransaction,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.commit_mutation_transaction_and_relational_internal(
            catalog,
            transaction,
            relational_transaction,
            limits,
            false,
        )
    }

    pub(crate) fn commit_rebased_mutation_transaction_and_relational(
        &mut self,
        catalog: &mut Catalog,
        transaction: GraphMutationTransaction,
        relational_transaction: RelationalTransaction,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        // The caller must hold locks that cover every read and write in the
        // staged transaction. Rebase only skips the coarse epoch check; current
        // graph and relational constraints are still validated before WAL.
        self.commit_mutation_transaction_and_relational_internal(
            catalog,
            transaction,
            relational_transaction,
            limits,
            true,
        )
    }

    fn commit_mutation_transaction_and_relational_internal(
        &mut self,
        catalog: &mut Catalog,
        transaction: GraphMutationTransaction,
        relational_transaction: RelationalTransaction,
        limits: MutationLimits,
        allow_stale_rebase: bool,
    ) -> Result<MutationSummary> {
        let read_only = transaction.ops.is_empty() && relational_transaction.writes.is_empty();
        if !read_only && !allow_stale_rebase && self.commit_epoch != transaction.base_commit_epoch {
            return Err(SkeinError::Execution(format!(
                "transaction snapshot is stale: started at commit epoch {}, current epoch is {}",
                transaction.base_commit_epoch, self.commit_epoch
            )));
        }
        let ops = compact_transaction_graph_ops(transaction.ops);
        self.commit_prepared_mutation_ops(
            catalog,
            transaction.catalog,
            ops,
            transaction.rows,
            Some(relational_transaction),
            limits,
            false,
            None,
        )
    }

    pub fn commit_mutation_with_limits(
        &mut self,
        catalog: &mut Catalog,
        mutation: GraphMutation,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.commit_mutations_internal(catalog, vec![mutation], None, limits, true, None)
    }

    pub fn commit_mutations_with_limits(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.commit_mutations_internal(catalog, mutations, None, limits, false, None)
    }

    pub(crate) fn relational_state(&self) -> &RelationalState {
        &self.relational_state
    }

    pub(crate) fn commit_relational_transaction(
        &mut self,
        catalog: &mut Catalog,
        transaction: RelationalTransaction,
    ) -> Result<MutationSummary> {
        self.commit_mutations_and_relational(
            catalog,
            Vec::new(),
            transaction,
            MutationLimits::default(),
        )
    }

    pub(crate) fn commit_mutations_and_relational(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
        transaction: RelationalTransaction,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.commit_mutations_internal(catalog, mutations, Some(transaction), limits, false, None)
    }

    fn commit_mutations_internal(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
        relational_transaction: Option<RelationalTransaction>,
        limits: MutationLimits,
        preserve_single_create_wal: bool,
        captured_graph_ops: Option<&mut Vec<WalOp>>,
    ) -> Result<MutationSummary> {
        let mut next_node_id = self.next_node_id;
        let mut next_rel_id = self.next_rel_id;
        let mut working_catalog = catalog.clone();
        let mut ops = Vec::new();
        let mut rows = Vec::new();
        let mut pending_nodes = Vec::new();
        let mut pending_relationships = Vec::new();

        for mutation in mutations {
            match mutation {
                GraphMutation::CreateNodeLabel { label } => {
                    if let Some(id) = working_catalog.label_id(&label) {
                        rows.push(BTreeMap::from([
                            ("label_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateNodeLabel {
                            label: label.clone(),
                        });
                        let id = working_catalog.get_or_create_label(&label);
                        rows.push(BTreeMap::from([
                            ("label_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipType { rel_type } => {
                    if let Some(id) = working_catalog.rel_type_id(&rel_type) {
                        rows.push(BTreeMap::from([
                            ("rel_type_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateRelationshipType {
                            rel_type: rel_type.clone(),
                        });
                        let id = working_catalog.get_or_create_rel_type(&rel_type);
                        rows.push(BTreeMap::from([
                            ("rel_type_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateNodeTable { name } => {
                    if let Some(id) = working_catalog.table_id(TableKind::Node, &name) {
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateNodeTable { name: name.clone() });
                        working_catalog.get_or_create_label(&name);
                        let id = working_catalog.get_or_create_table(TableKind::Node, &name);
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipTable { name } => {
                    if let Some(id) = working_catalog.table_id(TableKind::Relationship, &name) {
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateRelationshipTable { name: name.clone() });
                        working_catalog.get_or_create_rel_type(&name);
                        let id =
                            working_catalog.get_or_create_table(TableKind::Relationship, &name);
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateProperty {
                    table_kind,
                    table,
                    property,
                    value_type,
                    nullable,
                } => {
                    let table_id =
                        ensure_table_descriptor(&mut working_catalog, table_kind, &table);
                    if let Some(id) = working_catalog.property_descriptor_id(table_id, &property) {
                        rows.push(BTreeMap::from([
                            ("property_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        validate_property_descriptor(
                            &working_catalog,
                            self,
                            table_id,
                            &property,
                            value_type,
                            nullable,
                        )?;
                        ops.push(WalOp::CreateProperty {
                            table_kind,
                            table: table.clone(),
                            property: property.clone(),
                            value_type,
                            nullable,
                        });
                        let id = working_catalog
                            .get_or_create_property(table_id, &property, value_type, nullable);
                        rows.push(BTreeMap::from([
                            ("property_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::AlterTableState {
                    table_kind,
                    table,
                    state,
                } => {
                    let Some(id) = working_catalog.table_id(table_kind, &table) else {
                        return Err(SkeinError::Storage(format!(
                            "schema table '{table}' does not exist"
                        )));
                    };
                    let Some(descriptor) = working_catalog.table_descriptor(id) else {
                        return Err(SkeinError::Storage(format!(
                            "schema table '{table}' does not exist"
                        )));
                    };
                    let changed = descriptor.state != state;
                    if changed {
                        ops.push(WalOp::AlterTableState {
                            table_kind,
                            table: table.clone(),
                            state,
                        });
                        working_catalog.set_table_state(id, state);
                    }
                    rows.push(BTreeMap::from([
                        ("table_id".to_string(), Value::Int(id.0 as i64)),
                        ("changed".to_string(), Value::Bool(changed)),
                    ]));
                }
                GraphMutation::AlterPropertyState {
                    table_kind,
                    table,
                    property,
                    state,
                } => {
                    let Some(table_id) = working_catalog.table_id(table_kind, &table) else {
                        return Err(SkeinError::Storage(format!(
                            "schema table '{table}' does not exist"
                        )));
                    };
                    let Some(id) = working_catalog.property_descriptor_id(table_id, &property)
                    else {
                        return Err(SkeinError::Storage(format!(
                            "schema property '{table}.{property}' does not exist"
                        )));
                    };
                    let Some(descriptor) = working_catalog.property_descriptor(id) else {
                        return Err(SkeinError::Storage(format!(
                            "schema property '{table}.{property}' does not exist"
                        )));
                    };
                    let changed = descriptor.state != state;
                    if changed {
                        if state == SchemaObjectState::Public {
                            validate_property_descriptor(
                                &working_catalog,
                                self,
                                table_id,
                                &property,
                                descriptor.value_type,
                                descriptor.nullable,
                            )?;
                        }
                        ops.push(WalOp::AlterPropertyState {
                            table_kind,
                            table: table.clone(),
                            property: property.clone(),
                            state,
                        });
                        working_catalog.set_property_state(id, state);
                    }
                    rows.push(BTreeMap::from([
                        ("property_id".to_string(), Value::Int(id.0 as i64)),
                        ("changed".to_string(), Value::Bool(changed)),
                    ]));
                }
                GraphMutation::CreateIndex { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.property_index_id(label_id, &property) {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateIndex {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog.get_or_create_property_index(label_id, &property);
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateCompositeIndex { label, properties } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) =
                        working_catalog.composite_property_index_id(label_id, &properties)
                    {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateCompositeIndex {
                            label: label.clone(),
                            properties: properties.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_composite_property_index(label_id, &properties);
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRangeIndex { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.property_index_id_with_kind(
                        label_id,
                        &property,
                        IndexKind::Range,
                    ) {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateRangeIndex {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog.get_or_create_property_index_with_kind(
                            label_id,
                            &property,
                            IndexKind::Range,
                        );
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateFullTextIndex { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.property_index_id_with_kind(
                        label_id,
                        &property,
                        IndexKind::FullText,
                    ) {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateFullTextIndex {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog.get_or_create_property_index_with_kind(
                            label_id,
                            &property,
                            IndexKind::FullText,
                        );
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateUniqueConstraint { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.unique_constraint_id(label_id, &property) {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_unique_constraint(&working_catalog, label_id, &property)?;
                        ops.push(WalOp::CreateUniqueConstraint {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id =
                            working_catalog.get_or_create_unique_constraint(label_id, &property);
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateNodePropertyExistsConstraint { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) =
                        working_catalog.node_property_exists_constraint_id(label_id, &property)
                    {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_node_property_exists_constraint(
                            &working_catalog,
                            label_id,
                            &property,
                        )?;
                        ops.push(WalOp::CreateNodePropertyExistsConstraint {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_node_property_exists_constraint(label_id, &property);
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipUniqueConstraint { rel_type, property } => {
                    let rel_type_id = working_catalog.get_or_create_rel_type(&rel_type);
                    if let Some(id) =
                        working_catalog.relationship_unique_constraint_id(rel_type_id, &property)
                    {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_relationship_unique_constraint(
                            &working_catalog,
                            rel_type_id,
                            &property,
                        )?;
                        ops.push(WalOp::CreateRelationshipUniqueConstraint {
                            rel_type: rel_type.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_relationship_unique_constraint(rel_type_id, &property);
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipPropertyExistsConstraint {
                    rel_type,
                    property,
                } => {
                    let rel_type_id = working_catalog.get_or_create_rel_type(&rel_type);
                    if let Some(id) = working_catalog
                        .relationship_property_exists_constraint_id(rel_type_id, &property)
                    {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_relationship_property_exists_constraint(
                            &working_catalog,
                            rel_type_id,
                            &property,
                        )?;
                        ops.push(WalOp::CreateRelationshipPropertyExistsConstraint {
                            rel_type: rel_type.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_relationship_property_exists_constraint(
                                rel_type_id,
                                &property,
                            );
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateNode { label, properties } => {
                    let id = NodeId(next_node_id);
                    next_node_id += 1;
                    let label_id = working_catalog.get_or_create_label(&label);
                    ops.push(WalOp::CreateNode {
                        id,
                        label,
                        properties: properties.clone(),
                    });
                    pending_nodes.push((id, label_id, properties));
                    rows.push(BTreeMap::from([(
                        "node_id".to_string(),
                        Value::Int(id.0 as i64),
                    )]));
                }
                GraphMutation::MergeNode {
                    label,
                    match_properties,
                    on_create_properties,
                    on_match_assignments,
                    post_merge_assignments,
                } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    let current =
                        self.find_node_by_label_and_properties(label_id, &match_properties)?;
                    let pending = pending_nodes
                        .iter()
                        .find(|(_, pending_label, pending_properties)| {
                            *pending_label == label_id
                                && properties_contain_all(pending_properties, &match_properties)
                        })
                        .map(|(id, _, _)| *id);
                    if let Some(id) = current {
                        if !on_match_assignments.is_empty() || !post_merge_assignments.is_empty() {
                            let mut assignments = Vec::with_capacity(
                                on_match_assignments.len() + post_merge_assignments.len(),
                            );
                            assignments.extend(on_match_assignments);
                            assignments.extend(post_merge_assignments);
                            let set_ops = self.node_set_property_ops(&[id], &assignments)?;
                            ops.extend(set_ops);
                        }
                        rows.push(BTreeMap::from([
                            ("node_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else if let Some(id) = pending {
                        if !on_match_assignments.is_empty() || !post_merge_assignments.is_empty() {
                            let mut assignments = Vec::with_capacity(
                                on_match_assignments.len() + post_merge_assignments.len(),
                            );
                            assignments.extend(on_match_assignments);
                            assignments.extend(post_merge_assignments);
                            Self::apply_pending_node_assignments(
                                &mut ops,
                                &mut pending_nodes,
                                id,
                                &assignments,
                            )?;
                        }
                        rows.push(BTreeMap::from([
                            ("node_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        let mut properties = match_properties;
                        for (property, value) in on_create_properties {
                            properties.insert(property, value);
                        }
                        apply_node_assignments_to_properties(
                            &mut properties,
                            &post_merge_assignments,
                        )?;
                        let id = NodeId(next_node_id);
                        next_node_id += 1;
                        ops.push(WalOp::CreateNode {
                            id,
                            label,
                            properties: properties.clone(),
                        });
                        pending_nodes.push((id, label_id, properties));
                        rows.push(BTreeMap::from([
                            ("node_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::MergeConnectedNodes(request) => {
                    let source_label_id =
                        working_catalog.get_or_create_label(&request.source_label);
                    let target_label_id =
                        working_catalog.get_or_create_label(&request.target_label);
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let current_source = self.find_node_by_label_and_properties(
                        source_label_id,
                        &request.source_properties,
                    )?;
                    let current_target = self.find_node_by_label_and_properties(
                        target_label_id,
                        &request.target_properties,
                    )?;
                    let pending_source = pending_nodes
                        .iter()
                        .find(|(_, pending_label, pending_properties)| {
                            *pending_label == source_label_id
                                && pending_properties == &request.source_properties
                        })
                        .map(|(id, _, _)| *id);
                    let pending_target = pending_nodes
                        .iter()
                        .find(|(_, pending_label, pending_properties)| {
                            *pending_label == target_label_id
                                && pending_properties == &request.target_properties
                        })
                        .map(|(id, _, _)| *id);
                    let source = current_source.or(pending_source);
                    let target = current_target.or(pending_target);
                    if let (Some(source), Some(target)) = (source, target) {
                        let current_relationship = self.find_relationship_by_properties(
                            source,
                            target,
                            rel_type_id,
                            &request.rel_properties,
                        )?;
                        let pending_relationship = pending_relationships
                            .iter()
                            .find(
                                |(_, pending_source, pending_target, pending_type, properties)| {
                                    *pending_source == source
                                        && *pending_target == target
                                        && *pending_type == rel_type_id
                                        && properties == &request.rel_properties
                                },
                            )
                            .map(|(id, _, _, _, _)| *id);
                        if let Some(relationship) = current_relationship.or(pending_relationship) {
                            rows.push(merge_relationship_row(source, relationship, target, false));
                            continue;
                        }
                    }

                    let source_created = source.is_none();
                    let source = source.unwrap_or(NodeId(next_node_id));
                    if source.0 == next_node_id {
                        next_node_id += 1;
                        pending_nodes.push((
                            source,
                            source_label_id,
                            request.source_properties.clone(),
                        ));
                    }
                    let target_created = target.is_none();
                    let target = target.unwrap_or(NodeId(next_node_id));
                    if target.0 == next_node_id {
                        next_node_id += 1;
                        pending_nodes.push((
                            target,
                            target_label_id,
                            request.target_properties.clone(),
                        ));
                    }
                    let relationship = RelId(next_rel_id);
                    next_rel_id += 1;
                    if source_created {
                        ops.push(WalOp::CreateNode {
                            id: source,
                            label: request.source_label.clone(),
                            properties: request.source_properties.clone(),
                        });
                    }
                    if target_created {
                        ops.push(WalOp::CreateNode {
                            id: target,
                            label: request.target_label.clone(),
                            properties: request.target_properties.clone(),
                        });
                    }
                    ops.push(WalOp::CreateRelationship {
                        id: relationship,
                        source,
                        target,
                        rel_type: request.rel_type.clone(),
                        properties: request.rel_properties.clone(),
                    });
                    pending_relationships.push((
                        relationship,
                        source,
                        target,
                        rel_type_id,
                        request.rel_properties.clone(),
                    ));
                    rows.push(merge_relationship_row(source, relationship, target, true));
                }
                GraphMutation::SetNodeProperty {
                    label,
                    filter,
                    property,
                    value,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        let assignment = [NodeSetAssignment {
                            property: property.clone(),
                            value: NodeSetValue::Value(value.clone()),
                        }];
                        self.apply_set_node_properties_mutation(
                            &mut ops,
                            &mut rows,
                            &mut pending_nodes,
                            label_id,
                            filter.as_ref(),
                            &assignment,
                            limits,
                        )?;
                    }
                }
                GraphMutation::SetNodePropertyAddInt {
                    label,
                    filter,
                    property,
                    amount,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        let assignment = [NodeSetAssignment {
                            property: property.clone(),
                            value: NodeSetValue::AddInt { amount },
                        }];
                        self.apply_set_node_properties_mutation(
                            &mut ops,
                            &mut rows,
                            &mut pending_nodes,
                            label_id,
                            filter.as_ref(),
                            &assignment,
                            limits,
                        )?;
                    }
                }
                GraphMutation::SetNodeProperties {
                    label,
                    filter,
                    assignments,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        self.apply_set_node_properties_mutation(
                            &mut ops,
                            &mut rows,
                            &mut pending_nodes,
                            label_id,
                            filter.as_ref(),
                            &assignments,
                            limits,
                        )?;
                    }
                }
                GraphMutation::SetRelationshipProperty {
                    source_label,
                    filter,
                    rel_type,
                    target_label,
                    target_filter,
                    rel_filter,
                    property,
                    value,
                } => {
                    let assignments = vec![RelationshipSetAssignment { property, value }];
                    apply_set_relationship_properties_mutation(
                        self,
                        &working_catalog,
                        &mut ops,
                        &mut rows,
                        &pending_nodes,
                        &mut pending_relationships,
                        RelationshipPropertiesUpdate {
                            source_label,
                            filter,
                            rel_type,
                            target_label,
                            target_filter,
                            rel_filter,
                            assignments,
                        },
                        limits,
                    )?;
                }
                GraphMutation::SetRelationshipProperties {
                    source_label,
                    filter,
                    rel_type,
                    target_label,
                    target_filter,
                    rel_filter,
                    assignments,
                } => {
                    apply_set_relationship_properties_mutation(
                        self,
                        &working_catalog,
                        &mut ops,
                        &mut rows,
                        &pending_nodes,
                        &mut pending_relationships,
                        RelationshipPropertiesUpdate {
                            source_label,
                            filter,
                            rel_type,
                            target_label,
                            target_filter,
                            rel_filter,
                            assignments,
                        },
                        limits,
                    )?;
                }
                GraphMutation::DeleteNode {
                    label,
                    filter,
                    detach,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        let committed_ids = self.matching_node_ids_bounded(
                            label_id,
                            filter.as_ref(),
                            remaining_mutation_affected_rows(rows.len(), limits)?,
                            "max_mutation_affected_rows",
                        )?;
                        let pending_ids = Self::pending_node_ids_matching(
                            label_id,
                            filter.as_ref(),
                            &pending_nodes,
                        );
                        let mut delete_ids = committed_ids.clone();
                        delete_ids.extend(pending_ids.iter().copied());
                        let incident_pending_relationship_ids =
                            pending_relationship_ids_for_nodes(&pending_relationships, &delete_ids);
                        if !detach && !incident_pending_relationship_ids.is_empty() {
                            let id = delete_ids.first().copied().unwrap_or(NodeId(0));
                            return Err(SkeinError::Storage(format!(
                                "node {} has relationships; use DETACH DELETE",
                                id.0
                            )));
                        }
                        ensure_additional_mutation_limits(
                            ops.len(),
                            rows.len(),
                            0,
                            delete_ids.len(),
                            limits,
                        )?;
                        let delete_ops = self.delete_node_ops_bounded(
                            &committed_ids,
                            detach,
                            remaining_mutation_operations(ops.len(), limits)?,
                        )?;
                        if detach {
                            for relationship_id in incident_pending_relationship_ids {
                                remove_pending_relationship(
                                    &mut ops,
                                    &mut pending_relationships,
                                    relationship_id,
                                );
                            }
                        }
                        for id in &pending_ids {
                            remove_pending_node(&mut ops, &mut pending_nodes, *id);
                        }
                        for id in delete_ids {
                            rows.push(BTreeMap::from([(
                                "node_id".to_string(),
                                Value::Int(id.0 as i64),
                            )]));
                        }
                        ops.extend(delete_ops);
                    }
                }
                GraphMutation::DeleteRelationship {
                    source_label,
                    filter,
                    rel_type,
                    target_label,
                    target_filter,
                    rel_filter,
                } => {
                    if let (Some(source_label_id), Some(target_label_id), Some(rel_type_id)) = (
                        working_catalog.label_id(&source_label),
                        working_catalog.label_id(&target_label),
                        working_catalog.rel_type_id(&rel_type),
                    ) {
                        let source_ids = self
                            .matching_node_ids_with_pending_bounded(
                                Some(source_label_id),
                                filter.as_ref(),
                                &pending_nodes,
                                remaining_mutation_affected_rows(rows.len(), limits)?,
                                "max_mutation_affected_rows",
                            )?
                            .into_iter()
                            .collect::<BTreeSet<_>>();
                        let target_ids = target_filter
                            .as_ref()
                            .map(|filter| {
                                self.matching_node_ids_with_pending_bounded(
                                    Some(target_label_id),
                                    Some(filter),
                                    &pending_nodes,
                                    remaining_mutation_affected_rows(rows.len(), limits)?,
                                    "max_mutation_affected_rows",
                                )
                                .map(|ids| ids.into_iter().collect::<BTreeSet<_>>())
                            })
                            .transpose()?;
                        if target_ids.as_ref().is_some_and(BTreeSet::is_empty) {
                            continue;
                        }
                        for relationship in self.relationship_records_owned() {
                            let relationship = relationship?;
                            if relationship.rel_type != rel_type_id
                                || !source_ids.contains(&relationship.source)
                            {
                                continue;
                            }
                            if rel_filter
                                .as_ref()
                                .map(|filter| {
                                    !property_filter_matches(
                                        filter,
                                        relationship.id.0,
                                        &relationship.properties,
                                    )
                                })
                                .unwrap_or(false)
                            {
                                continue;
                            }
                            let target_matches = self
                                .node_owned(relationship.target)?
                                .map(|target| {
                                    target.labels.contains(&target_label_id)
                                        && target_ids
                                            .as_ref()
                                            .map(|ids| ids.contains(&relationship.target))
                                            .unwrap_or(true)
                                })
                                .unwrap_or(false);
                            if target_matches {
                                ensure_additional_mutation_limits(
                                    ops.len(),
                                    rows.len(),
                                    1,
                                    1,
                                    limits,
                                )?;
                                ops.push(WalOp::DeleteRelationship {
                                    id: relationship.id,
                                });
                                rows.push(BTreeMap::from([(
                                    "rel_id".to_string(),
                                    Value::Int(relationship.id.0 as i64),
                                )]));
                            }
                        }
                        let mut pending_delete_ids = Vec::new();
                        for (relationship_id, source, target, pending_rel_type_id, properties) in
                            &pending_relationships
                        {
                            if *pending_rel_type_id != rel_type_id
                                || !source_ids.contains(source)
                                || rel_filter.as_ref().is_some_and(|filter| {
                                    !property_filter_matches(filter, relationship_id.0, properties)
                                })
                                || !node_matches_label_and_filter(
                                    self,
                                    &pending_nodes,
                                    *target,
                                    target_label_id,
                                    target_filter.as_ref(),
                                )?
                            {
                                continue;
                            }
                            pending_delete_ids.push(*relationship_id);
                        }
                        for relationship_id in pending_delete_ids {
                            ensure_additional_mutation_limits(ops.len(), rows.len(), 0, 1, limits)?;
                            remove_pending_relationship(
                                &mut ops,
                                &mut pending_relationships,
                                relationship_id,
                            );
                            rows.push(BTreeMap::from([(
                                "rel_id".to_string(),
                                Value::Int(relationship_id.0 as i64),
                            )]));
                        }
                    }
                }
                GraphMutation::DeleteRelationshipTargetNodes(request) => {
                    let ids = self.relationship_target_node_ids_with_pending_bounded(
                        &working_catalog,
                        &request,
                        &pending_nodes,
                        &pending_relationships,
                        remaining_mutation_affected_rows(rows.len(), limits)?,
                    )?;
                    let mut committed_ids = Vec::new();
                    for id in &ids {
                        if self.node_owned(*id)?.is_some() {
                            committed_ids.push(*id);
                        }
                    }
                    let pending_ids = ids
                        .iter()
                        .copied()
                        .filter(|id| {
                            pending_nodes
                                .iter()
                                .any(|(pending_id, _, _)| pending_id == id)
                        })
                        .collect::<Vec<_>>();
                    let incident_pending_relationship_ids =
                        pending_relationship_ids_for_nodes(&pending_relationships, &ids);
                    if !request.detach && !incident_pending_relationship_ids.is_empty() {
                        let id = ids.first().copied().unwrap_or(NodeId(0));
                        return Err(SkeinError::Storage(format!(
                            "node {} has relationships; use DETACH DELETE",
                            id.0
                        )));
                    }
                    ensure_additional_mutation_limits(ops.len(), rows.len(), 0, ids.len(), limits)?;
                    let delete_ops = self.delete_node_ops_bounded(
                        &committed_ids,
                        request.detach,
                        remaining_mutation_operations(ops.len(), limits)?,
                    )?;
                    if request.detach {
                        for relationship_id in incident_pending_relationship_ids {
                            remove_pending_relationship(
                                &mut ops,
                                &mut pending_relationships,
                                relationship_id,
                            );
                        }
                    }
                    for id in &pending_ids {
                        remove_pending_node(&mut ops, &mut pending_nodes, *id);
                    }
                    ops.extend(delete_ops);
                    rows.extend(ids.into_iter().map(|id| {
                        BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))])
                    }));
                }
                GraphMutation::CreateRelationshipsBetweenMatches(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    let target_label_id =
                        optional_label_id(&working_catalog, &request.target_label);
                    if (!request.source_label.is_empty() && source_label_id.is_none())
                        || (!request.target_label.is_empty() && target_label_id.is_none())
                    {
                        continue;
                    }
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let remaining_rows = remaining_mutation_affected_rows(rows.len(), limits)?;
                    let sources = self.matching_node_ids_with_pending_bounded(
                        source_label_id,
                        request.source_filter.as_ref(),
                        &pending_nodes,
                        remaining_rows,
                        "max_mutation_affected_rows",
                    )?;
                    let targets = self.matching_node_ids_with_pending_bounded(
                        target_label_id,
                        request.target_filter.as_ref(),
                        &pending_nodes,
                        remaining_rows,
                        "max_mutation_affected_rows",
                    )?;
                    let pair_count = sources.len().checked_mul(targets.len()).ok_or_else(|| {
                        SkeinError::Execution("mutation Cartesian product overflow".to_string())
                    })?;
                    ensure_additional_mutation_limits(
                        ops.len(),
                        rows.len(),
                        pair_count,
                        pair_count,
                        limits,
                    )?;
                    for source in sources {
                        for target in &targets {
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.rel_type.clone(),
                                properties: request.rel_properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                rel_type_id,
                                request.rel_properties.clone(),
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsBetweenMatches(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    let target_label_id =
                        optional_label_id(&working_catalog, &request.target_label);
                    if (!request.source_label.is_empty() && source_label_id.is_none())
                        || (!request.target_label.is_empty() && target_label_id.is_none())
                    {
                        continue;
                    }
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let remaining_rows = remaining_mutation_affected_rows(rows.len(), limits)?;
                    let sources = self.matching_node_ids_with_pending_bounded(
                        source_label_id,
                        request.source_filter.as_ref(),
                        &pending_nodes,
                        remaining_rows,
                        "max_mutation_affected_rows",
                    )?;
                    let targets = self.matching_node_ids_with_pending_bounded(
                        target_label_id,
                        request.target_filter.as_ref(),
                        &pending_nodes,
                        remaining_rows,
                        "max_mutation_affected_rows",
                    )?;
                    let pair_count = sources.len().checked_mul(targets.len()).ok_or_else(|| {
                        SkeinError::Execution("mutation Cartesian product overflow".to_string())
                    })?;
                    ensure_additional_mutation_limits(
                        ops.len(),
                        rows.len(),
                        pair_count,
                        pair_count,
                        limits,
                    )?;
                    for source in sources {
                        for target in &targets {
                            let current = self.find_relationship_by_property_subset(
                                source,
                                *target,
                                rel_type_id,
                                &request.rel_match_properties,
                            )?;
                            let pending = pending_relationships
                                .iter()
                                .find(
                                    |(
                                        _,
                                        pending_source,
                                        pending_target,
                                        pending_type,
                                        properties,
                                    )| {
                                        *pending_source == source
                                            && *pending_target == *target
                                            && *pending_type == rel_type_id
                                            && properties_contain_all(
                                                properties,
                                                &request.rel_match_properties,
                                            )
                                    },
                                )
                                .map(|(id, _, _, _, _)| *id);
                            if let Some(relationship) = current.or(pending) {
                                rows.push(BTreeMap::from([
                                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                    ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                    ("created".to_string(), Value::Bool(false)),
                                ]));
                                continue;
                            }
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            let mut properties = request.rel_match_properties.clone();
                            for (property, value) in &request.on_create_properties {
                                properties.insert(property.clone(), value.clone());
                            }
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.rel_type.clone(),
                                properties: properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                rel_type_id,
                                properties,
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(true)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsToMatchedTarget(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    if !request.source_label.is_empty() && source_label_id.is_none() {
                        continue;
                    }
                    let Some(old_target_label_id) =
                        working_catalog.label_id(&request.old_target_label)
                    else {
                        continue;
                    };
                    let Some(new_target_label_id) =
                        working_catalog.label_id(&request.new_target_label)
                    else {
                        continue;
                    };
                    let Some(old_rel_type_id) = working_catalog.rel_type_id(&request.old_rel_type)
                    else {
                        continue;
                    };
                    let new_rel_type_id =
                        working_catalog.get_or_create_rel_type(&request.new_rel_type);
                    let remaining_rows = remaining_mutation_affected_rows(rows.len(), limits)?;
                    let source_ids = relationships_with_pending_matching_bounded(
                        self,
                        &pending_nodes,
                        &pending_relationships,
                        RelationshipMatchRequest {
                            rel_type_id: old_rel_type_id,
                            source_label_id,
                            source_filter: request.source_filter.as_ref(),
                            target_label_id: Some(old_target_label_id),
                            target_filter: request.old_target_filter.as_ref(),
                            rel_properties: &request.old_rel_filter,
                        },
                        remaining_rows,
                    )?
                    .into_iter()
                    .map(|relationship| relationship.source)
                    .collect::<BTreeSet<_>>();
                    let target_ids = self
                        .matching_node_ids_with_pending_bounded(
                            Some(new_target_label_id),
                            request.new_target_filter.as_ref(),
                            &pending_nodes,
                            remaining_rows,
                            "max_mutation_affected_rows",
                        )?
                        .into_iter()
                        .collect::<Vec<_>>();
                    let pair_count =
                        source_ids
                            .len()
                            .checked_mul(target_ids.len())
                            .ok_or_else(|| {
                                SkeinError::Execution(
                                    "mutation Cartesian product overflow".to_string(),
                                )
                            })?;
                    ensure_additional_mutation_limits(
                        ops.len(),
                        rows.len(),
                        pair_count,
                        pair_count,
                        limits,
                    )?;
                    for source in source_ids {
                        for target in &target_ids {
                            let current = self.find_relationship_by_property_subset(
                                source,
                                *target,
                                new_rel_type_id,
                                &request.new_rel_match_properties,
                            )?;
                            let pending = pending_relationships
                                .iter()
                                .find(
                                    |(
                                        _,
                                        pending_source,
                                        pending_target,
                                        pending_type,
                                        properties,
                                    )| {
                                        *pending_source == source
                                            && *pending_target == *target
                                            && *pending_type == new_rel_type_id
                                            && properties_contain_all(
                                                properties,
                                                &request.new_rel_match_properties,
                                            )
                                    },
                                )
                                .map(|(id, _, _, _, _)| *id);
                            if let Some(relationship) = current.or(pending) {
                                rows.push(BTreeMap::from([
                                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                    ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                    ("created".to_string(), Value::Bool(false)),
                                ]));
                                continue;
                            }
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            let mut properties = request.new_rel_match_properties.clone();
                            properties.extend(request.on_create_properties.clone());
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.new_rel_type.clone(),
                                properties: properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                new_rel_type_id,
                                properties,
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(true)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsFromMatchedTarget(request) => {
                    let old_source_label_id =
                        optional_label_id(&working_catalog, &request.old_source_label);
                    if !request.old_source_label.is_empty() && old_source_label_id.is_none() {
                        continue;
                    }
                    let Some(old_target_label_id) =
                        working_catalog.label_id(&request.old_target_label)
                    else {
                        continue;
                    };
                    let new_source_label_id =
                        optional_label_id(&working_catalog, &request.new_source_label);
                    if !request.new_source_label.is_empty() && new_source_label_id.is_none() {
                        continue;
                    }
                    let Some(old_rel_type_id) = working_catalog.rel_type_id(&request.old_rel_type)
                    else {
                        continue;
                    };
                    let new_rel_type_id =
                        working_catalog.get_or_create_rel_type(&request.new_rel_type);
                    let remaining_rows = remaining_mutation_affected_rows(rows.len(), limits)?;
                    let target_ids = relationships_with_pending_matching_bounded(
                        self,
                        &pending_nodes,
                        &pending_relationships,
                        RelationshipMatchRequest {
                            rel_type_id: old_rel_type_id,
                            source_label_id: old_source_label_id,
                            source_filter: request.old_source_filter.as_ref(),
                            target_label_id: Some(old_target_label_id),
                            target_filter: request.old_target_filter.as_ref(),
                            rel_properties: &request.old_rel_filter,
                        },
                        remaining_rows,
                    )?
                    .into_iter()
                    .map(|relationship| relationship.target)
                    .collect::<BTreeSet<_>>();
                    let source_ids = self
                        .matching_node_ids_with_pending_bounded(
                            new_source_label_id,
                            request.new_source_filter.as_ref(),
                            &pending_nodes,
                            remaining_rows,
                            "max_mutation_affected_rows",
                        )?
                        .into_iter()
                        .collect::<Vec<_>>();
                    let pair_count =
                        source_ids
                            .len()
                            .checked_mul(target_ids.len())
                            .ok_or_else(|| {
                                SkeinError::Execution(
                                    "mutation Cartesian product overflow".to_string(),
                                )
                            })?;
                    ensure_additional_mutation_limits(
                        ops.len(),
                        rows.len(),
                        pair_count,
                        pair_count,
                        limits,
                    )?;
                    for source in source_ids {
                        for target in &target_ids {
                            let current = self.find_relationship_by_property_subset(
                                source,
                                *target,
                                new_rel_type_id,
                                &request.new_rel_match_properties,
                            )?;
                            let pending = pending_relationships
                                .iter()
                                .find(
                                    |(
                                        _,
                                        pending_source,
                                        pending_target,
                                        pending_type,
                                        properties,
                                    )| {
                                        *pending_source == source
                                            && *pending_target == *target
                                            && *pending_type == new_rel_type_id
                                            && properties_contain_all(
                                                properties,
                                                &request.new_rel_match_properties,
                                            )
                                    },
                                )
                                .map(|(id, _, _, _, _)| *id);
                            if let Some(relationship) = current.or(pending) {
                                rows.push(BTreeMap::from([
                                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                    ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                    ("created".to_string(), Value::Bool(false)),
                                ]));
                                continue;
                            }
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            let mut properties = request.new_rel_match_properties.clone();
                            properties.extend(request.on_create_properties.clone());
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.new_rel_type.clone(),
                                properties: properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                new_rel_type_id,
                                properties,
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(true)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsFromMatchedRelationships(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    let target_label_id =
                        optional_label_id(&working_catalog, &request.target_label);
                    if (!request.source_label.is_empty() && source_label_id.is_none())
                        || (!request.target_label.is_empty() && target_label_id.is_none())
                    {
                        continue;
                    }
                    let Some(old_rel_type_id) = working_catalog.rel_type_id(&request.old_rel_type)
                    else {
                        continue;
                    };
                    let new_rel_type_id =
                        working_catalog.get_or_create_rel_type(&request.new_rel_type);
                    let old_relationships = relationships_with_pending_matching_bounded(
                        self,
                        &pending_nodes,
                        &pending_relationships,
                        RelationshipMatchRequest {
                            rel_type_id: old_rel_type_id,
                            source_label_id,
                            source_filter: request.source_filter.as_ref(),
                            target_label_id,
                            target_filter: request.target_filter.as_ref(),
                            rel_properties: &request.old_rel_filter,
                        },
                        remaining_mutation_affected_rows(rows.len(), limits)?,
                    )?;
                    ensure_additional_mutation_limits(
                        ops.len(),
                        rows.len(),
                        old_relationships.len(),
                        old_relationships.len(),
                        limits,
                    )?;
                    for old_relationship in old_relationships {
                        let current = self.find_relationship_by_property_subset(
                            old_relationship.source,
                            old_relationship.target,
                            new_rel_type_id,
                            &request.new_rel_match_properties,
                        )?;
                        let pending = pending_relationships
                            .iter()
                            .find(
                                |(_, pending_source, pending_target, pending_type, properties)| {
                                    *pending_source == old_relationship.source
                                        && *pending_target == old_relationship.target
                                        && *pending_type == new_rel_type_id
                                        && properties_contain_all(
                                            properties,
                                            &request.new_rel_match_properties,
                                        )
                                },
                            )
                            .map(|(id, _, _, _, _)| *id);
                        if let Some(relationship) = current.or(pending) {
                            rows.push(BTreeMap::from([
                                (
                                    "source_node_id".to_string(),
                                    Value::Int(old_relationship.source.0 as i64),
                                ),
                                (
                                    "target_node_id".to_string(),
                                    Value::Int(old_relationship.target.0 as i64),
                                ),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(false)),
                            ]));
                            continue;
                        }
                        let relationship = RelId(next_rel_id);
                        next_rel_id += 1;
                        let mut properties = request.new_rel_match_properties.clone();
                        for (property, value) in &request.on_create_properties {
                            let value = match value {
                                RelationshipOnCreatePropertyValue::Value(value) => value.clone(),
                                RelationshipOnCreatePropertyValue::MatchedRelationshipProperty {
                                    property,
                                } => old_relationship
                                    .properties
                                    .get(property)
                                    .cloned()
                                    .unwrap_or(Value::Null),
                            };
                            properties.insert(property.clone(), value);
                        }
                        ops.push(WalOp::CreateRelationship {
                            id: relationship,
                            source: old_relationship.source,
                            target: old_relationship.target,
                            rel_type: request.new_rel_type.clone(),
                            properties: properties.clone(),
                        });
                        pending_relationships.push((
                            relationship,
                            old_relationship.source,
                            old_relationship.target,
                            new_rel_type_id,
                            properties,
                        ));
                        rows.push(BTreeMap::from([
                            (
                                "source_node_id".to_string(),
                                Value::Int(old_relationship.source.0 as i64),
                            ),
                            (
                                "target_node_id".to_string(),
                                Value::Int(old_relationship.target.0 as i64),
                            ),
                            ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateConnectedNodes(request) => {
                    let source_label_id =
                        working_catalog.get_or_create_label(&request.source_label);
                    let target_label_id =
                        working_catalog.get_or_create_label(&request.target_label);
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let source_properties = request.source_properties.clone();
                    let target_properties = request.target_properties.clone();
                    let rel_properties = request.rel_properties.clone();
                    let source = NodeId(next_node_id);
                    let target = NodeId(next_node_id + 1);
                    let relationship = RelId(next_rel_id);
                    next_node_id += 2;
                    next_rel_id += 1;
                    ops.push(WalOp::CreateNode {
                        id: source,
                        label: request.source_label,
                        properties: request.source_properties,
                    });
                    ops.push(WalOp::CreateNode {
                        id: target,
                        label: request.target_label,
                        properties: request.target_properties,
                    });
                    ops.push(WalOp::CreateRelationship {
                        id: relationship,
                        source,
                        target,
                        rel_type: request.rel_type,
                        properties: request.rel_properties,
                    });
                    pending_relationships.push((
                        relationship,
                        source,
                        target,
                        rel_type_id,
                        rel_properties,
                    ));
                    pending_nodes.push((source, source_label_id, source_properties));
                    pending_nodes.push((target, target_label_id, target_properties));
                    rows.push(BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                    ]));
                }
            }
            ensure_mutation_commit_limits(&ops, &rows, limits)?;
        }

        self.commit_prepared_mutation_ops(
            catalog,
            working_catalog,
            ops,
            rows,
            relational_transaction,
            limits,
            preserve_single_create_wal,
            captured_graph_ops,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_prepared_mutation_ops(
        &mut self,
        catalog: &mut Catalog,
        working_catalog: Catalog,
        mut ops: Vec<WalOp>,
        rows: Vec<BTreeMap<String, Value>>,
        relational_transaction: Option<RelationalTransaction>,
        limits: MutationLimits,
        preserve_single_create_wal: bool,
        captured_graph_ops: Option<&mut Vec<WalOp>>,
    ) -> Result<MutationSummary> {
        ensure_mutation_commit_limits(&ops, &rows, limits)?;
        if let Some(captured_graph_ops) = captured_graph_ops {
            captured_graph_ops.extend(ops.iter().cloned());
        }
        let mut staged_relational_state = None;
        if let Some(transaction) = relational_transaction.filter(|value| !value.writes.is_empty()) {
            staged_relational_state = Some(
                self.relational_state
                    .stage_transaction(
                        transaction.clone(),
                        self.relational_mutation_limits,
                        self.relational_overflow_config,
                    )
                    .map_err(|error| SkeinError::Storage(error.to_string()))?,
            );
            let record = encode_relational_wal_batch(self.commit_epoch + 1, &transaction)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
            ops.push(WalOp::Relational {
                record: Arc::from(record),
            });
        }
        if ops.is_empty() {
            return Ok(MutationSummary { rows });
        }
        self.validate_constraints_for_ops(&working_catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            if preserve_single_create_wal
                && let [WalOp::CreateNode {
                    id,
                    label,
                    properties,
                }] = ops.as_slice()
            {
                durable.append_create_node(*id, label, properties)?;
            } else {
                durable.append_batch(ops.clone())?;
            }
        }
        *catalog = working_catalog;
        self.record_search_projection_graph_changes_for_ops(catalog, self.commit_epoch + 1, &ops);
        for op in ops {
            if matches!(op, WalOp::Relational { .. }) {
                self.relational_state = staged_relational_state
                    .take()
                    .expect("relational WAL operation must have staged state");
            } else {
                self.apply_wal_op(catalog, op)?;
            }
        }
        self.commit_epoch += 1;
        Ok(MutationSummary { rows })
    }

    fn apply_pending_node_assignments(
        ops: &mut [WalOp],
        pending_nodes: &mut [PendingNode],
        id: NodeId,
        assignments: &[NodeSetAssignment],
    ) -> Result<()> {
        let Some((_, _, properties)) = pending_nodes
            .iter_mut()
            .find(|(pending_id, _, _)| *pending_id == id)
        else {
            return Ok(());
        };
        for assignment in assignments {
            let value = evaluate_node_set_value(properties, assignment)?;
            properties.insert(assignment.property.clone(), value.clone());
            for op in ops.iter_mut() {
                if let WalOp::CreateNode {
                    id: create_id,
                    properties,
                    ..
                } = op
                    && *create_id == id
                {
                    properties.insert(assignment.property.clone(), value.clone());
                    break;
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_set_node_properties_mutation(
        &self,
        ops: &mut Vec<WalOp>,
        rows: &mut Vec<BTreeMap<String, Value>>,
        pending_nodes: &mut [PendingNode],
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
        assignments: &[NodeSetAssignment],
        limits: MutationLimits,
    ) -> Result<()> {
        let remaining_rows = remaining_mutation_affected_rows(rows.len(), limits)?;
        let committed_ids = self.matching_node_ids_bounded(
            label_id,
            filter,
            remaining_rows,
            "max_mutation_affected_rows",
        )?;
        let pending_ids = Self::pending_node_ids_matching(label_id, filter, pending_nodes);
        ensure_additional_mutation_limits(
            ops.len(),
            rows.len(),
            committed_ids.len().saturating_mul(assignments.len()),
            committed_ids.len().saturating_add(pending_ids.len()),
            limits,
        )?;
        ops.extend(self.node_set_property_ops(&committed_ids, assignments)?);
        for id in &pending_ids {
            Self::apply_pending_node_assignments(ops, pending_nodes, *id, assignments)?;
        }
        rows.extend(
            committed_ids
                .into_iter()
                .chain(pending_ids)
                .map(|id| BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))])),
        );
        Ok(())
    }

    pub fn checkpoint(&mut self, catalog: &Catalog) -> Result<()> {
        self.checkpoint_with_reader_epoch(catalog, None)
    }

    pub fn backup_to(
        &mut self,
        catalog: &Catalog,
        destination: impl AsRef<Path>,
    ) -> Result<StorageBackupReport> {
        if self.durable.is_none() {
            return Err(SkeinError::Storage(
                "an in-memory database cannot create a durable backup".to_string(),
            ));
        }
        self.checkpoint(catalog)?;
        self.durable
            .as_ref()
            .expect("durable store must exist after checkpoint")
            .backup_to(destination.as_ref())
    }

    pub fn scrub_storage(&mut self) -> Result<StorageScrubReport> {
        self.ensure_usable()?;
        let durable = self.durable.as_ref().ok_or_else(|| {
            SkeinError::Storage("an in-memory database has no durable storage to scrub".to_string())
        })?;
        let result = durable.scrub_storage();
        if result.is_err() {
            self.integrity_poisoned.store(true, AtomicOrdering::Release);
        }
        result
    }

    pub fn checkpoint_with_reader_epoch(
        &mut self,
        catalog: &Catalog,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> Result<()> {
        self.checkpoint_with_reader_epoch_and_build_config(
            catalog,
            oldest_reader_commit_epoch,
            DerivedArtifactBuildConfig::default(),
        )
    }

    fn checkpoint_with_reader_epoch_and_build_config(
        &mut self,
        catalog: &Catalog,
        oldest_reader_commit_epoch: Option<u64>,
        build_config: DerivedArtifactBuildConfig,
    ) -> Result<()> {
        let Some(prepared) = self.prepare_checkpoint_with_build_config(catalog, build_config)?
        else {
            return Ok(());
        };
        self.publish_prepared_checkpoint(prepared, oldest_reader_commit_epoch)
    }

    pub(crate) fn checkpoint_source(&self) -> Self {
        let mut source = self.snapshot();
        source.durable = self.durable.clone();
        source
    }

    pub(crate) fn prepare_checkpoint(
        &self,
        catalog: &Catalog,
    ) -> Result<Option<PreparedCheckpoint>> {
        self.prepare_checkpoint_with_build_config(catalog, DerivedArtifactBuildConfig::default())
    }

    fn prepare_checkpoint_with_build_config(
        &self,
        catalog: &Catalog,
        build_config: DerivedArtifactBuildConfig,
    ) -> Result<Option<PreparedCheckpoint>> {
        let Some(durable) = self.durable.as_ref() else {
            return Ok(None);
        };
        let estimated_record_bytes = self.estimated_logical_record_bytes();
        let checkpoint_out_of_core = match self.residency_mode {
            StorageResidencyMode::Materialized => false,
            StorageResidencyMode::OutOfCore => true,
            StorageResidencyMode::Auto => {
                self.canonical_base_out_of_core
                    || estimated_record_bytes > self.auto_materialize_checkpoint_bytes
            }
        };
        let (projected_graph_artifacts, artifacts) = if checkpoint_out_of_core {
            (None, BTreeMap::new())
        } else {
            let encoded =
                encode_projected_graph_artifacts(catalog, self, self.next_projection_epoch());
            let (_, artifacts) = decode_projected_graph_artifacts(&encoded)?;
            (Some(encoded), artifacts)
        };
        let source_scan_projection = (!checkpoint_out_of_core).then(|| {
            source_scan::build(
                self.commit_epoch,
                catalog.label_id("Source"),
                self.nodes.values(),
            )
        });
        let merged_nodes = self.canonical_base.as_ref().map(|_| {
            self.node_records_owned().map(|record| {
                record.map_err(|error| CanonicalSegmentError::Source(error.to_string()))
            })
        });
        let property_projection_nodes = self.canonical_base.as_ref().map(|_| {
            self.node_records_owned().map(|record| {
                record.map_err(|error| {
                    skein_storage::PersistentPropertyProjectionError::Source(error.to_string())
                })
            })
        });
        let property_projection_definitions = catalog
            .property_indexes()
            .filter_map(|index| {
                let kind = match index.kind {
                    IndexKind::Range => PersistentPropertyProjectionKind::Range,
                    IndexKind::FullText => PersistentPropertyProjectionKind::FullText,
                    IndexKind::Equality => return None,
                };
                Some(PersistentPropertyProjectionDefinition {
                    label_id: index.label_id,
                    property: index.property.clone(),
                    kind,
                    complete: false,
                })
            })
            .collect::<Vec<_>>();
        let merged_relationships = self.canonical_base.as_ref().map(|_| {
            self.relationship_records_owned().map(|record| {
                record.map_err(|error| CanonicalSegmentError::Source(error.to_string()))
            })
        });
        let adjacency_relationships = self.canonical_base.as_ref().map(|_| {
            self.relationship_records_owned().map(|record| {
                record.map_err(|error| {
                    skein_storage::CanonicalAdjacencyError::Source(error.to_string())
                })
            })
        });
        let commit_epoch = self.commit_epoch;
        let checkpoint_statistics = if checkpoint_out_of_core && !self.canonical_base_out_of_core {
            graph_statistics_from_basic(self.basic_statistics(), false)
        } else {
            self.statistics()
        };
        let generation = durable.checkpoint_epoch.saturating_add(1);
        let staging_path = durable.prepare_checkpoint_staging(generation)?;
        let prepared = (|| {
            if let Some(encoded) = projected_graph_artifacts.as_deref() {
                durable.write_projected_graph_artifacts_to(
                    &staging_path.join(PROJECTED_GRAPHS_FILE),
                    encoded,
                )?;
            }
            let source_scan_publication = source_scan_projection
                .map(|mut projection| source_scan::write(&staging_path, &mut projection))
                .transpose()?;
            let (canonical_manifest_artifact, property_spill_manifest_artifact) =
                match (merged_nodes, merged_relationships) {
                    (Some(nodes), Some(relationships)) => {
                        durable.write_canonical_segments(nodes, relationships, generation)?
                    }
                    (None, None) => durable.write_canonical_segments(
                        self.nodes.values().map(|node| Ok(node.clone())),
                        self.relationships
                            .values()
                            .map(|relationship| Ok(relationship.clone())),
                        generation,
                    )?,
                    _ => unreachable!("canonical base iterators are created together"),
                };
            let canonical_adjacency_manifest_artifact = match adjacency_relationships {
                Some(relationships) => durable.write_canonical_adjacency(
                    relationships,
                    generation,
                    build_config.adjacency,
                )?,
                None => durable.write_canonical_adjacency(
                    self.relationships.values().cloned().map(Ok),
                    generation,
                    build_config.adjacency,
                )?,
            };
            let property_projection_manifest_artifact = match property_projection_nodes {
                Some(nodes) => durable.write_persistent_property_projection(
                    property_projection_definitions,
                    nodes,
                    generation,
                    commit_epoch,
                    build_config.property_projection,
                )?,
                None => durable.write_persistent_property_projection(
                    property_projection_definitions,
                    self.nodes.values().cloned().map(Ok),
                    generation,
                    commit_epoch,
                    build_config.property_projection,
                )?,
            };
            let relational_checkpoint_artifact = durable.write_relational_checkpoint(
                &self.relational_state,
                commit_epoch,
                generation,
            )?;
            let checkpoint_relational_state = relational_checkpoint_artifact
                .map(|_| {
                    decode_relational_checkpoint_file(
                        &durable
                            .root_path()
                            .join(relational_checkpoint_generation_file(generation)),
                        RelationalDecodeLimits::checkpoint(),
                    )
                    .map(|checkpoint| checkpoint.state)
                    .map_err(|error| SkeinError::Storage(error.to_string()))
                })
                .transpose()?;
            let checkpoint_artifact = durable.write_checkpoint(
                CheckpointImage {
                    catalog,
                    commit_epoch,
                    next_node_id: self.next_node_id,
                    next_rel_id: self.next_rel_id,
                    search_projection_change_log_start_epoch: self
                        .search_projection_change_log_start_epoch,
                    search_projection_graph_changes: &self.search_projection_graph_changes,
                    statistics: &checkpoint_statistics,
                    projected_graphs: &self.projected_graphs,
                    initial_import_source_fingerprint: self
                        .initial_import_source_fingerprint
                        .as_deref(),
                    relational_checkpoint: relational_checkpoint_artifact,
                },
                generation,
            )?;
            checkpoint_publish_failpoint(CheckpointPublishStage::CheckpointPersisted)?;
            durable.prepare_wal_generation(generation)?;
            checkpoint_publish_failpoint(CheckpointPublishStage::WalPrepared)?;
            Ok(PreparedCheckpoint {
                source_commit_epoch: commit_epoch,
                source_checkpoint_epoch: durable.checkpoint_epoch,
                source_next_lsn: durable.next_lsn,
                generation,
                checkpoint_out_of_core,
                projected_graph_artifacts: artifacts,
                publish_projected_graph_artifacts: projected_graph_artifacts.is_some(),
                source_scan_publication,
                checkpoint_statistics,
                checkpoint_relational_state,
                manifest_artifacts: CheckpointManifestArtifacts {
                    checkpoint: checkpoint_artifact,
                    relational_checkpoint: relational_checkpoint_artifact,
                    canonical_manifest: canonical_manifest_artifact,
                    canonical_adjacency_manifest: canonical_adjacency_manifest_artifact,
                    property_spill_manifest: property_spill_manifest_artifact,
                    property_projection_manifest: property_projection_manifest_artifact,
                },
                staging_path: staging_path.clone(),
            })
        })();
        if prepared.is_err() {
            let _ = durable.discard_prepared_checkpoint(generation, &staging_path);
        }
        prepared.map(Some)
    }

    pub(crate) fn publish_prepared_checkpoint(
        &mut self,
        prepared: PreparedCheckpoint,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> Result<()> {
        let durable = self.durable.as_mut().ok_or_else(|| {
            SkeinError::Storage("prepared checkpoint requires durable storage".to_string())
        })?;
        if self.commit_epoch != prepared.source_commit_epoch
            || durable.checkpoint_epoch != prepared.source_checkpoint_epoch
            || durable.next_lsn != prepared.source_next_lsn
        {
            durable.discard_prepared_checkpoint(prepared.generation, &prepared.staging_path)?;
            return Err(SkeinError::Storage(format!(
                "checkpoint source changed before publication: prepared commit/checkpoint/lsn=({},{},{}), current=({},{},{}); retry checkpoint",
                prepared.source_commit_epoch,
                prepared.source_checkpoint_epoch,
                prepared.source_next_lsn,
                self.commit_epoch,
                durable.checkpoint_epoch,
                durable.next_lsn,
            )));
        }
        if let Err(error) = durable.publish_checkpoint_sidecars(
            &prepared.staging_path,
            prepared.publish_projected_graph_artifacts,
            prepared.source_scan_publication,
        ) {
            let _ =
                durable.discard_prepared_checkpoint(prepared.generation, &prepared.staging_path);
            return Err(error);
        }
        durable.publish_checkpoint_manifest(
            prepared.generation,
            prepared.manifest_artifacts,
            prepared.source_commit_epoch,
            oldest_reader_commit_epoch,
            prepared.source_scan_publication,
        )?;
        let source_scan_manifest = prepared
            .source_scan_publication
            .map(|publication| {
                source_scan::load(
                    durable.root_path(),
                    prepared.source_commit_epoch,
                    publication.descriptor_checksum(),
                )
            })
            .transpose()?
            .flatten();
        self.projected_graph_artifacts = prepared.projected_graph_artifacts.into();
        self.source_scan_manifest = source_scan_manifest.into();
        self.checkpoint_statistics = prepared.checkpoint_statistics;
        if let Some(relational_state) = prepared.checkpoint_relational_state {
            self.relational_state = relational_state;
        }
        if prepared.checkpoint_out_of_core {
            self.canonical_base = durable.canonical_segments.clone();
            self.canonical_adjacency = durable.canonical_adjacency.clone();
            self.persistent_property_projection = durable.persistent_property_projection.clone();
            self.canonical_base_out_of_core = true;
            self.nodes = CowSegmentedMap::default();
            self.relationships = CowSegmentedMap::default();
            self.node_tombstones = CowSegment::default();
            self.relationship_tombstones = CowSegment::default();
            self.outgoing = CowSegmentedMap::default();
            self.incoming = CowSegmentedMap::default();
            self.property_index = CowSegmentedMap::default();
            self.composite_property_index = CowSegmentedMap::default();
            self.full_text_property_index = CowSegmentedMap::default();
            self.relationship_property_index = CowSegmentedMap::default();
        }
        Ok(())
    }

    pub fn rebuild_projected_graph_artifacts(&mut self, catalog: &Catalog) -> Result<()> {
        let projection_epoch = self.next_projection_epoch();
        let projected_graph_artifacts =
            encode_projected_graph_artifacts(catalog, self, projection_epoch);
        let (_, artifacts) = decode_projected_graph_artifacts(&projected_graph_artifacts)?;
        if let Some(durable) = &self.durable {
            durable.write_projected_graph_artifacts(&projected_graph_artifacts)?;
        }
        self.projected_graph_artifacts = artifacts.into();
        Ok(())
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
            retained_mutation_count: self.search_projection_graph_changes.len(),
            restart_recoverable: self.durable.is_some(),
        }
    }

    pub fn set_max_search_projection_change_log_entries(&mut self, max_entries: Option<usize>) {
        self.max_search_projection_change_log_entries = max_entries;
        self.trim_search_projection_graph_change_log();
    }

    pub fn set_telemetry_sink(&mut self, telemetry: Option<Arc<dyn TelemetrySink>>) {
        if let Some(durable) = &mut self.durable {
            durable.telemetry = telemetry;
        }
    }

    pub fn stable_id_mapping(&self) -> StoreStableIdMapping {
        (*self.stable_id_mapping).clone()
    }

    pub fn initial_import_source_fingerprint(&self) -> Option<&str> {
        self.initial_import_source_fingerprint.as_deref()
    }

    pub fn replace_stable_id_mapping(&mut self, mapping: StoreStableIdMapping) -> Result<()> {
        if self
            .durable
            .as_ref()
            .is_some_and(|durable| durable.read_only)
        {
            return Err(SkeinError::Storage(
                "stable id mapping persistence is not allowed in read-only mode".to_string(),
            ));
        }
        self.stable_id_mapping = mapping.into();
        self.write_stable_id_mapping()
    }

    pub fn ensure_stable_id_mapping(&mut self) -> Result<StoreStableIdMapping> {
        if self
            .durable
            .as_ref()
            .is_some_and(|durable| durable.read_only)
        {
            return Err(SkeinError::Storage(
                "stable id mapping persistence is not allowed in read-only mode".to_string(),
            ));
        }
        let mut changed = false;
        for node in self.nodes.values() {
            if node.properties.contains_key("id")
                || self
                    .stable_id_mapping
                    .node_stable_ids
                    .contains_key(&node.id)
            {
                continue;
            }
            self.stable_id_mapping
                .node_stable_ids
                .insert(node.id, generated_stable_id("node", node.id.0));
            changed = true;
        }
        for relationship in self.relationships.values() {
            if relationship.properties.contains_key("id")
                || self
                    .stable_id_mapping
                    .relationship_stable_ids
                    .contains_key(&relationship.id)
            {
                continue;
            }
            self.stable_id_mapping.relationship_stable_ids.insert(
                relationship.id,
                generated_stable_id("relationship", relationship.id.0),
            );
            changed = true;
        }
        if changed {
            self.write_stable_id_mapping()?;
        }
        Ok(self.stable_id_mapping())
    }

    pub fn storage_reclamation_watermark(
        &self,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> StorageReclamationWatermark {
        match &self.durable {
            Some(durable) => {
                let safe_reclaim_commit_epoch =
                    if oldest_reader_commit_epoch == durable.oldest_reader_commit_epoch {
                        durable.safe_reclaim_commit_epoch
                    } else {
                        safe_reclaim_commit_epoch(
                            durable.checkpoint_commit_epoch,
                            oldest_reader_commit_epoch,
                        )
                    };
                StorageReclamationWatermark {
                    current_commit_epoch: self.commit_epoch,
                    checkpoint_epoch: Some(durable.checkpoint_epoch),
                    checkpoint_commit_epoch: Some(durable.checkpoint_commit_epoch),
                    oldest_reader_commit_epoch,
                    safe_reclaim_commit_epoch,
                    durable: true,
                }
            }
            None => StorageReclamationWatermark {
                current_commit_epoch: self.commit_epoch,
                checkpoint_epoch: None,
                checkpoint_commit_epoch: None,
                oldest_reader_commit_epoch,
                safe_reclaim_commit_epoch: safe_reclaim_commit_epoch(
                    self.commit_epoch,
                    oldest_reader_commit_epoch,
                ),
                durable: false,
            },
        }
    }

    pub fn storage_recovery_report(&self) -> StorageRecoveryReport {
        self.storage_recovery_report.clone()
    }

    pub fn segment_cache_snapshot(&self) -> Option<SegmentCacheSnapshot> {
        self.durable
            .as_ref()
            .map(|durable| durable.segment_cache.snapshot())
    }

    pub fn canonical_segment_manifest(&self) -> Option<&CanonicalSegmentManifest> {
        self.durable
            .as_ref()?
            .canonical_segments
            .as_ref()
            .map(CanonicalSegmentReader::manifest)
    }

    pub fn canonical_adjacency_manifest(&self) -> Option<&CanonicalAdjacencyManifest> {
        self.durable
            .as_ref()?
            .canonical_adjacency
            .as_ref()
            .map(CanonicalAdjacencyReader::manifest)
    }

    pub fn property_spill_manifest(&self) -> Option<&PropertySpillManifest> {
        self.durable
            .as_ref()?
            .canonical_segments
            .as_ref()?
            .property_spill_manifest()
    }

    pub fn persistent_property_projection_manifest(
        &self,
    ) -> Option<&PersistentPropertyProjectionManifest> {
        self.durable
            .as_ref()?
            .persistent_property_projection
            .as_ref()
            .map(PersistentPropertyProjectionReader::manifest)
    }

    pub fn canonical_node_from_segments(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        self.durable
            .as_ref()
            .and_then(|durable| durable.canonical_segments.as_ref())
            .map(|reader| reader.get_node(id).map_err(canonical_segment_error))
            .transpose()
            .map(Option::flatten)
    }

    pub fn canonical_relationship_from_segments(&self, id: RelId) -> Result<Option<RelRecord>> {
        self.durable
            .as_ref()
            .and_then(|durable| durable.canonical_segments.as_ref())
            .map(|reader| reader.get_relationship(id).map_err(canonical_segment_error))
            .transpose()
            .map(Option::flatten)
    }

    pub fn storage_residency_report(&self) -> StorageResidencyReport {
        let manifest = self
            .canonical_base
            .as_ref()
            .map(CanonicalSegmentReader::manifest);
        let estimated_delta_resident_bytes = self.estimated_delta_resident_bytes();
        let cache = self.segment_cache_snapshot().unwrap_or_default();
        StorageResidencyReport {
            out_of_core: self.canonical_base_out_of_core,
            canonical_generation: manifest.map(|manifest| manifest.generation.0),
            canonical_artifact_bytes: manifest.map_or(0, |manifest| manifest.artifact_len),
            canonical_node_count: manifest.map_or(0, |manifest| manifest.node_count),
            canonical_relationship_count: manifest
                .map_or(0, |manifest| manifest.relationship_count),
            delta_node_count: self.nodes.len(),
            delta_relationship_count: self.relationships.len(),
            node_tombstone_count: self.node_tombstones.len(),
            relationship_tombstone_count: self.relationship_tombstones.len(),
            estimated_delta_resident_bytes,
            max_out_of_core_delta_bytes: self.max_out_of_core_delta_bytes,
            delta_within_budget: self
                .max_out_of_core_delta_bytes
                .is_none_or(|limit| estimated_delta_resident_bytes <= limit),
            checkpoint_statistics_commit_epoch: self.checkpoint_statistics.computed_at_commit_epoch,
            checkpoint_statistics_complete: self.checkpoint_statistics.advanced_statistics_complete,
            checkpoint_statistics_stale: !self.checkpoint_statistics.advanced_statistics_complete
                || self.checkpoint_statistics.computed_at_commit_epoch < self.commit_epoch,
            segment_cache_capacity_bytes: cache.capacity_bytes,
            segment_cache_resident_bytes: cache.resident_bytes,
            segment_cache_pinned_bytes: cache.pinned_bytes,
            segment_cache_hit_count: cache.hit_count,
            segment_cache_miss_count: cache.miss_count,
            segment_cache_eviction_count: cache.eviction_count,
            segment_cache_admission_rejection_count: cache.admission_rejection_count,
            segment_cache_digest_mismatch_count: cache.digest_mismatch_count,
        }
    }

    pub fn storage_pressure_snapshot(
        &self,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> StoragePressureSnapshot {
        let cache = self.segment_cache_snapshot().unwrap_or_default();
        let checkpoint_commit_epoch = self
            .durable
            .as_ref()
            .map_or(self.commit_epoch, |durable| durable.checkpoint_commit_epoch);
        let (wal_bytes, wal_age_millis, max_wal_bytes, obsolete_generation_bytes) =
            self.durable.as_ref().map_or((0, 0, None, 0), |durable| {
                (
                    durable.wal_bytes,
                    (self.commit_epoch > durable.checkpoint_commit_epoch)
                        .then(|| durable.wal_age_millis())
                        .flatten()
                        .unwrap_or_default(),
                    durable.max_wal_bytes,
                    durable.obsolete_generation_bytes(oldest_reader_commit_epoch),
                )
            });
        let has_checkpoint_debt =
            self.durable.is_some() && self.commit_epoch > checkpoint_commit_epoch;
        let estimated_checkpoint_temporary_bytes = if has_checkpoint_debt {
            self.estimated_logical_record_bytes()
                .saturating_add(self.relational_state.estimated_checkpoint_bytes())
                .saturating_mul(CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER)
                .max(MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES)
        } else {
            0
        };
        let property_projection_debt =
            self.persistent_property_projection
                .as_ref()
                .map_or(0, |reader| {
                    usize::try_from(
                        self.commit_epoch
                            .saturating_sub(reader.manifest().source_commit_epoch),
                    )
                    .unwrap_or(usize::MAX)
                });

        StorageDebtController.evaluate(StoragePressureSignals {
            current_commit_epoch: self.commit_epoch,
            checkpoint_commit_epoch,
            wal_bytes,
            wal_age_millis,
            max_wal_bytes,
            delta_bytes: if self.canonical_base_out_of_core {
                self.estimated_delta_resident_bytes()
            } else {
                0
            },
            max_delta_bytes: if self.canonical_base_out_of_core {
                self.max_out_of_core_delta_bytes
            } else {
                None
            },
            adjacency_debt_entries: self.adjacency_consolidation_plan().estimated_entries,
            projection_debt_operations: property_projection_debt,
            oldest_reader_commit_epoch,
            obsolete_generation_bytes,
            estimated_checkpoint_temporary_bytes,
            available_free_space_bytes: self
                .durable
                .as_ref()
                .and_then(|durable| available_storage_space(durable.root_path())),
            cache_capacity_bytes: cache.capacity_bytes,
            cache_resident_bytes: cache.resident_bytes,
            cache_pinned_bytes: cache.pinned_bytes,
            integrity_poisoned: self.storage_handle_poisoned(),
        })
    }

    pub(crate) fn checkpoint_estimated_operations(&self) -> usize {
        let statistics = self.basic_statistics();
        let graph_operations = statistics
            .node_count
            .saturating_add(statistics.relationship_count);
        usize::try_from(graph_operations)
            .unwrap_or(usize::MAX)
            .saturating_add(self.relational_state.total_row_count())
            .max(1)
    }

    fn estimated_delta_resident_bytes(&self) -> u64 {
        let record_bytes = self
            .nodes
            .values()
            .fold(0u64, |bytes, node| {
                bytes.saturating_add(estimated_node_record_bytes(node))
            })
            .saturating_add(
                self.relationships
                    .values()
                    .fold(0u64, |bytes, relationship| {
                        bytes.saturating_add(estimated_relationship_record_bytes(relationship))
                    }),
            );
        let tombstone_bytes = (self
            .node_tombstones
            .len()
            .saturating_add(self.relationship_tombstones.len())
            as u64)
            .saturating_mul(32);
        let adjacency_bytes =
            self.outgoing
                .values()
                .chain(self.incoming.values())
                .fold(0u64, |bytes, posting| {
                    bytes
                        .saturating_add(48)
                        .saturating_add((posting.len() as u64).saturating_mul(24))
                });
        let node_index_bytes = self
            .property_index
            .values()
            .chain(self.composite_property_index.values())
            .chain(self.full_text_property_index.values())
            .fold(0u64, |bytes, posting| {
                bytes
                    .saturating_add(64)
                    .saturating_add((posting.len() as u64).saturating_mul(24))
            });
        let relationship_index_bytes =
            self.relationship_property_index
                .values()
                .fold(0u64, |bytes, posting| {
                    bytes
                        .saturating_add(64)
                        .saturating_add((posting.len() as u64).saturating_mul(24))
                });
        record_bytes
            .saturating_add(tombstone_bytes)
            .saturating_add(adjacency_bytes)
            .saturating_add(node_index_bytes)
            .saturating_add(relationship_index_bytes)
    }

    fn estimated_logical_record_bytes(&self) -> u64 {
        let base_bytes = self
            .canonical_base
            .as_ref()
            .map_or(0, |reader| reader.manifest().artifact_len);
        self.nodes
            .values()
            .fold(base_bytes, |bytes, node| {
                bytes.saturating_add(estimated_node_record_bytes(node))
            })
            .saturating_add(self.relationships.values().fold(0, |bytes, relationship| {
                bytes.saturating_add(estimated_relationship_record_bytes(relationship))
            }))
    }

    pub fn node_records_owned(&self) -> GraphNodeIterator {
        GraphNodeIterator {
            base: self
                .canonical_base
                .as_ref()
                .map(CanonicalSegmentReader::node_records)
                .map(Iterator::peekable),
            delta: self
                .nodes
                .values()
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .peekable(),
            tombstones: self.node_tombstones.clone(),
        }
    }

    pub fn relationship_records_owned(&self) -> GraphRelationshipIterator {
        GraphRelationshipIterator {
            base: self
                .canonical_base
                .as_ref()
                .map(CanonicalSegmentReader::relationship_records)
                .map(Iterator::peekable),
            delta: self
                .relationships
                .values()
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .peekable(),
            tombstones: self.relationship_tombstones.clone(),
        }
    }

    pub fn node_owned(&self, id: NodeId) -> Result<Option<NodeRecord>> {
        if self.node_tombstones.contains(&id) {
            return Ok(None);
        }
        if let Some(node) = self.nodes.get(&id) {
            return Ok(Some(node.clone()));
        }
        self.canonical_base
            .as_ref()
            .map(|reader| reader.get_node(id).map_err(canonical_segment_error))
            .transpose()
            .map(Option::flatten)
    }

    pub fn relationship_owned(&self, id: RelId) -> Result<Option<RelRecord>> {
        if self.relationship_tombstones.contains(&id) {
            return Ok(None);
        }
        if let Some(relationship) = self.relationships.get(&id) {
            return Ok(Some(relationship.clone()));
        }
        self.canonical_base
            .as_ref()
            .map(|reader| reader.get_relationship(id).map_err(canonical_segment_error))
            .transpose()
            .map(Option::flatten)
    }

    pub fn visit_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        mut consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            for node in self.nodes.values() {
                if self.node_matches_label(node, label_id)
                    && consumer(node.clone()) == GraphScanControl::Stop
                {
                    return Ok(GraphScanControl::Stop);
                }
            }
            return Ok(GraphScanControl::Continue);
        };

        let mut delta = self.nodes.iter().peekable();
        let mut graph_control = GraphScanControl::Continue;
        let (_, canonical_control) = reader
            .scan_nodes_control(|base| {
                while delta.peek().is_some_and(|(id, _)| **id < base.id) {
                    let (id, node) = delta.next().expect("peeked delta node exists");
                    if !self.node_tombstones.contains(id)
                        && self.node_matches_label(node, label_id)
                        && consumer(node.clone()) == GraphScanControl::Stop
                    {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                }
                if delta.peek().is_some_and(|(id, _)| **id == base.id) {
                    let (id, node) = delta.next().expect("matching delta node exists");
                    if !self.node_tombstones.contains(id)
                        && self.node_matches_label(node, label_id)
                        && consumer(node.clone()) == GraphScanControl::Stop
                    {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                    return Ok(CanonicalScanControl::Continue);
                }
                if !self.node_tombstones.contains(&base.id)
                    && self.node_matches_label(&base, label_id)
                    && consumer(base) == GraphScanControl::Stop
                {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(canonical_segment_error)?;
        if canonical_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for (id, node) in delta {
            if !self.node_tombstones.contains(id)
                && self.node_matches_label(node, label_id)
                && consumer(node.clone()) == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn try_visit_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        mut consumer: impl FnMut(NodeRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let mut consumer_error = None;
        let control = self.visit_nodes_owned(label_id, |node| match consumer(node) {
            Ok(control) => control,
            Err(error) => {
                consumer_error = Some(error);
                GraphScanControl::Stop
            }
        })?;
        match consumer_error {
            Some(error) => Err(error),
            None => Ok(control),
        }
    }

    pub fn visit_relationships_owned(
        &self,
        rel_type: Option<RelTypeId>,
        mut consumer: impl FnMut(RelRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            for relationship in self.relationships.values() {
                if self.relationship_matches_type(relationship, rel_type)
                    && consumer(relationship.clone()) == GraphScanControl::Stop
                {
                    return Ok(GraphScanControl::Stop);
                }
            }
            return Ok(GraphScanControl::Continue);
        };

        let mut delta = self.relationships.iter().peekable();
        let mut graph_control = GraphScanControl::Continue;
        let (_, canonical_control) = reader
            .scan_relationships_control(|base| {
                while delta.peek().is_some_and(|(id, _)| **id < base.id) {
                    let (id, relationship) =
                        delta.next().expect("peeked delta relationship exists");
                    if !self.relationship_tombstones.contains(id)
                        && self.relationship_matches_type(relationship, rel_type)
                        && consumer(relationship.clone()) == GraphScanControl::Stop
                    {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                }
                if delta.peek().is_some_and(|(id, _)| **id == base.id) {
                    let (id, relationship) =
                        delta.next().expect("matching delta relationship exists");
                    if !self.relationship_tombstones.contains(id)
                        && self.relationship_matches_type(relationship, rel_type)
                        && consumer(relationship.clone()) == GraphScanControl::Stop
                    {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                    return Ok(CanonicalScanControl::Continue);
                }
                if !self.relationship_tombstones.contains(&base.id)
                    && self.relationship_matches_type(&base, rel_type)
                    && consumer(base) == GraphScanControl::Stop
                {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(canonical_segment_error)?;
        if canonical_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for (id, relationship) in delta {
            if !self.relationship_tombstones.contains(id)
                && self.relationship_matches_type(relationship, rel_type)
                && consumer(relationship.clone()) == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn try_visit_relationships_owned(
        &self,
        rel_type: Option<RelTypeId>,
        mut consumer: impl FnMut(RelRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let mut consumer_error = None;
        let control = self.visit_relationships_owned(rel_type, |relationship| {
            match consumer(relationship) {
                Ok(control) => control,
                Err(error) => {
                    consumer_error = Some(error);
                    GraphScanControl::Stop
                }
            }
        })?;
        match consumer_error {
            Some(error) => Err(error),
            None => Ok(control),
        }
    }

    pub fn statistics(&self) -> GraphStatistics {
        if !self.canonical_base_out_of_core {
            return compute_statistics_with_basic(
                &self.nodes,
                &self.relationships,
                self.basic_statistics(),
            );
        }
        let mut statistics = self.checkpoint_statistics.clone();
        let basic = self.basic_statistics();
        statistics.node_count = basic.node_count;
        statistics.relationship_count = basic.relationship_count;
        statistics.label_counts = basic.label_counts;
        statistics.rel_type_counts = basic.rel_type_counts;
        statistics
    }

    pub fn basic_statistics(&self) -> BasicGraphStatistics {
        let mut statistics = self.basic_statistics.clone();
        statistics.computed_at_commit_epoch = self.commit_epoch;
        statistics
    }

    pub fn basic_statistics_consistency_report(&self) -> BasicStatisticsConsistencyReport {
        BasicStatisticsConsistencyReport::new(
            self.basic_statistics(),
            compute_basic_statistics(&self.nodes, &self.relationships, self.commit_epoch),
        )
    }

    pub fn adjacency_consistency_report(&self) -> AdjacencyConsistencyReport {
        AdjacencyConsistencyReport::new(
            self.commit_epoch,
            self.relationships.len(),
            maintained_adjacency_groups(&self.outgoing, &self.incoming),
            recompute_adjacency_groups(&self.relationships),
            &self.relationships,
        )
    }

    pub fn degree_statistics_consistency_report(&self) -> DegreeStatisticsConsistencyReport {
        DegreeStatisticsConsistencyReport::new(
            self.commit_epoch,
            compute_degree_statistics_from_adjacency(&self.nodes, &self.outgoing, &self.incoming),
            compute_degree_statistics_from_relationships(&self.nodes, &self.relationships),
        )
    }

    pub fn distinct_value_statistics_consistency_report(
        &self,
        catalog: &Catalog,
    ) -> DistinctValueStatisticsConsistencyReport {
        let recomputed = compute_statistics(&self.nodes, &self.relationships, self.commit_epoch);
        // The index-derived side can only speak for declared properties, so
        // the recomputed side is narrowed to the same keys. Comparing against
        // every property would flag the undeclared ones forever.
        let recomputed_property_distinct_counts = recomputed
            .property_distinct_counts
            .into_iter()
            .filter(|((label_id, property), _)| {
                catalog.property_index_id(*label_id, property).is_some()
            })
            .collect();
        DistinctValueStatisticsConsistencyReport::new(
            self.commit_epoch,
            compute_node_property_distinct_counts_from_index(&self.property_index),
            recomputed_property_distinct_counts,
            compute_relationship_property_distinct_counts_from_index(
                &self.relationship_property_index,
            ),
            recomputed.rel_property_distinct_counts,
        )
    }

    pub fn property_index_consistency_report(
        &self,
        catalog: &Catalog,
    ) -> PropertyIndexConsistencyReport {
        let recomputed_node_index = recompute_node_property_index(&self.nodes, catalog);
        let recomputed_relationship_index =
            recompute_relationship_property_index(&self.relationships);
        PropertyIndexConsistencyReport::new(
            self.commit_epoch,
            &self.property_index,
            &recomputed_node_index,
            &self.relationship_property_index,
            &recomputed_relationship_index,
        )
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
            max_search_projection_change_log_entries: self.max_search_projection_change_log_entries,
            source_scan_manifest: self.source_scan_manifest.clone(),
            storage_recovery_report: self.storage_recovery_report.clone(),
            canonical_base: self.canonical_base.clone(),
            canonical_adjacency: self.canonical_adjacency.clone(),
            persistent_property_projection: self.persistent_property_projection.clone(),
            canonical_base_out_of_core: self.canonical_base_out_of_core,
            node_tombstones: self.node_tombstones.clone(),
            relationship_tombstones: self.relationship_tombstones.clone(),
            residency_mode: self.residency_mode,
            auto_materialize_checkpoint_bytes: self.auto_materialize_checkpoint_bytes,
            max_out_of_core_delta_bytes: self.max_out_of_core_delta_bytes,
            post_wal_apply_poisoned: self.post_wal_apply_poisoned,
            integrity_poisoned: Arc::clone(&self.integrity_poisoned),
            relational_state: self.relational_state.clone(),
            relational_mutation_limits: self.relational_mutation_limits,
            relational_overflow_config: self.relational_overflow_config,
            durable: None,
        }
    }

    fn record_search_projection_graph_changes_for_ops(
        &mut self,
        catalog: &Catalog,
        commit_epoch: u64,
        ops: &[WalOp],
    ) {
        let mut upsert_node_ids = BTreeSet::new();
        let mut delete_document_ids = BTreeSet::new();
        self.collect_search_projection_graph_changes_for_ops(
            catalog,
            ops,
            &mut upsert_node_ids,
            &mut delete_document_ids,
        );
        if upsert_node_ids.is_empty() && delete_document_ids.is_empty() {
            return;
        }
        self.search_projection_graph_changes
            .push(SearchProjectionGraphChange {
                commit_epoch,
                upsert_node_ids: upsert_node_ids.into_iter().map(|id| id.0).collect(),
                delete_document_ids: delete_document_ids.into_iter().collect(),
            });
        self.trim_search_projection_graph_change_log();
    }

    fn trim_search_projection_graph_change_log(&mut self) {
        let Some(max_entries) = self.max_search_projection_change_log_entries else {
            return;
        };
        if self.search_projection_graph_changes.len() <= max_entries {
            return;
        }
        let remove_count = self.search_projection_graph_changes.len() - max_entries;
        if remove_count > 0 {
            if let Some(last_removed) = self
                .search_projection_graph_changes
                .get(remove_count.saturating_sub(1))
            {
                self.search_projection_change_log_start_epoch = self
                    .search_projection_change_log_start_epoch
                    .max(last_removed.commit_epoch);
            }
            self.search_projection_graph_changes.drain(0..remove_count);
        }
    }

    fn refresh_basic_statistics_epoch(&mut self) {
        self.basic_statistics.computed_at_commit_epoch = self.commit_epoch;
    }

    fn add_node_to_basic_statistics(&mut self, node: &NodeRecord) {
        self.basic_statistics.node_count += 1;
        for label_id in &node.labels {
            *self
                .basic_statistics
                .label_counts
                .entry(*label_id)
                .or_default() += 1;
        }
        self.refresh_basic_statistics_epoch();
    }

    fn remove_node_from_basic_statistics(&mut self, node: &NodeRecord) {
        self.basic_statistics.node_count = self.basic_statistics.node_count.saturating_sub(1);
        for label_id in &node.labels {
            decrement_counter(&mut self.basic_statistics.label_counts, label_id);
        }
        self.refresh_basic_statistics_epoch();
    }

    fn add_relationship_to_basic_statistics(&mut self, relationship: &RelRecord) {
        self.basic_statistics.relationship_count += 1;
        *self
            .basic_statistics
            .rel_type_counts
            .entry(relationship.rel_type)
            .or_default() += 1;
        self.refresh_basic_statistics_epoch();
    }

    fn remove_relationship_from_basic_statistics(&mut self, relationship: &RelRecord) {
        self.basic_statistics.relationship_count =
            self.basic_statistics.relationship_count.saturating_sub(1);
        decrement_counter(
            &mut self.basic_statistics.rel_type_counts,
            &relationship.rel_type,
        );
        self.refresh_basic_statistics_epoch();
    }

    fn collect_search_projection_graph_changes_for_ops(
        &self,
        catalog: &Catalog,
        ops: &[WalOp],
        upsert_node_ids: &mut BTreeSet<NodeId>,
        delete_document_ids: &mut BTreeSet<String>,
    ) {
        for op in ops {
            match op {
                WalOp::CreateNode {
                    id,
                    label,
                    properties,
                } => {
                    if search_projection_document_id_for_label_and_properties(
                        label, properties, *id,
                    )
                    .is_some()
                    {
                        upsert_node_ids.insert(*id);
                    }
                }
                WalOp::SetNodeProperty { id, property, .. } => {
                    if let Some(node) = self.nodes.get(id)
                        && let Some(document_id) =
                            search_projection_document_id_for_node(catalog, node)
                    {
                        if property == "id" {
                            delete_document_ids.insert(document_id);
                        }
                        upsert_node_ids.insert(*id);
                    }
                    if matches!(property.as_str(), "id" | "name" | "canonical_name") {
                        self.collect_label_projection_neighbors(catalog, *id, upsert_node_ids);
                    }
                }
                WalOp::DeleteNode { id } => {
                    if let Some(node) = self.nodes.get(id)
                        && let Some(document_id) =
                            search_projection_document_id_for_node(catalog, node)
                    {
                        delete_document_ids.insert(document_id);
                    }
                    self.collect_label_projection_neighbors(catalog, *id, upsert_node_ids);
                }
                WalOp::Batch(batch_ops) => self.collect_search_projection_graph_changes_for_ops(
                    catalog,
                    batch_ops,
                    upsert_node_ids,
                    delete_document_ids,
                ),
                WalOp::CreateRelationship {
                    source,
                    target,
                    rel_type,
                    ..
                } => {
                    if rel_type == "HAS_LABEL" {
                        self.collect_has_label_projection_endpoints(
                            catalog,
                            *source,
                            *target,
                            upsert_node_ids,
                        );
                    }
                }
                WalOp::DeleteRelationship { id } => {
                    if let Some(relationship) = self.relationships.get(id) {
                        self.collect_has_label_projection_endpoints_for_relationship(
                            catalog,
                            relationship,
                            upsert_node_ids,
                        );
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
                | WalOp::SetRelationshipProperty { .. }
                | WalOp::ProjectGraph { .. }
                | WalOp::MarkInitialImportSource { .. }
                | WalOp::Relational { .. }
                | WalOp::RelationalSnapshot { .. } => {}
            }
        }
    }

    fn collect_label_projection_neighbors(
        &self,
        catalog: &Catalog,
        label_node_id: NodeId,
        upsert_node_ids: &mut BTreeSet<NodeId>,
    ) {
        let Some(label_node) = self.nodes.get(&label_node_id) else {
            return;
        };
        if !Self::node_has_label(catalog, label_node, "Label") {
            return;
        }
        let Some(has_label_type_id) = catalog.rel_type_id("HAS_LABEL") else {
            return;
        };
        for relationship in self.scan_relationships(Some(has_label_type_id)) {
            if relationship.source == label_node_id {
                self.insert_projection_node_if_any(catalog, relationship.target, upsert_node_ids);
            } else if relationship.target == label_node_id {
                self.insert_projection_node_if_any(catalog, relationship.source, upsert_node_ids);
            }
        }
    }

    fn collect_has_label_projection_endpoints_for_relationship(
        &self,
        catalog: &Catalog,
        relationship: &RelRecord,
        upsert_node_ids: &mut BTreeSet<NodeId>,
    ) {
        if catalog.rel_type_name(relationship.rel_type) != Some("HAS_LABEL") {
            return;
        }
        self.collect_has_label_projection_endpoints(
            catalog,
            relationship.source,
            relationship.target,
            upsert_node_ids,
        );
    }

    fn collect_has_label_projection_endpoints(
        &self,
        catalog: &Catalog,
        source: NodeId,
        target: NodeId,
        upsert_node_ids: &mut BTreeSet<NodeId>,
    ) {
        let source_is_label = self
            .nodes
            .get(&source)
            .is_some_and(|node| Self::node_has_label(catalog, node, "Label"));
        let target_is_label = self
            .nodes
            .get(&target)
            .is_some_and(|node| Self::node_has_label(catalog, node, "Label"));
        if source_is_label {
            self.insert_projection_node_if_any(catalog, target, upsert_node_ids);
        }
        if target_is_label {
            self.insert_projection_node_if_any(catalog, source, upsert_node_ids);
        }
    }

    fn insert_projection_node_if_any(
        &self,
        catalog: &Catalog,
        node_id: NodeId,
        upsert_node_ids: &mut BTreeSet<NodeId>,
    ) {
        if self
            .nodes
            .get(&node_id)
            .and_then(|node| search_projection_document_id_for_node(catalog, node))
            .is_some()
        {
            upsert_node_ids.insert(node_id);
        }
    }

    fn node_has_label(catalog: &Catalog, node: &NodeRecord, label: &str) -> bool {
        catalog
            .label_id(label)
            .is_some_and(|label_id| node.labels.contains(&label_id))
    }

    fn apply_create_node(
        &mut self,
        catalog: &Catalog,
        id: NodeId,
        label_id: LabelId,
        properties: BTreeMap<String, Value>,
    ) {
        self.apply_create_node_with_labels(catalog, id, BTreeSet::from([label_id]), properties);
    }

    fn apply_create_node_with_labels(
        &mut self,
        catalog: &Catalog,
        id: NodeId,
        labels: BTreeSet<LabelId>,
        properties: BTreeMap<String, Value>,
    ) {
        self.next_node_id = self.next_node_id.max(id.0 + 1);
        self.node_tombstones.remove(&id);
        if let Some(old_node) = self.nodes.remove(&id) {
            self.remove_node_from_basic_statistics(&old_node);
        }
        self.nodes.insert(
            id,
            NodeRecord {
                id,
                labels,
                properties,
            },
        );
        if let Some(node) = self.nodes.get(&id).cloned() {
            self.add_node_to_basic_statistics(&node);
            for label_id in &node.labels {
                for (property, value) in &node.properties {
                    if catalog.property_index_id(*label_id, property).is_none() {
                        continue;
                    }
                    self.property_index
                        .entry_or_default((*label_id, property.clone(), value.clone()))
                        .insert(id);
                }
            }
            self.add_node_to_composite_property_indexes(catalog, &node);
            self.add_node_to_full_text_property_indexes(catalog, &node);
        }
    }

    fn add_node_to_composite_property_indexes(&mut self, catalog: &Catalog, node: &NodeRecord) {
        for index in catalog.composite_property_indexes() {
            if !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(key) = composite_property_index_key(node, &index.properties) else {
                continue;
            };
            self.composite_property_index
                .entry_or_default((index.label_id, key))
                .insert(node.id);
        }
    }

    fn remove_node_from_composite_property_indexes(
        &mut self,
        catalog: &Catalog,
        node: &NodeRecord,
    ) {
        for index in catalog.composite_property_indexes() {
            if !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(key) = composite_property_index_key(node, &index.properties) else {
                continue;
            };
            let map_key = (index.label_id, key);
            if let Some(ids) = self.composite_property_index.get_mut(&map_key) {
                ids.remove(&node.id);
                if ids.is_empty() {
                    self.composite_property_index.remove(&map_key);
                }
            }
        }
    }

    /// Indexes the nodes that already carry `property` under `label_id`.
    ///
    /// Only declared properties are indexed on write, so a newly declared
    /// index starts empty and would be missing exactly the nodes written
    /// before the declaration. The pruner treats a declared index as
    /// complete, so an unbackfilled one makes queries omit rows rather than
    /// run slowly.
    fn backfill_property_index(&mut self, label_id: LabelId, property: &str) {
        let nodes = self.nodes.values().cloned().collect::<Vec<_>>();
        let mut distinct = BTreeSet::new();
        for node in nodes {
            if !node.labels.contains(&label_id) {
                continue;
            }
            let Some(value) = node.properties.get(property) else {
                continue;
            };
            distinct.insert(value.clone());
            self.property_index
                .entry_or_default((label_id, property.to_string(), value.clone()))
                .insert(node.id);
        }
        // The backfill already walked every node, so the distinct count costs
        // nothing extra here. Deferring it to the next checkpoint would leave
        // the optimizer on its no-statistics fallback for a property the user
        // just asked to index, which is the case where a good estimate is
        // most likely to be wanted.
        if distinct.is_empty() {
            self.checkpoint_statistics
                .property_distinct_counts
                .remove(&(label_id, property.to_string()));
        } else {
            self.checkpoint_statistics
                .property_distinct_counts
                .insert((label_id, property.to_string()), distinct.len() as u64);
        }
    }

    fn rebuild_composite_property_index_for_descriptor(
        &mut self,
        label_id: LabelId,
        properties: &[String],
    ) {
        self.rebuild_composite_property_index_projection(label_id, properties);
    }

    fn rebuild_composite_property_index_projection(
        &mut self,
        label_id: LabelId,
        properties: &[String],
    ) -> usize {
        self.composite_property_index
            .retain(|(candidate_label_id, key), _| {
                *candidate_label_id != label_id
                    || key
                        .iter()
                        .map(|(property, _)| property)
                        .ne(properties.iter())
            });
        let mut indexed_entries = 0usize;
        let nodes = self.nodes.values().cloned().collect::<Vec<_>>();
        for node in nodes {
            if !node.labels.contains(&label_id) {
                continue;
            }
            let Some(key) = composite_property_index_key(&node, properties) else {
                continue;
            };
            self.composite_property_index
                .entry_or_default((label_id, key))
                .insert(node.id);
            indexed_entries = indexed_entries.saturating_add(1);
        }
        indexed_entries
    }

    fn add_node_to_full_text_property_indexes(&mut self, catalog: &Catalog, node: &NodeRecord) {
        for index in catalog.property_indexes() {
            if index.kind != IndexKind::FullText || !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(Value::String(value)) = node.properties.get(&index.property) else {
                continue;
            };
            for token in full_text_index_tokens(value) {
                self.full_text_property_index
                    .entry_or_default((index.label_id, index.property.clone(), token))
                    .insert(node.id);
            }
        }
    }

    fn remove_node_from_full_text_property_indexes(
        &mut self,
        catalog: &Catalog,
        node: &NodeRecord,
    ) {
        for index in catalog.property_indexes() {
            if index.kind != IndexKind::FullText || !node.labels.contains(&index.label_id) {
                continue;
            }
            let Some(Value::String(value)) = node.properties.get(&index.property) else {
                continue;
            };
            for token in full_text_index_tokens(value) {
                let map_key = (index.label_id, index.property.clone(), token);
                if let Some(ids) = self.full_text_property_index.get_mut(&map_key) {
                    ids.remove(&node.id);
                    if ids.is_empty() {
                        self.full_text_property_index.remove(&map_key);
                    }
                }
            }
        }
    }

    fn rebuild_full_text_property_index_for_descriptor(
        &mut self,
        label_id: LabelId,
        property: &str,
    ) {
        self.rebuild_full_text_property_index_projection(label_id, property);
    }

    fn rebuild_full_text_property_index_projection(
        &mut self,
        label_id: LabelId,
        property: &str,
    ) -> usize {
        self.full_text_property_index
            .retain(|(candidate_label_id, candidate_property, _), _| {
                *candidate_label_id != label_id || candidate_property != property
            });
        let mut indexed_entries = 0usize;
        let nodes = self.nodes.values().cloned().collect::<Vec<_>>();
        for node in nodes {
            if !node.labels.contains(&label_id) {
                continue;
            }
            let Some(Value::String(value)) = node.properties.get(property) else {
                continue;
            };
            for token in full_text_index_tokens(value) {
                self.full_text_property_index
                    .entry_or_default((label_id, property.to_string(), token))
                    .insert(node.id);
                indexed_entries = indexed_entries.saturating_add(1);
            }
        }
        indexed_entries
    }

    fn apply_schema_maintenance_op(&mut self, catalog: &mut Catalog, op: WalOp) {
        match op {
            WalOp::AlterTableState {
                table_kind,
                table,
                state,
            } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.set_table_state(id, state);
                }
            }
            WalOp::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table)
                    && let Some(id) = catalog.property_descriptor_id(table_id, &property)
                {
                    catalog.set_property_state(id, state);
                }
            }
            WalOp::GcPropertyDescriptor {
                table_kind,
                table,
                property,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table)
                    && let Some(id) = catalog.property_descriptor_id(table_id, &property)
                {
                    catalog.remove_property_descriptor(id);
                }
            }
            WalOp::GcTableDescriptor { table_kind, table } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.remove_table_descriptor(id);
                }
            }
            _ => {}
        }
    }

    fn apply_create_relationship(
        &mut self,
        id: RelId,
        source: NodeId,
        target: NodeId,
        rel_type: RelTypeId,
        properties: BTreeMap<String, Value>,
    ) {
        self.next_rel_id = self.next_rel_id.max(id.0 + 1);
        self.relationship_tombstones.remove(&id);
        if let Some(old_relationship) = self.relationships.remove(&id) {
            self.remove_relationship_from_basic_statistics(&old_relationship);
            self.remove_relationship_from_property_index(&old_relationship);
            self.remove_relationship_from_adjacency(&old_relationship);
        }
        self.relationships.insert(
            id,
            RelRecord {
                id,
                source,
                target,
                rel_type,
                properties,
            },
        );
        if let Some(relationship) = self.relationships.get(&id).cloned() {
            self.add_relationship_to_basic_statistics(&relationship);
            self.add_relationship_to_property_index(&relationship);
        }
        self.outgoing
            .entry_or_default((source, rel_type))
            .insert(id);
        self.incoming
            .entry_or_default((target, rel_type))
            .insert(id);
    }

    pub fn scan_nodes<'a>(
        &'a self,
        label_id: Option<LabelId>,
    ) -> impl Iterator<Item = &'a NodeRecord> + 'a {
        self.nodes
            .values()
            .filter(move |node| label_id.map(|id| node.labels.contains(&id)).unwrap_or(true))
    }

    pub fn scan_nodes_with_filter_pruning<'a>(
        &'a self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
    ) -> ScanPrunedNodeScan<'a> {
        let candidate =
            filter.and_then(|filter| self.prune_node_candidates(catalog, label_id, filter));
        let Some(candidate) = candidate else {
            let candidate_count_before_filter = self.node_count_for_label(label_id);
            let nodes = self
                .scan_nodes(label_id)
                .filter(|node| {
                    filter
                        .map(|filter| property_filter_matches(filter, node.id.0, &node.properties))
                        .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            let output_count = nodes.len();
            return ScanPrunedNodeScan {
                nodes,
                report: ScanPruningReport {
                    target_kind: ScanPruningTargetKind::Node,
                    label_id,
                    rel_type_id: None,
                    strategy: ScanPruningStrategy::FullLabelScan,
                    pruned: false,
                    exact_empty: false,
                    candidate_count_before_pruning: candidate_count_before_filter,
                    pruned_candidate_count: 0,
                    candidate_count_before_filter,
                    output_count,
                    filtered_out_count: candidate_count_before_filter.saturating_sub(output_count),
                },
            };
        };

        let candidate_count_before_pruning = self.node_count_for_label(label_id);
        let candidate_count_before_filter = candidate.node_ids.len();
        let nodes = candidate
            .node_ids
            .iter()
            .filter_map(|node_id| self.nodes.get(node_id))
            .filter(|node| self.node_matches_label(node, label_id))
            .filter(|node| {
                filter
                    .map(|filter| property_filter_matches(filter, node.id.0, &node.properties))
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();
        let output_count = nodes.len();
        ScanPrunedNodeScan {
            nodes,
            report: ScanPruningReport {
                target_kind: ScanPruningTargetKind::Node,
                label_id,
                rel_type_id: None,
                strategy: candidate.strategy,
                pruned: true,
                exact_empty: candidate.exact_empty,
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning
                    .saturating_sub(candidate_count_before_filter),
                candidate_count_before_filter,
                output_count,
                filtered_out_count: candidate_count_before_filter.saturating_sub(output_count),
            },
        }
    }

    pub fn node_count_for_label(&self, label_id: Option<LabelId>) -> usize {
        let count = label_id.map_or(self.basic_statistics.node_count, |label_id| {
            self.basic_statistics
                .label_counts
                .get(&label_id)
                .copied()
                .unwrap_or_default()
        });
        usize::try_from(count).unwrap_or(usize::MAX)
    }

    fn node_matches_label(&self, node: &NodeRecord, label_id: Option<LabelId>) -> bool {
        label_id
            .map(|label_id| node.labels.contains(&label_id))
            .unwrap_or(true)
    }

    /// Whether an equality index is declared for `property`, considering the
    /// label the scan is restricted to.
    ///
    /// An unlabelled scan would have to consult every label's index, so it
    /// only prunes when every label that declares the property agrees. The
    /// conservative answer is to decline, which costs a scan rather than a
    /// wrong result.
    fn indexes_property(
        &self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        property: &str,
    ) -> bool {
        match label_id {
            Some(label_id) => catalog.property_index_id(label_id, property).is_some(),
            None => false,
        }
    }

    fn prune_node_candidates(
        &self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filter: &PropertyFilter,
    ) -> Option<ScanPruningCandidate> {
        // Every branch below that reads `property_index` first passes through
        // `indexes_property`. The index only holds declared properties, so a
        // candidate set built from an undeclared one would be empty rather
        // than complete, and the caller treats candidates as exact.
        match filter {
            PropertyFilter::And(filters) => {
                self.prune_and_node_candidates(catalog, label_id, filters)
            }
            PropertyFilter::Or(filters) => {
                self.prune_or_node_candidates(catalog, label_id, filters)
            }
            PropertyFilter::Not(_) => None,
            PropertyFilter::IdEq { value } => Some(ScanPruningCandidate::exact(
                ScanPruningStrategy::IdEq,
                self.node_ids_for_id_values(label_id, std::slice::from_ref(value)),
            )),
            PropertyFilter::IdNotEq { .. } => None,
            PropertyFilter::IdRange { lower, upper } => {
                if lower.is_none() && upper.is_none() {
                    return None;
                }
                Some(ScanPruningCandidate::exact(
                    ScanPruningStrategy::IdRange,
                    self.node_ids_for_id_range(label_id, lower.as_ref(), upper.as_ref()),
                ))
            }
            PropertyFilter::IdIn { values } => Some(ScanPruningCandidate::exact(
                if values.is_empty() {
                    ScanPruningStrategy::Empty
                } else {
                    ScanPruningStrategy::IdIn
                },
                self.node_ids_for_id_values(label_id, values),
            )),
            PropertyFilter::Eq { property, value } => {
                self.indexes_property(catalog, label_id, property).then(|| {
                    ScanPruningCandidate::exact(
                        ScanPruningStrategy::PropertyEq {
                            property: property.clone(),
                        },
                        self.node_ids_for_property_values(
                            label_id,
                            property,
                            std::slice::from_ref(value),
                        ),
                    )
                })
            }
            PropertyFilter::NotEq { property, value } => {
                self.indexes_property(catalog, label_id, property).then(|| {
                    ScanPruningCandidate::exact(
                        ScanPruningStrategy::PropertyNotEq {
                            property: property.clone(),
                        },
                        self.node_ids_for_property_not_in_values(
                            label_id,
                            property,
                            std::slice::from_ref(value),
                        ),
                    )
                })
            }
            PropertyFilter::IsNull { property } => {
                self.indexes_property(catalog, label_id, property).then(|| {
                    ScanPruningCandidate::exact(
                        ScanPruningStrategy::PropertyMissingOrNull {
                            property: property.clone(),
                        },
                        self.node_ids_for_property_missing_or_null(label_id, property),
                    )
                })
            }
            PropertyFilter::IsNotNull { property } => {
                self.indexes_property(catalog, label_id, property).then(|| {
                    ScanPruningCandidate::exact(
                        ScanPruningStrategy::PropertyExists {
                            property: property.clone(),
                        },
                        self.node_ids_for_property_exists(label_id, property),
                    )
                })
            }
            PropertyFilter::ListContains { .. }
            | PropertyFilter::ListContainsLower { .. }
            | PropertyFilter::Contains { .. }
            | PropertyFilter::StartsWith { .. }
            | PropertyFilter::EndsWith { .. }
            | PropertyFilter::RegexMatch { .. } => None,
            PropertyFilter::DefaultIfNullOrEq {
                property,
                empty,
                default,
                value,
                negated,
            } => {
                if !self.indexes_property(catalog, label_id, property) {
                    return None;
                }
                let strategy = if *negated {
                    ScanPruningStrategy::PropertyDefaultIfNullNotEq {
                        property: property.clone(),
                    }
                } else {
                    ScanPruningStrategy::PropertyDefaultIfNullEq {
                        property: property.clone(),
                    }
                };
                let node_ids = if *negated {
                    self.node_ids_for_default_if_null_not_eq(
                        label_id, property, empty, default, value,
                    )
                } else {
                    self.node_ids_for_default_if_null_eq(label_id, property, empty, default, value)
                };
                Some(ScanPruningCandidate::exact(strategy, node_ids))
            }
            PropertyFilter::In { property, values } => {
                self.indexes_property(catalog, label_id, property).then(|| {
                    ScanPruningCandidate::exact(
                        if values.is_empty() {
                            ScanPruningStrategy::Empty
                        } else {
                            ScanPruningStrategy::PropertyIn {
                                property: property.clone(),
                            }
                        },
                        self.node_ids_for_property_values(label_id, property, values),
                    )
                })
            }
            PropertyFilter::Range {
                property,
                lower,
                upper,
            } => {
                if lower.is_none() && upper.is_none() {
                    return None;
                }
                if !self.indexes_property(catalog, label_id, property) {
                    return None;
                }
                Some(ScanPruningCandidate::exact(
                    ScanPruningStrategy::PropertyRange {
                        property: property.clone(),
                    },
                    self.node_ids_for_property_range(
                        label_id,
                        property,
                        lower.as_ref(),
                        upper.as_ref(),
                    ),
                ))
            }
        }
    }

    fn prune_and_node_candidates(
        &self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filters: &[PropertyFilter],
    ) -> Option<ScanPruningCandidate> {
        let mut best: Option<ScanPruningCandidate> = None;
        for filter in filters {
            let Some(candidate) = self.prune_node_candidates(catalog, label_id, filter) else {
                continue;
            };
            if candidate.exact_empty {
                return Some(candidate);
            }
            if best
                .as_ref()
                .map(|best| candidate.node_ids.len() < best.node_ids.len())
                .unwrap_or(true)
            {
                best = Some(candidate);
            }
        }
        best
    }

    fn prune_or_node_candidates(
        &self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filters: &[PropertyFilter],
    ) -> Option<ScanPruningCandidate> {
        if filters.is_empty() {
            return Some(ScanPruningCandidate {
                strategy: ScanPruningStrategy::Empty,
                node_ids: BTreeSet::new(),
                exact_empty: true,
            });
        }

        let mut node_ids = BTreeSet::new();
        for filter in filters {
            let candidate = self.prune_node_candidates(catalog, label_id, filter)?;
            node_ids.extend(candidate.node_ids);
        }
        Some(ScanPruningCandidate::exact(
            ScanPruningStrategy::OrUnion,
            node_ids,
        ))
    }

    fn node_ids_for_id_values(
        &self,
        label_id: Option<LabelId>,
        values: &[Value],
    ) -> BTreeSet<NodeId> {
        values
            .iter()
            .filter_map(|value| match value {
                Value::Int(value) => u64::try_from(*value).ok().map(NodeId),
                _ => None,
            })
            .filter(|node_id| {
                self.nodes
                    .get(node_id)
                    .map(|node| self.node_matches_label(node, label_id))
                    .unwrap_or(false)
            })
            .collect()
    }

    fn node_ids_for_label(&self, label_id: Option<LabelId>) -> BTreeSet<NodeId> {
        self.nodes
            .values()
            .filter(|node| self.node_matches_label(node, label_id))
            .map(|node| node.id)
            .collect()
    }

    fn node_ids_for_id_range(
        &self,
        label_id: Option<LabelId>,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> BTreeSet<NodeId> {
        self.nodes
            .keys()
            .copied()
            .filter(|node_id| range_bounds_match(&Value::Int(node_id.0 as i64), lower, upper))
            .filter(|node_id| {
                self.nodes
                    .get(node_id)
                    .map(|node| self.node_matches_label(node, label_id))
                    .unwrap_or(false)
            })
            .collect()
    }

    fn node_ids_for_property_values(
        &self,
        label_id: Option<LabelId>,
        property: &str,
        values: &[Value],
    ) -> BTreeSet<NodeId> {
        if values.is_empty() {
            return BTreeSet::new();
        }
        let values = values.iter().collect::<BTreeSet<_>>();
        self.property_index
            .iter()
            .filter(|((candidate_label_id, candidate_property, value), _)| {
                label_id
                    .map(|label_id| *candidate_label_id == label_id)
                    .unwrap_or(true)
                    && candidate_property == property
                    && values.contains(value)
            })
            .flat_map(|(_, node_ids)| node_ids.iter().copied())
            .collect()
    }

    fn node_ids_for_property_not_in_values(
        &self,
        label_id: Option<LabelId>,
        property: &str,
        values: &[Value],
    ) -> BTreeSet<NodeId> {
        let values = values.iter().collect::<BTreeSet<_>>();
        self.property_index
            .iter()
            .filter(|((candidate_label_id, candidate_property, value), _)| {
                label_id
                    .map(|label_id| *candidate_label_id == label_id)
                    .unwrap_or(true)
                    && candidate_property == property
                    && !values.contains(value)
            })
            .flat_map(|(_, node_ids)| node_ids.iter().copied())
            .collect()
    }

    fn node_ids_for_property_exists(
        &self,
        label_id: Option<LabelId>,
        property: &str,
    ) -> BTreeSet<NodeId> {
        self.property_index
            .iter()
            .filter(|((candidate_label_id, candidate_property, value), _)| {
                label_id
                    .map(|label_id| *candidate_label_id == label_id)
                    .unwrap_or(true)
                    && candidate_property == property
                    && value != &Value::Null
            })
            .flat_map(|(_, node_ids)| node_ids.iter().copied())
            .collect()
    }

    fn node_ids_for_property_missing_or_null(
        &self,
        label_id: Option<LabelId>,
        property: &str,
    ) -> BTreeSet<NodeId> {
        let non_null = self.node_ids_for_property_exists(label_id, property);
        self.nodes
            .values()
            .filter(|node| self.node_matches_label(node, label_id))
            .filter(|node| !non_null.contains(&node.id))
            .map(|node| node.id)
            .collect()
    }

    fn node_ids_for_default_if_null_eq(
        &self,
        label_id: Option<LabelId>,
        property: &str,
        empty: &Value,
        default: &Value,
        value: &Value,
    ) -> BTreeSet<NodeId> {
        if value == default {
            let mut node_ids = self.node_ids_for_property_missing_or_null(label_id, property);
            let mut values = vec![empty.clone()];
            if value != empty {
                values.push(value.clone());
            }
            node_ids.extend(self.node_ids_for_property_values(label_id, property, &values));
            return node_ids;
        }

        if value == empty || value == &Value::Null {
            return BTreeSet::new();
        }
        self.node_ids_for_property_values(label_id, property, std::slice::from_ref(value))
    }

    fn node_ids_for_default_if_null_not_eq(
        &self,
        label_id: Option<LabelId>,
        property: &str,
        empty: &Value,
        default: &Value,
        value: &Value,
    ) -> BTreeSet<NodeId> {
        let equal_node_ids =
            self.node_ids_for_default_if_null_eq(label_id, property, empty, default, value);
        self.node_ids_for_label(label_id)
            .difference(&equal_node_ids)
            .copied()
            .collect()
    }

    fn node_ids_for_property_range(
        &self,
        label_id: Option<LabelId>,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> BTreeSet<NodeId> {
        self.property_index
            .iter()
            .filter(|((candidate_label_id, candidate_property, value), _)| {
                label_id
                    .map(|label_id| *candidate_label_id == label_id)
                    .unwrap_or(true)
                    && candidate_property == property
                    && range_bounds_match(value, lower, upper)
            })
            .flat_map(|(_, node_ids)| node_ids.iter().copied())
            .collect()
    }

    pub fn seek_nodes_by_property<'a>(
        &'a self,
        label_id: LabelId,
        property: &str,
        value: &Value,
    ) -> impl Iterator<Item = &'a NodeRecord> + 'a {
        self.property_index
            .get(&(label_id, property.to_string(), value.clone()))
            .into_iter()
            .flat_map(|node_ids| node_ids.iter())
            .filter_map(|node_id| self.nodes.get(node_id))
    }

    pub fn visit_nodes_by_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[Value],
        mut consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            return self.visit_nodes_owned(Some(label_id), |node| {
                if node
                    .properties
                    .get(property)
                    .is_some_and(|candidate| values.iter().any(|value| candidate == value))
                {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            });
        };
        let mut seen = BTreeSet::new();
        for value in values {
            let mut graph_control = GraphScanControl::Continue;
            let (_, canonical_control) = reader
                .scan_nodes_by_property_control(label_id, property, value, |node| {
                    if self.node_tombstones.contains(&node.id)
                        || self.nodes.contains_key(&node.id)
                        || !seen.insert(node.id)
                    {
                        return Ok(CanonicalScanControl::Continue);
                    }
                    if consumer(node) == GraphScanControl::Stop {
                        graph_control = GraphScanControl::Stop;
                        return Ok(CanonicalScanControl::Stop);
                    }
                    Ok(CanonicalScanControl::Continue)
                })
                .map_err(canonical_segment_error)?;
            if canonical_control == CanonicalScanControl::Stop {
                return Ok(graph_control);
            }
        }
        for node in self.nodes.values() {
            if node.labels.contains(&label_id)
                && node
                    .properties
                    .get(property)
                    .is_some_and(|candidate| values.iter().any(|value| candidate == value))
                && consumer(node.clone()) == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn seek_nodes_by_composite_property<'a>(
        &'a self,
        label_id: LabelId,
        predicates: &[(String, Value)],
    ) -> Vec<&'a NodeRecord> {
        self.composite_property_index
            .get(&(label_id, predicates.to_vec()))
            .into_iter()
            .flat_map(|node_ids| node_ids.iter())
            .filter_map(|node_id| self.nodes.get(node_id))
            .collect()
    }

    pub fn visit_nodes_by_composite_property_owned(
        &self,
        label_id: LabelId,
        predicates: &[(String, Value)],
        mut consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some((first_property, first_value)) = predicates.first() else {
            return self.visit_nodes_owned(Some(label_id), consumer);
        };
        self.visit_nodes_by_property_owned(
            label_id,
            first_property,
            std::slice::from_ref(first_value),
            |node| {
                if predicates
                    .iter()
                    .all(|(property, value)| node.properties.get(property) == Some(value))
                {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            },
        )
    }

    pub fn seek_nodes_by_property_range<'a>(
        &'a self,
        label_id: LabelId,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> Vec<&'a NodeRecord> {
        self.property_index
            .iter()
            .filter_map(
                |((candidate_label_id, candidate_property, value), node_ids)| {
                    if *candidate_label_id != label_id || candidate_property != property {
                        return None;
                    }
                    if range_bounds_match(value, lower, upper) {
                        Some(node_ids)
                    } else {
                        None
                    }
                },
            )
            .flat_map(|node_ids| node_ids.iter())
            .filter_map(|node_id| self.nodes.get(node_id))
            .collect()
    }

    pub fn visit_nodes_by_property_range_owned(
        &self,
        label_id: LabelId,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
        mut consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            return self.visit_nodes_owned(Some(label_id), |node| {
                if node
                    .properties
                    .get(property)
                    .is_some_and(|value| range_bounds_match(value, lower, upper))
                {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            });
        };
        let Some(projection) = self
            .persistent_property_projection
            .as_ref()
            .filter(|projection| {
                projection.manifest().supports(
                    label_id,
                    property,
                    PersistentPropertyProjectionKind::Range,
                )
            })
        else {
            return self.visit_nodes_owned(Some(label_id), |node| {
                if node
                    .properties
                    .get(property)
                    .is_some_and(|value| range_bounds_match(value, lower, upper))
                {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            });
        };

        let mut graph_control = GraphScanControl::Continue;
        let (_, projection_control) = projection
            .scan_range_candidates(label_id, property, lower, upper, |node_id| {
                if self.node_tombstones.contains(&node_id) || self.nodes.contains_key(&node_id) {
                    return Ok(CanonicalScanControl::Continue);
                }
                let node = reader.get_node(node_id)?.ok_or_else(|| {
                    PersistentPropertyProjectionError::Corrupt(format!(
                        "property projection references missing canonical node {}",
                        node_id.0
                    ))
                })?;
                if !node.labels.contains(&label_id)
                    || !node
                        .properties
                        .get(property)
                        .is_some_and(|value| range_bounds_match(value, lower, upper))
                {
                    return Err(PersistentPropertyProjectionError::Corrupt(format!(
                        "property projection candidate {} fails its canonical range predicate",
                        node_id.0
                    )));
                }
                if consumer(node) == GraphScanControl::Stop {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
        if projection_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for node in self.nodes.values() {
            if self.node_tombstones.contains(&node.id) {
                continue;
            }
            if node.labels.contains(&label_id)
                && node
                    .properties
                    .get(property)
                    .is_some_and(|value| range_bounds_match(value, lower, upper))
                && consumer(node.clone()) == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn seek_nodes_by_full_text_property<'a>(
        &'a self,
        label_id: LabelId,
        property: &str,
        query: &str,
    ) -> Vec<&'a NodeRecord> {
        let tokens = full_text_query_tokens(query);
        let Some((first, rest)) = tokens.split_first() else {
            return Vec::new();
        };
        let mut candidates = self
            .full_text_property_index
            .get(&(label_id, property.to_string(), first.clone()))
            .map(|node_ids| node_ids.iter().copied().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        for token in rest {
            let Some(ids) =
                self.full_text_property_index
                    .get(&(label_id, property.to_string(), token.clone()))
            else {
                return Vec::new();
            };
            candidates = candidates.intersection(ids).copied().collect();
            if candidates.is_empty() {
                return Vec::new();
            }
        }
        candidates
            .into_iter()
            .filter_map(|node_id| self.nodes.get(&node_id))
            .collect()
    }

    pub fn visit_nodes_by_full_text_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        query: &str,
        mut consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let query_tokens = full_text_query_tokens(query);
        if query_tokens.is_empty() {
            return Ok(GraphScanControl::Continue);
        }
        let matches_query = |node: &NodeRecord| match node.properties.get(property) {
            Some(Value::String(value)) => {
                let tokens = full_text_index_tokens(value);
                query_tokens.iter().all(|token| tokens.contains(token))
            }
            _ => false,
        };
        let Some(reader) = &self.canonical_base else {
            return self.visit_nodes_owned(Some(label_id), |node| {
                if matches_query(&node) {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            });
        };
        let Some(projection) = self
            .persistent_property_projection
            .as_ref()
            .filter(|projection| {
                projection.manifest().supports(
                    label_id,
                    property,
                    PersistentPropertyProjectionKind::FullText,
                )
            })
        else {
            return self.visit_nodes_owned(Some(label_id), |node| {
                if matches_query(&node) {
                    consumer(node)
                } else {
                    GraphScanControl::Continue
                }
            });
        };
        let seed_token = query_tokens
            .iter()
            .min_by_key(|token| {
                projection.estimate_full_text_token_entries(label_id, property, token)
            })
            .expect("non-empty full-text query has a seed token");
        let mut graph_control = GraphScanControl::Continue;
        let (_, projection_control) = projection
            .scan_full_text_token_candidates(label_id, property, seed_token, |node_id| {
                if self.node_tombstones.contains(&node_id) || self.nodes.contains_key(&node_id) {
                    return Ok(CanonicalScanControl::Continue);
                }
                let node = reader.get_node(node_id)?.ok_or_else(|| {
                    PersistentPropertyProjectionError::Corrupt(format!(
                        "property projection references missing canonical node {}",
                        node_id.0
                    ))
                })?;
                if !node.labels.contains(&label_id) || !matches_query(&node) {
                    return Ok(CanonicalScanControl::Continue);
                }
                if consumer(node) == GraphScanControl::Stop {
                    graph_control = GraphScanControl::Stop;
                    return Ok(CanonicalScanControl::Stop);
                }
                Ok(CanonicalScanControl::Continue)
            })
            .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
        if projection_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for node in self.nodes.values() {
            if self.node_tombstones.contains(&node.id) {
                continue;
            }
            if node.labels.contains(&label_id)
                && matches_query(node)
                && consumer(node.clone()) == GraphScanControl::Stop
            {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    #[inline]
    pub fn outgoing_relationships<'a>(
        &'a self,
        source: NodeId,
        rel_type: RelTypeId,
    ) -> impl Iterator<Item = &'a RelRecord> + 'a {
        self.outgoing
            .get(&(source, rel_type))
            .into_iter()
            .flat_map(AdjacencyPostingList::iter_copied)
            .filter_map(|rel_id| self.relationships.get(&rel_id))
    }

    #[inline]
    pub fn incoming_relationships<'a>(
        &'a self,
        target: NodeId,
        rel_type: RelTypeId,
    ) -> impl Iterator<Item = &'a RelRecord> + 'a {
        self.incoming
            .get(&(target, rel_type))
            .into_iter()
            .flat_map(AdjacencyPostingList::iter_copied)
            .filter_map(|rel_id| self.relationships.get(&rel_id))
    }

    pub fn visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        mut consumer: impl FnMut(RelRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        let Some(reader) = &self.canonical_base else {
            return self.visit_relationships_owned(rel_type, |relationship| {
                let adjacent = match direction {
                    AdjacencyDirection::Outgoing => relationship.source == node_id,
                    AdjacencyDirection::Incoming => relationship.target == node_id,
                };
                if adjacent {
                    consumer(relationship)
                } else {
                    GraphScanControl::Continue
                }
            });
        };
        let mut graph_control = GraphScanControl::Continue;
        let canonical_control = {
            let mut consume_canonical = |relationship: RelRecord| {
                if self.relationship_tombstones.contains(&relationship.id)
                    || self.relationships.contains_key(&relationship.id)
                {
                    return CanonicalScanControl::Continue;
                }
                if consumer(relationship) == GraphScanControl::Stop {
                    graph_control = GraphScanControl::Stop;
                    CanonicalScanControl::Stop
                } else {
                    CanonicalScanControl::Continue
                }
            };
            if let Some(adjacency) = &self.canonical_adjacency {
                adjacency
                    .scan_endpoint_entries_control(node_id, direction, rel_type, |entry| {
                        let relationship = match entry {
                            CanonicalAdjacencyEntry::Inline(relationship) => relationship,
                            CanonicalAdjacencyEntry::CanonicalReference { relationship_id } => {
                                reader
                                    .get_relationship(relationship_id)
                                    .map_err(|error| {
                                        skein_storage::CanonicalAdjacencyError::Source(
                                            error.to_string(),
                                        )
                                    })?
                                    .ok_or_else(|| {
                                        skein_storage::CanonicalAdjacencyError::Corrupt(format!(
                                            "canonical adjacency references missing relationship {}",
                                            relationship_id.0
                                        ))
                                    })?
                            }
                        };
                        Ok(consume_canonical(relationship))
                    })
                    .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?
                    .1
            } else {
                let endpoint_direction = match direction {
                    AdjacencyDirection::Outgoing => CanonicalEndpointDirection::Source,
                    AdjacencyDirection::Incoming => CanonicalEndpointDirection::Target,
                };
                reader
                    .scan_relationships_for_endpoint_control(
                        node_id,
                        endpoint_direction,
                        rel_type,
                        |relationship| Ok(consume_canonical(relationship)),
                    )
                    .map_err(canonical_segment_error)?
                    .1
            }
        };
        if canonical_control == CanonicalScanControl::Stop {
            return Ok(graph_control);
        }
        for relationship in self.relationships.values() {
            if rel_type.is_some_and(|rel_type| relationship.rel_type != rel_type) {
                continue;
            }
            let adjacent = match direction {
                AdjacencyDirection::Outgoing => relationship.source == node_id,
                AdjacencyDirection::Incoming => relationship.target == node_id,
            };
            if adjacent && consumer(relationship.clone()) == GraphScanControl::Stop {
                return Ok(GraphScanControl::Stop);
            }
        }
        Ok(GraphScanControl::Continue)
    }

    pub fn try_visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        mut consumer: impl FnMut(RelRecord) -> Result<GraphScanControl>,
    ) -> Result<GraphScanControl> {
        let mut consumer_error = None;
        let control = self.visit_adjacent_relationships_owned(
            node_id,
            rel_type,
            direction,
            |relationship| match consumer(relationship) {
                Ok(control) => control,
                Err(error) => {
                    consumer_error = Some(error);
                    GraphScanControl::Stop
                }
            },
        )?;
        match consumer_error {
            Some(error) => Err(error),
            None => Ok(control),
        }
    }

    pub fn adjacency_group_stats(
        &self,
        node_id: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
    ) -> AdjacencyGroupStats {
        let degree = self
            .adjacency_relationship_ids(node_id, rel_type, direction)
            .map(AdjacencyPostingList::len)
            .unwrap_or_default();
        AdjacencyGroupStats {
            node_id,
            rel_type,
            direction,
            degree,
            layout: adjacency_layout_for_degree(degree),
        }
    }

    /// Plans physical adjacency consolidation without changing graph contents,
    /// epochs, WAL, or checkpoint state.
    pub fn adjacency_consolidation_plan(&self) -> AdjacencyConsolidationPlan {
        adjacency_consolidation_plan(&self.adjacency_consolidation_candidates())
    }

    pub fn bounded_adjacency_consolidation_estimated_entries(
        &self,
        max_estimated_entries: usize,
    ) -> usize {
        let mut remaining_budget = max_estimated_entries;
        let mut estimated_entries = 0usize;
        for candidate in self.adjacency_consolidation_candidates() {
            if candidate.estimated_entries > remaining_budget {
                continue;
            }
            remaining_budget = remaining_budget.saturating_sub(candidate.estimated_entries);
            estimated_entries = estimated_entries.saturating_add(candidate.estimated_entries);
        }
        estimated_entries
    }

    /// Consolidates complete posting groups whose estimated entry work fits in
    /// the caller-provided budget. Oversized groups remain streaming deltas.
    pub fn consolidate_bounded_adjacency_deltas(
        &mut self,
        max_estimated_entries: usize,
    ) -> AdjacencyConsolidationReport {
        let candidates = self.adjacency_consolidation_candidates();
        let planned = adjacency_consolidation_plan(&candidates);
        let mut remaining_budget = max_estimated_entries;
        let mut consolidated_group_count = 0usize;
        let mut consolidated_delta_entry_count = 0usize;
        let mut consolidated_estimated_entries = 0usize;

        for candidate in candidates {
            if candidate.estimated_entries > remaining_budget {
                continue;
            }
            let adjacency = match candidate.direction {
                AdjacencyDirection::Outgoing => &mut self.outgoing,
                AdjacencyDirection::Incoming => &mut self.incoming,
            };
            let Some(posting) = adjacency.get_mut(&candidate.key) else {
                continue;
            };
            if !posting.needs_consolidation() || !posting.consolidate() {
                continue;
            }
            remaining_budget = remaining_budget.saturating_sub(candidate.estimated_entries);
            consolidated_group_count = consolidated_group_count.saturating_add(1);
            consolidated_delta_entry_count =
                consolidated_delta_entry_count.saturating_add(candidate.delta_entry_count);
            consolidated_estimated_entries =
                consolidated_estimated_entries.saturating_add(candidate.estimated_entries);
        }

        AdjacencyConsolidationReport {
            planned,
            consolidated_group_count,
            consolidated_delta_entry_count,
            consolidated_estimated_entries,
            remaining: self.adjacency_consolidation_plan(),
        }
    }

    pub fn adjacency_group_stats_for_node(
        &self,
        node_id: NodeId,
        direction: AdjacencyDirection,
    ) -> Vec<AdjacencyGroupStats> {
        let adjacency = match direction {
            AdjacencyDirection::Outgoing => &self.outgoing,
            AdjacencyDirection::Incoming => &self.incoming,
        };
        let mut stats = adjacency
            .iter()
            .filter_map(|((group_node, rel_type), rel_ids)| {
                (*group_node == node_id).then_some(AdjacencyGroupStats {
                    node_id,
                    rel_type: *rel_type,
                    direction,
                    degree: rel_ids.len(),
                    layout: adjacency_layout_for_degree(rel_ids.len()),
                })
            })
            .collect::<Vec<_>>();
        stats.sort_by_key(|stats| {
            (
                stats.rel_type,
                adjacency_direction_sort_key(stats.direction),
            )
        });
        stats
    }

    pub fn ordered_adjacency_entries(
        &self,
        node_id: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
    ) -> Vec<OrderedAdjacencyEntry> {
        let mut entries = self
            .adjacency_relationship_ids(node_id, rel_type, direction)
            .into_iter()
            .flat_map(AdjacencyPostingList::iter_copied)
            .filter_map(|rel_id| {
                let relationship = self.relationships.get(&rel_id)?;
                Some(OrderedAdjacencyEntry {
                    relationship_id: relationship.id,
                    neighbor_id: match direction {
                        AdjacencyDirection::Outgoing => relationship.target,
                        AdjacencyDirection::Incoming => relationship.source,
                    },
                })
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| (entry.neighbor_id, entry.relationship_id));
        entries
    }

    pub fn ordered_adjacency_entries_for_node(
        &self,
        node_id: NodeId,
        direction: AdjacencyDirection,
    ) -> Vec<OrderedAdjacencyEntry> {
        let adjacency = match direction {
            AdjacencyDirection::Outgoing => &self.outgoing,
            AdjacencyDirection::Incoming => &self.incoming,
        };
        let mut entries = adjacency
            .iter()
            .filter(|((group_node, _), _)| *group_node == node_id)
            .flat_map(|(_, rel_ids)| rel_ids.iter_copied())
            .filter_map(|rel_id| {
                let relationship = self.relationships.get(&rel_id)?;
                Some(OrderedAdjacencyEntry {
                    relationship_id: relationship.id,
                    neighbor_id: match direction {
                        AdjacencyDirection::Outgoing => relationship.target,
                        AdjacencyDirection::Incoming => relationship.source,
                    },
                })
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| (entry.neighbor_id, entry.relationship_id));
        entries
    }

    pub fn scan_relationships<'a>(
        &'a self,
        rel_type: Option<RelTypeId>,
    ) -> impl Iterator<Item = &'a RelRecord> + 'a {
        self.relationships.values().filter(move |relationship| {
            rel_type
                .map(|rel_type| relationship.rel_type == rel_type)
                .unwrap_or(true)
        })
    }

    pub fn scan_relationships_with_filter_pruning<'a>(
        &'a self,
        rel_type: Option<RelTypeId>,
        filter: Option<&PropertyFilter>,
    ) -> ScanPrunedRelationshipScan<'a> {
        let candidate =
            filter.and_then(|filter| self.prune_relationship_candidates(rel_type, filter));
        let Some(candidate) = candidate else {
            let candidate_count_before_filter = self.relationship_count_for_type(rel_type);
            let relationships = self
                .scan_relationships(rel_type)
                .filter(|relationship| {
                    filter
                        .map(|filter| {
                            property_filter_matches(
                                filter,
                                relationship.id.0,
                                &relationship.properties,
                            )
                        })
                        .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            let output_count = relationships.len();
            return ScanPrunedRelationshipScan {
                relationships,
                report: ScanPruningReport {
                    target_kind: ScanPruningTargetKind::Relationship,
                    label_id: None,
                    rel_type_id: rel_type,
                    strategy: ScanPruningStrategy::FullLabelScan,
                    pruned: false,
                    exact_empty: false,
                    candidate_count_before_pruning: candidate_count_before_filter,
                    pruned_candidate_count: 0,
                    candidate_count_before_filter,
                    output_count,
                    filtered_out_count: candidate_count_before_filter.saturating_sub(output_count),
                },
            };
        };

        let candidate_count_before_pruning = self.relationship_count_for_type(rel_type);
        let candidate_count_before_filter = candidate.rel_ids.len();
        let relationships = candidate
            .rel_ids
            .iter()
            .filter_map(|rel_id| self.relationships.get(rel_id))
            .filter(|relationship| self.relationship_matches_type(relationship, rel_type))
            .filter(|relationship| {
                filter
                    .map(|filter| {
                        property_filter_matches(filter, relationship.id.0, &relationship.properties)
                    })
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();
        let output_count = relationships.len();
        ScanPrunedRelationshipScan {
            relationships,
            report: ScanPruningReport {
                target_kind: ScanPruningTargetKind::Relationship,
                label_id: None,
                rel_type_id: rel_type,
                strategy: candidate.strategy,
                pruned: true,
                exact_empty: candidate.exact_empty,
                candidate_count_before_pruning,
                pruned_candidate_count: candidate_count_before_pruning
                    .saturating_sub(candidate_count_before_filter),
                candidate_count_before_filter,
                output_count,
                filtered_out_count: candidate_count_before_filter.saturating_sub(output_count),
            },
        }
    }

    pub fn relationship_count_for_type(&self, rel_type: Option<RelTypeId>) -> usize {
        let count = rel_type.map_or(self.basic_statistics.relationship_count, |rel_type| {
            self.basic_statistics
                .rel_type_counts
                .get(&rel_type)
                .copied()
                .unwrap_or_default()
        });
        usize::try_from(count).unwrap_or(usize::MAX)
    }

    fn relationship_matches_type(
        &self,
        relationship: &RelRecord,
        rel_type: Option<RelTypeId>,
    ) -> bool {
        rel_type
            .map(|rel_type| relationship.rel_type == rel_type)
            .unwrap_or(true)
    }

    fn prune_relationship_candidates(
        &self,
        rel_type: Option<RelTypeId>,
        filter: &PropertyFilter,
    ) -> Option<RelationshipScanPruningCandidate> {
        match filter {
            PropertyFilter::And(filters) => {
                self.prune_and_relationship_candidates(rel_type, filters)
            }
            PropertyFilter::Or(filters) => self.prune_or_relationship_candidates(rel_type, filters),
            PropertyFilter::Not(_) => None,
            PropertyFilter::IdEq { value } => Some(RelationshipScanPruningCandidate::exact(
                ScanPruningStrategy::IdEq,
                self.rel_ids_for_id_values(rel_type, std::slice::from_ref(value)),
            )),
            PropertyFilter::IdNotEq { .. } => None,
            PropertyFilter::IdRange { lower, upper } => {
                if lower.is_none() && upper.is_none() {
                    return None;
                }
                Some(RelationshipScanPruningCandidate::exact(
                    ScanPruningStrategy::IdRange,
                    self.rel_ids_for_id_range(rel_type, lower.as_ref(), upper.as_ref()),
                ))
            }
            PropertyFilter::IdIn { values } => Some(RelationshipScanPruningCandidate::exact(
                if values.is_empty() {
                    ScanPruningStrategy::Empty
                } else {
                    ScanPruningStrategy::IdIn
                },
                self.rel_ids_for_id_values(rel_type, values),
            )),
            PropertyFilter::Eq { property, value } => {
                Some(RelationshipScanPruningCandidate::exact(
                    ScanPruningStrategy::PropertyEq {
                        property: property.clone(),
                    },
                    self.rel_ids_for_property_values(
                        rel_type,
                        property,
                        std::slice::from_ref(value),
                    ),
                ))
            }
            PropertyFilter::NotEq { property, value } => {
                Some(RelationshipScanPruningCandidate::exact(
                    ScanPruningStrategy::PropertyNotEq {
                        property: property.clone(),
                    },
                    self.rel_ids_for_property_not_in_values(
                        rel_type,
                        property,
                        std::slice::from_ref(value),
                    ),
                ))
            }
            PropertyFilter::IsNull { property } => Some(RelationshipScanPruningCandidate::exact(
                ScanPruningStrategy::PropertyMissingOrNull {
                    property: property.clone(),
                },
                self.rel_ids_for_property_missing_or_null(rel_type, property),
            )),
            PropertyFilter::IsNotNull { property } => {
                Some(RelationshipScanPruningCandidate::exact(
                    ScanPruningStrategy::PropertyExists {
                        property: property.clone(),
                    },
                    self.rel_ids_for_property_exists(rel_type, property),
                ))
            }
            PropertyFilter::ListContains { .. }
            | PropertyFilter::ListContainsLower { .. }
            | PropertyFilter::Contains { .. }
            | PropertyFilter::StartsWith { .. }
            | PropertyFilter::EndsWith { .. }
            | PropertyFilter::RegexMatch { .. } => None,
            PropertyFilter::DefaultIfNullOrEq {
                property,
                empty,
                default,
                value,
                negated,
            } => {
                let strategy = if *negated {
                    ScanPruningStrategy::PropertyDefaultIfNullNotEq {
                        property: property.clone(),
                    }
                } else {
                    ScanPruningStrategy::PropertyDefaultIfNullEq {
                        property: property.clone(),
                    }
                };
                let rel_ids = if *negated {
                    self.rel_ids_for_default_if_null_not_eq(
                        rel_type, property, empty, default, value,
                    )
                } else {
                    self.rel_ids_for_default_if_null_eq(rel_type, property, empty, default, value)
                };
                Some(RelationshipScanPruningCandidate::exact(strategy, rel_ids))
            }
            PropertyFilter::In { property, values } => {
                Some(RelationshipScanPruningCandidate::exact(
                    if values.is_empty() {
                        ScanPruningStrategy::Empty
                    } else {
                        ScanPruningStrategy::PropertyIn {
                            property: property.clone(),
                        }
                    },
                    self.rel_ids_for_property_values(rel_type, property, values),
                ))
            }
            PropertyFilter::Range {
                property,
                lower,
                upper,
            } => {
                if lower.is_none() && upper.is_none() {
                    return None;
                }
                Some(RelationshipScanPruningCandidate::exact(
                    ScanPruningStrategy::PropertyRange {
                        property: property.clone(),
                    },
                    self.rel_ids_for_property_range(
                        rel_type,
                        property,
                        lower.as_ref(),
                        upper.as_ref(),
                    ),
                ))
            }
        }
    }

    fn prune_and_relationship_candidates(
        &self,
        rel_type: Option<RelTypeId>,
        filters: &[PropertyFilter],
    ) -> Option<RelationshipScanPruningCandidate> {
        let mut best: Option<RelationshipScanPruningCandidate> = None;
        for filter in filters {
            let Some(candidate) = self.prune_relationship_candidates(rel_type, filter) else {
                continue;
            };
            if candidate.exact_empty {
                return Some(candidate);
            }
            if best
                .as_ref()
                .map(|best| candidate.rel_ids.len() < best.rel_ids.len())
                .unwrap_or(true)
            {
                best = Some(candidate);
            }
        }
        best
    }

    fn prune_or_relationship_candidates(
        &self,
        rel_type: Option<RelTypeId>,
        filters: &[PropertyFilter],
    ) -> Option<RelationshipScanPruningCandidate> {
        if filters.is_empty() {
            return Some(RelationshipScanPruningCandidate {
                strategy: ScanPruningStrategy::Empty,
                rel_ids: BTreeSet::new(),
                exact_empty: true,
            });
        }

        let mut rel_ids = BTreeSet::new();
        for filter in filters {
            let candidate = self.prune_relationship_candidates(rel_type, filter)?;
            rel_ids.extend(candidate.rel_ids);
        }
        Some(RelationshipScanPruningCandidate::exact(
            ScanPruningStrategy::OrUnion,
            rel_ids,
        ))
    }

    fn rel_ids_for_id_values(
        &self,
        rel_type: Option<RelTypeId>,
        values: &[Value],
    ) -> BTreeSet<RelId> {
        values
            .iter()
            .filter_map(|value| match value {
                Value::Int(value) => u64::try_from(*value).ok().map(RelId),
                _ => None,
            })
            .filter(|rel_id| {
                self.relationships
                    .get(rel_id)
                    .map(|relationship| self.relationship_matches_type(relationship, rel_type))
                    .unwrap_or(false)
            })
            .collect()
    }

    fn rel_ids_for_type(&self, rel_type: Option<RelTypeId>) -> BTreeSet<RelId> {
        self.relationships
            .values()
            .filter(|relationship| self.relationship_matches_type(relationship, rel_type))
            .map(|relationship| relationship.id)
            .collect()
    }

    fn rel_ids_for_id_range(
        &self,
        rel_type: Option<RelTypeId>,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> BTreeSet<RelId> {
        self.relationships
            .keys()
            .copied()
            .filter(|rel_id| range_bounds_match(&Value::Int(rel_id.0 as i64), lower, upper))
            .filter(|rel_id| {
                self.relationships
                    .get(rel_id)
                    .map(|relationship| self.relationship_matches_type(relationship, rel_type))
                    .unwrap_or(false)
            })
            .collect()
    }

    fn rel_ids_for_property_values(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
        values: &[Value],
    ) -> BTreeSet<RelId> {
        if values.is_empty() {
            return BTreeSet::new();
        }
        let values = values.iter().collect::<BTreeSet<_>>();
        self.relationship_property_index
            .iter()
            .filter(|((candidate_rel_type, candidate_property, value), _)| {
                rel_type
                    .map(|rel_type| *candidate_rel_type == rel_type)
                    .unwrap_or(true)
                    && candidate_property == property
                    && values.contains(value)
            })
            .flat_map(|(_, rel_ids)| rel_ids.iter().copied())
            .collect()
    }

    fn rel_ids_for_property_not_in_values(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
        values: &[Value],
    ) -> BTreeSet<RelId> {
        let values = values.iter().collect::<BTreeSet<_>>();
        self.relationship_property_index
            .iter()
            .filter(|((candidate_rel_type, candidate_property, value), _)| {
                rel_type
                    .map(|rel_type| *candidate_rel_type == rel_type)
                    .unwrap_or(true)
                    && candidate_property == property
                    && !values.contains(value)
            })
            .flat_map(|(_, rel_ids)| rel_ids.iter().copied())
            .collect()
    }

    fn rel_ids_for_property_exists(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
    ) -> BTreeSet<RelId> {
        self.relationship_property_index
            .iter()
            .filter(|((candidate_rel_type, candidate_property, value), _)| {
                rel_type
                    .map(|rel_type| *candidate_rel_type == rel_type)
                    .unwrap_or(true)
                    && candidate_property == property
                    && value != &Value::Null
            })
            .flat_map(|(_, rel_ids)| rel_ids.iter().copied())
            .collect()
    }

    fn rel_ids_for_property_missing_or_null(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
    ) -> BTreeSet<RelId> {
        let non_null = self.rel_ids_for_property_exists(rel_type, property);
        self.relationships
            .values()
            .filter(|relationship| self.relationship_matches_type(relationship, rel_type))
            .filter(|relationship| !non_null.contains(&relationship.id))
            .map(|relationship| relationship.id)
            .collect()
    }

    fn rel_ids_for_default_if_null_eq(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
        empty: &Value,
        default: &Value,
        value: &Value,
    ) -> BTreeSet<RelId> {
        if value == default {
            let mut rel_ids = self.rel_ids_for_property_missing_or_null(rel_type, property);
            let mut values = vec![empty.clone()];
            if value != empty {
                values.push(value.clone());
            }
            rel_ids.extend(self.rel_ids_for_property_values(rel_type, property, &values));
            return rel_ids;
        }

        if value == empty || value == &Value::Null {
            return BTreeSet::new();
        }
        self.rel_ids_for_property_values(rel_type, property, std::slice::from_ref(value))
    }

    fn rel_ids_for_default_if_null_not_eq(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
        empty: &Value,
        default: &Value,
        value: &Value,
    ) -> BTreeSet<RelId> {
        let equal_rel_ids =
            self.rel_ids_for_default_if_null_eq(rel_type, property, empty, default, value);
        self.rel_ids_for_type(rel_type)
            .difference(&equal_rel_ids)
            .copied()
            .collect()
    }

    fn rel_ids_for_property_range(
        &self,
        rel_type: Option<RelTypeId>,
        property: &str,
        lower: Option<&(Value, bool)>,
        upper: Option<&(Value, bool)>,
    ) -> BTreeSet<RelId> {
        self.relationship_property_index
            .iter()
            .filter(|((candidate_rel_type, candidate_property, value), _)| {
                rel_type
                    .map(|rel_type| *candidate_rel_type == rel_type)
                    .unwrap_or(true)
                    && candidate_property == property
                    && range_bounds_match(value, lower, upper)
            })
            .flat_map(|(_, rel_ids)| rel_ids.iter().copied())
            .collect()
    }

    pub fn relationship(&self, id: RelId) -> Option<&RelRecord> {
        self.relationships.get(&id)
    }

    pub fn node(&self, id: NodeId) -> Option<&NodeRecord> {
        self.nodes.get(&id)
    }

    /// Selects a checkpoint-published segment manifest for the current graph
    /// snapshot. A missing or stale manifest is an explicit graph-scan fallback,
    /// never permission to use an older physical projection.
    pub fn plan_checkpoint_segment_scan(
        &self,
        manifest: Option<&ScanSegmentManifest>,
        predicate: &ScanPredicate,
    ) -> ScanSegmentAccessPlan {
        manifest.map_or_else(
            || ScanSegmentAccessPlan::fallback(ScanSegmentFallback::NoManifest),
            |manifest| manifest.plan_scan(self.commit_epoch, predicate),
        )
    }

    /// Plans the checkpoint-published Source sidecar for the current graph
    /// snapshot. An unavailable, corrupted, or stale sidecar is represented as
    /// an explicit fallback so callers keep the canonical graph authoritative.
    pub fn plan_published_source_scan(&self, predicate: &ScanPredicate) -> ScanSegmentAccessPlan {
        self.plan_checkpoint_segment_scan(self.source_scan_manifest.as_ref(), predicate)
    }

    /// Reads only the persisted Source ranges selected by the current
    /// checkpoint-published manifest. This is a physical candidate operator,
    /// not a substitute for query residual evaluation.
    pub fn read_published_source_scan_candidates(
        &self,
        predicate: &ScanPredicate,
        io_depth: NonZeroUsize,
        max_coalesced_bytes: NonZeroU64,
        max_wave_bytes: NonZeroU64,
    ) -> Result<SourceScanCandidateRead> {
        self.read_published_source_scan_candidates_internal(
            predicate,
            io_depth,
            max_coalesced_bytes,
            max_wave_bytes,
            None,
            None,
        )
    }

    pub fn read_published_source_scan_candidates_with_context(
        &self,
        predicate: &ScanPredicate,
        io_depth: NonZeroUsize,
        max_coalesced_bytes: NonZeroU64,
        max_wave_bytes: NonZeroU64,
        task_context: &RuntimeTaskContext,
    ) -> Result<SourceScanCandidateRead> {
        self.read_published_source_scan_candidates_internal(
            predicate,
            io_depth,
            max_coalesced_bytes,
            max_wave_bytes,
            None,
            Some(task_context),
        )
    }

    pub(crate) fn read_published_source_scan_candidates_bounded(
        &self,
        predicate: &ScanPredicate,
        io_depth: NonZeroUsize,
        max_coalesced_bytes: NonZeroU64,
        max_wave_bytes: NonZeroU64,
        max_candidate_bytes: NonZeroUsize,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<SourceScanCandidateRead> {
        self.read_published_source_scan_candidates_internal(
            predicate,
            io_depth,
            max_coalesced_bytes,
            max_wave_bytes,
            Some(max_candidate_bytes.get()),
            task_context,
        )
    }

    fn read_published_source_scan_candidates_internal(
        &self,
        predicate: &ScanPredicate,
        io_depth: NonZeroUsize,
        max_coalesced_bytes: NonZeroU64,
        max_wave_bytes: NonZeroU64,
        max_candidate_bytes: Option<usize>,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<SourceScanCandidateRead> {
        let plan = self.plan_published_source_scan(predicate);
        let ScanSegmentAccessPlan::Read(plan) = plan else {
            let ScanSegmentAccessPlan::Fallback(reason) = plan else {
                unreachable!("source scan plan is read or fallback")
            };
            return Ok(SourceScanCandidateRead::Fallback(reason));
        };
        let Some(durable) = &self.durable else {
            return Ok(SourceScanCandidateRead::Fallback(
                ScanSegmentFallback::NoManifest,
            ));
        };

        let reader = &durable.source_scan_reader;
        let ranges = plan
            .segments
            .iter()
            .map(|segment| (segment.segment_id, segment.payload_range.clone()))
            .collect::<BTreeMap<_, _>>();
        let checksums = self
            .source_scan_manifest
            .as_ref()
            .expect("read source scan must have a manifest")
            .segments()
            .iter()
            .map(|segment| (segment.summary.segment_id, segment.payload_range.checksum))
            .collect::<BTreeMap<_, _>>();
        let mut candidates = plan
            .segments
            .iter()
            .map(|segment| {
                let positions = segment.candidates.clone().map(|mut cursor| {
                    cursor
                        .next_batch(usize::MAX)
                        .into_iter()
                        .collect::<BTreeSet<_>>()
                });
                (segment.segment_id, positions)
            })
            .collect::<BTreeMap<_, _>>();
        let schedule = SegmentReadScheduler::new(io_depth, max_coalesced_bytes)
            .schedule_with_wave_budget(ranges.values().cloned(), max_wave_bytes);
        let mut rows = Vec::new();
        let mut candidate_bytes = 0usize;
        let mut consume = |payload: SegmentReadPayload| {
            for segment_id in &payload.range.segment_ids {
                let range = ranges.get(segment_id).ok_or_else(|| {
                    SkeinError::StorageIntegrity(format!(
                        "source scan reader returned unknown segment {segment_id}"
                    ))
                })?;
                let start = usize::try_from(range.offset.saturating_sub(payload.range.offset))
                    .map_err(|_| {
                        SkeinError::StorageIntegrity(
                            "source scan payload offset exceeds address space".to_string(),
                        )
                    })?;
                let end = start
                    .checked_add(usize::try_from(range.length.get()).map_err(|_| {
                        SkeinError::StorageIntegrity(
                            "source scan payload length exceeds address space".to_string(),
                        )
                    })?)
                    .ok_or_else(|| {
                        SkeinError::StorageIntegrity(
                            "source scan payload slice overflows".to_string(),
                        )
                    })?;
                let bytes = payload.bytes.get(start..end).ok_or_else(|| {
                    SkeinError::StorageIntegrity(
                        "source scan coalesced payload does not cover a segment".to_string(),
                    )
                })?;
                if checksum_bytes(bytes) != checksums[segment_id] {
                    return Err(SkeinError::StorageIntegrity(format!(
                            "source scan segment {segment_id} checksum changed after manifest validation"
                        )));
                }
                let segment_rows = source_scan::decode_payload(bytes)
                    .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
                let positions = candidates.remove(segment_id).flatten();
                for (row_id, row) in segment_rows.into_iter().enumerate() {
                    if positions
                        .as_ref()
                        .is_some_and(|positions| !positions.contains(&(row_id as u64)))
                    {
                        continue;
                    }
                    let row_bytes = std::mem::size_of::<SourceScanRow>().saturating_add(
                        usize::try_from(estimated_properties_bytes(&row.properties))
                            .unwrap_or(usize::MAX),
                    );
                    let next_candidate_bytes = candidate_bytes.saturating_add(row_bytes);
                    if max_candidate_bytes.is_some_and(|limit| next_candidate_bytes > limit) {
                        return Err(SkeinError::Execution(format!(
                            "SourceSegmentScan candidates exceed blocking_operator_bytes {}",
                            max_candidate_bytes.unwrap_or_default()
                        )));
                    }
                    candidate_bytes = next_candidate_bytes;
                    rows.push(row);
                }
            }
            Ok::<_, SkeinError>(())
        };
        let executor = SegmentReadExecutor::new(max_wave_bytes);
        let report = match task_context {
            Some(task_context) => {
                executor.execute_with_context(reader, &schedule, task_context, &mut consume)
            }
            None => executor.execute(reader, &schedule, &mut consume),
        }
        .map_err(|error| match error {
            SegmentReadExecutionError::Stopped(reason) => {
                SkeinError::Execution(format!("runtime task stopped: {reason}"))
            }
            error => SkeinError::StorageIntegrity(error.to_string()),
        })?;
        Ok(SourceScanCandidateRead::Rows {
            graph_epoch: plan.graph_epoch,
            skipped_segment_count: plan.skipped_segment_count,
            report,
            rows,
        })
    }

    fn load_source_scan_manifest(&mut self) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        self.source_scan_manifest = durable.load_source_scan_manifest(self.commit_epoch)?.into();
        Ok(())
    }

    fn adjacency_relationship_ids(
        &self,
        node_id: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
    ) -> Option<&AdjacencyPostingList> {
        match direction {
            AdjacencyDirection::Outgoing => self.outgoing.get(&(node_id, rel_type)),
            AdjacencyDirection::Incoming => self.incoming.get(&(node_id, rel_type)),
        }
    }

    fn adjacency_consolidation_candidates(&self) -> Vec<AdjacencyConsolidationCandidate> {
        [
            (AdjacencyDirection::Outgoing, &self.outgoing),
            (AdjacencyDirection::Incoming, &self.incoming),
        ]
        .into_iter()
        .flat_map(|(direction, adjacency)| {
            adjacency.iter().filter_map(move |(key, posting)| {
                posting
                    .needs_consolidation()
                    .then_some(AdjacencyConsolidationCandidate {
                        direction,
                        key: *key,
                        delta_entry_count: posting.mini_delta_len(),
                        estimated_entries: posting
                            .pivot_len()
                            .saturating_add(posting.mini_delta_len()),
                    })
            })
        })
        .collect()
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
        self.commit_epoch += 1;
        Ok(())
    }

    pub fn projected_graph_definition(&self, name: &str) -> Option<&ProjectedGraphDefinition> {
        self.projected_graphs.get(name)
    }

    pub fn projected_graph_artifact(
        &self,
        name: &str,
        definition: &ProjectedGraphDefinition,
    ) -> Option<&ProjectedGraph> {
        let artifact = self.projected_graph_artifacts.get(name)?;
        (artifact.commit_epoch == self.commit_epoch && &artifact.definition == definition)
            .then_some(&artifact.graph)
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
                    node_count: artifact.map(|artifact| artifact.graph.node_count()),
                    edge_count: artifact.map(|artifact| artifact.graph.edge_count()),
                    reusable,
                }
            })
            .collect()
    }

    fn apply_project_graph_definition(
        &mut self,
        name: String,
        definition: ProjectedGraphDefinition,
    ) {
        self.projected_graph_artifacts.remove(&name);
        self.projected_graphs.insert(name, definition);
    }

    fn load_projected_graph_artifacts(&mut self) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        self.projected_graph_artifacts = durable
            .load_projected_graph_artifacts()?
            .into_iter()
            .filter(|(name, artifact)| {
                artifact.commit_epoch == self.commit_epoch
                    && self
                        .projected_graphs
                        .get(name)
                        .is_some_and(|definition| definition == &artifact.definition)
            })
            .collect::<BTreeMap<_, _>>()
            .into();
        Ok(())
    }

    fn load_stable_id_mapping(&mut self) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        self.stable_id_mapping = durable.load_stable_id_mapping()?.into();
        Ok(())
    }

    fn write_stable_id_mapping(&self) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        durable.write_stable_id_mapping(&self.stable_id_mapping)
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

    fn find_node_by_label_and_properties(
        &self,
        label_id: LabelId,
        properties: &BTreeMap<String, Value>,
    ) -> Result<Option<NodeId>> {
        let mut found = None;
        self.visit_nodes_owned(Some(label_id), |node| {
            if properties
                .iter()
                .all(|(key, value)| node.properties.get(key) == Some(value))
            {
                found = Some(node.id);
                GraphScanControl::Stop
            } else {
                GraphScanControl::Continue
            }
        })?;
        Ok(found)
    }

    fn find_relationship_by_properties(
        &self,
        source: NodeId,
        target: NodeId,
        rel_type_id: RelTypeId,
        properties: &BTreeMap<String, Value>,
    ) -> Result<Option<RelId>> {
        let mut found = None;
        self.visit_adjacent_relationships_owned(
            source,
            Some(rel_type_id),
            AdjacencyDirection::Outgoing,
            |relationship| {
                if relationship.target == target && &relationship.properties == properties {
                    found = Some(relationship.id);
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            },
        )?;
        Ok(found)
    }

    fn find_relationship_by_property_subset(
        &self,
        source: NodeId,
        target: NodeId,
        rel_type_id: RelTypeId,
        properties: &BTreeMap<String, Value>,
    ) -> Result<Option<RelId>> {
        let mut found = None;
        self.visit_adjacent_relationships_owned(
            source,
            Some(rel_type_id),
            AdjacencyDirection::Outgoing,
            |relationship| {
                if relationship.target == target
                    && properties_contain_all(&relationship.properties, properties)
                {
                    found = Some(relationship.id);
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            },
        )?;
        Ok(found)
    }

    fn merge_connected_node_ops(
        &self,
        request: &ConnectedNodesCreate,
        source: NodeId,
        target: NodeId,
        relationship: RelId,
    ) -> Result<Vec<WalOp>> {
        let mut ops = Vec::new();
        if self.node_owned(source)?.is_none() {
            ops.push(WalOp::CreateNode {
                id: source,
                label: request.source_label.clone(),
                properties: request.source_properties.clone(),
            });
        }
        if self.node_owned(target)?.is_none() {
            ops.push(WalOp::CreateNode {
                id: target,
                label: request.target_label.clone(),
                properties: request.target_properties.clone(),
            });
        }
        ops.push(WalOp::CreateRelationship {
            id: relationship,
            source,
            target,
            rel_type: request.rel_type.clone(),
            properties: request.rel_properties.clone(),
        });
        Ok(ops)
    }

    fn matching_node_ids(
        &self,
        catalog: &Catalog,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
    ) -> Result<Vec<NodeId>> {
        if !self.canonical_base_out_of_core {
            return Ok(self
                .scan_nodes_with_filter_pruning(catalog, label_id, filter)
                .nodes
                .into_iter()
                .map(|node| node.id)
                .collect());
        }
        let mut ids = Vec::new();
        self.visit_nodes_owned(label_id, |node| {
            if filter
                .is_none_or(|filter| property_filter_matches(filter, node.id.0, &node.properties))
            {
                ids.push(node.id);
            }
            GraphScanControl::Continue
        })?;
        Ok(ids)
    }

    fn matching_node_ids_bounded(
        &self,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
        max_ids: usize,
        limit_name: &str,
    ) -> Result<Vec<NodeId>> {
        let mut ids = Vec::with_capacity(max_ids.min(1024));
        let mut exceeded = false;
        self.visit_nodes_owned(label_id, |node| {
            if filter
                .is_none_or(|filter| property_filter_matches(filter, node.id.0, &node.properties))
            {
                if ids.len() == max_ids {
                    exceeded = true;
                    return GraphScanControl::Stop;
                }
                ids.push(node.id);
            }
            GraphScanControl::Continue
        })?;
        if exceeded {
            return Err(SkeinError::Execution(format!(
                "mutation would exceed {limit_name} {max_ids}"
            )));
        }
        Ok(ids)
    }

    fn matching_node_ids_with_pending_bounded(
        &self,
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
        pending_nodes: &[PendingNode],
        max_ids: usize,
        limit_name: &str,
    ) -> Result<Vec<NodeId>> {
        let mut ids = self.matching_node_ids_bounded(label_id, filter, max_ids, limit_name)?;
        for id in Self::pending_node_ids_matching(label_id, filter, pending_nodes) {
            if ids.len() == max_ids {
                return Err(SkeinError::Execution(format!(
                    "mutation would exceed {limit_name} {max_ids}"
                )));
            }
            ids.push(id);
        }
        Ok(ids)
    }

    fn pending_node_ids_matching(
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
        pending_nodes: &[PendingNode],
    ) -> Vec<NodeId> {
        pending_nodes
            .iter()
            .filter(|(id, pending_label_id, properties)| {
                label_id.is_none_or(|label_id| *pending_label_id == label_id)
                    && filter.is_none_or(|filter| property_filter_matches(filter, id.0, properties))
            })
            .map(|(id, _, _)| *id)
            .collect()
    }

    fn apply_set_node_property(
        &mut self,
        catalog: &Catalog,
        id: NodeId,
        property: String,
        value: Value,
    ) {
        if let Some(node) = self.nodes.get(&id).cloned() {
            self.remove_node_from_composite_property_indexes(catalog, &node);
            self.remove_node_from_full_text_property_indexes(catalog, &node);
        }
        let Some(node) = self.nodes.get_mut(&id) else {
            return;
        };
        let old_value = node.properties.insert(property.clone(), value.clone());
        let labels = node.labels.clone();
        self.nodes.rebalance_key(&id);
        for label_id in labels {
            if let Some(old_value) = &old_value {
                let key = (label_id, property.clone(), old_value.clone());
                if let Some(ids) = self.property_index.get_mut(&key) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.property_index.remove(&key);
                    }
                }
            }
            if catalog.property_index_id(label_id, &property).is_some() {
                self.property_index
                    .entry_or_default((label_id, property.clone(), value.clone()))
                    .insert(id);
            }
        }
        if let Some(node) = self.nodes.get(&id).cloned() {
            self.add_node_to_composite_property_indexes(catalog, &node);
            self.add_node_to_full_text_property_indexes(catalog, &node);
        }
    }

    fn apply_set_relationship_property(&mut self, id: RelId, property: String, value: Value) {
        let Some(relationship) = self.relationships.get_mut(&id) else {
            return;
        };
        let rel_type = relationship.rel_type;
        let old_value = relationship
            .properties
            .insert(property.clone(), value.clone());
        self.relationships.rebalance_key(&id);
        if let Some(old_value) = old_value {
            let key = (rel_type, property.clone(), old_value);
            if let Some(ids) = self.relationship_property_index.get_mut(&key) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.relationship_property_index.remove(&key);
                }
            }
        }
        self.relationship_property_index
            .entry_or_default((rel_type, property, value))
            .insert(id);
    }

    fn validate_constraints_for_ops(&self, catalog: &Catalog, ops: &[WalOp]) -> Result<()> {
        self.ensure_out_of_core_delta_admission(ops)?;
        if self.canonical_base_out_of_core {
            return self.validate_out_of_core_record_changes(catalog, ops);
        }
        let mut nodes = self.nodes.clone();
        let mut relationships = self.relationships.clone();
        for op in ops {
            apply_wal_op_to_snapshot(catalog, &mut nodes, &mut relationships, op);
        }
        validate_unique_constraints(catalog, &nodes)?;
        validate_relationship_unique_constraints(catalog, &relationships)?;
        validate_node_property_exists_constraints(catalog, &nodes)?;
        validate_relationship_property_exists_constraints(catalog, &relationships)?;
        validate_property_schemas(catalog, &nodes, &relationships)
    }

    fn ensure_out_of_core_delta_admission(&self, ops: &[WalOp]) -> Result<()> {
        self.ensure_out_of_core_delta_admission_mode(ops, true)
    }

    fn ensure_out_of_core_delta_replay_admission(&self, ops: &[WalOp]) -> Result<()> {
        self.ensure_out_of_core_delta_admission_mode(ops, false)
    }

    fn ensure_out_of_core_delta_admission_mode(
        &self,
        ops: &[WalOp],
        apply_live_backpressure: bool,
    ) -> Result<()> {
        if !self.canonical_base_out_of_core {
            return Ok(());
        }
        let Some(limit) = self.max_out_of_core_delta_bytes else {
            return Ok(());
        };
        let current = self.estimated_delta_resident_bytes();
        let mut touched_nodes = BTreeSet::new();
        let mut touched_relationships = BTreeSet::new();
        let additional = self.estimated_mutation_delta_bytes(
            ops,
            &mut touched_nodes,
            &mut touched_relationships,
        )?;
        let projected = current.saturating_add(additional);
        if projected > limit {
            return Err(SkeinError::Storage(format!(
                "out-of-core mutation delta admission rejected {projected} estimated bytes under the {limit} byte limit; checkpoint the database or raise max_out_of_core_delta_bytes"
            )));
        }
        let pressure = StorageDebtController.evaluate(StoragePressureSignals {
            delta_bytes: projected,
            max_delta_bytes: Some(limit),
            ..StoragePressureSignals::default()
        });
        if apply_live_backpressure && pressure.state == StoragePressureState::DelayMutation {
            return Err(SkeinError::Storage(format!(
                "out-of-core mutation delayed by storage pressure at {projected} estimated bytes under the {limit} byte limit; checkpoint the database before retrying"
            )));
        }
        Ok(())
    }

    fn estimated_mutation_delta_bytes(
        &self,
        ops: &[WalOp],
        touched_nodes: &mut BTreeSet<NodeId>,
        touched_relationships: &mut BTreeSet<RelId>,
    ) -> Result<u64> {
        let mut bytes = 0u64;
        for op in ops {
            match op {
                WalOp::CreateNode { id, properties, .. } => {
                    if touched_nodes.insert(*id) && !self.nodes.contains_key(id) {
                        bytes = bytes.saturating_add(64).saturating_add(
                            estimated_properties_bytes(properties).saturating_mul(2),
                        );
                    }
                }
                WalOp::SetNodeProperty {
                    id,
                    property,
                    value,
                } => {
                    if touched_nodes.insert(*id)
                        && !self.nodes.contains_key(id)
                        && let Some(node) = self.node_owned(*id)?
                    {
                        bytes = bytes.saturating_add(estimated_node_record_bytes(&node));
                    }
                    bytes = bytes
                        .saturating_add(64)
                        .saturating_add(property.len() as u64)
                        .saturating_add(estimated_value_bytes(value).saturating_mul(2));
                }
                WalOp::DeleteNode { id } => {
                    if touched_nodes.insert(*id)
                        && !self.nodes.contains_key(id)
                        && let Some(node) = self.node_owned(*id)?
                    {
                        bytes = bytes
                            .saturating_add(estimated_node_record_bytes(&node))
                            .saturating_add(32);
                    }
                }
                WalOp::CreateRelationship { id, properties, .. } => {
                    if touched_relationships.insert(*id) && !self.relationships.contains_key(id) {
                        bytes = bytes.saturating_add(160).saturating_add(
                            estimated_properties_bytes(properties).saturating_mul(2),
                        );
                    }
                }
                WalOp::SetRelationshipProperty {
                    id,
                    property,
                    value,
                } => {
                    if touched_relationships.insert(*id)
                        && !self.relationships.contains_key(id)
                        && let Some(relationship) = self.relationship_owned(*id)?
                    {
                        bytes = bytes
                            .saturating_add(estimated_relationship_record_bytes(&relationship))
                            .saturating_add(96);
                    }
                    bytes = bytes
                        .saturating_add(64)
                        .saturating_add(property.len() as u64)
                        .saturating_add(estimated_value_bytes(value).saturating_mul(2));
                }
                WalOp::DeleteRelationship { id } => {
                    if touched_relationships.insert(*id)
                        && !self.relationships.contains_key(id)
                        && let Some(relationship) = self.relationship_owned(*id)?
                    {
                        bytes = bytes
                            .saturating_add(estimated_relationship_record_bytes(&relationship))
                            .saturating_add(128);
                    }
                }
                WalOp::Batch(batch) => {
                    bytes = bytes.saturating_add(self.estimated_mutation_delta_bytes(
                        batch,
                        touched_nodes,
                        touched_relationships,
                    )?);
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
                | WalOp::RelationalSnapshot { .. } => {}
            }
        }
        Ok(bytes)
    }

    fn validate_out_of_core_record_changes(&self, catalog: &Catalog, ops: &[WalOp]) -> Result<()> {
        let mut node_changes = BTreeMap::<NodeId, Option<NodeRecord>>::new();
        let mut relationship_changes = BTreeMap::<RelId, Option<RelRecord>>::new();
        self.collect_out_of_core_record_changes(
            catalog,
            ops,
            &mut node_changes,
            &mut relationship_changes,
        )?;

        for node in node_changes.values().flatten() {
            validate_node_record_constraints(catalog, node)?;
        }
        for relationship in relationship_changes.values().flatten() {
            validate_relationship_record_constraints(catalog, relationship)?;
        }
        validate_changed_node_uniqueness(self, catalog, &node_changes)?;
        validate_changed_relationship_uniqueness(self, catalog, &relationship_changes)
    }

    fn collect_out_of_core_record_changes(
        &self,
        catalog: &Catalog,
        ops: &[WalOp],
        nodes: &mut BTreeMap<NodeId, Option<NodeRecord>>,
        relationships: &mut BTreeMap<RelId, Option<RelRecord>>,
    ) -> Result<()> {
        for op in ops {
            match op {
                WalOp::CreateNode {
                    id,
                    label,
                    properties,
                } => {
                    let label_id = catalog.label_id(label).ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "node label '{label}' is missing during out-of-core validation"
                        ))
                    })?;
                    nodes.insert(
                        *id,
                        Some(NodeRecord {
                            id: *id,
                            labels: BTreeSet::from([label_id]),
                            properties: properties.clone(),
                        }),
                    );
                }
                WalOp::SetNodeProperty {
                    id,
                    property,
                    value,
                } => {
                    if !nodes.contains_key(id) {
                        nodes.insert(*id, self.node_owned(*id)?);
                    }
                    if let Some(node) = nodes.get_mut(id).and_then(Option::as_mut) {
                        node.properties.insert(property.clone(), value.clone());
                    }
                }
                WalOp::DeleteNode { id } => {
                    nodes.insert(*id, None);
                }
                WalOp::CreateRelationship {
                    id,
                    source,
                    target,
                    rel_type,
                    properties,
                } => {
                    let rel_type = catalog.rel_type_id(rel_type).ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "relationship type '{rel_type}' is missing during out-of-core validation"
                        ))
                    })?;
                    relationships.insert(
                        *id,
                        Some(RelRecord {
                            id: *id,
                            source: *source,
                            target: *target,
                            rel_type,
                            properties: properties.clone(),
                        }),
                    );
                }
                WalOp::SetRelationshipProperty {
                    id,
                    property,
                    value,
                } => {
                    if !relationships.contains_key(id) {
                        relationships.insert(*id, self.relationship_owned(*id)?);
                    }
                    if let Some(relationship) = relationships.get_mut(id).and_then(Option::as_mut) {
                        relationship
                            .properties
                            .insert(property.clone(), value.clone());
                    }
                }
                WalOp::DeleteRelationship { id } => {
                    relationships.insert(*id, None);
                }
                WalOp::Batch(ops) => {
                    self.collect_out_of_core_record_changes(catalog, ops, nodes, relationships)?
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn validate_unique_constraint(
        &self,
        catalog: &Catalog,
        label_id: LabelId,
        property: &str,
    ) -> Result<()> {
        if self.canonical_base_out_of_core {
            return validate_unique_property_streaming(self, catalog, label_id, property);
        }
        validate_unique_property(catalog, &self.nodes, label_id, property)
    }

    fn validate_node_property_exists_constraint(
        &self,
        catalog: &Catalog,
        label_id: LabelId,
        property: &str,
    ) -> Result<()> {
        if self.canonical_base_out_of_core {
            let mut violation = None;
            self.visit_nodes_owned(Some(label_id), |node| {
                if !node
                    .properties
                    .get(property)
                    .is_some_and(|value| value != &Value::Null)
                {
                    violation = Some(node.id);
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            if let Some(id) = violation {
                let label = catalog.label_name(label_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "node property exists constraint violation on :{label}({property}) for node {}",
                    id.0
                )));
            }
            return Ok(());
        }
        validate_node_property_exists(catalog, &self.nodes, label_id, property)
    }

    fn validate_relationship_unique_constraint(
        &self,
        catalog: &Catalog,
        rel_type_id: RelTypeId,
        property: &str,
    ) -> Result<()> {
        if self.canonical_base_out_of_core {
            return validate_unique_relationship_property_streaming(
                self,
                catalog,
                rel_type_id,
                property,
            );
        }
        validate_unique_relationship_property(catalog, &self.relationships, rel_type_id, property)
    }

    fn validate_relationship_property_exists_constraint(
        &self,
        catalog: &Catalog,
        rel_type_id: RelTypeId,
        property: &str,
    ) -> Result<()> {
        if self.canonical_base_out_of_core {
            let mut violation = None;
            self.visit_relationships_owned(Some(rel_type_id), |relationship| {
                if !relationship
                    .properties
                    .get(property)
                    .is_some_and(|value| value != &Value::Null)
                {
                    violation = Some(relationship.id);
                    GraphScanControl::Stop
                } else {
                    GraphScanControl::Continue
                }
            })?;
            if let Some(id) = violation {
                let rel_type = catalog.rel_type_name(rel_type_id).unwrap_or("<unknown>");
                return Err(SkeinError::Storage(format!(
                    "relationship property exists constraint violation on :{rel_type}({property}) for relationship {}",
                    id.0
                )));
            }
            return Ok(());
        }
        validate_relationship_property_exists(catalog, &self.relationships, rel_type_id, property)
    }

    fn validate_relationship_endpoints(&self) -> Result<()> {
        if self.canonical_base_out_of_core {
            let mut validation_error = None;
            self.visit_relationships_owned(None, |relationship| {
                for (kind, node_id) in [
                    ("source", relationship.source),
                    ("target", relationship.target),
                ] {
                    match self.node_owned(node_id) {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            validation_error = Some(SkeinError::Storage(format!(
                                "relationship {} references missing {kind} node {}",
                                relationship.id.0, node_id.0
                            )));
                            return GraphScanControl::Stop;
                        }
                        Err(error) => {
                            validation_error = Some(error);
                            return GraphScanControl::Stop;
                        }
                    }
                }
                GraphScanControl::Continue
            })?;
            return validation_error.map_or(Ok(()), Err);
        }
        for relationship in self.relationships.values() {
            if !self.nodes.contains_key(&relationship.source) {
                return Err(SkeinError::Storage(format!(
                    "relationship {} references missing source node {}",
                    relationship.id.0, relationship.source.0
                )));
            }
            if !self.nodes.contains_key(&relationship.target) {
                return Err(SkeinError::Storage(format!(
                    "relationship {} references missing target node {}",
                    relationship.id.0, relationship.target.0
                )));
            }
        }
        Ok(())
    }

    fn delete_node_ops(&self, ids: &[NodeId], detach: bool) -> Result<Vec<WalOp>> {
        self.delete_node_ops_bounded(ids, detach, usize::MAX)
    }

    fn delete_node_ops_bounded(
        &self,
        ids: &[NodeId],
        detach: bool,
        max_operations: usize,
    ) -> Result<Vec<WalOp>> {
        let mut relationship_ids = BTreeSet::new();
        for id in ids {
            for relationship in self.relationship_records_owned() {
                let relationship = relationship?;
                if relationship.source == *id || relationship.target == *id {
                    if !detach {
                        return Err(SkeinError::Storage(format!(
                            "node {} has relationships; use DETACH DELETE",
                            id.0
                        )));
                    }
                    relationship_ids.insert(relationship.id);
                    if relationship_ids.len().saturating_add(ids.len()) > max_operations {
                        return Err(SkeinError::Execution(format!(
                            "mutation would exceed max_mutation_operations {max_operations}"
                        )));
                    }
                }
            }
        }
        if ids.len() > max_operations {
            return Err(SkeinError::Execution(format!(
                "mutation would exceed max_mutation_operations {max_operations}"
            )));
        }
        let mut ops = relationship_ids
            .into_iter()
            .map(|id| WalOp::DeleteRelationship { id })
            .collect::<Vec<_>>();
        ops.extend(ids.iter().copied().map(|id| WalOp::DeleteNode { id }));
        Ok(ops)
    }

    fn apply_delete_relationship(&mut self, id: RelId) {
        let Some(relationship) = self.relationships.remove(&id) else {
            return;
        };
        self.remove_relationship_from_basic_statistics(&relationship);
        self.remove_relationship_from_property_index(&relationship);
        self.remove_relationship_from_adjacency(&relationship);
    }

    fn remove_relationship_from_adjacency(&mut self, relationship: &RelRecord) {
        let outgoing_key = (relationship.source, relationship.rel_type);
        if let Some(ids) = self.outgoing.get_mut(&outgoing_key) {
            ids.remove(&relationship.id);
            if ids.is_empty() {
                self.outgoing.remove(&outgoing_key);
            }
        }
        let incoming_key = (relationship.target, relationship.rel_type);
        if let Some(ids) = self.incoming.get_mut(&incoming_key) {
            ids.remove(&relationship.id);
            if ids.is_empty() {
                self.incoming.remove(&incoming_key);
            }
        }
    }

    fn add_relationship_to_property_index(&mut self, relationship: &RelRecord) {
        for (property, value) in &relationship.properties {
            self.relationship_property_index
                .entry_or_default((relationship.rel_type, property.clone(), value.clone()))
                .insert(relationship.id);
        }
    }

    fn remove_relationship_from_property_index(&mut self, relationship: &RelRecord) {
        for (property, value) in &relationship.properties {
            let key = (relationship.rel_type, property.clone(), value.clone());
            if let Some(ids) = self.relationship_property_index.get_mut(&key) {
                ids.remove(&relationship.id);
                if ids.is_empty() {
                    self.relationship_property_index.remove(&key);
                }
            }
        }
    }

    fn apply_delete_node(&mut self, catalog: &Catalog, id: NodeId) {
        let Some(node) = self.nodes.remove(&id) else {
            return;
        };
        self.remove_node_from_basic_statistics(&node);
        self.remove_node_from_composite_property_indexes(catalog, &node);
        self.remove_node_from_full_text_property_indexes(catalog, &node);
        for label_id in node.labels {
            for (property, value) in &node.properties {
                let key = (label_id, property.clone(), value.clone());
                if let Some(ids) = self.property_index.get_mut(&key) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.property_index.remove(&key);
                    }
                }
            }
        }
    }

    fn load_checkpoint(&mut self, catalog: &mut Catalog, config: WalReplayConfig) -> Result<()> {
        let Some(durable) = &self.durable else {
            return Ok(());
        };
        if !durable.checkpoint_path.exists() {
            if durable.checkpoint_commit_epoch != 0 {
                return Err(SkeinError::Storage(format!(
                    "manifest checkpoint generation {} is missing",
                    durable.checkpoint_epoch
                )));
            }
            return Ok(());
        }
        let expected_generation = durable.checkpoint_epoch;
        let expected_commit_epoch = durable.checkpoint_commit_epoch;
        let durable_root_path = durable.root_path.clone();
        let text = durable.read_checkpoint_text(config)?;
        let (body, checksum) = split_checkpoint_checksum(&text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "checkpoint checksum mismatch: expected {checksum}, got {actual}"
            )));
        }
        let relational_checkpoint = relational_checkpoint_metadata(body)?;
        let mut loaded_search_projection_change_log_start_epoch = None;
        let mut loaded_generation = None;
        let mut loaded_commit_epoch = None;
        let mut loaded_statistics_complete = None;
        let mut saw_checkpoint_statistics = false;
        let mut canonical_records = false;
        let mut lines = body.lines();
        if lines.next() != Some(CHECKPOINT_HEADER_V1) {
            return Err(SkeinError::Storage(
                "checkpoint is missing the V1 format header".to_string(),
            ));
        }
        let mut saw_storage_version = false;
        for line in lines {
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["version", version] => {
                    if saw_storage_version {
                        return Err(SkeinError::Storage(
                            "checkpoint has duplicate storage version".to_string(),
                        ));
                    }
                    validate_storage_version(version)?;
                    saw_storage_version = true;
                }
                ["generation", raw] => {
                    loaded_generation = Some(parse_u64(raw, "checkpoint generation")?);
                }
                ["next_node_id", raw] => {
                    self.next_node_id = parse_u64(raw, "next_node_id")?;
                }
                ["next_rel_id", raw] => {
                    self.next_rel_id = parse_u64(raw, "next_rel_id")?;
                }
                ["canonical_records", "true"] => {
                    canonical_records = true;
                }
                ["commit_epoch", raw] => {
                    self.commit_epoch = parse_u64(raw, "commit_epoch")?;
                    loaded_commit_epoch = Some(self.commit_epoch);
                }
                ["relational_checkpoint_encoded_len", _]
                | ["relational_checkpoint_encoded_checksum", _]
                | ["relational_checkpoint_encoded_sha256", _] => {}
                ["search_projection_change_log_start_epoch", raw] => {
                    if loaded_search_projection_change_log_start_epoch.is_some() {
                        return Err(SkeinError::Storage(
                            "checkpoint contains duplicate search projection change log start epoch"
                                .to_string(),
                        ));
                    }
                    let start_epoch = parse_u64(raw, "search projection change log start epoch")?;
                    loaded_search_projection_change_log_start_epoch = Some(start_epoch);
                    self.search_projection_change_log_start_epoch = start_epoch;
                }
                ["initial_import_source_fingerprint", raw] => {
                    if self.initial_import_source_fingerprint.is_some() {
                        return Err(SkeinError::Storage(
                            "checkpoint contains duplicate initial import source fingerprint"
                                .to_string(),
                        ));
                    }
                    self.initial_import_source_fingerprint = Some(decode_string(raw)?);
                }
                ["search_projection_change", raw_commit_epoch, raw_upsert_node_ids, raw_delete_document_ids] =>
                {
                    self.search_projection_graph_changes
                        .push(SearchProjectionGraphChange {
                            commit_epoch: parse_u64(
                                raw_commit_epoch,
                                "search projection change commit epoch",
                            )?,
                            upsert_node_ids: decode_u64_vec(
                                raw_upsert_node_ids,
                                "search projection change upsert node id",
                            )?,
                            delete_document_ids: decode_string_vec(raw_delete_document_ids)?,
                        });
                }
                ["label", raw_id, raw_name] => {
                    let id = LabelId(parse_u32(raw_id, "label id")?);
                    catalog.import_label(id, decode_string(raw_name)?);
                }
                ["rel_type", raw_id, raw_name] => {
                    let id = RelTypeId(parse_u32(raw_id, "rel type id")?);
                    catalog.import_rel_type(id, decode_string(raw_name)?);
                }
                ["property_index", raw_id, raw_label_id, raw_property] => {
                    catalog.import_property_index(
                        crate::schema::IndexId(parse_u32(raw_id, "property index id")?),
                        LabelId(parse_u32(raw_label_id, "property index label id")?),
                        decode_string(raw_property)?,
                    );
                }
                ["property_index", raw_id, raw_label_id, raw_property, raw_kind] => {
                    catalog.import_property_index_with_kind(
                        crate::schema::IndexId(parse_u32(raw_id, "property index id")?),
                        LabelId(parse_u32(raw_label_id, "property index label id")?),
                        decode_string(raw_property)?,
                        decode_index_kind(raw_kind)?,
                    );
                }
                ["composite_property_index", raw_id, raw_label_id, raw_properties] => {
                    catalog.import_composite_property_index(
                        crate::schema::IndexId(parse_u32(raw_id, "composite property index id")?),
                        LabelId(parse_u32(
                            raw_label_id,
                            "composite property index label id",
                        )?),
                        decode_string_vec(raw_properties)?,
                    );
                }
                ["table", raw_id, raw_kind, raw_name, raw_state] => {
                    let kind = decode_table_kind(raw_kind)?;
                    let name = decode_string(raw_name)?;
                    match kind {
                        TableKind::Node => {
                            catalog.get_or_create_label(&name);
                        }
                        TableKind::Relationship => {
                            catalog.get_or_create_rel_type(&name);
                        }
                    }
                    catalog.import_table(
                        TableId(parse_u32(raw_id, "table id")?),
                        kind,
                        name,
                        decode_schema_object_state(raw_state)?,
                    );
                }
                ["property", raw_id, raw_table_id, raw_name, raw_type, raw_nullable, raw_state] => {
                    catalog.import_property_descriptor(
                        PropertyId(parse_u32(raw_id, "property id")?),
                        TableId(parse_u32(raw_table_id, "property table id")?),
                        decode_string(raw_name)?,
                        decode_property_type(raw_type)?,
                        decode_nullable(raw_nullable)?,
                        decode_schema_object_state(raw_state)?,
                    );
                }
                ["unique_constraint", raw_id, raw_label_id, raw_property] => {
                    catalog.import_unique_constraint(
                        ConstraintId(parse_u32(raw_id, "unique constraint id")?),
                        LabelId(parse_u32(raw_label_id, "unique constraint label id")?),
                        decode_string(raw_property)?,
                    );
                }
                ["node_property_exists_constraint", raw_id, raw_label_id, raw_property] => {
                    catalog.import_node_property_exists_constraint(
                        ConstraintId(parse_u32(raw_id, "node property exists constraint id")?),
                        LabelId(parse_u32(
                            raw_label_id,
                            "node property exists constraint label id",
                        )?),
                        decode_string(raw_property)?,
                    );
                }
                ["relationship_property_exists_constraint", raw_id, raw_rel_type_id, raw_property] =>
                {
                    catalog.import_relationship_property_exists_constraint(
                        ConstraintId(parse_u32(
                            raw_id,
                            "relationship property exists constraint id",
                        )?),
                        RelTypeId(parse_u32(
                            raw_rel_type_id,
                            "relationship property exists constraint rel type id",
                        )?),
                        decode_string(raw_property)?,
                    );
                }
                ["relationship_unique_constraint", raw_id, raw_rel_type_id, raw_property] => {
                    catalog.import_relationship_unique_constraint(
                        ConstraintId(parse_u32(raw_id, "relationship unique constraint id")?),
                        RelTypeId(parse_u32(
                            raw_rel_type_id,
                            "relationship unique constraint rel type id",
                        )?),
                        decode_string(raw_property)?,
                    );
                }
                ["stat_commit_epoch", raw] => {
                    saw_checkpoint_statistics = true;
                    let epoch = parse_u64(raw, "statistics commit epoch")?;
                    self.basic_statistics.computed_at_commit_epoch = epoch;
                    self.checkpoint_statistics.computed_at_commit_epoch = epoch;
                }
                ["stat_advanced_complete", raw] => {
                    if loaded_statistics_complete.is_some() {
                        return Err(SkeinError::Storage(
                            "checkpoint contains duplicate statistics completeness flag"
                                .to_string(),
                        ));
                    }
                    loaded_statistics_complete =
                        Some(decode_bool(raw, "statistics advanced completeness flag")?);
                }
                ["stat_histogram_sample_limit", raw] => {
                    self.checkpoint_statistics.histogram_sample_limit =
                        parse_usize(raw, "statistics histogram sample limit")?;
                }
                ["stat_node_count", raw] => {
                    let count = parse_u64(raw, "statistics node count")?;
                    self.basic_statistics.node_count = count;
                    self.checkpoint_statistics.node_count = count;
                }
                ["stat_relationship_count", raw] => {
                    let count = parse_u64(raw, "statistics relationship count")?;
                    self.basic_statistics.relationship_count = count;
                    self.checkpoint_statistics.relationship_count = count;
                }
                ["stat_label_count", raw_label_id, raw_count] => {
                    let label_id = LabelId(parse_u32(raw_label_id, "statistics label id")?);
                    let count = parse_u64(raw_count, "statistics label count")?;
                    self.basic_statistics.label_counts.insert(label_id, count);
                    self.checkpoint_statistics
                        .label_counts
                        .insert(label_id, count);
                }
                ["stat_rel_type_count", raw_rel_type_id, raw_count] => {
                    let rel_type_id = RelTypeId(parse_u32(
                        raw_rel_type_id,
                        "statistics relationship type id",
                    )?);
                    let count = parse_u64(raw_count, "statistics relationship type count")?;
                    self.basic_statistics
                        .rel_type_counts
                        .insert(rel_type_id, count);
                    self.checkpoint_statistics
                        .rel_type_counts
                        .insert(rel_type_id, count);
                }
                ["stat_rel_type_source_count", raw_rel_type_id, raw_count] => {
                    self.checkpoint_statistics.rel_type_source_counts.insert(
                        RelTypeId(parse_u32(
                            raw_rel_type_id,
                            "statistics relationship type id",
                        )?),
                        parse_u64(raw_count, "statistics relationship source count")?,
                    );
                }
                ["stat_rel_type_target_count", raw_rel_type_id, raw_count] => {
                    self.checkpoint_statistics.rel_type_target_counts.insert(
                        RelTypeId(parse_u32(
                            raw_rel_type_id,
                            "statistics relationship type id",
                        )?),
                        parse_u64(raw_count, "statistics relationship target count")?,
                    );
                }
                ["stat_path_count", raw_source, raw_rel_type, raw_target, raw_count] => {
                    self.checkpoint_statistics.path_counts.insert(
                        parse_statistics_path_key(raw_source, raw_rel_type, raw_target)?,
                        parse_u64(raw_count, "statistics path count")?,
                    );
                }
                ["stat_path_source_distinct_count", raw_source, raw_rel_type, raw_target, raw_count] =>
                {
                    self.checkpoint_statistics
                        .path_source_distinct_counts
                        .insert(
                            parse_statistics_path_key(raw_source, raw_rel_type, raw_target)?,
                            parse_u64(raw_count, "statistics path source distinct count")?,
                        );
                }
                ["stat_path_target_distinct_count", raw_source, raw_rel_type, raw_target, raw_count] =>
                {
                    self.checkpoint_statistics
                        .path_target_distinct_counts
                        .insert(
                            parse_statistics_path_key(raw_source, raw_rel_type, raw_target)?,
                            parse_u64(raw_count, "statistics path target distinct count")?,
                        );
                }
                ["stat_bounded_path_count", raw_source, raw_rel_type, raw_target, raw_hops, raw_count] =>
                {
                    self.checkpoint_statistics.bounded_path_counts.insert(
                        parse_statistics_bounded_path_key(
                            raw_source,
                            raw_rel_type,
                            raw_target,
                            raw_hops,
                        )?,
                        parse_u64(raw_count, "statistics bounded path count")?,
                    );
                }
                ["stat_bounded_path_source_distinct_count", raw_source, raw_rel_type, raw_target, raw_hops, raw_count] =>
                {
                    self.checkpoint_statistics
                        .bounded_path_source_distinct_counts
                        .insert(
                            parse_statistics_bounded_path_key(
                                raw_source,
                                raw_rel_type,
                                raw_target,
                                raw_hops,
                            )?,
                            parse_u64(raw_count, "statistics bounded path source distinct count")?,
                        );
                }
                ["stat_bounded_path_target_distinct_count", raw_source, raw_rel_type, raw_target, raw_hops, raw_count] =>
                {
                    self.checkpoint_statistics
                        .bounded_path_target_distinct_counts
                        .insert(
                            parse_statistics_bounded_path_key(
                                raw_source,
                                raw_rel_type,
                                raw_target,
                                raw_hops,
                            )?,
                            parse_u64(raw_count, "statistics bounded path target distinct count")?,
                        );
                }
                ["stat_property_distinct_count", raw_label_id, raw_property, raw_count] => {
                    self.checkpoint_statistics.property_distinct_counts.insert(
                        (
                            LabelId(parse_u32(raw_label_id, "statistics label id")?),
                            decode_string(raw_property)?,
                        ),
                        parse_u64(raw_count, "statistics property distinct count")?,
                    );
                }
                ["stat_rel_property_distinct_count", raw_rel_type_id, raw_property, raw_count] => {
                    self.checkpoint_statistics
                        .rel_property_distinct_counts
                        .insert(
                            (
                                RelTypeId(parse_u32(
                                    raw_rel_type_id,
                                    "statistics relationship type id",
                                )?),
                                decode_string(raw_property)?,
                            ),
                            parse_u64(
                                raw_count,
                                "statistics relationship property distinct count",
                            )?,
                        );
                }
                ["stat_rel_property_histogram", raw_rel_type_id, raw_property, raw_values] => {
                    self.checkpoint_statistics.rel_property_histograms.insert(
                        (
                            RelTypeId(parse_u32(
                                raw_rel_type_id,
                                "statistics relationship type id",
                            )?),
                            decode_string(raw_property)?,
                        ),
                        decode_value_vec(raw_values)?,
                    );
                }
                ["stat_property_histogram", raw_label_id, raw_property, raw_values] => {
                    self.checkpoint_statistics.property_histograms.insert(
                        (
                            LabelId(parse_u32(raw_label_id, "statistics label id")?),
                            decode_string(raw_property)?,
                        ),
                        decode_value_vec(raw_values)?,
                    );
                }
                ["stat_rel_property_histogram_sampled", raw_rel_type_id, raw_property, raw_sampled] =>
                {
                    self.checkpoint_statistics
                        .sampled_rel_property_histograms
                        .insert(
                            (
                                RelTypeId(parse_u32(
                                    raw_rel_type_id,
                                    "statistics relationship type id",
                                )?),
                                decode_string(raw_property)?,
                            ),
                            decode_bool(raw_sampled, "statistics sampled flag")?,
                        );
                }
                ["stat_property_histogram_sampled", raw_label_id, raw_property, raw_sampled] => {
                    self.checkpoint_statistics
                        .sampled_property_histograms
                        .insert(
                            (
                                LabelId(parse_u32(raw_label_id, "statistics label id")?),
                                decode_string(raw_property)?,
                            ),
                            decode_bool(raw_sampled, "statistics sampled flag")?,
                        );
                }
                ["project_graph", raw_name, raw_node_labels, raw_rel_types] => {
                    self.apply_project_graph_definition(
                        decode_string(raw_name)?,
                        ProjectedGraphDefinition {
                            node_labels: decode_string_vec(raw_node_labels)?,
                            rel_types: decode_string_vec(raw_rel_types)?,
                        },
                    );
                }
                ["node", raw_id, raw_labels, raw_properties] => {
                    let id = NodeId(parse_u64(raw_id, "node id")?);
                    let labels = parse_label_set(raw_labels)?;
                    let properties = decode_properties(raw_properties)?;
                    self.apply_create_node_with_labels(catalog, id, labels, properties);
                }
                ["rel", raw_id, raw_source, raw_target, raw_type, raw_properties] => {
                    self.apply_create_relationship(
                        RelId(parse_u64(raw_id, "rel id")?),
                        NodeId(parse_u64(raw_source, "rel source")?),
                        NodeId(parse_u64(raw_target, "rel target")?),
                        RelTypeId(parse_u32(raw_type, "rel type")?),
                        decode_properties(raw_properties)?,
                    );
                }
                [""] => {}
                _ => {
                    return Err(SkeinError::Storage(format!(
                        "invalid checkpoint line: {line}"
                    )));
                }
            }
        }
        if !saw_storage_version {
            return Err(SkeinError::Storage(
                "checkpoint is missing its storage version".to_string(),
            ));
        }
        if saw_checkpoint_statistics {
            self.checkpoint_statistics.advanced_statistics_complete =
                loaded_statistics_complete.unwrap_or(true);
        }
        if loaded_generation != Some(expected_generation) {
            return Err(SkeinError::Storage(format!(
                "checkpoint generation {:?} does not match manifest generation {expected_generation}",
                loaded_generation
            )));
        }
        if loaded_commit_epoch != Some(expected_commit_epoch) {
            return Err(SkeinError::Storage(format!(
                "checkpoint commit epoch {:?} does not match manifest commit epoch {expected_commit_epoch}",
                loaded_commit_epoch
            )));
        }
        if let Some(metadata) = relational_checkpoint {
            let path =
                durable_root_path.join(relational_checkpoint_generation_file(expected_generation));
            let max_bytes = RelationalDecodeLimits::checkpoint().max_record_bytes;
            let file_len = fs::metadata(&path)?.len();
            if file_len > max_bytes as u64 {
                return Err(SkeinError::Storage(format!(
                    "relational checkpoint contains {file_len} bytes, exceeding max_record_bytes {max_bytes}"
                )));
            }
            let bytes = fs::read(&path)?;
            verify_integrity(
                &bytes,
                metadata.encoded_len,
                metadata.encoded_checksum,
                metadata.encoded_sha256,
                "relational checkpoint",
            )?;
            let checkpoint =
                decode_relational_checkpoint_file(&path, RelationalDecodeLimits::checkpoint())
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
            if checkpoint.epoch != self.commit_epoch {
                return Err(SkeinError::Storage(format!(
                    "relational checkpoint epoch {} does not match graph commit epoch {}",
                    checkpoint.epoch, self.commit_epoch
                )));
            }
            self.relational_state = checkpoint.state;
        }
        match loaded_search_projection_change_log_start_epoch {
            Some(start_epoch) => {
                validate_search_projection_checkpoint_changes(
                    start_epoch,
                    self.commit_epoch,
                    &self.search_projection_graph_changes,
                )?;
            }
            None => {
                return Err(SkeinError::Storage(
                    "checkpoint search projection changes are missing their start epoch"
                        .to_string(),
                ));
            }
        }
        if canonical_records {
            let reader = self
                .durable
                .as_ref()
                .and_then(|durable| durable.canonical_segments.clone())
                .ok_or_else(|| {
                    SkeinError::Storage(
                        "checkpoint delegates records to missing canonical segments".to_string(),
                    )
                })?;
            let materialize = match config.residency_mode {
                StorageResidencyMode::Materialized => true,
                StorageResidencyMode::OutOfCore => false,
                StorageResidencyMode::Auto => {
                    reader.manifest().artifact_len <= config.auto_materialize_checkpoint_bytes
                }
            };
            if materialize {
                self.basic_statistics = BasicGraphStatistics::default();
                reader
                    .scan_nodes(|node| {
                        self.apply_create_node_with_labels(
                            catalog,
                            node.id,
                            node.labels,
                            node.properties,
                        );
                        Ok(())
                    })
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
                reader
                    .scan_relationships(|relationship| {
                        self.apply_create_relationship(
                            relationship.id,
                            relationship.source,
                            relationship.target,
                            relationship.rel_type,
                            relationship.properties,
                        );
                        Ok(())
                    })
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
            } else {
                self.basic_statistics.node_count = reader.manifest().node_count;
                self.basic_statistics.relationship_count = reader.manifest().relationship_count;
                self.canonical_base = Some(reader);
                self.canonical_adjacency = self
                    .durable
                    .as_ref()
                    .and_then(|durable| durable.canonical_adjacency.clone());
                self.persistent_property_projection = self
                    .durable
                    .as_ref()
                    .and_then(|durable| durable.persistent_property_projection.clone());
                self.canonical_base_out_of_core = true;
            }
        }
        if let Some(durable) = &mut self.durable {
            durable.relational_checkpoint_encoded_len =
                relational_checkpoint.map(|metadata| metadata.encoded_len);
            durable.relational_checkpoint_encoded_checksum =
                relational_checkpoint.map(|metadata| metadata.encoded_checksum);
            durable.relational_checkpoint_encoded_sha256 =
                relational_checkpoint.map(|metadata| metadata.encoded_sha256);
        }
        Ok(())
    }

    fn replay_wal(
        &mut self,
        catalog: &mut Catalog,
        config: WalReplayConfig,
    ) -> Result<StorageRecoveryReport> {
        let Some(durable) = &self.durable else {
            return Ok(StorageRecoveryReport::default());
        };
        let wal_path = durable.wal_path.clone();
        let checkpoint_epoch = durable.checkpoint_epoch;
        let checkpoint_commit_epoch = durable.checkpoint_commit_epoch;
        let wal_replay_start_lsn = durable.wal_replay_start_lsn;
        let wal_generation = durable.wal_generation;
        let read_only = durable.read_only;
        let checkpoint_present = durable.checkpoint_path.exists();
        let mut replayed_entries = 0_usize;
        let mut replayed_bytes = 0u64;
        let wal_present = wal_path.exists();
        if !wal_path.exists() {
            if checkpoint_epoch > 0 {
                return Err(SkeinError::Storage(format!(
                    "manifest WAL generation {wal_generation} is missing"
                )));
            }
            return Ok(StorageRecoveryReport {
                durable: true,
                recovery_mode: config.recovery_mode,
                max_wal_replay_entries: config.max_entries,
                max_wal_replay_bytes: config.max_bytes,
                max_wal_record_bytes: config.max_record_bytes,
                checkpoint_epoch: checkpoint_present.then_some(checkpoint_epoch),
                checkpoint_commit_epoch: checkpoint_present.then_some(checkpoint_commit_epoch),
                wal_present,
                wal_generation: Some(wal_generation),
                wal_replay_start_lsn: Some(wal_replay_start_lsn),
                next_lsn_after_replay: Some(wal_replay_start_lsn),
                replayed_wal_entries: replayed_entries,
                replayed_wal_bytes: replayed_bytes,
                torn_tail_ignored: false,
                torn_tail_repaired: false,
                discarded_wal_tail_bytes: 0,
                torn_tail_reason: None,
                recovered_commit_epoch: self.commit_epoch,
            });
        }
        let wal_len = fs::metadata(&wal_path)?.len();
        if config.max_bytes.is_some_and(|limit| wal_len > limit) {
            return Err(SkeinError::Storage(format!(
                "WAL replay byte limit exceeded: max_wal_replay_bytes={}",
                config.max_bytes.unwrap_or_default()
            )));
        }
        let mut cursor = match WalRecordCursor::open(&wal_path, config.max_record_bytes)? {
            WalOpenOutcome::Cursor(cursor) => cursor,
            WalOpenOutcome::MissingHeader => {
                return Err(SkeinError::Storage(format!(
                    "WAL generation {wal_generation} is missing its header"
                )));
            }
            WalOpenOutcome::HeaderTorn { reason } => {
                return Err(SkeinError::Storage(reason));
            }
            WalOpenOutcome::HeaderCorrupt { reason } => {
                return reject_corrupt_wal_record(&wal_path, wal_generation, read_only, 0, reason);
            }
        };
        if cursor.generation() != wal_generation || cursor.start_lsn() != wal_replay_start_lsn {
            return Err(SkeinError::Storage(format!(
                "WAL header generation/start ({}, {}) does not match manifest ({wal_generation}, {wal_replay_start_lsn})",
                cursor.generation(),
                cursor.start_lsn()
            )));
        }
        let mut expected_lsn = wal_replay_start_lsn;
        loop {
            let (entry, record_start, record_encoded_len) = match cursor.next()? {
                WalCursorEvent::Eof => break,
                WalCursorEvent::TornTail { reason, .. } => {
                    return Err(SkeinError::Storage(format!(
                        "strict WAL recovery rejected torn tail: {reason}; use DatabaseDoctor to inspect and explicitly repair the incomplete final record"
                    )));
                }
                WalCursorEvent::Corrupt { offset, reason } => {
                    return reject_corrupt_wal_record(
                        &wal_path,
                        wal_generation,
                        read_only,
                        offset,
                        reason,
                    );
                }
                WalCursorEvent::Entry {
                    entry,
                    start_offset,
                    encoded_len,
                } => (entry, start_offset, encoded_len),
            };
            if entry.lsn != expected_lsn {
                quarantine_corrupt_wal(&wal_path, wal_generation, read_only)?;
                return Err(SkeinError::Storage(format!(
                    "WAL LSN sequence mismatch at byte offset {record_start}: expected {expected_lsn}, got {}",
                    entry.lsn
                )));
            }
            if let Some(max_entries) = config.max_entries
                && replayed_entries >= max_entries
            {
                return Err(SkeinError::Storage(format!(
                    "WAL replay entry limit exceeded: max_wal_replay_entries={max_entries}"
                )));
            }
            if let WalOp::Batch(ops) = &entry.op
                && config
                    .max_batch_operations
                    .is_some_and(|limit| ops.len() > limit)
            {
                return Err(SkeinError::Storage(format!(
                    "WAL batch operation limit exceeded: max_wal_batch_operations={}",
                    config.max_batch_operations.unwrap_or_default()
                )));
            }
            replayed_entries += 1;
            replayed_bytes = replayed_bytes.saturating_add(record_encoded_len);
            expected_lsn = expected_lsn
                .checked_add(1)
                .ok_or_else(|| SkeinError::Storage("WAL LSN overflow during replay".to_string()))?;
            match entry.op {
                WalOp::Batch(ops) => {
                    self.ensure_out_of_core_delta_replay_admission(&ops)?;
                    let commit_epoch = self.commit_epoch + 1;
                    self.record_search_projection_graph_changes_for_ops(
                        catalog,
                        commit_epoch,
                        &ops,
                    );
                    for op in ops {
                        self.apply_wal_op(catalog, op)?;
                    }
                    self.commit_epoch += 1;
                }
                op => {
                    self.ensure_out_of_core_delta_replay_admission(std::slice::from_ref(&op))?;
                    let commit_epoch = self.commit_epoch + 1;
                    self.record_search_projection_graph_changes_for_ops(
                        catalog,
                        commit_epoch,
                        std::slice::from_ref(&op),
                    );
                    self.apply_wal_op(catalog, op)?;
                    self.commit_epoch += 1;
                }
            }
        }
        if let Some(durable) = &mut self.durable {
            durable.next_lsn = expected_lsn;
            durable.wal_commit_epoch = self.commit_epoch;
        }
        Ok(StorageRecoveryReport {
            durable: true,
            recovery_mode: config.recovery_mode,
            max_wal_replay_entries: config.max_entries,
            max_wal_replay_bytes: config.max_bytes,
            max_wal_record_bytes: config.max_record_bytes,
            checkpoint_epoch: checkpoint_present.then_some(checkpoint_epoch),
            checkpoint_commit_epoch: checkpoint_present.then_some(checkpoint_commit_epoch),
            wal_present,
            wal_generation: Some(wal_generation),
            wal_replay_start_lsn: Some(wal_replay_start_lsn),
            next_lsn_after_replay: Some(expected_lsn),
            replayed_wal_entries: replayed_entries,
            replayed_wal_bytes: replayed_bytes,
            torn_tail_ignored: false,
            torn_tail_repaired: false,
            discarded_wal_tail_bytes: 0,
            torn_tail_reason: None,
            recovered_commit_epoch: self.commit_epoch,
        })
    }

    fn materialize_node_for_write(&mut self, id: NodeId) -> Result<bool> {
        if self.node_tombstones.contains(&id) {
            return Ok(false);
        }
        if self.nodes.contains_key(&id) {
            return Ok(true);
        }
        let Some(node) = self
            .canonical_base
            .as_ref()
            .map(|reader| reader.get_node(id).map_err(canonical_segment_error))
            .transpose()?
            .flatten()
        else {
            return Ok(false);
        };
        self.nodes.insert(id, node);
        Ok(true)
    }

    fn materialize_relationship_for_write(&mut self, id: RelId) -> Result<bool> {
        if self.relationship_tombstones.contains(&id) {
            return Ok(false);
        }
        if self.relationships.contains_key(&id) {
            return Ok(true);
        }
        let Some(relationship) = self
            .canonical_base
            .as_ref()
            .map(|reader| reader.get_relationship(id).map_err(canonical_segment_error))
            .transpose()?
            .flatten()
        else {
            return Ok(false);
        };
        self.relationships.insert(id, relationship);
        Ok(true)
    }

    fn apply_wal_op(&mut self, catalog: &mut Catalog, op: WalOp) -> Result<()> {
        let result = self.apply_wal_op_inner(catalog, op);
        if result.is_err() && self.durable.is_some() {
            self.post_wal_apply_poisoned = true;
        }
        result
    }

    fn apply_wal_op_inner(&mut self, catalog: &mut Catalog, op: WalOp) -> Result<()> {
        wal_apply_failpoint()?;
        match op {
            WalOp::CreateNodeLabel { label } => {
                catalog.get_or_create_label(&label);
            }
            WalOp::CreateRelationshipType { rel_type } => {
                catalog.get_or_create_rel_type(&rel_type);
            }
            WalOp::CreateNodeTable { name } => {
                catalog.get_or_create_label(&name);
                catalog.get_or_create_table(TableKind::Node, &name);
            }
            WalOp::CreateRelationshipTable { name } => {
                catalog.get_or_create_rel_type(&name);
                catalog.get_or_create_table(TableKind::Relationship, &name);
            }
            WalOp::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => {
                let table_id = ensure_table_descriptor(catalog, table_kind, &table);
                catalog.get_or_create_property(table_id, &property, value_type, nullable);
            }
            WalOp::AlterTableState {
                table_kind,
                table,
                state,
            } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.set_table_state(id, state);
                }
            }
            WalOp::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table)
                    && let Some(id) = catalog.property_descriptor_id(table_id, &property)
                {
                    catalog.set_property_state(id, state);
                }
            }
            WalOp::GcPropertyDescriptor {
                table_kind,
                table,
                property,
            } => {
                if let Some(table_id) = catalog.table_id(table_kind, &table)
                    && let Some(id) = catalog.property_descriptor_id(table_id, &property)
                {
                    catalog.remove_property_descriptor(id);
                }
            }
            WalOp::GcTableDescriptor { table_kind, table } => {
                if let Some(id) = catalog.table_id(table_kind, &table) {
                    catalog.remove_table_descriptor(id);
                }
            }
            WalOp::CreateIndex { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_property_index(label_id, &property);
                // Replay order decides whether anything is here to backfill:
                // an index declared before its nodes finds none, and one
                // declared after them finds exactly the nodes that were
                // written while the property was unindexed.
                self.backfill_property_index(label_id, &property);
            }
            WalOp::CreateCompositeIndex { label, properties } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_composite_property_index(label_id, &properties);
                self.rebuild_composite_property_index_for_descriptor(label_id, &properties);
            }
            WalOp::CreateRangeIndex { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_property_index_with_kind(
                    label_id,
                    &property,
                    IndexKind::Range,
                );
            }
            WalOp::CreateFullTextIndex { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_property_index_with_kind(
                    label_id,
                    &property,
                    IndexKind::FullText,
                );
                self.rebuild_full_text_property_index_for_descriptor(label_id, &property);
            }
            WalOp::CreateUniqueConstraint { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_unique_constraint(label_id, &property);
            }
            WalOp::CreateNodePropertyExistsConstraint { label, property } => {
                let label_id = catalog.get_or_create_label(&label);
                catalog.get_or_create_node_property_exists_constraint(label_id, &property);
            }
            WalOp::CreateRelationshipUniqueConstraint { rel_type, property } => {
                let rel_type_id = catalog.get_or_create_rel_type(&rel_type);
                catalog.get_or_create_relationship_unique_constraint(rel_type_id, &property);
            }
            WalOp::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
                let rel_type_id = catalog.get_or_create_rel_type(&rel_type);
                catalog
                    .get_or_create_relationship_property_exists_constraint(rel_type_id, &property);
            }
            WalOp::CreateNode {
                id,
                label,
                properties,
            } => {
                let label_id = catalog.get_or_create_label(&label);
                self.apply_create_node(catalog, id, label_id, properties);
            }
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type,
                properties,
            } => {
                let rel_type_id = catalog.get_or_create_rel_type(&rel_type);
                self.apply_create_relationship(id, source, target, rel_type_id, properties);
            }
            WalOp::SetNodeProperty {
                id,
                property,
                value,
            } => {
                self.materialize_node_for_write(id)?;
                self.apply_set_node_property(catalog, id, property, value);
            }
            WalOp::SetRelationshipProperty {
                id,
                property,
                value,
            } => {
                self.materialize_relationship_for_write(id)?;
                self.apply_set_relationship_property(id, property, value);
            }
            WalOp::DeleteNode { id } => {
                let base_exists = self
                    .canonical_base
                    .as_ref()
                    .map(|reader| reader.get_node(id).map_err(canonical_segment_error))
                    .transpose()?
                    .flatten()
                    .is_some();
                self.materialize_node_for_write(id)?;
                self.apply_delete_node(catalog, id);
                if base_exists {
                    self.node_tombstones.insert(id);
                }
            }
            WalOp::DeleteRelationship { id } => {
                let base_exists = self
                    .canonical_base
                    .as_ref()
                    .map(|reader| reader.get_relationship(id).map_err(canonical_segment_error))
                    .transpose()?
                    .flatten()
                    .is_some();
                self.materialize_relationship_for_write(id)?;
                self.apply_delete_relationship(id);
                if base_exists {
                    self.relationship_tombstones.insert(id);
                }
            }
            WalOp::ProjectGraph {
                name,
                node_labels,
                rel_types,
            } => {
                self.apply_project_graph_definition(
                    name,
                    ProjectedGraphDefinition {
                        node_labels,
                        rel_types,
                    },
                );
            }
            WalOp::MarkInitialImportSource { source_fingerprint } => {
                self.initial_import_source_fingerprint = Some(source_fingerprint);
            }
            WalOp::Relational { record } => {
                let batch = decode_relational_wal_batch(&record, RelationalDecodeLimits::wal())
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
                let expected_epoch = self.commit_epoch.saturating_add(1);
                if batch.epoch != expected_epoch {
                    return Err(SkeinError::Storage(format!(
                        "relational WAL epoch mismatch: expected {expected_epoch}, got {}",
                        batch.epoch
                    )));
                }
                self.relational_state = self
                    .relational_state
                    .stage_transaction(
                        batch.transaction,
                        self.relational_mutation_limits,
                        self.relational_overflow_config,
                    )
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
            }
            WalOp::RelationalSnapshot { record } => {
                let checkpoint =
                    decode_relational_checkpoint(&record, RelationalDecodeLimits::checkpoint())
                        .map_err(|error| SkeinError::Storage(error.to_string()))?;
                let expected_epoch = self.commit_epoch.saturating_add(1);
                if checkpoint.epoch != expected_epoch {
                    return Err(SkeinError::Storage(format!(
                        "relational snapshot WAL epoch mismatch: expected {expected_epoch}, got {}",
                        checkpoint.epoch
                    )));
                }
                self.relational_state = checkpoint.state;
            }
            WalOp::Batch(ops) => {
                for op in ops {
                    self.apply_wal_op(catalog, op)?;
                }
            }
        }
        Ok(())
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
        | WalOp::RelationalSnapshot { .. } => {}
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
        body.push_str(&format!(
            "graph\t{}\t{}\t{}\t{}\t{}\n",
            encode_string(name),
            encode_string_vec(&definition.node_labels),
            encode_string_vec(&definition.rel_types),
            graph.node_count(),
            graph.edge_count()
        ));
        body.push_str(&format!(
            "nodes\t{}\n",
            encode_u64_vec(graph.nodes().iter().map(|node| node.0))
        ));
        body.push_str(&format!(
            "csr_offsets\t{}\n",
            encode_usize_vec(graph.csr_offsets().iter().copied())
        ));
        body.push_str(&format!(
            "csr_targets\t{}\n",
            encode_usize_vec(graph.csr_targets().iter().copied())
        ));
        body.push_str(&format!(
            "csc_offsets\t{}\n",
            encode_usize_vec(graph.csc_offsets().iter().copied())
        ));
        body.push_str(&format!(
            "csc_sources\t{}\n",
            encode_usize_vec(graph.csc_sources().iter().copied())
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
                let graph = ProjectedGraph::from_parts(
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
                        graph,
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

fn compute_statistics(
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    relationships: &CowSegmentedMap<RelId, RelRecord>,
    computed_at_commit_epoch: u64,
) -> GraphStatistics {
    compute_statistics_with_basic(
        nodes,
        relationships,
        compute_basic_statistics(nodes, relationships, computed_at_commit_epoch),
    )
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
    basic_statistics: BasicGraphStatistics,
) -> GraphStatistics {
    let mut statistics = graph_statistics_from_basic(basic_statistics, true);
    let mut property_values = BTreeMap::<(LabelId, String), BTreeSet<Value>>::new();
    let mut rel_property_values = BTreeMap::<(RelTypeId, String), BTreeSet<Value>>::new();
    let mut rel_type_sources = BTreeMap::<RelTypeId, BTreeSet<NodeId>>::new();
    let mut rel_type_targets = BTreeMap::<RelTypeId, BTreeSet<NodeId>>::new();
    let mut path_sources = BTreeMap::<(LabelId, RelTypeId, LabelId), BTreeSet<NodeId>>::new();
    let mut path_targets = BTreeMap::<(LabelId, RelTypeId, LabelId), BTreeSet<NodeId>>::new();
    let mut outgoing_by_source_type = BTreeMap::<(NodeId, RelTypeId), Vec<NodeId>>::new();

    for node in nodes.values() {
        for label_id in &node.labels {
            for (property, value) in &node.properties {
                property_values
                    .entry((*label_id, property.clone()))
                    .or_default()
                    .insert(value.clone());
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
            rel_property_values
                .entry((relationship.rel_type, property.clone()))
                .or_default()
                .insert(value.clone());
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

fn compute_node_property_distinct_counts_from_index(
    property_index: &NodePropertyIndex,
) -> BTreeMap<(LabelId, String), u64> {
    let mut counts = BTreeMap::new();
    for (label_id, property, _) in property_index.keys() {
        *counts.entry((*label_id, property.clone())).or_default() += 1;
    }
    counts
}

fn compute_relationship_property_distinct_counts_from_index(
    relationship_property_index: &RelationshipPropertyIndex,
) -> BTreeMap<(RelTypeId, String), u64> {
    let mut counts = BTreeMap::new();
    for (rel_type, property, _) in relationship_property_index.keys() {
        *counts.entry((*rel_type, property.clone())).or_default() += 1;
    }
    counts
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
                if catalog.property_index_id(*label_id, property).is_none() {
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
            rel_ids.iter_copied().collect(),
        );
    }
    for ((node_id, rel_type), rel_ids) in incoming.iter() {
        groups.insert(
            AdjacencyGroupKey {
                node_id: *node_id,
                rel_type: *rel_type,
                direction: AdjacencyDirection::Incoming,
            },
            rel_ids.iter_copied().collect(),
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

fn split_stable_id_mapping_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "stable id mapping missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "stable id mapping checksum")?;
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

fn encode_stable_id_mapping(mapping: &StoreStableIdMapping) -> String {
    let mut body = String::new();
    body.push_str("SKEIN_STABLE_ID_MAPPING_V1\n");
    body.push_str(&format!("version\t{STORAGE_VERSION}\n"));
    for (id, stable_id) in &mapping.node_stable_ids {
        body.push_str(&format!("node\t{}\t{}\n", id.0, encode_value(stable_id)));
    }
    for (id, stable_id) in &mapping.relationship_stable_ids {
        body.push_str(&format!("rel\t{}\t{}\n", id.0, encode_value(stable_id)));
    }
    body
}

fn decode_stable_id_mapping(body: &str) -> Result<StoreStableIdMapping> {
    let mut mapping = StoreStableIdMapping::default();
    for line in body.lines() {
        if line == "SKEIN_STABLE_ID_MAPPING_V1" {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["version", version] => validate_storage_version(version)?,
            ["node", raw_id, raw_value] => {
                mapping.node_stable_ids.insert(
                    NodeId(parse_u64(raw_id, "stable id node id")?),
                    decode_value(raw_value)?,
                );
            }
            ["rel", raw_id, raw_value] => {
                mapping.relationship_stable_ids.insert(
                    RelId(parse_u64(raw_id, "stable id relationship id")?),
                    decode_value(raw_value)?,
                );
            }
            [""] => {}
            _ => {
                return Err(SkeinError::Storage(format!(
                    "invalid stable id mapping line: {line}"
                )));
            }
        }
    }
    Ok(mapping)
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
        "l" => decode_list_value(rest),
        "m" => decode_map_value(rest),
        _ => Err(SkeinError::Storage(format!(
            "invalid encoded value: {input}"
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
    input
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn decode_string(input: &str) -> Result<String> {
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
    String::from_utf8(bytes).map_err(|error| SkeinError::Storage(error.to_string()))
}

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn encode_bytes_base64(input: &[u8]) -> String {
    let mut output = String::with_capacity(input.len().div_ceil(3).saturating_mul(4));
    for chunk in input.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(BASE64_ALPHABET[(first >> 2) as usize] as char);
        output.push(BASE64_ALPHABET[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        if chunk.len() > 1 {
            output.push(BASE64_ALPHABET[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char);
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(BASE64_ALPHABET[(third & 0x3f) as usize] as char);
        } else {
            output.push('=');
        }
    }
    output
}

fn decode_bytes_base64(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(4) {
        return Err(SkeinError::Storage(
            "invalid base64 byte string length".to_string(),
        ));
    }
    let mut output = Vec::with_capacity(input.len() / 4 * 3);
    let chunks = input.as_bytes().chunks_exact(4);
    let chunk_count = chunks.len();
    for (index, chunk) in chunks.enumerate() {
        let last = index + 1 == chunk_count;
        let a = decode_base64_digit(chunk[0])?;
        let b = decode_base64_digit(chunk[1])?;
        let c_padding = chunk[2] == b'=';
        let d_padding = chunk[3] == b'=';
        if !last && (c_padding || d_padding) || c_padding && !d_padding {
            return Err(SkeinError::Storage(
                "invalid base64 byte string padding".to_string(),
            ));
        }
        let c = if c_padding {
            0
        } else {
            decode_base64_digit(chunk[2])?
        };
        let d = if d_padding {
            0
        } else {
            decode_base64_digit(chunk[3])?
        };
        if c_padding && b & 0x0f != 0 || d_padding && !c_padding && c & 0x03 != 0 {
            return Err(SkeinError::Storage(
                "non-canonical base64 byte string padding".to_string(),
            ));
        }
        output.push((a << 2) | (b >> 4));
        if !c_padding {
            output.push((b << 4) | (c >> 2));
        }
        if !d_padding {
            output.push((c << 6) | d);
        }
    }
    Ok(output)
}

fn decode_base64_digit(value: u8) -> Result<u8> {
    match value {
        b'A'..=b'Z' => Ok(value - b'A'),
        b'a'..=b'z' => Ok(value - b'a' + 26),
        b'0'..=b'9' => Ok(value - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(SkeinError::Storage(
            "invalid base64 byte string digit".to_string(),
        )),
    }
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
        Value::List(values) => values.iter().fold(16u64, |bytes, value| {
            bytes.saturating_add(estimated_value_bytes(value))
        }),
        Value::Map(values) => estimated_properties_bytes(values),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_adjacency_artifact_generation_file, canonical_adjacency_manifest_generation_file,
        checksum_bytes, compute_statistics, encode_durable_text,
        property_projection_artifact_generation_file, property_spill_artifact_generation_file,
        read_durable_text, restore_storage_backup, set_checkpoint_failpoint,
        set_wal_apply_failpoint, source_scan, AdjacencyConsolidationPlan, AdjacencyDirection,
        AdjacencyGroupStats, AdjacencyLayout, CheckpointPublishStage, ConnectedNodesCreate,
        CowSegmentedMap, DatabaseDoctor, DegreeStatisticsEntry, DegreeStatisticsKey,
        DurableCompression, GraphScanControl, GraphStore, NodeId, NodeRecord, NodeSetAssignment,
        NodeSetValue, OrderedAdjacencyEntry, ProjectedGraphDefinition, PropertyFilter, RelId,
        RelRecord, RelTypeId, RelationshipDeleteRequest, ScanPruningStrategy,
        ScanPruningTargetKind, SearchProjectionGraphChange, SourceScanCandidateRead,
        WalDoctorOptions, COW_MAP_TARGET_SEGMENT_BYTES, DENSE_ADJACENCY_DEGREE_THRESHOLD,
        DURABLE_COMPRESSION_HEADER, MANIFEST_FILE,
    };
    use crate::schema::{Catalog, LabelId};
    use crate::value::Value;
    use skein_integrity::integrity_digest;
    use skein_storage::{
        DurabilityPolicy, GraphMutation, MutationLimits, RelationalColumnSchema,
        RelationalHydrationBudget, RelationalInsertMode, RelationalKey, RelationalRow,
        RelationalScalarType, RelationalTableSchema, RelationalTransaction, RelationalValue,
        RelationalWrite, ScanPredicate, ScanSegmentAccessPlan, ScanSegmentFallback,
        ScanSegmentManifest, StorageResidencyMode, WalReplayConfig,
    };
    use std::collections::{BTreeMap, BTreeSet};
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::num::{NonZeroU64, NonZeroUsize};

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
        let report = {
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
                    properties([("weight", Value::Int(7))]),
                )
                .unwrap();
            store.backup_to(&catalog, &backup).unwrap()
        };
        assert!(report.generation > 0);
        assert_eq!(report.file_count, 11);

        let restore = restore_storage_backup(&backup, &restored).unwrap();
        assert_eq!(restore.generation, report.generation);
        assert_eq!(restore.manifest_checksum, report.manifest_checksum);
        let mut catalog = Catalog::default();
        let store = GraphStore::open(&restored, &mut catalog).unwrap();
        assert_eq!(store.scan_nodes(None).count(), 2);
        assert_eq!(store.scan_relationships(None).count(), 1);
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
        assert!(backup_error.to_string().contains("already exists"));

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
            let manifest = store.canonical_adjacency_manifest().unwrap();
            let mention_type = catalog.rel_type_id("MENTIONS").unwrap();
            let outgoing = manifest
                .blocks
                .iter()
                .filter(|block| {
                    block.direction == AdjacencyDirection::Outgoing
                        && block.endpoint == source
                        && block.rel_type == mention_type
                })
                .collect::<Vec<_>>();
            assert!(!outgoing.is_empty());
            assert!(outgoing
                .iter()
                .all(|block| block.layout == AdjacencyLayout::Dense));
            corrupt_offset = outgoing[0].offset + 48;

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
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_checkpoint_requires_adjacency_metadata() {
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
                if line.starts_with("canonical_adjacency_manifest_") {
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
        fs::remove_file(path.join(canonical_adjacency_manifest_generation_file(1))).unwrap();

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
            let spill_manifest = store.property_spill_manifest().unwrap();
            assert_eq!(spill_manifest.value_count, 2);
            assert!(spill_manifest.value_bytes > 64 * 1024);
            corrupt_offset = spill_manifest.blocks[0].offset + 40;
            let canonical_manifest = store.canonical_segment_manifest().unwrap();
            assert!(canonical_manifest
                .node_segments()
                .all(|segment| segment.length.get() < 16 * 1024));
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
                .create_full_text_property_index(&mut catalog, "Memory", "content")
                .unwrap();
            first_id = store
                .create_node(
                    &mut catalog,
                    "Memory",
                    properties([
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
                        ("rank", Value::Int(30)),
                        ("content", Value::String("delta".to_string())),
                    ]),
                )
                .unwrap();
            store.checkpoint(&catalog).unwrap();

            let label_id = catalog.label_id("Memory").unwrap();
            let manifest = store.persistent_property_projection_manifest().unwrap();
            assert!(manifest.supports(label_id, "rank", PersistentPropertyProjectionKind::Range));
            assert!(manifest.supports(
                label_id,
                "content",
                PersistentPropertyProjectionKind::FullText
            ));
            corrupt_offset = manifest
                .blocks
                .iter()
                .find(|block| block.kind == PersistentPropertyProjectionKind::Range)
                .map(|block| block.offset + block.length.get() - 1)
                .unwrap();

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
            let mut full_text_ids = Vec::new();
            store
                .visit_nodes_by_full_text_property_owned(label_id, "content", "beta", |node| {
                    full_text_ids.push(node.id);
                    GraphScanControl::Continue
                })
                .unwrap();
            assert_eq!(full_text_ids, vec![first_id, second_id]);

            let second_filter = PropertyFilter::IdEq {
                value: Value::Int(second_id.0 as i64),
            };
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
            full_text_ids.clear();
            store
                .visit_nodes_by_full_text_property_owned(label_id, "content", "beta", |node| {
                    full_text_ids.push(node.id);
                    GraphScanControl::Continue
                })
                .unwrap();
            assert_eq!(full_text_ids, vec![delta_id]);
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
            let lower = (Value::Int(0), true);
            let upper = (Value::Int(50), true);
            let error = store
                .visit_nodes_by_property_range_owned(
                    label_id,
                    "rank",
                    Some(&lower),
                    Some(&upper),
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
                },
                SearchProjectionGraphChange {
                    commit_epoch: 2,
                    upsert_node_ids: Vec::new(),
                    delete_document_ids: vec!["memory:deleted-memory".to_string()],
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
            std::fs::remove_dir_all(path).unwrap();
        }
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
        assert!(!path.join("adjacency.1.skein").exists());
        assert!(!path.join("adjacency.1.manifest.skein").exists());
        assert!(!path.join("properties.1.skein").exists());
        assert!(!path.join("properties.1.manifest.skein").exists());
        assert!(!path.join("property-index.1.skein").exists());
        assert!(!path.join("property-index.1.manifest.skein").exists());
        assert!(path.join("checkpoint.2.skein").exists());
        assert!(path.join("wal.2.skein").exists());
        assert!(path.join("canonical.2.skein").exists());
        assert!(path.join("canonical.2.manifest.skein").exists());
        assert!(path.join("adjacency.2.skein").exists());
        assert!(path.join("adjacency.2.manifest.skein").exists());
        assert!(path.join("properties.2.skein").exists());
        assert!(path.join("properties.2.manifest.skein").exists());
        assert!(path.join("property-index.2.skein").exists());
        assert!(path.join("property-index.2.manifest.skein").exists());
        assert!(path.join("checkpoint.3.skein").exists());
        assert!(path.join("wal.3.skein").exists());
        assert!(path.join("canonical.3.skein").exists());
        assert!(path.join("canonical.3.manifest.skein").exists());
        assert!(path.join("adjacency.3.skein").exists());
        assert!(path.join("adjacency.3.manifest.skein").exists());
        assert!(path.join("properties.3.skein").exists());
        assert!(path.join("properties.3.manifest.skein").exists());
        assert!(path.join("property-index.3.skein").exists());
        assert!(path.join("property-index.3.manifest.skein").exists());
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
            let payload = super::encode_binary_wal_record(entry, index as u64 + 1);
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
        super::decode_wal_records_as_v1_text(&active_wal_path(path))
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
