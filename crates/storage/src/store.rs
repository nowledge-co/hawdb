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
use crate::error::{HawDBError, Result};
use crate::schema::{
    AdvancedStatisticsFreshness, BasicGraphStatistics, Catalog, ConstraintId, GraphStatistics,
    IndexId, IndexKind, IndexStatisticsSample, LabelId, PropertyId, PropertyType, RelTypeId,
    SchemaObjectState, TableDescriptor, TableId, TableKind,
};
use crate::telemetry::TelemetrySink;
use crate::value::Value;
use hawdb_core::{DeclaredPropertyDefault, PropertyDefault, RuntimeTaskContext};
use hawdb_integrity::{checksum_u64, Sha256Digest};
use hawdb_storage::mutation::compact_transaction_graph_ops;
use hawdb_storage::projection::projection_document_id_for_node as search_projection_document_id_for_node;

#[derive(Debug)]
struct RuntimeGovernorBackgroundAdmission(hawdb_qos::RuntimeGovernor);

#[derive(Debug)]
struct RuntimeGovernorBackgroundPermit {
    _permit: hawdb_qos::RuntimePermit,
}

impl hawdb_storage::background::BackgroundWorkPermit for RuntimeGovernorBackgroundPermit {}

struct RootStorageTelemetry(Arc<dyn TelemetrySink>);

impl std::fmt::Debug for RootStorageTelemetry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RootStorageTelemetry")
    }
}

impl hawdb_storage::telemetry::StorageTelemetrySink for RootStorageTelemetry {
    fn record_wal_append(&self, event: hawdb_storage::telemetry::WalAppendTelemetry) {
        self.0.record_kernel(hawdb_telemetry::KernelTelemetry {
            operation: hawdb_telemetry::KernelTelemetryOperation::WalAppend,
            success: event.success,
            elapsed_micros: event.elapsed_micros,
            item_count: event.operation_count,
            byte_count: event.byte_count,
            fsync_micros: event.fsync_micros,
            generation: Some(event.generation),
        });
    }
}

impl hawdb_storage::background::BackgroundWorkAdmission for RuntimeGovernorBackgroundAdmission {
    fn try_admit(
        &self,
        request: hawdb_storage::background::BackgroundWorkRequest,
    ) -> std::result::Result<Box<dyn hawdb_storage::background::BackgroundWorkPermit>, String> {
        self.0
            .try_admit(hawdb_qos::RuntimeWorkRequest {
                priority: hawdb_qos::RuntimeWorkPriority::Background,
                kind: hawdb_qos::RuntimeWorkKind::Control,
                cpu_slots: request.cpu_slots,
                memory_bytes: request.memory_bytes,
                retained_memory_bytes: 0,
                io_slots: request.io_slots,
                io_reservation_scope: hawdb_qos::RuntimeIoReservationScope::Task,
                result_bytes: 0,
                blocking: false,
            })
            .map(|permit| {
                Box::new(RuntimeGovernorBackgroundPermit { _permit: permit })
                    as Box<dyn hawdb_storage::background::BackgroundWorkPermit>
            })
            .map_err(|error| error.to_string())
    }
}
#[path = "store/append_tables.rs"]
mod append_tables;
#[path = "store/backup.rs"]
mod backup;
#[path = "store/derived_repair.rs"]
#[doc(hidden)]
pub mod derived_repair;
#[path = "store/doctor.rs"]
#[doc(hidden)]
pub mod doctor;
#[path = "store/durable.rs"]
mod durable;
#[path = "store/graph_apply.rs"]
mod graph_apply;
#[path = "store/graph_checkpoint.rs"]
mod graph_checkpoint;
mod wal_fence;
#[doc(hidden)]
pub use graph_checkpoint::CheckpointPreparation;
#[path = "store/graph_checkpoint_candidate.rs"]
mod graph_checkpoint_candidate;
#[path = "store/graph_checkpoint_projection.rs"]
mod graph_checkpoint_projection;
#[doc(hidden)]
pub use graph_checkpoint_candidate::{
    CheckpointCandidate, CheckpointDebtSnapshot, CheckpointReclamationContext,
    CheckpointSourceIdentity, RelationalLiveOverlayDebt,
};
use graph_checkpoint_projection::checkpoint_projected_graph_from_definition;
#[path = "store/graph_columnar_shadow.rs"]
mod graph_columnar_shadow;
#[path = "store/graph_commit.rs"]
mod graph_commit;
#[path = "store/graph_indexes.rs"]
mod graph_indexes;
#[path = "store/graph_mutation.rs"]
mod graph_mutation;
#[doc(hidden)]
pub use graph_read::{record_reads_for_test, RecordReadsForTest};
#[path = "store/graph_read.rs"]
mod graph_read;
#[doc(hidden)]
pub use graph_read::{
    adjacency_visits_for_test, MaterializedNodePointRead, MaterializedNodePointSource,
    MaterializedNodeReadSource, MaterializedReadHealth,
};
#[path = "store/read_dependencies.rs"]
mod read_dependencies;
pub use graph_read::{EndpointCandidates, RelationshipEndpoints, RelationshipSeedConstraints};
pub use read_dependencies::ReadDependencyValidationFailures;
#[doc(hidden)]
pub use read_dependencies::{GraphReadDependencies, GraphReadDependencyError};
mod format_admission;
#[path = "store/graph_recovery.rs"]
mod graph_recovery;
#[path = "store/immutable_root.rs"]
mod immutable_root;
#[doc(hidden)]
pub use immutable_root::{
    AdmittedBranchStore, BranchAdmissionError, BranchAdmissionRequest,
    CapturedBranchAdmissionRequest, PreparedImmutableRootHandoff, SealedBranchSource,
};
#[path = "store/pending_nodes.rs"]
mod pending_nodes;
use pending_nodes::{PendingMatch, PendingNodes};
#[path = "store/projection_full_source.rs"]
mod projection_full_source;
#[path = "store/relational_index_shadow.rs"]
mod relational_index_shadow;
#[path = "store/relational_row_pages.rs"]
#[doc(hidden)]
pub mod relational_row_pages;
#[path = "store/search_projection_capture.rs"]
mod search_projection_capture;
#[path = "store/search_projection_change_log.rs"]
mod search_projection_change_log;
#[doc(hidden)]
pub use projection_full_source::{
    ProjectionFullSourceCursor, ProjectionFullSourceItem, ProjectionFullSourceLimits,
};
#[path = "store/statistics_refresh.rs"]
mod statistics_refresh;
#[path = "store/statistics_state.rs"]
mod statistics_state;
use statistics_state::{
    decrement_statistic_counter, BasicStatisticsState, CheckpointStatisticsState,
};
#[path = "store/wal_codec.rs"]
mod wal_codec;
use crate::file_io as fs;
pub use backup::restore_storage_backup;
use backup::{remove_source_scan_artifacts, validate_backup_files};
pub use derived_repair::{
    DerivedArtifactBranchSource, DerivedArtifactHealth, DerivedArtifactHealthReport,
    DerivedArtifactHealthState, DerivedArtifactKind, DerivedArtifactRebuildOptions,
    DerivedArtifactRepairPlan, DerivedArtifactRepairReport, DERIVED_ARTIFACT_REPAIR_PROTOCOL,
};
pub use doctor::{
    DatabaseDoctor, WalDoctorOptions, WalRepairAcknowledgement, WalTailRepairPlan,
    WalTailRepairReason, WalTailRepairReport, WAL_DOCTOR_REPAIR_PROTOCOL,
};
use durable::{
    load_published_canonical_adjacency, load_published_property_projection, CheckpointImage,
    CheckpointManifestArtifacts, DerivedArtifactBuildConfig, DurableManifest, DurableOpenMode,
    DurableStore, GraphManifestOpenBudget,
};
#[doc(hidden)]
pub use durable::{CheckpointWalTail, PreparedCheckpoint};
pub use graph_columnar_shadow::ColumnarShadowAdmission;
use graph_columnar_shadow::ColumnarShadowState;
use hawdb_storage::artifact_files::{
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
pub use hawdb_storage::column_group::shadow::{
    ColumnarShadowCheckpointReport, ColumnarShadowCheckpointStatus, ColumnarShadowRecoveryStatus,
    COLUMN_GROUP_SHADOW_DIR,
};
#[cfg(test)]
use hawdb_storage::cow::COW_MAP_TARGET_SEGMENT_BYTES;
#[doc(hidden)]
pub use hawdb_storage::durability::{WalSyncGroupFlush, WalSyncGroupProgress};
use hawdb_storage::graph_constraints::{
    validate_node_property_exists, validate_node_property_exists_constraints,
    validate_node_property_exists_constraints_for_records, validate_node_record_constraints,
    validate_property_schema_value, validate_property_schemas,
    validate_property_schemas_for_records, validate_relationship_property_exists,
    validate_relationship_property_exists_constraints,
    validate_relationship_property_exists_constraints_for_records,
    validate_relationship_record_constraints, validate_relationship_unique_constraints,
    validate_relationship_unique_constraints_for_records, validate_unique_constraints,
    validate_unique_constraints_for_records, validate_unique_property,
    validate_unique_relationship_property, wal_ops_touched_records,
};
use hawdb_storage::graph_index_metrics::GraphIndexReadMetrics;
#[doc(hidden)]
pub use hawdb_storage::mutation::evaluate::{
    apply_node_assignments_to_properties, evaluate_node_set_value,
};
use hawdb_storage::predicate::{
    properties_contain_all, property_filter_matches, range_bounds_match,
};
use hawdb_storage::projection::artifact::{
    decode_projected_graph_artifacts, split_projected_graph_artifact_checksum,
};
pub use hawdb_storage::relational::{
    RelationalIndexArtifactMetadata, RelationalIndexGenerationArtifacts,
};
pub use hawdb_storage::scan::{ScanPrunedNodeScan, ScanPrunedRelationshipScan};
#[doc(hidden)]
pub use hawdb_storage::source_scan;
pub use hawdb_storage::source_scan::SourceScanRow;
#[cfg(test)]
#[doc(hidden)]
pub use hawdb_storage::statistics::compute_statistics;
#[doc(hidden)]
pub use hawdb_storage::statistics::{
    composite_property_index_key, composite_property_index_unique_values, compute_basic_statistics,
    compute_index_statistics_samples, compute_node_property_distinct_counts_from_index,
    compute_relationship_property_distinct_counts_from_index, compute_statistics_for_catalog,
    compute_statistics_with_basic, full_text_index_tokens, full_text_query_tokens,
    graph_statistics_from_basic, recompute_node_property_index,
    recompute_relationship_property_index, scalar_property_index_cardinality,
};
use hawdb_storage::statistics_refresh::{
    node_property_supports_optimizer_statistics, MAX_BOUNDED_PATH_STAT_HOPS,
};
#[doc(hidden)]
pub use hawdb_storage::statistics_refresh::{
    retain_supported_property_statistics, retain_valid_index_statistics_samples,
    OptimizerStatisticsRefreshWork,
};
pub use hawdb_storage::statistics_refresh::{
    OptimizerStatisticsRefreshOptions, OptimizerStatisticsRefreshReport,
};
#[cfg(test)]
use hawdb_storage::text::encode_properties;
#[cfg(test)]
use hawdb_storage::text::envelope::DURABLE_COMPRESSION_HEADER;
#[doc(hidden)]
pub use hawdb_storage::text::envelope::{
    encode_durable_text, read_durable_text_bytes, read_durable_text_bytes_with_limit,
};
use hawdb_storage::{
    adjacency::AdjacencyPostingList,
    append_table::{
        decode_append_wal_batch, encode_append_wal_batch, AppendDecodeLimits,
        AppendGenerationReader, AppendMutationLimits, AppendPublicationConfig,
        AppendPublicationState, AppendPublisher, AppendState,
    },
    backup::{
        copy_backup_file, copy_file_with_checksum, file_checksum, validate_new_backup_destination,
    },
    canonical::{CanonicalEndpointDirection, CanonicalSegmentError},
    durability::sync_parent_directory,
    pressure::available_storage_space,
    property_projection::PersistentPropertyProjectionDefinitionAdmission,
    relational::{
        decode_relational_checkpoint_file_with_index_load,
        decode_relational_checkpoint_from_row_root, decode_relational_checkpoint_with_index_load,
        decode_relational_wal_batch, encode_relational_checkpoint, RelationalCheckpointIndexLoad,
        RelationalDecodeLimits, RelationalMutationLimits, RelationalOverflowConfig,
        RelationalOverflowPublicationConfig, RelationalOverflowPublisher,
        RelationalRecoverySourceBuilder, RelationalRowPageGenerationRequest,
        RelationalRowPagePublisher, RelationalSparseLiveStage, RelationalState,
        RelationalTransaction,
    },
};
pub use hawdb_storage::{
    adjacency::{
        AdjacencyDirection, AdjacencyGroupConsistencyMismatch, AdjacencyGroupKey,
        AdjacencyGroupStats, AdjacencyLayout, OrderedAdjacencyEntry,
    },
    append_table::{
        AppendGeneratedRow, AppendMutationOutcome, AppendOrderMode, AppendSegmentReadOutput,
        AppendStorageResidencyReport, AppendTableRow, AppendTableSchema, AppendTransaction,
        AppendWrite,
    },
    backup::{
        BackupFileEntry, BackupManifest, StorageBackupReport, StorageRestoreReport,
        StorageScrubReport,
    },
    cache::{ManifestGeneration, SegmentBytes, SegmentCache, SegmentCacheSnapshot, StoreId},
    canonical::{
        CanonicalScanControl, CanonicalSegmentConfig, CanonicalSegmentManifest,
        CanonicalSegmentReader, CanonicalSegmentWriter,
    },
    canonical_adjacency::{
        CanonicalAdjacencyBuildReport, CanonicalAdjacencyConfig, CanonicalAdjacencyEntry,
        CanonicalAdjacencyReadReport, CanonicalAdjacencyReader, CanonicalAdjacencyWriter,
    },
    config::{
        CheckpointGenerationRetention, DurabilityPolicy, DurableCompression, RecoveryMode,
        RelationalIndexMode, StorageResidencyMode, WalReplayConfig,
    },
    mutation::{
        ConnectedNodesCreate, EntityPropertyUpdate, GraphMutation, MatchedRelationshipCopyMerge,
        MatchedRelationshipCreate, MatchedRelationshipMerge, MatchedRelationshipRetargetMerge,
        MatchedRelationshipSourceRetargetMerge, MutationEntity, MutationLimits, MutationSummary,
        NodeSetAssignment, NodeSetValue, PropertyFilter, RelationshipDeleteRequest,
        RelationshipOnCreatePropertyValue, RelationshipPropertiesUpdate,
        RelationshipPropertyUpdate, RelationshipSetAssignment, RelationshipTargetNodeDelete,
    },
    pressure::{
        StorageDebtController, StoragePressureReasonCode, StoragePressureSignals,
        StoragePressureSnapshot, StoragePressureState, STORAGE_PRESSURE_DEFER_RATIO_PER_MILLION,
        STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION,
    },
    projection::{
        ProjectedGraphDefinition, ProjectedGraphStatus, PropertyIndexProjectionRebuildAction,
        SchemaMaintenanceAction, SchemaMaintenancePlanItem, SearchProjectionChangefeedReadiness,
        SearchProjectionChangefeedStatus, SearchProjectionGraphChange, SearchProjectionMutationId,
        StorageOpenTimings, StorageReclamationWatermark, StorageRecoveryReport,
        StoreStableIdMapping,
    },
    property_projection::{
        PersistentPropertyProjectionConfig, PersistentPropertyProjectionDefinition,
        PersistentPropertyProjectionError, PersistentPropertyProjectionKind,
        PersistentPropertyProjectionManifest, PersistentPropertyProjectionReader,
        PersistentPropertyProjectionWriter,
    },
    property_spill::{PropertySpillConfig, PropertySpillManifest, PropertySpillReader},
    relational::{
        RelationalColumnSchema, RelationalIndexReadLimits, RelationalIndexReadReport,
        RelationalIndexRecoveryReadReport, RelationalKey, RelationalRow, RelationalScalarType,
        RelationalValue,
    },
    scan::{
        FileSegmentRangeReader, ScanPredicate, ScanPruningReport, ScanPruningStrategy,
        ScanPruningTargetKind, ScanSegmentAccessPlan, ScanSegmentFallback, ScanSegmentManifest,
        SegmentRangeReader, SegmentReadError, SegmentReadExecutionError,
        SegmentReadExecutionReport, SegmentReadExecutor, SegmentReadPayload, SegmentReadRange,
        SegmentReadSchedule, SegmentReadScheduler, SegmentReadWave,
    },
    NodeId, NodeRecord, ProjectedNodeRecord, RelId, RelRecord,
};
use hawdb_storage::{
    cow::{CowSegment, CowSegmentedMap},
    projection::{ProjectedGraphArtifact, ProjectedGraphArtifactData},
};
pub use hawdb_storage::{
    graph_index_metrics::{GraphIndexReadMetricsSnapshot, PersistentGraphIndexClass},
    read_view::PublishedReadView,
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
#[doc(hidden)]
pub use relational_index_shadow::{RelationalIndexProbeStatistics, RelationalTransactionIndexView};
pub use relational_row_pages::RelationalRowPageRecoveryStatus;
use relational_row_pages::RelationalRowPageState;
#[doc(hidden)]
pub use relational_row_pages::RelationalTransactionRowView;
use search_projection_change_log::SearchProjectionChangeLog;
#[doc(hidden)]
pub use search_projection_change_log::{
    ProjectionFeedCursor, ProjectionFeedError, ProjectionFeedItem, ProjectionFeedLimits,
    ProjectionFeedResource, ProjectionFeedStep, ProjectionFeedUsage,
};
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use wal_codec::binary::encode_binary_wal_record;
use wal_codec::frame::{
    encode_binary_wal_header, frame_binary_wal_record, WAL_BINARY_FILE_HEADER_BYTES,
};
use wal_codec::{
    reject_corrupt_wal_record, WalCursorEvent, WalEntry, WalOp, WalOpenOutcome, WalRecordCursor,
};

use hawdb_storage::durable_manifest::safe_reclaim_commit_epoch;
#[doc(hidden)]
pub const MANIFEST_FILE: &str = "manifest.hawdb";
const PROJECTED_GRAPHS_FILE: &str = "projected_graphs.hawdb";
const STABLE_ID_MAPPING_FILE: &str = "stable_ids.hawdb";
#[doc(hidden)]
pub use hawdb_storage::checkpoint::{relational_checkpoint_metadata, split_checkpoint_checksum};
const BACKUP_MANIFEST_FILE: &str = "backup.hawdb";
const CANONICAL_MANIFEST_MAX_BYTES: u64 = 256 * 1024 * 1024;
const PROPERTY_SPILL_MANIFEST_MAX_BYTES: u64 = 64 * 1024;
const PROPERTY_PROJECTION_MANIFEST_MAX_BYTES: u64 = 32 * 1024 * 1024;
const CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER: u64 = 4;
const MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES: u64 = 64 * 1024;
pub use hawdb_storage::consistency::DENSE_ADJACENCY_DEGREE_THRESHOLD;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum CheckpointPublishStage {
    CheckpointPersisted,
    WalPrepared,
    ManifestPublished,
    BranchHeadPublished,
}

#[cfg(any(test, feature = "test-support", debug_assertions))]
#[doc(hidden)]
const PROCESS_CRASH_POINT_ENV: &str = "HAWDB_TEST_PROCESS_CRASH_POINT";

/// Subprocess crash injection for checked test/development builds only.
///
/// Bazel root tests build the storage dependency without `cfg(test)` or the
/// `test-support` feature, but with `debug_assertions` enabled. Release builds
/// compile this to an empty inline function, so a host environment variable
/// cannot become a production exit path.
#[cfg(any(test, feature = "test-support", debug_assertions))]
fn process_crash_failpoint(point: &str) {
    if std::env::var(PROCESS_CRASH_POINT_ENV).as_deref() == Ok(point) {
        std::process::exit(86);
    }
    let _ = point;
}

#[cfg(not(any(test, feature = "test-support", debug_assertions)))]
#[inline(always)]
fn process_crash_failpoint(_point: &str) {}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum WalAppendFailure {
    PartialWrite,
    Rollback,
    Sync,
}

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static WAL_APPEND_FAILURE: std::cell::Cell<Option<WalAppendFailure>> = const {
        std::cell::Cell::new(None)
    };
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn set_wal_append_failpoint(failure: WalAppendFailure) {
    WAL_APPEND_FAILURE.set(Some(failure));
}

#[cfg(any(test, feature = "test-support"))]
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
        CheckpointPublishStage::BranchHeadPublished => {
            process_crash_failpoint("after_branch_head_publication");
        }
    }
    #[cfg(any(test, feature = "test-support"))]
    if CHECKPOINT_FAILPOINT.with(|failpoint| failpoint.get()) == Some(stage) {
        return Err(HawDBError::Storage(format!(
            "injected checkpoint failure at {stage:?}"
        )));
    }
    Ok(())
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn set_checkpoint_failpoint(stage: Option<CheckpointPublishStage>) {
    CHECKPOINT_FAILPOINT.with(|failpoint| failpoint.set(stage));
}

thread_local! {
    static MATCHED_NODE_LABEL_SCANS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Counts label scans by matched-node mutation filters without an internal-id
/// fast path on this thread. Diagnostic for tests; always compiled so test
/// builds that do not enable `test-support` (for example Bazel) still link.
#[doc(hidden)]
pub fn matched_node_label_scans_for_test() -> u64 {
    MATCHED_NODE_LABEL_SCANS.with(std::cell::Cell::get)
}

fn record_matched_node_label_scan() {
    MATCHED_NODE_LABEL_SCANS.with(|scans| scans.set(scans.get().saturating_add(1)));
}

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static GENERATION_RECLAMATION_REMOVE_FAILPOINT: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

fn remove_generation_reclamation_candidate(path: &Path) -> std::io::Result<()> {
    #[cfg(any(test, feature = "test-support"))]
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

/// Test-only faults for the branch-private WAL sweep (#1727): the head
/// re-read and the directory barrier that precede its first removal.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BranchWalSweepFailpoint {
    HeadRead,
    DirectorySync,
}

#[cfg(test)]
thread_local! {
    static BRANCH_WAL_SWEEP_FAILPOINT: std::cell::Cell<Option<BranchWalSweepFailpoint>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn set_branch_wal_sweep_failpoint(failpoint: Option<BranchWalSweepFailpoint>) {
    BRANCH_WAL_SWEEP_FAILPOINT.with(|slot| slot.set(failpoint));
}

/// Reads the selected branch head before a branch-private WAL sweep.
fn read_branch_head_for_wal_sweep(
    path: &Path,
) -> std::result::Result<hawdb_storage::branch_head::BranchHead, ()> {
    #[cfg(test)]
    if BRANCH_WAL_SWEEP_FAILPOINT.with(|slot| slot.get()) == Some(BranchWalSweepFailpoint::HeadRead)
    {
        return Err(());
    }
    hawdb_storage::branch_head::read_branch_head(path).map_err(|_| ())
}

/// Synchronizes the branch directory before a branch-private WAL sweep.
fn sync_branch_directory_for_wal_sweep(directory: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    if BRANCH_WAL_SWEEP_FAILPOINT.with(|slot| slot.get())
        == Some(BranchWalSweepFailpoint::DirectorySync)
    {
        return Err(std::io::Error::other(
            "injected branch WAL sweep directory sync failure",
        ));
    }
    hawdb_storage::durability::sync_directory(directory)
}

/// Fails the next removal of the named obsolete generation file on this
/// thread, leaving generation reclamation debt for pressure assertions.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn set_generation_reclamation_remove_failpoint(file_name: Option<String>) {
    GENERATION_RECLAMATION_REMOVE_FAILPOINT.with(|failpoint| {
        *failpoint.borrow_mut() = file_name;
    });
}

thread_local! {
    static WAL_APPLY_FAILPOINT_REMAINING: std::cell::Cell<Option<usize>> = const {
        std::cell::Cell::new(None)
    };
    static WAL_GROUP_SYNC_FAILPOINT: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

fn wal_apply_failpoint() -> Result<()> {
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
            return Err(HawDBError::Storage(
                "injected failure while applying a durable WAL batch".to_string(),
            ));
        }
    }
    Ok(())
}

thread_local! {
    static NODE_SCANS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Counts node scans (`visit_nodes_with_allocation`) started on this thread.
/// Diagnostic for tests; always compiled so test builds without the
/// `test-support` feature still link.
#[doc(hidden)]
pub fn node_scans_for_test() -> u64 {
    NODE_SCANS.with(std::cell::Cell::get)
}

fn record_node_scan() {
    NODE_SCANS.with(|scans| scans.set(scans.get().saturating_add(1)));
}

/// Test support: renders binary WAL records as deterministic text lines.
/// The on-disk v1 format remains binary; this representation is never read
/// by recovery. A torn tail ends the rendering, while corruption appends a
/// terminal marker so damaged-file comparisons remain deterministic.
#[doc(hidden)]
pub fn render_wal_records_for_test(path: &Path) -> std::io::Result<String> {
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
#[doc(hidden)]
pub fn append_stale_generation_wal_fragment(path: &Path) -> Result<()> {
    use std::io::Write as _;

    let bytes = fs::read(path)?;
    let (generation, _) = wal_codec::frame::decode_binary_wal_header(&bytes)?;
    let position = bytes.len() as u64 - WAL_BINARY_FILE_HEADER_BYTES as u64;
    let stale_generation = generation.wrapping_sub(1);
    let framed = frame_binary_wal_record(
        stale_generation,
        b"recycled-region-record-from-a-previous-generation",
        position,
    );
    let mut file = crate::file_io::OpenOptions::new().append(true).open(path)?;
    file.write_all(&framed)?;
    file.sync_all()?;
    Ok(())
}

#[doc(hidden)]
pub fn set_wal_apply_failpoint(operations_before_failure: Option<usize>) {
    WAL_APPLY_FAILPOINT_REMAINING.with(|remaining| remaining.set(operations_before_failure));
}

#[doc(hidden)]
pub fn set_wal_group_sync_failpoint(enabled: bool) {
    WAL_GROUP_SYNC_FAILPOINT.with(|failpoint| failpoint.set(enabled));
}

fn wal_group_sync_failpoint() -> Result<()> {
    if WAL_GROUP_SYNC_FAILPOINT.with(std::cell::Cell::take) {
        return Err(HawDBError::Storage(
            "injected WAL group sync failure".to_string(),
        ));
    }
    Ok(())
}

type PendingNode = (NodeId, LabelId, BTreeMap<String, Value>);
type PendingRelationship = (RelId, NodeId, NodeId, RelTypeId, BTreeMap<String, Value>);
pub type GraphSnapshotNodeImport = (NodeId, String, BTreeMap<String, Value>);
pub type GraphSnapshotRelationshipImport = (RelId, NodeId, NodeId, String, BTreeMap<String, Value>);

#[doc(hidden)]
pub struct HawDBSnapshotRowsImport {
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

#[doc(hidden)]
pub use hawdb_storage::graph_index::{
    CompositePropertyIndex, FullTextPropertyIndex, NodePropertyIndex, RelationshipPropertyIndex,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KernelWriteBatch {
    pub graph: Vec<GraphMutation>,
    pub relational: RelationalTransaction,
    pub append: AppendTransaction,
}

#[derive(Default)]
struct MutationCommitOptions<'a> {
    relational: Option<RelationalTransaction>,
    capture_relational_mutation_rows: Option<bool>,
    append: Option<AppendTransaction>,
    preserve_single_create_wal: bool,
    mvcc_read_epoch: Option<u64>,
    captured_graph_ops: Option<&'a mut Vec<WalOp>>,
    prepared_wal: Option<crate::wal::binary::PreparedWalBatch>,
    /// Receives the unconsumed inputs when the commit fits an empty
    /// relational live overlay but not its remaining headroom.
    retain_on_live_headroom: Option<&'a mut Option<RetainedLiveHeadroomCommit>>,
    /// The writing transaction's `current_timestamp()` default instant.
    /// `None` reads the clock at most once for this commit.
    creation_clock: Option<hawdb_core::temporal::PlainDateTime>,
}

/// Inputs of a commit rejected before WAL append only for live-overlay
/// headroom. Relational and append records were removed from `ops`.
struct RetainedLiveHeadroomCommit {
    shortfall: hawdb_storage::relational::RelationalLiveOverlayShortfall,
    ops: Vec<WalOp>,
    rows: Vec<BTreeMap<String, Value>>,
    relational: Option<RelationalTransaction>,
    append: Option<AppendTransaction>,
}

/// Outcome of a commit that may first need the relational live overlay
/// drained by a checkpoint (#1479).
#[doc(hidden)]
#[derive(Debug)]
pub enum LiveHeadroomCommit<T> {
    Committed(MutationSummary),
    /// The commit fits an empty overlay but not the remaining headroom.
    /// Nothing was applied: `inputs` can be committed once a drain made room,
    /// and `error` is the pre-WAL rejection to report without one.
    NeedsHeadroom {
        shortfall: hawdb_storage::relational::RelationalLiveOverlayShortfall,
        inputs: T,
        error: HawDBError,
    },
}

/// Everything one explicit transaction commit consumes.
#[doc(hidden)]
#[derive(Debug)]
pub struct PreparedTransactionCommit {
    pub transaction: PreparedGraphMutationTransaction,
    pub relational: RelationalTransaction,
    pub append: AppendTransaction,
    pub capture_relational_mutation_rows: bool,
}

/// An isolated graph mutation workspace for a single explicit transaction.
///
/// The workspace applies each statement to a COW snapshot so subsequent reads
/// observe prior writes. It also retains the exact low-level operations chosen
/// by each statement; the live store publishes those operations as one WAL
/// batch instead of recomputing predicates against the pre-transaction state.
#[derive(Debug)]
#[doc(hidden)]
pub struct GraphMutationTransaction {
    base_commit_epoch: u64,
    base_catalog: Catalog,
    catalog: Catalog,
    store: GraphStore,
    ops: Vec<WalOp>,
    rows: Vec<BTreeMap<String, Value>>,
    persistent_commit: bool,
    /// One `current_timestamp()` default instant for every record this
    /// transaction creates (Kùzu evaluates such defaults per transaction).
    creation_clock: hawdb_core::temporal::PlainDateTime,
}

/// A sealed transaction preparation. No statement can mutate its compacted
/// operations or encoded graph WAL before authoritative commit validation.
#[derive(Debug)]
#[doc(hidden)]
pub struct PreparedGraphMutationTransaction {
    transaction: GraphMutationTransaction,
    wal: Option<crate::wal::binary::PreparedWalBatch>,
    had_graph_writes: bool,
}

#[derive(Debug)]
#[doc(hidden)]
pub struct GraphMutationSavepoint {
    catalog: Catalog,
    store: GraphStore,
    op_len: usize,
    row_len: usize,
}

impl GraphMutationSavepoint {
    /// The graph state when the savepoint was taken, before later statements.
    #[doc(hidden)]
    pub fn store(&self) -> &GraphStore {
        &self.store
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[doc(hidden)]
pub struct GraphAdjacencyLockIdentity {
    #[doc(hidden)]
    pub node_id: NodeId,
    #[doc(hidden)]
    pub rel_type: Option<RelTypeId>,
    #[doc(hidden)]
    pub direction: AdjacencyDirection,
}

#[derive(Debug, Default)]
#[doc(hidden)]
pub struct GraphMutationLockFootprint {
    #[doc(hidden)]
    pub requires_database_lock: bool,
    #[doc(hidden)]
    pub allocates_node_ids: bool,
    #[doc(hidden)]
    pub allocates_relationship_ids: bool,
    #[doc(hidden)]
    pub node_label_names: BTreeSet<String>,
    #[doc(hidden)]
    pub exclusive_node_label_names: BTreeSet<String>,
    #[doc(hidden)]
    pub node_label_read_names: BTreeSet<String>,
    #[doc(hidden)]
    pub relationship_type_names: BTreeSet<String>,
    #[doc(hidden)]
    pub exclusive_relationship_type_names: BTreeSet<String>,
    #[doc(hidden)]
    pub node_writes: BTreeSet<NodeId>,
    #[doc(hidden)]
    pub relationship_writes: BTreeSet<RelId>,
    #[doc(hidden)]
    pub node_delete_guard_reads: BTreeSet<NodeId>,
    #[doc(hidden)]
    pub node_delete_guard_writes: BTreeSet<NodeId>,
    #[doc(hidden)]
    pub adjacency_writes: BTreeSet<GraphAdjacencyLockIdentity>,
}

#[doc(hidden)]
pub use hawdb_storage::mutation::{
    ensure_additional_mutation_limits, ensure_mutation_commit_limits, estimated_properties_bytes,
    estimated_value_bytes, remaining_mutation_affected_rows, remaining_mutation_operations,
};

use hawdb_storage::consistency::{
    adjacency_consolidation_plan, adjacency_direction_sort_key, adjacency_layout_for_degree,
    compute_degree_statistics_from_adjacency, compute_degree_statistics_from_relationships,
    maintained_adjacency_groups, recompute_adjacency_groups,
};

pub use hawdb_storage::consistency::{
    AdjacencyConsistencyReport, AdjacencyConsolidationCandidate, AdjacencyConsolidationPlan,
    AdjacencyConsolidationReport, BasicStatisticsConsistencyReport,
    DegreeStatisticsConsistencyReport, DegreeStatisticsEntry, DegreeStatisticsKey,
    DistinctValueStatisticsConsistencyReport, PropertyIndexConsistencyReport,
};

/// Largest resident graph, in committed nodes plus relationships, whose planner
/// statistics are recomputed exactly on every planning request.
///
/// Below this bound a full recompute costs about 1-2 ms, which stays bounded
/// and independent of database growth, and tiny in-memory databases keep exact
/// statistics. Larger graphs plan from cached distributions that commits,
/// background maintenance and checkpoints refresh (#2017).
pub const SYNC_STATISTICS_MAX_RECORDS: u64 = 1_000;

/// Smallest number of committed graph mutation operations since the last
/// refresh that makes cached optimizer statistics due for a refresh.
const MIN_STATISTICS_REFRESH_DIRTY_OPERATIONS: u64 = 1_000;

/// Dirty operations above which cached statistics of a graph with `records`
/// committed records are refreshed: `max(1000, 10% of records)`.
pub(crate) const fn statistics_refresh_dirty_operation_threshold(records: u64) -> u64 {
    let proportional = records / 10;
    if proportional > MIN_STATISTICS_REFRESH_DIRTY_OPERATIONS {
        proportional
    } else {
        MIN_STATISTICS_REFRESH_DIRTY_OPERATIONS
    }
}

/// Mutation domains that invalidate an out-of-core advanced-statistics snapshot.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct AdvancedStatisticsDirtyState {
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

/// Caller sites of a full optimizer-statistics recomputation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatisticsRecomputeSite {
    Planning,
    Commit,
    Background,
    Checkpoint,
    Forced,
}

/// Per-database counts of full statistics recomputations. Snapshots and
/// transaction workspaces share the owning database's counters.
#[derive(Debug, Default)]
struct StatisticsRecomputeCounters {
    planning: AtomicU64,
    commit: AtomicU64,
    background: AtomicU64,
    checkpoint: AtomicU64,
    forced: AtomicU64,
}

impl StatisticsRecomputeCounters {
    fn record(&self, site: StatisticsRecomputeSite) {
        let counter = match site {
            StatisticsRecomputeSite::Planning => &self.planning,
            StatisticsRecomputeSite::Commit => &self.commit,
            StatisticsRecomputeSite::Background => &self.background,
            StatisticsRecomputeSite::Checkpoint => &self.checkpoint,
            StatisticsRecomputeSite::Forced => &self.forced,
        };
        counter.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn snapshot(&self) -> OptimizerStatisticsRecomputeCounts {
        OptimizerStatisticsRecomputeCounts {
            planning: self.planning.load(AtomicOrdering::Relaxed),
            commit: self.commit.load(AtomicOrdering::Relaxed),
            background: self.background.load(AtomicOrdering::Relaxed),
            checkpoint: self.checkpoint.load(AtomicOrdering::Relaxed),
            forced: self.forced.load(AtomicOrdering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
struct SnapshotResidencyObserver {
    segment_cache: Arc<SegmentCache>,
    checkpoint_commit_epoch: u64,
    graph_manifest_open_budget_bytes: u64,
    graph_manifest_encoded_bytes: u64,
}

#[derive(Debug, Default)]
pub struct GraphStore {
    next_node_id: u64,
    next_rel_id: u64,
    commit_epoch: u64,
    /// Commit epoch of the newest commit that changed graph data or graph
    /// schema; relational-only and append-only commits leave it unchanged.
    graph_change_commit_epoch: u64,
    // Latest committed operation that can invalidate inline relationship data
    // or adjacency topology; compared with checkpoint coverage, not build epoch.
    adjacency_change_commit_epoch: u64,
    /// Whether the WAL record most recently appended for a pending commit
    /// changes the graph. `finish_non_relational_commit` consumes it so live
    /// commits classify exactly like WAL replay.
    pending_commit_changes_graph: bool,
    pending_commit_changes_adjacency: bool,
    // A maintenance capture's monotonic floor for any post-snapshot debt.
    // Live writers normally have no capture clock; handoff restores that fact.
    checkpoint_capture_started: Option<std::time::Instant>,
    version_index: hawdb_storage::version::VersionIndex,
    version_snapshot_pins: hawdb_storage::version::VersionSnapshotPins,
    version_snapshot_pin: Option<hawdb_storage::version::VersionSnapshotPin>,
    read_dependency_readers: Arc<AtomicUsize>,
    read_dependency_validation_failures: Arc<read_dependencies::ValidationFailureCounters>,
    read_summary_collapses: Arc<AtomicU64>,
    nodes: crate::cow::CowNodeMap,
    relationships: crate::cow::CowRelMap,
    basic_statistics: BasicStatisticsState,
    checkpoint_statistics: CheckpointStatisticsState,
    advanced_statistics_dirty: AdvancedStatisticsDirtyState,
    statistics_recomputes: Arc<StatisticsRecomputeCounters>,
    outgoing: CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
    incoming: CowSegmentedMap<(NodeId, RelTypeId), AdjacencyPostingList>,
    property_index: NodePropertyIndex,
    composite_property_index: CompositePropertyIndex,
    full_text_property_index: FullTextPropertyIndex,
    relationship_property_index: RelationshipPropertyIndex,
    projected_graphs: CowSegment<BTreeMap<String, ProjectedGraphDefinition>>,
    projected_graph_artifacts: crate::projection::artifact::CheckpointProjectedGraphRoot,
    stable_id_mapping: CowSegment<StoreStableIdMapping>,
    initial_import_source_fingerprint: Option<String>,
    search_projection_database_identity: Option<hawdb_core::Uuid>,
    search_projection_change_log_start_epoch: u64,
    search_projection_graph_changes: SearchProjectionChangeLog,
    search_projection_change_log_retained_bytes: usize,
    max_search_projection_change_log_entries: Option<usize>,
    max_search_projection_change_log_bytes: Option<usize>,
    search_projection_primary_key_capture_limits:
        hawdb_storage::relational::RelationalPrimaryKeyChangeCaptureLimits,
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
    post_wal_apply_poisoned: Arc<AtomicBool>,
    integrity_poisoned: Arc<AtomicBool>,
    relational_state: RelationalState,
    append_state: AppendState,
    append_mutation_limits: AppendMutationLimits,
    append_publication_config: AppendPublicationConfig,
    append_generation_reader: Option<AppendGenerationReader>,
    relational_mutation_limits: RelationalMutationLimits,
    relational_overflow_config: RelationalOverflowConfig,
    /// Also write the legacy full-row relational checkpoint file (#1737).
    legacy_relational_checkpoint_file: bool,
    columnar_shadow: ColumnarShadowState,
    relational_index_shadow: RelationalIndexShadowState,
    relational_row_pages: RelationalRowPageState,
    projection_generations: Option<hawdb_storage::projection_generation::ProjectionGenerationStore>,
    /// The engine's runtime governor, threaded down from the embedding
    /// layer (`HawDBEmbedded` / `EmbeddedGraph`) so background shadow
    /// work can request admission. The store never constructs its own.
    runtime_governor: Option<Arc<dyn hawdb_storage::background::BackgroundWorkAdmission>>,
    branch_lease: Option<Arc<hawdb_storage::ownership::DatabaseDirectoryLease>>,
    branch_root_pin: Option<Arc<crate::branch_root_pins::RootPin>>,
    branch_runtime_owner: Option<Arc<crate::file_descriptors::AdmittedRuntimeOwner>>,
    snapshot_file_context: Option<crate::file_descriptors::FileOpenContext>,
    snapshot_residency_observer: Option<SnapshotResidencyObserver>,
    durable: Option<DurableStore>,
    // Data/COW runtime owners above must drop before decoded allocation leases.
    checkpoint_allocations: crate::background::CheckpointAllocationOwner,
}

impl hawdb_storage::graph_engine::GraphMutationEngine for GraphStore {
    fn plan_schema_maintenance(
        &self,
        catalog: &hawdb_core::Catalog,
    ) -> Vec<hawdb_storage::projection::SchemaMaintenancePlanItem> {
        GraphStore::plan_schema_maintenance(self, catalog)
    }

    fn run_schema_maintenance(
        &mut self,
        catalog: &mut hawdb_core::Catalog,
    ) -> hawdb_core::Result<Vec<hawdb_storage::projection::SchemaMaintenanceAction>> {
        GraphStore::run_schema_maintenance(self, catalog)
    }

    fn rebuild_projected_graph_artifacts(
        &mut self,
        catalog: &hawdb_core::Catalog,
    ) -> hawdb_core::Result<()> {
        GraphStore::rebuild_projected_graph_artifacts(self, catalog)
    }

    fn rebuild_bounded_property_index_projections(
        &mut self,
        catalog: &hawdb_core::Catalog,
        max_estimated_operations: usize,
    ) -> Vec<hawdb_storage::projection::PropertyIndexProjectionRebuildAction> {
        GraphStore::rebuild_bounded_property_index_projections(
            self,
            catalog,
            max_estimated_operations,
        )
    }

    fn scrub_storage(&mut self) -> hawdb_core::Result<hawdb_storage::backup::StorageScrubReport> {
        GraphStore::scrub_storage(self)
    }

    fn backup_to(
        &mut self,
        catalog: &hawdb_core::Catalog,
        destination: impl AsRef<std::path::Path>,
    ) -> hawdb_core::Result<hawdb_storage::backup::StorageBackupReport> {
        GraphStore::backup_to(self, catalog, destination)
    }

    fn register_projected_graph(
        &mut self,
        name: &str,
        definition: hawdb_storage::projection::ProjectedGraphDefinition,
    ) -> hawdb_core::Result<()> {
        GraphStore::register_projected_graph(self, name, definition)
    }
}

pub use hawdb_storage::scan::GraphScanControl;

/// Root-internal engine configuration that must not join the storage contract:
/// the sink type is a facade observability interface, not a storage concept.
#[doc(hidden)]
pub trait StoreTelemetry {
    fn set_telemetry_sink(&mut self, telemetry: Option<std::sync::Arc<dyn TelemetrySink>>);
}

impl StoreTelemetry for GraphStore {
    fn set_telemetry_sink(&mut self, telemetry: Option<std::sync::Arc<dyn TelemetrySink>>) {
        GraphStore::set_telemetry_sink(self, telemetry);
    }
}

/// Root-internal engine surface that intentionally stays out of the storage
/// contract: these methods are `pub(crate)` in the root crate, so publishing
/// them through the `pub` read/mutation contract would expose them downstream.
#[doc(hidden)]
pub trait InternalGraphEngine {
    fn ensure_usable(&self) -> Result<()>;

    /// Root-shaped adjacency visitor: the embedded facade's helpers use this
    /// shape (`GraphScanControl`), while the executor crate's own contract
    /// exposes the same method with `hawdb_executor::store::ScanControl`.
    fn visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        consumer: impl FnMut(RelRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl>;

    fn visit_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl>;

    fn relationship_owned(&self, id: RelId) -> Result<Option<RelRecord>>;

    fn adjacency_group_stats(
        &self,
        node_id: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
    ) -> AdjacencyGroupStats;

    fn adjacency_group_stats_for_node(
        &self,
        node_id: NodeId,
        direction: AdjacencyDirection,
    ) -> Vec<AdjacencyGroupStats>;

    fn visit_nodes_by_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[Value],
        consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl>;

    fn append_state(&self) -> &hawdb_storage::append_table::AppendState;

    fn relational_state(&self) -> &hawdb_storage::relational::RelationalState;

    fn poison_on_storage_error<T>(&self, result: &Result<T>);
}

impl InternalGraphEngine for GraphStore {
    fn ensure_usable(&self) -> Result<()> {
        GraphStore::ensure_usable(self)
    }

    fn visit_adjacent_relationships_owned(
        &self,
        node_id: NodeId,
        rel_type: Option<RelTypeId>,
        direction: AdjacencyDirection,
        consumer: impl FnMut(RelRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        GraphStore::visit_adjacent_relationships_owned(self, node_id, rel_type, direction, consumer)
    }

    fn visit_nodes_owned(
        &self,
        label_id: Option<LabelId>,
        consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        GraphStore::visit_nodes_owned(self, label_id, consumer)
    }

    fn relationship_owned(&self, id: RelId) -> Result<Option<RelRecord>> {
        GraphStore::relationship_owned(self, id)
    }

    fn adjacency_group_stats(
        &self,
        node_id: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
    ) -> AdjacencyGroupStats {
        GraphStore::adjacency_group_stats(self, node_id, rel_type, direction)
    }

    fn adjacency_group_stats_for_node(
        &self,
        node_id: NodeId,
        direction: AdjacencyDirection,
    ) -> Vec<AdjacencyGroupStats> {
        GraphStore::adjacency_group_stats_for_node(self, node_id, direction)
    }

    fn visit_nodes_by_property_owned(
        &self,
        label_id: LabelId,
        property: &str,
        values: &[Value],
        consumer: impl FnMut(NodeRecord) -> GraphScanControl,
    ) -> Result<GraphScanControl> {
        GraphStore::visit_nodes_by_property_owned(self, label_id, property, values, consumer)
    }

    fn append_state(&self) -> &hawdb_storage::append_table::AppendState {
        GraphStore::append_state(self)
    }

    fn relational_state(&self) -> &hawdb_storage::relational::RelationalState {
        GraphStore::relational_state(self)
    }

    fn poison_on_storage_error<T>(&self, result: &Result<T>) {
        GraphStore::poison_on_storage_error(self, result);
    }
}

pub use hawdb_storage::relational::{
    RelationalOverflowCompactionConfig, RelationalOverflowCompactionReport,
    RelationalRowPageCompactionConfig, RelationalRowPageCompactionReport,
};
pub use hawdb_storage::{
    relational_index_view::RelationalIndexStorageResidencyReport,
    residency::{
        OptimizerStatisticsMaintenanceReport, OptimizerStatisticsPath,
        OptimizerStatisticsRecomputeCounts, RelationalRowStorageResidencyReport,
        StorageResidencyReport,
    },
};

pub use hawdb_storage::graph_overlay::{GraphNodeIterator, GraphRelationshipIterator};

fn canonical_segment_error(error: CanonicalSegmentError) -> HawDBError {
    let refused = crate::reader_failure::is_admission_or_cancellation(&error);
    match HawDBError::from_storage_error(error) {
        HawDBError::Storage(message) if !refused => HawDBError::StorageIntegrity(message),
        error => error,
    }
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
#[doc(hidden)]
pub enum SourceScanCandidateVisit {
    Rows {
        graph_epoch: u64,
        skipped_segment_count: usize,
        report: SegmentReadExecutionReport,
        candidate_count: usize,
    },
    Fallback(ScanSegmentFallback),
}

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub struct SourceScanCandidateLimits {
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

    #[doc(hidden)]
    pub const fn bounded(
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

struct ScanPruningCandidate {
    strategy: ScanPruningStrategy,
    node_ids: crate::read_view::AdmittedKeySet<NodeId>,
    exact_empty: bool,
}

impl ScanPruningCandidate {
    fn exact(
        strategy: ScanPruningStrategy,
        node_ids: crate::read_view::AdmittedKeySet<NodeId>,
    ) -> Self {
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

impl PreparedGraphMutationTransaction {
    #[doc(hidden)]
    pub fn encode_graph_wal(&mut self, graph_only: bool) -> Result<()> {
        if self.wal.is_none()
            && self.transaction.persistent_commit
            && graph_only
            && !self.transaction.ops.is_empty()
        {
            self.wal = Some(crate::wal::binary::PreparedWalBatch::prepare(
                &self.transaction.ops,
            )?);
        }
        Ok(())
    }
}

impl GraphMutationTransaction {
    #[doc(hidden)]
    pub fn prepare_commit(
        self,
        limits: MutationLimits,
        graph_only: bool,
    ) -> Result<PreparedGraphMutationTransaction> {
        let mut prepared = self.compact_for_commit(limits)?;
        prepared.encode_graph_wal(graph_only)?;
        Ok(prepared)
    }

    #[doc(hidden)]
    pub fn compact_for_commit(
        mut self,
        limits: MutationLimits,
    ) -> Result<PreparedGraphMutationTransaction> {
        let had_graph_writes = !self.ops.is_empty();
        self.ops = compact_transaction_graph_ops(self.ops);
        ensure_mutation_commit_limits(&self.ops, &self.rows, limits)?;
        Ok(PreparedGraphMutationTransaction {
            transaction: self,
            wal: None,
            had_graph_writes,
        })
    }

    #[doc(hidden)]
    pub fn is_read_only(&self) -> bool {
        self.ops.is_empty()
    }

    #[doc(hidden)]
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    #[doc(hidden)]
    pub fn store(&self) -> &GraphStore {
        &self.store
    }

    #[doc(hidden)]
    pub fn catalog_and_store_mut(&mut self) -> (&mut Catalog, &mut GraphStore) {
        (&mut self.catalog, &mut self.store)
    }

    #[doc(hidden)]
    pub fn savepoint(&self) -> GraphMutationSavepoint {
        GraphMutationSavepoint {
            catalog: self.catalog.clone(),
            store: self.store.snapshot(),
            op_len: self.ops.len(),
            row_len: self.rows.len(),
        }
    }

    #[doc(hidden)]
    pub fn restore(&mut self, savepoint: GraphMutationSavepoint) {
        self.catalog = savepoint.catalog;
        self.store = savepoint.store;
        self.ops.truncate(savepoint.op_len);
        self.rows.truncate(savepoint.row_len);
    }

    #[doc(hidden)]
    pub fn lock_footprint_since(
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

    #[doc(hidden)]
    pub fn stage_mutation_with_limits(
        &mut self,
        mutation: GraphMutation,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.stage_mutations_with_limits(vec![mutation], limits)
    }

    #[doc(hidden)]
    pub fn stage_mutations_with_limits(
        &mut self,
        mutations: Vec<GraphMutation>,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.stage_mutations(mutations, limits, true)
    }

    #[doc(hidden)]
    pub fn stage_mutation_without_commit_rows(
        &mut self,
        mutation: GraphMutation,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.stage_mutations(vec![mutation], limits, false)
    }

    #[doc(hidden)]
    pub fn stage_mutations_without_commit_rows(
        &mut self,
        mutations: Vec<GraphMutation>,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.stage_mutations(mutations, limits, false)
    }

    fn stage_mutations(
        &mut self,
        mutations: Vec<GraphMutation>,
        limits: MutationLimits,
        retain_commit_rows: bool,
    ) -> Result<MutationSummary> {
        if mutations.is_empty() {
            return Ok(MutationSummary {
                rows: Vec::new(),
                relational_mutation_outcomes: Vec::new(),
                append_mutation_outcomes: Vec::new(),
            });
        }
        let mut captured_ops = Vec::new();
        let summary = self.store.commit_mutations_internal(
            &mut self.catalog,
            mutations,
            limits,
            MutationCommitOptions {
                captured_graph_ops: Some(&mut captured_ops),
                creation_clock: Some(self.creation_clock),
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
                    HawDBError::Execution(format!(
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

fn ensure_graph_read_flags_usable(
    integrity_poisoned: &AtomicBool,
    post_wal_apply_poisoned: &AtomicBool,
) -> Result<()> {
    if integrity_poisoned.load(AtomicOrdering::Acquire) {
        return Err(HawDBError::Storage(
            "database handle is poisoned after a runtime storage integrity failure; close and reopen the database before issuing more operations"
                .to_string(),
        ));
    }
    if post_wal_apply_poisoned.load(AtomicOrdering::Acquire) {
        return Err(HawDBError::Storage(
            "database handle is poisoned after a durable WAL batch failed during in-memory apply; close and reopen the database before issuing more operations"
                .to_string(),
        ));
    }
    Ok(())
}

impl GraphStore {
    pub fn in_memory() -> Self {
        Self::default()
    }

    #[doc(hidden)]
    pub fn ensure_usable(&self) -> Result<()> {
        ensure_graph_read_flags_usable(&self.integrity_poisoned, &self.post_wal_apply_poisoned)?;
        self.validate_authoritative_relational_index_open()?;
        Ok(())
    }

    pub fn post_wal_apply_poisoned(&self) -> bool {
        self.post_wal_apply_poisoned.load(AtomicOrdering::Acquire)
    }

    #[doc(hidden)]
    pub fn projection_generation_store(
        &self,
    ) -> Result<hawdb_storage::projection_generation::ProjectionGenerationStore> {
        self.projection_generations.clone().ok_or_else(|| {
            HawDBError::Storage(
                "projection generation catalog requires a durable database".to_string(),
            )
        })
    }

    pub fn storage_handle_poisoned(&self) -> bool {
        self.post_wal_apply_poisoned() || self.integrity_poisoned.load(AtomicOrdering::Acquire)
    }

    #[doc(hidden)]
    pub fn poison_on_storage_error<T>(&self, result: &Result<T>) {
        if matches!(result, Err(HawDBError::StorageIntegrity(_))) {
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
        if replay_config.recovery_mode == RecoveryMode::DoctorRepairTornTail {
            return Err(HawDBError::Storage(
                "WAL repair is not available through database open; use DatabaseDoctor to plan and explicitly apply repair before opening in strict mode"
                    .to_string(),
            ));
        }
        if matches!(mode, DurableOpenMode::ExistingOnly)
            && replay_config.recovery_mode == RecoveryMode::AutoRepairTornTail
        {
            return Err(HawDBError::Storage(
                "automatic WAL tail repair requires a writable database open".to_string(),
            ));
        }
        let total_open_started = std::time::Instant::now();
        if replay_config.max_graph_manifest_open_bytes == 0 {
            return Err(HawDBError::Storage(
                "max_graph_manifest_open_bytes must be non-zero".into(),
            ));
        }
        let _project_files = match mode {
            DurableOpenMode::CreateIfMissing => {
                crate::file_descriptors::ProjectFileDescriptors::acquire_with_capacity_limit(
                    path.as_ref(),
                    replay_config.max_open_files,
                    replay_config.strict_capacity_limit,
                )?
            }
            DurableOpenMode::ExistingOnly => {
                // Preserve the read-only path contract before descriptor-domain
                // canonicalization can replace it with an unclassified IO error.
                DurableStore::validate_read_only_path(path.as_ref())?;
                crate::file_descriptors::ProjectFileDescriptors::acquire_existing_with_capacity_limit(
                    path.as_ref(),
                    replay_config.max_open_files,
                    replay_config.strict_capacity_limit,
                )?
            }
        };
        let _descriptor_operation = _project_files.io_context().internal_operation()?;
        let durable_manifest_open_started = std::time::Instant::now();
        let durable = match mode {
            DurableOpenMode::CreateIfMissing => {
                DurableStore::open(path.as_ref(), durability, replay_config)?
            }
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
        Self::finish_open_with_replay(
            durable,
            catalog,
            replay_config,
            durable_manifest_open_micros,
            |store, catalog, replay_config| store.replay_wal(catalog, replay_config),
        )
    }

    fn finish_open_with_replay(
        durable: DurableStore,
        catalog: &mut Catalog,
        replay_config: WalReplayConfig,
        durable_manifest_open_micros: u64,
        replay: impl FnOnce(&mut Self, &mut Catalog, WalReplayConfig) -> Result<StorageRecoveryReport>,
    ) -> Result<(Self, Catalog)> {
        // Legacy stores do not pass through immutable branch admission.
        // Check all selected codec capabilities before cleanup or runtime writes.
        if durable.wal_path.parent() == Some(durable.root_path.as_path()) {
            let manifest = DurableManifest::load(durable.manifest_path())?;
            let mut limits = replay_config;
            format_admission::validate_checkpoint_formats(manifest, limits, |name| {
                Ok(durable.root_path.join(name))
            })?;
            if fs::try_exists(&durable.wal_path)? && fs::metadata(&durable.wal_path)?.len() != 0 {
                format_admission::validate_wal_formats(
                    &durable.wal_path,
                    manifest.checkpoint_commit_epoch,
                    &mut limits,
                    true,
                )?;
            }
        }
        if !durable.read_only {
            cleanup_abandoned_checkpoint_preparations(
                &durable.root_path,
                durable.checkpoint_epoch,
            )?;
        }
        let projection_generation_root = durable.root_path.join("projection-generations");
        let projection_generations = if durable.read_only {
            if projection_generation_root.exists() {
                Some(
                    hawdb_storage::projection_generation::ProjectionGenerationStore::open_existing(
                        &projection_generation_root,
                    )
                    .map_err(HawDBError::from_storage_error)?,
                )
            } else {
                None
            }
        } else {
            Some(
                hawdb_storage::projection_generation::ProjectionGenerationStore::open(
                    &projection_generation_root,
                )
                .map_err(HawDBError::from_storage_error)?,
            )
        };
        let mut store = Self {
            next_node_id: 0,
            next_rel_id: 0,
            commit_epoch: 0,
            graph_change_commit_epoch: 0,
            adjacency_change_commit_epoch: 0,
            pending_commit_changes_graph: false,
            pending_commit_changes_adjacency: false,
            checkpoint_capture_started: None,
            version_index: hawdb_storage::version::VersionIndex::default(),
            version_snapshot_pins: Default::default(),
            version_snapshot_pin: None,
            read_dependency_readers: Arc::new(AtomicUsize::new(0)),
            read_dependency_validation_failures: Arc::new(Default::default()),
            read_summary_collapses: Arc::new(AtomicU64::new(0)),
            nodes: crate::cow::CowNodeMap::default(),
            relationships: crate::cow::CowRelMap::default(),
            basic_statistics: BasicStatisticsState::default(),
            checkpoint_statistics: CheckpointStatisticsState::default(),
            advanced_statistics_dirty: AdvancedStatisticsDirtyState::default(),
            statistics_recomputes: Arc::default(),
            outgoing: CowSegmentedMap::default(),
            incoming: CowSegmentedMap::default(),
            property_index: CowSegmentedMap::default(),
            composite_property_index: CowSegmentedMap::default(),
            full_text_property_index: CowSegmentedMap::default(),
            relationship_property_index: CowSegmentedMap::default(),
            projected_graphs: CowSegment::default(),
            projected_graph_artifacts: Default::default(),
            stable_id_mapping: CowSegment::default(),
            initial_import_source_fingerprint: None,
            search_projection_database_identity: None,
            search_projection_change_log_start_epoch: 0,
            search_projection_graph_changes: SearchProjectionChangeLog::default(),
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
            post_wal_apply_poisoned: Arc::new(AtomicBool::new(false)),
            integrity_poisoned: Arc::new(AtomicBool::new(false)),
            relational_state: RelationalState::default(),
            append_state: AppendState::default(),
            append_mutation_limits: AppendMutationLimits::default(),
            append_publication_config: AppendPublicationConfig::default(),
            append_generation_reader: None,
            relational_mutation_limits: RelationalMutationLimits::default(),
            relational_overflow_config: RelationalOverflowConfig::default(),
            legacy_relational_checkpoint_file: false,
            columnar_shadow: ColumnarShadowState::default(),
            relational_index_shadow: RelationalIndexShadowState::new(
                replay_config.relational_index_mode,
            ),
            relational_row_pages: RelationalRowPageState::default(),
            projection_generations,
            runtime_governor: None,
            branch_lease: None,
            branch_root_pin: None,
            branch_runtime_owner: None,
            snapshot_file_context: None,
            snapshot_residency_observer: None,
            durable: Some(durable),
            checkpoint_allocations: Default::default(),
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
        let mut storage_recovery_report = replay(&mut store, catalog, replay_config)?;
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
        mut replay_config: WalReplayConfig,
    ) -> Result<(Self, Catalog)> {
        if replay_config.recovery_mode != RecoveryMode::Strict {
            return Err(HawDBError::Storage(
                "derived repair requires strict WAL replay".to_string(),
            ));
        }
        let directory = doctor::repair_directory(path)?;
        let path = directory.as_path();
        let project_files = doctor::repair_file_descriptors(path)?;
        replay_config.max_open_files = project_files.metrics().limit;
        if path
            .parent()
            .is_some_and(|parent| parent.file_name().is_some_and(|name| name == "branches"))
        {
            let id = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| HawDBError::StorageIntegrity("repair branch has no UUID".into()))?;
            let branch_id = crate::branch_catalog::BranchId::parse(id)
                .map_err(HawDBError::from_storage_error)?;
            let catalog_path = path.parent().unwrap().join("catalog.hawdb");
            let catalog = crate::branch_catalog::read_catalog(&catalog_path)?;
            let record = catalog
                .branches
                .iter()
                .find(|record| record.id == branch_id)
                .ok_or_else(|| {
                    HawDBError::StorageIntegrity("repair branch is absent from its catalog".into())
                })?;
            let head_path = path.join("branch.head");
            let objects = path.parent().unwrap().join("objects");
            return Self::admit_branch_for_derived_repair(BranchAdmissionRequest {
                catalog_path: &catalog_path,
                branch_id,
                expected_metadata_revision: record.metadata_revision,
                head_path: &head_path,
                immutable_store_root: &objects,
                durability: DurabilityPolicy::default(),
                replay_config,
            })
            .map(|admitted| admitted.into_parts())
            .map_err(|error| match error {
                BranchAdmissionError::Recovery(error) => error,
                error => HawDBError::from_storage_error(error),
            });
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
        let (mut store, _) = Self::finish_open(
            durable,
            &mut recovered_catalog,
            replay_config,
            durable_manifest_open_micros,
        )?;
        store.storage_recovery_report.open_timings.total_open_micros =
            elapsed_micros(total_open_started);
        Ok((store, recovered_catalog))
    }

    fn enable_derived_repair_writes(&mut self) -> Result<()> {
        let durable = self.durable.as_mut().ok_or_else(|| {
            HawDBError::Storage("derived repair requires durable storage".to_string())
        })?;
        durable.read_only = false;
        Ok(())
    }

    #[doc(hidden)]
    pub fn relational_state(&self) -> &RelationalState {
        &self.relational_state
    }

    /// Outer manifest revision emitted by this writer; payload codecs have independent revisions.
    pub fn storage_version(&self) -> &'static str {
        crate::durable_manifest::MANIFEST_STORAGE_VERSION
    }

    pub fn is_out_of_core(&self) -> bool {
        self.canonical_base_out_of_core
    }

    pub fn commit_epoch(&self) -> u64 {
        self.commit_epoch
    }

    /// Returns the commit epoch of the newest commit that changed graph nodes,
    /// relationships, their properties, or graph schema (tables, labels,
    /// relationship types, property definitions, indexes, constraints and
    /// projected graphs). Relational-only and append-only commits do not
    /// advance it. It is independent of the search projection change log, so
    /// pruning never regresses it. It is not persisted: recovery starts it at
    /// the checkpoint's commit epoch and WAL replay advances it exactly. It is
    /// monotonic only over surviving history: a crash under
    /// `SyncOnCheckpoint` can drop acknowledged commits, so hosts must discard
    /// any baseline captured before a reopen.
    ///
    /// Refused with the storage error once this store is poisoned (for
    /// example after a failed durability barrier), because the epoch of an
    /// uncertain commit cannot be relied on.
    pub fn graph_change_commit_epoch(&self) -> Result<u64> {
        ensure_graph_read_flags_usable(&self.integrity_poisoned, &self.post_wal_apply_poisoned)?;
        Ok(self.graph_change_commit_epoch)
    }

    /// The raw value, for capturing a transaction's base snapshot; readers
    /// that report it use [`Self::graph_change_commit_epoch`].
    #[doc(hidden)]
    pub fn graph_change_commit_epoch_unchecked(&self) -> u64 {
        self.graph_change_commit_epoch
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
        .with_graph_generation(
            checkpoint.map(|durable| ManifestGeneration(durable.graph_generation())),
        )
        .with_adjacency_generation(
            checkpoint.map(|durable| ManifestGeneration(durable.adjacency_generation())),
        )
    }

    #[doc(hidden)]
    pub fn begin_wal_sync_group(&mut self) -> Result<bool> {
        let Some(durable) = &mut self.durable else {
            return Ok(false);
        };
        durable.begin_wal_sync_group()
    }

    #[doc(hidden)]
    pub fn wal_sync_group_progress(&self) -> WalSyncGroupProgress {
        self.durable
            .as_ref()
            .map_or_else(WalSyncGroupProgress::default, |durable| {
                durable.wal_sync_group_progress()
            })
    }

    #[doc(hidden)]
    pub fn wal_sync_group_active(&self) -> bool {
        self.durable
            .as_ref()
            .is_some_and(DurableStore::wal_sync_group_active)
    }

    #[doc(hidden)]
    pub fn finish_wal_sync_group(&mut self) -> Result<WalSyncGroupFlush> {
        let Some(durable) = &mut self.durable else {
            return Ok(WalSyncGroupFlush::default());
        };
        match durable.finish_wal_sync_group() {
            Ok(flush) => Ok(flush),
            Err(error) => {
                self.post_wal_apply_poisoned
                    .store(true, AtomicOrdering::Release);
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
            .map(|change| change.as_ref().clone())
            .collect()
    }

    /// Captures immutable feed input. Completion is not projection authorization.
    #[doc(hidden)]
    pub fn capture_projection_feed(
        &self,
        start_epoch: u64,
        limits: ProjectionFeedLimits,
    ) -> std::result::Result<ProjectionFeedCursor, ProjectionFeedError> {
        self.ensure_usable().map_err(ProjectionFeedError::Source)?;
        ProjectionFeedCursor::capture(
            &self.search_projection_graph_changes,
            self.search_projection_change_log_start_epoch,
            self.commit_epoch,
            start_epoch,
            limits,
        )
    }

    pub fn search_projection_changes_after(
        &self,
        commit_epoch: u64,
    ) -> Vec<hawdb_storage::projection::SearchProjectionChange> {
        self.search_projection_graph_changes_after(commit_epoch)
    }

    pub fn search_projection_changefeed_status(&self) -> SearchProjectionChangefeedStatus {
        SearchProjectionChangefeedStatus {
            graph_commit_epoch: self.commit_epoch,
            resume_floor_commit_epoch: self.search_projection_change_log_start_epoch,
            oldest_retained_mutation_id: self
                .search_projection_graph_changes
                .first()
                .map(|change| change.mutation_id()),
            newest_retained_mutation_id: self
                .search_projection_graph_changes
                .last()
                .map(|change| change.mutation_id()),
            first_rebuild_required_mutation_id: self
                .search_projection_graph_changes
                .iter()
                .find(|change| change.relational_primary_key_changes.requires_rebuild())
                .map(|change| change.mutation_id()),
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
        limits: hawdb_storage::relational::RelationalPrimaryKeyChangeCaptureLimits,
    ) {
        self.search_projection_primary_key_capture_limits = limits;
        for change in self.search_projection_graph_changes.iter_mut() {
            if change.relational_primary_key_changes.exceeds_limits(limits) {
                Arc::make_mut(change).relational_primary_key_changes =
                    hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::RequiresRebuild {
                        reason: hawdb_storage::relational::RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded,
                    };
            }
        }
        self.search_projection_change_log_retained_bytes = self
            .search_projection_graph_changes
            .iter()
            .map(|change| change.estimated_retained_bytes())
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

    #[doc(hidden)]
    pub fn search_projection_database_identity(&self) -> Option<hawdb_core::Uuid> {
        self.search_projection_database_identity
    }

    #[doc(hidden)]
    pub fn set_search_projection_database_identity(&mut self, identity: hawdb_core::Uuid) {
        self.search_projection_database_identity = Some(identity);
    }

    #[doc(hidden)]
    pub fn search_projection_registry_root(&self) -> Option<&Path> {
        self.durable.as_ref().map(|durable| durable.root_path())
    }

    /// Returns the durable database root for library-owned metadata such as
    /// the branch catalog. In-memory stores do not have a filesystem root.
    #[doc(hidden)]
    pub fn durable_root_path(&self) -> Option<&Path> {
        self.durable.as_ref().map(|durable| durable.root_path())
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
            return Err(HawDBError::Storage(
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
            return Err(HawDBError::Storage(
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

    /// Captures query data without protecting canonical conflict history.
    /// Read consumers cannot observe the compressed conflict metadata. New
    /// transactions start at this snapshot's epoch; any older transaction
    /// submitted to this detached store is conservatively rejected by a barrier.
    /// Upgrading an old read snapshot to a writable workspace may conflict if
    /// the canonical store has already reclaimed its validation history.
    /// Writable workspaces/savepoints must use `snapshot` to retain precision.
    #[doc(hidden)]
    pub fn snapshot_for_read(&self) -> Self {
        let mut snapshot = self.snapshot_with_history(false);
        snapshot.version_index = self.version_index.read_baseline(self.commit_epoch);
        snapshot
    }

    pub fn snapshot(&self) -> Self {
        self.snapshot_with_history(true)
    }

    fn snapshot_with_history(&self, protect_history: bool) -> Self {
        // Private statement execution can advance commit_epoch without
        // advancing the transaction's read epoch. A savepoint or descendant
        // must inherit the original protection floor, even after restore
        // drops its parent workspace.
        let pin_epoch = self
            .version_snapshot_pin
            .as_ref()
            .map_or(self.commit_epoch, |pin| pin.epoch().min(self.commit_epoch));
        Self {
            next_node_id: self.next_node_id,
            next_rel_id: self.next_rel_id,
            commit_epoch: self.commit_epoch,
            graph_change_commit_epoch: self.graph_change_commit_epoch,
            adjacency_change_commit_epoch: self.adjacency_change_commit_epoch,
            pending_commit_changes_graph: self.pending_commit_changes_graph,
            pending_commit_changes_adjacency: self.pending_commit_changes_adjacency,
            checkpoint_capture_started: self.checkpoint_capture_started,
            version_index: self.version_index.clone(),
            version_snapshot_pins: self.version_snapshot_pins.clone(),
            version_snapshot_pin: Some(if protect_history {
                self.version_snapshot_pins.pin(pin_epoch)
            } else {
                hawdb_storage::version::VersionSnapshotPin::read_only(pin_epoch)
            }),
            read_dependency_readers: Arc::clone(&self.read_dependency_readers),
            read_dependency_validation_failures: Arc::clone(
                &self.read_dependency_validation_failures,
            ),
            read_summary_collapses: Arc::clone(&self.read_summary_collapses),
            nodes: self.nodes.clone(),
            relationships: self.relationships.clone(),
            basic_statistics: self.basic_statistics.clone(),
            checkpoint_statistics: self.checkpoint_statistics.clone(),
            advanced_statistics_dirty: self.advanced_statistics_dirty,
            statistics_recomputes: Arc::clone(&self.statistics_recomputes),
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
            search_projection_database_identity: self.search_projection_database_identity,
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
            post_wal_apply_poisoned: Arc::clone(&self.post_wal_apply_poisoned),
            integrity_poisoned: Arc::clone(&self.integrity_poisoned),
            relational_state: self.relational_state.clone(),
            append_state: self.append_state.clone(),
            append_mutation_limits: self.append_mutation_limits,
            append_publication_config: self.append_publication_config,
            append_generation_reader: self.append_generation_reader.clone(),
            relational_mutation_limits: self.relational_mutation_limits,
            relational_overflow_config: self.relational_overflow_config,
            legacy_relational_checkpoint_file: self.legacy_relational_checkpoint_file,
            columnar_shadow: self.columnar_shadow.clone(),
            relational_index_shadow: self
                .relational_index_shadow
                .snapshot_at_epoch(self.commit_epoch),
            relational_row_pages: self
                .relational_row_pages
                .snapshot_at_epoch(self.commit_epoch),
            projection_generations: None,
            runtime_governor: self.runtime_governor.clone(),
            branch_lease: self.branch_lease.clone(),
            branch_root_pin: self.branch_root_pin.clone(),
            branch_runtime_owner: self.branch_runtime_owner.clone(),
            snapshot_file_context: self.file_descriptor_context(),
            snapshot_residency_observer: self.residency_observer(),
            durable: None,
            checkpoint_allocations: self.checkpoint_allocations.clone(),
        }
    }

    /// Threads the engine's runtime governor into the store so background
    /// shadow work can request admission (`WorkClass::Shadow`, background
    /// priority). The store never constructs a governor of its own.
    pub fn set_runtime_governor(&mut self, governor: hawdb_qos::RuntimeGovernor) {
        self.runtime_governor = Some(Arc::new(RuntimeGovernorBackgroundAdmission(governor)));
    }

    pub fn register_projected_graph(
        &mut self,
        name: &str,
        definition: ProjectedGraphDefinition,
    ) -> Result<()> {
        self.append_durable_wal_single(WalOp::ProjectGraph {
            name: name.to_string(),
            node_labels: definition.node_labels.clone(),
            rel_types: definition.rel_types.clone(),
            relationship_predicates: definition.relationship_predicates.clone(),
        })?;
        self.apply_project_graph_definition(name.to_string(), definition)?;
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

#[doc(hidden)]
pub fn sync_parent_dir(path: &Path) -> Result<()> {
    sync_parent_directory(path)?;
    Ok(())
}

#[doc(hidden)]
pub use hawdb_storage::schema::{ensure_table_descriptor, reserve_schema_maintenance_budget};

fn validate_property_descriptor(
    catalog: &Catalog,
    store: &GraphStore,
    table_id: TableId,
    property: &str,
    value_type: PropertyType,
    nullable: bool,
) -> Result<()> {
    validate_property_descriptor_with_table_state(
        catalog,
        store,
        table_id,
        PropertyCheck {
            property,
            value_type,
            nullable,
            force: false,
            allow_absent: false,
        },
    )
}

/// One descriptor validation over a table's existing records.
#[derive(Clone, Copy)]
struct PropertyCheck<'a> {
    property: &'a str,
    value_type: PropertyType,
    nullable: bool,
    /// Validate even when the table descriptor is not `Public`.
    force: bool,
    /// Accept records lacking the property (a defaulted declaration's
    /// backfill fills them before validation).
    allow_absent: bool,
}

fn validate_property_descriptor_with_table_state(
    catalog: &Catalog,
    store: &GraphStore,
    table_id: TableId,
    check: PropertyCheck<'_>,
) -> Result<()> {
    let Some(table) = catalog.table_descriptor(table_id) else {
        return Err(HawDBError::Storage(format!(
            "property schema references missing table {}",
            table_id.0
        )));
    };
    if !check.force && table.state != SchemaObjectState::Public {
        return Ok(());
    }
    let validate = |value: Option<&Value>, record: &dyn Fn() -> String| {
        if check.allow_absent && value.is_none() {
            return Ok(());
        }
        validate_property_schema_value(
            &table.name,
            check.property,
            check.value_type,
            check.nullable,
            value,
            &record(),
        )
    };
    match table.kind {
        TableKind::Node => {
            let Some(label_id) = catalog.label_id(&table.name) else {
                return Ok(());
            };
            for node in store.nodes.values() {
                if node.labels.contains(&label_id) {
                    validate(node.properties.get(check.property), &|| {
                        format!("node {}", node.id.0)
                    })?;
                }
            }
        }
        TableKind::Relationship => {
            let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
                return Ok(());
            };
            for relationship in store.relationships.values() {
                if relationship.rel_type == rel_type_id {
                    validate(relationship.properties.get(check.property), &|| {
                        format!("relationship {}", relationship.id.0)
                    })?;
                }
            }
        }
    }
    Ok(())
}

/// Rejects a declared default that the descriptor itself would reject, so a
/// default can never make a later creation fail validation.
fn validate_property_default(
    table: &str,
    property: &str,
    value_type: PropertyType,
    nullable: bool,
    default: &PropertyDefault,
) -> Result<()> {
    let sample = match default {
        PropertyDefault::Value(Value::Null) => {
            return Err(HawDBError::Storage(format!(
                "property schema {table}.{property} DEFAULT NULL is not a default; omit DEFAULT to leave the property absent"
            )));
        }
        PropertyDefault::Value(value) => {
            crate::canonical::validate_property_value(value).map_err(|error| {
                HawDBError::or_stop(&error, || {
                    HawDBError::Storage(format!(
                    "property schema {table}.{property} default violates storage limits: {error}"
                ))
                })
            })?;
            value.clone()
        }
        PropertyDefault::CurrentTimestamp => {
            PropertyDefault::CurrentTimestamp.resolve(PropertyDefault::creation_clock())
        }
    };
    validate_property_schema_value(
        table,
        property,
        value_type,
        nullable,
        Some(&sample),
        "default value",
    )
}

/// A property descriptor declaration, as written by `CREATE PROPERTY`.
pub(crate) struct PropertyDeclaration<'a> {
    pub(crate) table_kind: TableKind,
    pub(crate) table: &'a str,
    pub(crate) table_id: TableId,
    pub(crate) property: &'a str,
    pub(crate) value_type: PropertyType,
    pub(crate) nullable: bool,
    pub(crate) default: Option<PropertyDefault>,
}

/// Ops declaring a property descriptor. A defaulted property declared on a
/// table that has records without it enters `Backfill`: schema maintenance
/// writes the default into those records before validation and `Public`.
fn property_declaration_ops(
    catalog: &Catalog,
    store: &GraphStore,
    declaration: &PropertyDeclaration<'_>,
) -> Result<(
    Vec<WalOp>,
    Option<DeclaredPropertyDefault>,
    SchemaObjectState,
)> {
    let PropertyDeclaration {
        table_kind,
        table,
        table_id,
        property,
        value_type,
        nullable,
        ref default,
    } = *declaration;
    let create = |default: Option<DeclaredPropertyDefault>| WalOp::CreateProperty {
        table_kind,
        table: table.to_string(),
        property: property.to_string(),
        value_type,
        nullable,
        default: default.map(Box::new),
    };
    let Some(default) = default.clone() else {
        validate_property_descriptor(catalog, store, table_id, property, value_type, nullable)?;
        return Ok((vec![create(None)], None, SchemaObjectState::Public));
    };
    validate_property_default(table, property, value_type, nullable, &default)?;
    let declared = DeclaredPropertyDefault::declare(default);
    let needs_backfill = catalog
        .table_descriptor(table_id)
        .is_some_and(|descriptor| {
            !property_backfill_ops(
                catalog,
                store,
                descriptor,
                property,
                &declared.backfill_value,
                Some(1),
            )
            .0
            .is_empty()
        });
    // Records lacking the property are filled by the backfill; present values
    // (including explicit NULLs) must already satisfy the descriptor.
    validate_property_descriptor_with_table_state(
        catalog,
        store,
        table_id,
        PropertyCheck {
            property,
            value_type,
            nullable,
            force: false,
            allow_absent: needs_backfill,
        },
    )?;
    let mut ops = vec![create(Some(declared.clone()))];
    let state = if needs_backfill {
        ops.push(WalOp::AlterPropertyState {
            table_kind,
            table: table.to_string(),
            property: property.to_string(),
            state: SchemaObjectState::Backfill,
        });
        SchemaObjectState::Backfill
    } else {
        SchemaObjectState::Public
    };
    Ok((ops, Some(declared), state))
}

/// Set ops writing `value` into records of `table` that lack `property`
/// (the key is absent; an explicit stored NULL is kept), up to `limit`
/// records. The flag reports whether every lacking record was covered.
fn property_backfill_ops(
    catalog: &Catalog,
    store: &GraphStore,
    table: &TableDescriptor,
    property: &str,
    value: &Value,
    limit: Option<usize>,
) -> (Vec<WalOp>, bool) {
    let mut ops = Vec::new();
    let full = |ops: &Vec<WalOp>| limit.is_some_and(|limit| ops.len() >= limit);
    match table.kind {
        TableKind::Node => {
            let Some(label_id) = catalog.label_id(&table.name) else {
                return (ops, true);
            };
            for node in store.nodes.values() {
                if node.labels.contains(&label_id) && !node.properties.contains_key(property) {
                    if full(&ops) {
                        return (ops, false);
                    }
                    ops.push(WalOp::SetNodeProperty {
                        id: node.id,
                        property: property.to_string(),
                        value: value.clone(),
                    });
                }
            }
        }
        TableKind::Relationship => {
            let Some(rel_type_id) = catalog.rel_type_id(&table.name) else {
                return (ops, true);
            };
            for relationship in store.relationships.values() {
                if relationship.rel_type == rel_type_id
                    && !relationship.properties.contains_key(property)
                {
                    if full(&ops) {
                        return (ops, false);
                    }
                    ops.push(WalOp::SetRelationshipProperty {
                        id: relationship.id,
                        property: property.to_string(),
                        value: value.clone(),
                    });
                }
            }
        }
    }
    (ops, true)
}

/// Fills declared defaults into every created record whose creating write
/// did not mention the property. Present keys, including explicit NULLs, are
/// never touched, and nothing is filled at read time. `clock` is the writing
/// transaction's `current_timestamp()` instant, read lazily at most once.
pub(crate) fn fill_creation_defaults(
    catalog: &Catalog,
    ops: &mut [WalOp],
    clock: &mut impl FnMut() -> hawdb_core::temporal::PlainDateTime,
) {
    if !catalog.has_property_defaults() {
        return;
    }
    for op in ops {
        match op {
            WalOp::CreateNode {
                label, properties, ..
            } => catalog.fill_creation_defaults(TableKind::Node, label, properties, clock),
            WalOp::CreateRelationship {
                rel_type,
                properties,
                ..
            } => {
                catalog.fill_creation_defaults(TableKind::Relationship, rel_type, properties, clock)
            }
            WalOp::Batch(ops) => fill_creation_defaults(catalog, ops, clock),
            _ => {}
        }
    }
}

/// A lazily read, then fixed, creation clock.
pub(crate) fn lazy_creation_clock(
    fixed: Option<hawdb_core::temporal::PlainDateTime>,
) -> impl FnMut() -> hawdb_core::temporal::PlainDateTime {
    let mut instant = fixed;
    move || *instant.get_or_insert_with(PropertyDefault::creation_clock)
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
            PropertyCheck {
                property: &property.name,
                value_type: property.value_type,
                nullable: property.nullable,
                force: true,
                allow_absent: false,
            },
        )?;
    }
    Ok(())
}

#[doc(hidden)]
pub use hawdb_storage::wal::apply_wal_op_to_snapshot;
fn encode_projected_graph_artifacts(
    catalog: &Catalog,
    store: &GraphStore,
    projection_epoch: u64,
) -> String {
    hawdb_storage::projection::artifact::encode_projected_graph_artifacts(
        projection_epoch,
        store.commit_epoch,
        store.projected_graphs.iter().map(|(name, definition)| {
            let graph = projected_graph_from_definition(catalog, store, definition);
            let data = ProjectedGraphArtifactData::new(
                graph.nodes().to_vec(),
                graph.csr_offsets().to_vec(),
                graph.csr_targets().to_vec(),
                graph.csc_offsets().to_vec(),
                graph.csc_sources().to_vec(),
            )
            .expect("analytics graph has valid adjacency arrays");
            (name.as_str(), definition, data)
        }),
    )
}

fn encode_projected_graph_artifacts_with_work_context(
    catalog: &Catalog,
    store: &GraphStore,
    projection_epoch: u64,
    work: &crate::background::CheckpointWorkContext,
) -> Result<hawdb_storage::projection::artifact::CheckpointProjectedGraphText> {
    hawdb_storage::projection::artifact::encode_projected_graph_artifacts_with_work_context(
        projection_epoch,
        store.commit_epoch,
        store.projected_graphs.iter().map(|(name, definition)| {
            let data =
                checkpoint_projected_graph_from_definition(catalog, store, definition, work)?;
            Ok((name.as_str(), definition, data))
        }),
        work,
    )
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
    let relationship_predicates = definition
        .relationship_predicates
        .iter()
        .filter_map(|(rel_type, predicate)| {
            catalog
                .rel_type_id(rel_type)
                .map(|rel_type_id| (rel_type_id, predicate))
        })
        .collect::<BTreeMap<_, _>>();
    ProjectedGraph::try_from_store_labels_and_rel_types_with_filters_and_layout(
        store,
        &label_ids,
        &rel_type_ids,
        |_| true,
        |relationship| {
            relationship_predicates
                .get(&relationship.rel_type)
                .is_none_or(|predicate| predicate.matches(&relationship.properties))
        },
        crate::analytics::ProjectionLayout::Bidirectional,
        crate::analytics::ProjectionMemoryBudget::unlimited(),
    )
    .expect("unlimited checkpoint projection is admitted")
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
                return Err(HawDBError::Storage(format!(
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
                return Err(HawDBError::Storage(format!(
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
                return Err(HawDBError::Storage(format!(
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
                return Err(HawDBError::Storage(format!(
                    "relationship unique constraint violation on :{rel_type}({}) for relationships {} and {}",
                    constraint.property, existing_id.0, changed_id.0
                )));
            }
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
            validation_error = Some(HawDBError::Storage(format!(
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
            validation_error = Some(HawDBError::Storage(format!(
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

fn generated_stable_id(kind: &str, physical_id: u64) -> Value {
    Value::Map(BTreeMap::from([
        (
            "source".to_string(),
            Value::String("hawdb-stable-id-v1".to_string()),
        ),
        ("kind".to_string(), Value::String(kind.to_string())),
        (
            "physical_id".to_string(),
            Value::String(physical_id.to_string()),
        ),
    ]))
}

fn optional_label_id(catalog: &Catalog, label: &str) -> Option<LabelId> {
    if label.is_empty() {
        None
    } else {
        catalog.label_id(label)
    }
}

/// Resolves an endpoint label of a relationship mutation's pattern. An
/// unlabeled pattern node (`""`) matches any node: `Some(None)`. A label the
/// catalog does not know matches no node: `None`.
fn endpoint_label_id(catalog: &Catalog, label: &str) -> Option<Option<LabelId>> {
    if label.is_empty() {
        Some(None)
    } else {
        catalog.label_id(label).map(Some)
    }
}

fn pending_node_index(pending_nodes: &[PendingNode], id: NodeId) -> Option<usize> {
    // Allocation appends increasing IDs; deletion retains the order and may leave gaps.
    pending_nodes
        .binary_search_by_key(&id, |(pending_id, _, _)| *pending_id)
        .ok()
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
        endpoint_label_id(catalog, &update.source_label),
        endpoint_label_id(catalog, &update.target_label),
        catalog.rel_type_id(&update.rel_type),
    ) else {
        return Ok(());
    };
    // `None`: an unconstrained source matches every relationship.
    let source_ids = (source_label_id.is_some() || update.filter.is_some())
        .then(|| {
            store.matching_node_ids_with_pending_bounded(
                source_label_id,
                update.filter.as_ref(),
                pending_nodes,
                remaining_mutation_affected_rows(rows.len(), limits)?,
                "max_mutation_affected_rows",
            )
        })
        .transpose()?
        .map(|ids| ids.into_iter().collect::<BTreeSet<_>>());
    let source_matches =
        |source: &NodeId| source_ids.as_ref().is_none_or(|ids| ids.contains(source));
    for relationship in store.relationship_records_owned() {
        let relationship = relationship?;
        if relationship.rel_type != rel_type_id || !source_matches(&relationship.source) {
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
                target_label_id.is_none_or(|label| target.labels.contains(&label))
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
        if *pending_rel_type_id != rel_type_id || !source_matches(source) {
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
    label_id: Option<LabelId>,
    filter: Option<&PropertyFilter>,
) -> Result<bool> {
    if let Some(node) = store.node_owned(id)? {
        return Ok(label_id.is_none_or(|label| node.labels.contains(&label))
            && filter
                .map(|filter| property_filter_matches(filter, id.0, &node.properties))
                .unwrap_or(true));
    }
    Ok(pending_node_index(pending_nodes, id)
        .map(|index| &pending_nodes[index])
        .map(|(_, pending_label_id, properties)| {
            label_id.is_none_or(|label| *pending_label_id == label)
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
            return Err(HawDBError::Execution(format!(
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
            return Err(HawDBError::Execution(format!(
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
    Ok(pending_node_index(pending_nodes, id)
        .map(|index| &pending_nodes[index])
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

fn remove_pending_node(ops: &mut Vec<WalOp>, pending_nodes: &mut PendingNodes, id: NodeId) {
    pending_nodes.remove(id);
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

#[doc(hidden)]
pub fn read_durable_text(path: &Path, name: &str) -> Result<String> {
    let bytes = fs::read(path)?;
    read_durable_text_bytes(&bytes, name)
}

fn validate_search_projection_checkpoint_changes<'a>(
    start_epoch: u64,
    checkpoint_commit_epoch: u64,
    changes: impl IntoIterator<Item = &'a SearchProjectionGraphChange>,
) -> Result<()> {
    if start_epoch > checkpoint_commit_epoch {
        return Err(HawDBError::Storage(format!(
            "search projection change log start epoch {start_epoch} exceeds checkpoint commit epoch {checkpoint_commit_epoch}"
        )));
    }
    let mut previous_epoch = start_epoch;
    for change in changes {
        if change.commit_epoch <= previous_epoch {
            return Err(HawDBError::Storage(format!(
                "search projection change commit epoch {} is not greater than previous epoch {previous_epoch}",
                change.commit_epoch
            )));
        }
        if change.commit_epoch > checkpoint_commit_epoch {
            return Err(HawDBError::Storage(format!(
                "search projection change commit epoch {} exceeds checkpoint commit epoch {checkpoint_commit_epoch}",
                change.commit_epoch
            )));
        }
        if !change
            .upsert_node_ids
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(HawDBError::Storage(format!(
                "search projection change at commit epoch {} has unordered or duplicate upsert node ids",
                change.commit_epoch
            )));
        }
        if !change
            .delete_document_ids
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(HawDBError::Storage(format!(
                "search projection change at commit epoch {} has unordered or duplicate delete document ids",
                change.commit_epoch
            )));
        }
        if let hawdb_storage::relational::RelationalPrimaryKeyChangeCapture::Captured {
            tables,
            ..
        } = &change.relational_primary_key_changes
        {
            if !tables.windows(2).all(|pair| pair[0].table < pair[1].table) {
                return Err(HawDBError::Storage(format!(
                    "search projection change at commit epoch {} has unordered or duplicate relational tables",
                    change.commit_epoch
                )));
            }
            for table in tables {
                if table.primary_keys.is_empty()
                    || !table.primary_keys.windows(2).all(|pair| pair[0] < pair[1])
                {
                    return Err(HawDBError::Storage(format!(
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

#[doc(hidden)]
pub fn checksum_bytes(bytes: &[u8]) -> u64 {
    checksum_u64(bytes)
}

use hawdb_storage::artifact_binding::verify_integrity;

fn verify_file_integrity(
    path: &Path,
    expected_len: u64,
    expected_checksum: u64,
    expected_sha256: Sha256Digest,
    artifact: &str,
) -> Result<()> {
    let (actual_len, actual_checksum, actual_sha256) = file_checksum(path)?;
    if actual_len != expected_len {
        return Err(HawDBError::StorageIntegrity(format!(
            "{artifact} encoded length mismatch: expected {expected_len}, got {actual_len}"
        )));
    }
    if actual_checksum != expected_checksum {
        return Err(HawDBError::StorageIntegrity(format!(
            "{artifact} CRC32C mismatch: expected {expected_checksum}, got {actual_checksum}"
        )));
    }
    if actual_sha256 != expected_sha256 {
        return Err(HawDBError::StorageIntegrity(format!(
            "{artifact} SHA-256 mismatch: expected {expected_sha256}, got {actual_sha256}"
        )));
    }
    Ok(())
}

fn elapsed_micros(started: std::time::Instant) -> u64 {
    started.elapsed().as_micros().min(u64::MAX as u128) as u64
}

fn estimated_node_record_bytes(node: &NodeRecord) -> u64 {
    estimated_node_record_bytes_with_visit(node, &mut || Ok::<_, std::convert::Infallible>(()))
        .unwrap_or_else(|never| match never {})
}

fn estimated_node_record_bytes_with_visit<E>(
    node: &NodeRecord,
    visit: &mut impl FnMut() -> std::result::Result<(), E>,
) -> std::result::Result<u64, E> {
    visit()?;
    Ok(32u64
        .saturating_add((node.labels.len() as u64).saturating_mul(4))
        .saturating_add(crate::mutation::estimated_properties_bytes_with_visit(
            &node.properties,
            visit,
        )?))
}

fn estimated_relationship_record_bytes(relationship: &RelRecord) -> u64 {
    estimated_relationship_record_bytes_with_visit(relationship, &mut || {
        Ok::<_, std::convert::Infallible>(())
    })
    .unwrap_or_else(|never| match never {})
}

fn estimated_relationship_record_bytes_with_visit<E>(
    relationship: &RelRecord,
    visit: &mut impl FnMut() -> std::result::Result<(), E>,
) -> std::result::Result<u64, E> {
    visit()?;
    Ok(
        40u64.saturating_add(crate::mutation::estimated_properties_bytes_with_visit(
            &relationship.properties,
            visit,
        )?),
    )
}

impl hawdb_storage::graph_engine::GraphReadEngine for GraphStore {
    fn storage_version(&self) -> &'static str {
        GraphStore::storage_version(self)
    }

    fn commit_epoch(&self) -> u64 {
        GraphStore::commit_epoch(self)
    }

    fn storage_handle_poisoned(&self) -> bool {
        GraphStore::storage_handle_poisoned(self)
    }

    fn published_read_view(&self) -> hawdb_storage::read_view::PublishedReadView {
        GraphStore::published_read_view(self)
    }

    fn basic_statistics(&self) -> hawdb_core::BasicGraphStatistics {
        GraphStore::basic_statistics(self)
    }

    fn statistics(&self, catalog: &hawdb_core::Catalog) -> hawdb_core::GraphStatistics {
        GraphStore::statistics(self, catalog)
    }

    fn supports_ordered_node_range(&self, label: LabelId, property: &str) -> bool {
        self.canonical_base.is_none()
            || self
                .persistent_property_projection
                .as_ref()
                .is_some_and(|projection| {
                    projection.manifest().supports(
                        label,
                        property,
                        PersistentPropertyProjectionKind::Range,
                    )
                })
    }

    fn supports_ordered_node_composite(&self, label: LabelId, properties: &[String]) -> bool {
        self.canonical_base.is_none()
            || self
                .persistent_property_projection
                .as_ref()
                .is_some_and(|projection| {
                    projection
                        .manifest()
                        .supports_composite_equality(label, properties)
                })
    }

    fn storage_recovery_report(&self) -> hawdb_storage::projection::StorageRecoveryReport {
        GraphStore::storage_recovery_report(self)
    }

    fn segment_cache_snapshot(&self) -> Option<hawdb_storage::cache::SegmentCacheSnapshot> {
        GraphStore::segment_cache_snapshot(self)
    }

    fn storage_residency_report(&self) -> hawdb_storage::residency::StorageResidencyReport {
        GraphStore::storage_residency_report(self)
    }

    fn adjacency_consistency_report(
        &self,
    ) -> hawdb_storage::consistency::AdjacencyConsistencyReport {
        GraphStore::adjacency_consistency_report(self)
    }

    fn adjacency_consolidation_plan(
        &self,
    ) -> hawdb_storage::consistency::AdjacencyConsolidationPlan {
        GraphStore::adjacency_consolidation_plan(self)
    }

    fn degree_statistics_consistency_report(
        &self,
    ) -> hawdb_storage::consistency::DegreeStatisticsConsistencyReport {
        GraphStore::degree_statistics_consistency_report(self)
    }

    fn property_index_consistency_report(
        &self,
        catalog: &hawdb_core::Catalog,
    ) -> hawdb_storage::consistency::PropertyIndexConsistencyReport {
        GraphStore::property_index_consistency_report(self, catalog)
    }

    fn columnar_shadow_checkpoint_report(
        &self,
    ) -> Option<hawdb_storage::column_group::shadow::ColumnarShadowCheckpointReport> {
        GraphStore::columnar_shadow_checkpoint_report(self)
    }

    fn columnar_shadow_recovery_status(
        &self,
    ) -> hawdb_storage::column_group::shadow::ColumnarShadowRecoveryStatus {
        GraphStore::columnar_shadow_recovery_status(self)
    }

    fn projected_graph_statuses(&self) -> Vec<hawdb_storage::projection::ProjectedGraphStatus> {
        GraphStore::projected_graph_statuses(self)
    }

    fn storage_pressure_snapshot(
        &self,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> hawdb_storage::pressure::StoragePressureSnapshot {
        GraphStore::storage_pressure_snapshot(self, oldest_reader_commit_epoch)
    }

    fn append_storage_residency_report(
        &self,
    ) -> hawdb_storage::append_table::AppendStorageResidencyReport {
        GraphStore::append_storage_residency_report(self)
    }

    fn columnar_shadow_admission_bytes(&self) -> u64 {
        GraphStore::columnar_shadow_admission_bytes(self)
    }

    fn relational_index_recovery_report(
        &self,
    ) -> Option<&hawdb_storage::relational::RelationalIndexRecoveryReport> {
        GraphStore::relational_index_recovery_report(self)
    }

    fn relational_index_shadow_checkpoint_report(
        &self,
    ) -> Option<&hawdb_storage::relational::RelationalIndexShadowCheckpointReport> {
        GraphStore::relational_index_shadow_checkpoint_report(self)
    }

    fn relational_index_shadow_recovery_status(
        &self,
    ) -> &hawdb_storage::relational::RelationalIndexShadowRecoveryStatus {
        GraphStore::relational_index_shadow_recovery_status(self)
    }

    fn search_projection_changefeed_status(
        &self,
    ) -> hawdb_storage::projection::SearchProjectionChangefeedStatus {
        GraphStore::search_projection_changefeed_status(self)
    }

    fn append_table_schema(
        &self,
        table: &str,
    ) -> Option<&hawdb_storage::append_table::AppendTableSchema> {
        GraphStore::append_table_schema(self, table)
    }

    fn initial_import_source_fingerprint(&self) -> Option<&str> {
        GraphStore::initial_import_source_fingerprint(self)
    }

    fn read_append_partition_bounded(
        &self,
        table: &str,
        partition: &hawdb_storage::relational::RelationalKey,
        after: Option<&hawdb_storage::relational::RelationalKey>,
        max_rows: usize,
        max_payload_bytes: usize,
    ) -> hawdb_core::Result<hawdb_storage::append_table::AppendSegmentReadOutput> {
        GraphStore::read_append_partition_bounded(
            self,
            table,
            partition,
            after,
            max_rows,
            max_payload_bytes,
        )
    }

    fn node_records_owned(&self) -> hawdb_storage::graph_overlay::GraphNodeIterator {
        GraphStore::node_records_owned(self)
    }

    fn relationship_records_owned(
        &self,
    ) -> hawdb_storage::graph_overlay::GraphRelationshipIterator {
        GraphStore::relationship_records_owned(self)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "store/snapshot_retention_tests.rs"]
mod snapshot_retention_tests;

#[cfg(test)]
mod tests {
    mod id_eq_mutation;
    mod id_in_mutation;
    mod pending_node_ids;

    use super::{
        canonical_adjacency_artifact_generation_file, canonical_manifest_generation_file,
        checksum_bytes, compute_statistics, encode_durable_text, estimated_properties_bytes,
        property_projection_artifact_generation_file, property_spill_artifact_generation_file,
        read_durable_text, restore_storage_backup, retain_supported_property_statistics,
        set_checkpoint_failpoint, set_generation_reclamation_remove_failpoint,
        set_wal_apply_failpoint, source_scan, AdjacencyConsolidationPlan, AdjacencyDirection,
        AdjacencyGroupStats, AdjacencyLayout, CheckpointPublishStage, ConnectedNodesCreate,
        CowSegmentedMap, DatabaseDoctor, DegreeStatisticsEntry, DegreeStatisticsKey,
        DurableCompression, DurableManifest, GraphScanControl, GraphStore, HawDBError, NodeId,
        NodeRecord, NodeSetAssignment, NodeSetValue, OrderedAdjacencyEntry,
        PersistentGraphIndexClass, ProjectedGraphDefinition, PropertyFilter, RelId, RelRecord,
        RelTypeId, RelationalIndexStorageResidencyReport, RelationalRowStorageResidencyReport,
        RelationshipDeleteRequest, ScanPruningStrategy, ScanPruningTargetKind,
        SearchProjectionGraphChange, SourceScanCandidateLimits, SourceScanCandidateRead,
        SourceScanCandidateVisit, SourceScanRow, StorageResidencyReport, WalDoctorOptions, WalOp,
        COW_MAP_TARGET_SEGMENT_BYTES, DENSE_ADJACENCY_DEGREE_THRESHOLD, DURABLE_COMPRESSION_HEADER,
        MANIFEST_FILE,
    };
    use crate::schema::{Catalog, GraphStatistics, LabelId, PropertyType, TableKind};
    use crate::value::Value;
    use hawdb_integrity::integrity_digest;
    use hawdb_storage::{
        config::{DurabilityPolicy, StorageResidencyMode, WalReplayConfig},
        mutation::{GraphMutation, MutationLimits, RelationshipPropertyUpdate},
        projection::ProjectedRelationshipPredicate,
        relational::{
            RelationalColumnSchema, RelationalHydrationBudget, RelationalInsertMode, RelationalKey,
            RelationalRow, RelationalScalarType, RelationalTableSchema, RelationalTransaction,
            RelationalValue, RelationalWrite,
        },
        scan::{ScanPredicate, ScanSegmentAccessPlan, ScanSegmentFallback, ScanSegmentManifest},
    };
    use std::any::TypeId;
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

    fn private_checkpoint_recovery_publication(
        mode: crate::config::RelationalIndexMode,
        durability: DurabilityPolicy,
        uncertain: bool,
    ) {
        use crate::relational::{
            RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE, RELATIONAL_ROW_DELTA_MANIFEST_FILE,
        };

        let path = unique_test_dir(&format!(
            "checkpoint_candidate_private_publish_{mode:?}_{durability:?}_{uncertain}"
        ));
        let replay = WalReplayConfig {
            relational_index_mode: mode,
            residency_mode: StorageResidencyMode::OutOfCore,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            durability,
            WalReplayConfig {
                // Authoritative opens require a pre-existing canonical index
                // binding. Bootstrap it through the supported shadow path.
                relational_index_mode: crate::config::RelationalIndexMode::Shadow,
                ..replay
            },
        )
        .unwrap();
        let row = |id: usize| {
            RelationalRow::new(vec![
                RelationalValue::Text(id.to_string()),
                RelationalValue::Text(format!("complete row {id}: 雪")),
            ])
        };
        let insert = |id| RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "documents".into(),
                mode: RelationalInsertMode::Error,
                rows: vec![row(id)],
            }],
        };
        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![
                        RelationalWrite::CreateTable(RelationalTableSchema {
                            materialized_view: None,
                            name: "documents".into(),
                            columns: ["id", "body"]
                                .into_iter()
                                .map(|name| RelationalColumnSchema {
                                    name: name.into(),
                                    scalar_type: RelationalScalarType::Text,
                                    nullable: false,
                                    default: None,
                                })
                                .collect(),
                            primary_key: vec!["id".into()],
                            unique_constraints: Vec::new(),
                            foreign_keys: Vec::new(),
                            indexes: Vec::new(),
                        }),
                        insert(0).writes.pop().unwrap(),
                    ],
                },
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        store
            .commit_relational_transaction(&mut catalog, insert(1))
            .unwrap();
        drop(store);
        // Ordinary writable recovery establishes exact source selectors before
        // any private future generation exists.
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            durability,
            replay,
        )
        .unwrap();
        let source = store.checkpoint_source();
        let mut candidate = source
            .prepare_checkpoint_candidate(&catalog)
            .unwrap()
            .unwrap();
        drop(source);
        store
            .commit_relational_transaction(&mut catalog, insert(2))
            .unwrap();
        store
            .create_node(&mut catalog, "Memory", properties([("id", Value::Int(7))]))
            .unwrap();
        let before_manifest = fs::read(path.join(MANIFEST_FILE)).unwrap();
        let before_wal = fs::read(active_wal_path(&path)).unwrap();
        let selectors = [
            RELATIONAL_ROW_DELTA_MANIFEST_FILE,
            RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE,
        ]
        .map(|name| (name, fs::read(path.join(name)).unwrap()));
        let captured = store.checkpoint_source();
        candidate.catch_up(&captured).unwrap();
        drop(captured);
        candidate.finish_catch_up().unwrap();
        assert_eq!(fs::read(path.join(MANIFEST_FILE)).unwrap(), before_manifest);
        assert_eq!(fs::read(active_wal_path(&path)).unwrap(), before_wal);
        for (name, expected) in selectors {
            assert_eq!(
                fs::read(path.join(name)).unwrap(),
                expected,
                "private {name}"
            );
        }
        let epoch = store.commit_epoch();
        // Locate this candidate, rather than allocate another generation.
        let generation = candidate.prepared_generation_for_test();
        let private_wal = path.join(super::wal_generation_file(generation));
        if uncertain {
            super::set_checkpoint_failpoint(Some(super::CheckpointPublishStage::ManifestPublished));
        }
        let published =
            store.publish_checkpoint_candidate_deferred_reclamation(&mut candidate, None);
        super::set_checkpoint_failpoint(None);
        if uncertain {
            assert!(matches!(published, Err(HawDBError::StorageIntegrity(_))));
            assert!(store.ensure_usable().is_err());
        } else {
            published.unwrap();
            assert_eq!(store.commit_epoch(), epoch);
            assert_private_checkpoint_recovery_rows(&store, epoch, &row);
        }
        drop(candidate);
        assert!(private_wal.exists());
        assert!(path
            .join(super::checkpoint_generation_file(generation))
            .exists());
        drop(store);
        // Read-only recovery must work immediately, without a writable open
        // publishing replacement derived selectors first.
        let read_only = GraphStore::open_read_only_with_durability_and_replay_config(
            &path,
            &mut catalog,
            durability,
            WalReplayConfig {
                recovery_mode: crate::config::RecoveryMode::Strict,
                ..replay
            },
        )
        .unwrap();
        assert_private_checkpoint_recovery_rows(&read_only, epoch, &row);
        drop(read_only);
        let recovered = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            durability,
            replay,
        )
        .unwrap();
        assert_private_checkpoint_recovery_rows(&recovered, epoch, &row);
        drop(recovered);
        fs::remove_dir_all(path).unwrap();
    }

    fn assert_private_checkpoint_recovery_rows(
        store: &GraphStore,
        epoch: u64,
        row: &impl Fn(usize) -> RelationalRow,
    ) {
        use crate::relational::{
            RelationalHydrationBudget, RelationalKey, RelationalRowPageSnapshotReadLimits,
        };
        assert_eq!(store.commit_epoch(), epoch);
        assert_eq!(store.node_count_for_label(None), 1);
        assert_eq!(
            store
                .node_owned(NodeId(0))
                .unwrap()
                .unwrap()
                .properties
                .get("id"),
            Some(&Value::Int(7))
        );
        store
            .validate_authoritative_relational_index_open()
            .unwrap();
        let reader = store
            .open_relational_row_snapshot_reader()
            .unwrap()
            .unwrap();
        let mut hydration = RelationalHydrationBudget::default();
        for id in 0..3 {
            let (actual, report) = reader
                .point_projected(
                    "documents",
                    &RelationalKey(vec![RelationalValue::Text(id.to_string())]),
                    &[0, 1],
                    RelationalRowPageSnapshotReadLimits::default(),
                    &mut hydration,
                    &hawdb_core::RuntimeTaskContext::default(),
                )
                .unwrap();
            let actual = actual
                .unwrap()
                .fields
                .into_iter()
                .map(|field| field.value)
                .collect::<Vec<_>>();
            assert_eq!(actual.as_slice(), row(id).values());
            assert_eq!(report.identity.visible_commit_epoch, epoch);
            let key = RelationalKey(vec![RelationalValue::Text(id.to_string())]);
            let mut postings = Vec::new();
            store
                .visit_relational_index_read_view_prefix(
                    "documents",
                    crate::relational::RELATIONAL_PRIMARY_INDEX_NAME,
                    &key,
                    crate::relational::RelationalIndexReadLimits::default(),
                    |posting| {
                        postings.push(posting.clone());
                        true
                    },
                )
                .unwrap()
                .unwrap();
            assert_eq!(postings, vec![key]);
        }
    }

    fn checkpoint_projection_fixture() -> (Catalog, GraphStore, Vec<(NodeId, NodeId, String)>) {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::default();
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        for id in 0..1025 {
            nodes.push(
                store
                    .create_node(
                        &mut catalog,
                        if id % 2 == 0 { "Memory" } else { "Source" },
                        properties([("id", Value::Int(id))]),
                    )
                    .unwrap(),
            );
        }
        for index in 0..nodes.len() {
            let mut add = |source, target, rel_type: &str| {
                store
                    .create_relationship(&mut catalog, source, target, rel_type, BTreeMap::new())
                    .unwrap();
                edges.push((source, target, rel_type.to_string()));
            };
            add(nodes[0], nodes[index], "LINKS");
            if index % 3 == 0 {
                add(nodes[0], nodes[index], "LINKS");
            }
            if index % 5 == 0 {
                add(nodes[index], nodes[0], "BACK");
            }
            if index % 7 == 0 {
                add(nodes[0], nodes[index], "OTHER");
            }
        }
        (catalog, store, edges)
    }

    #[derive(Debug)]
    struct CancelCheckpointAtFile {
        probe: std::sync::Arc<crate::background::CheckpointWorkProbe>,
        path: std::path::PathBuf,
        observed_file: std::sync::atomic::AtomicBool,
        completed_after_file: std::sync::atomic::AtomicUsize,
        cancel_after_file: usize,
        file_bytes_at_cancellation: std::sync::atomic::AtomicU64,
    }

    /// Cancels once `cancel_after_file` units have ended after the private
    /// file appeared, independently of local admission batches (#1616).
    impl hawdb_qos::QosTelemetrySink for CancelCheckpointAtFile {
        fn record_qos(&self, event: hawdb_qos::QosTelemetryEvent) {
            self.probe.record_qos(event);
        }

        fn observes_units(&self) -> bool {
            true
        }

        fn record_qos_unit(&self, event: hawdb_qos::QosUnitEvent) {
            use std::sync::atomic::Ordering;
            let completed = event.phase != hawdb_qos::QosUnitPhase::Started;
            self.probe.record_qos_unit(event);
            if completed && self.path.exists() {
                self.observed_file.store(true, Ordering::SeqCst);
                let completed = self.completed_after_file.fetch_add(1, Ordering::SeqCst) + 1;
                if completed >= self.cancel_after_file {
                    self.file_bytes_at_cancellation.store(
                        std::fs::metadata(&self.path).unwrap().len(),
                        Ordering::SeqCst,
                    );
                    self.probe.cancellation.cancel();
                }
            }
        }
    }

    fn assert_cancelled_derived_artifact_unit_preserves_authority(artifact: &str) {
        use crate::background::{CheckpointWorkContext, CheckpointWorkProbe};
        use hawdb_core::RuntimeTaskContext;
        use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
        use std::sync::atomic::Ordering;
        use std::sync::Arc;

        let path = unique_test_dir(&format!("checkpoint_{artifact}_unit_cancel"));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let label = if artifact == "source-scan" {
            "Source"
        } else {
            "Memory"
        };
        let nodes = (0..64i64)
            .map(|id| {
                store
                    .create_node(&mut catalog, label, properties([("id", Value::Int(id))]))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut relationships = nodes[1..]
            .iter()
            .enumerate()
            .map(|(id, target)| {
                store
                    .create_relationship(
                        &mut catalog,
                        nodes[0],
                        *target,
                        "LINKS",
                        properties([("id", Value::Int(id as i64))]),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        if artifact == "projected-graph" {
            store
                .register_projected_graph(
                    "CheckpointGraph",
                    crate::projection::ProjectedGraphDefinition {
                        node_labels: Vec::new(),
                        rel_types: Vec::new(),
                        relationship_predicates: Default::default(),
                    },
                )
                .unwrap();
        }
        store.checkpoint(&catalog).unwrap();
        relationships.push(
            store
                .create_relationship(&mut catalog, nodes[1], nodes[0], "LINKS", BTreeMap::new())
                .unwrap(),
        );
        let identity = store.checkpoint_source_identity();
        let durable = store.durable.as_ref().unwrap();
        let generation = durable.next_checkpoint_generation().unwrap();
        let wal_path = durable.wal_path.clone();
        let manifest_path = durable.manifest_path().to_path_buf();
        let wal = std::fs::read(&wal_path).unwrap();
        let manifest = std::fs::read(&manifest_path).unwrap();
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        });
        let probe = Arc::new(CheckpointWorkProbe::default());
        let temporary = if artifact == "projected-graph" {
            path.join(format!(".checkpoint.{generation}.prepare"))
                .join(super::PROJECTED_GRAPHS_FILE)
                .with_extension("hawdb.tmp")
        } else if artifact == "source-scan" {
            path.join(format!(".checkpoint.{generation}.prepare"))
                .join(source_scan::SOURCE_SCAN_PAYLOAD_FILE)
                .with_extension("hawdb.tmp")
        } else {
            path.join(format!(".{artifact}.{generation}.run.0.tmp"))
        };
        let observer = Arc::new(CancelCheckpointAtFile {
            probe: probe.clone(),
            path: temporary,
            observed_file: Default::default(),
            completed_after_file: Default::default(),
            cancel_after_file: 1,
            file_bytes_at_cancellation: Default::default(),
        });
        scheduler.set_telemetry_sink(Some(observer.clone()));
        let task = RuntimeTaskContext::without_deadline(probe.cancellation.clone())
            .with_io_wave_controller(probe.clone());
        let work = CheckpointWorkContext::new(task).with_scheduler(scheduler.clone());
        let source = store.checkpoint_source();
        let error = source
            .prepare_checkpoint_candidate_with_work_context(&catalog, &work)
            .unwrap_err();
        assert!(observer.observed_file.load(Ordering::SeqCst));
        assert!(error.to_string().contains("runtime task stopped"));
        probe.assert_released(&scheduler);
        assert_eq!(store.checkpoint_source_identity(), identity);
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
        assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
        assert!(!observer.path.exists());
        assert!(!path
            .join(format!(".checkpoint.{generation}.prepare"))
            .exists());
        source.ensure_usable().unwrap();
        store.ensure_usable().unwrap();
        drop(source);

        relationships.push(
            store
                .create_relationship(&mut catalog, nodes[2], nodes[0], "LINKS", BTreeMap::new())
                .unwrap(),
        );
        let retry = Arc::new(CheckpointWorkProbe::default());
        let work = retry.context(scheduler.clone());
        let source = store.checkpoint_source();
        let mut candidate = source
            .prepare_checkpoint_candidate_with_work_context(&catalog, &work)
            .unwrap()
            .unwrap();
        drop(source);
        let captured = store.checkpoint_source();
        candidate.catch_up(&captured).unwrap();
        drop(captured);
        store
            .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
            .unwrap();
        drop(candidate);
        retry.assert_released(&scheduler);
        let expected = relationships
            .into_iter()
            .map(|id| store.relationship_owned(id).unwrap().unwrap())
            .collect::<Vec<_>>();
        let expected_nodes = nodes
            .into_iter()
            .map(|id| store.node_owned(id).unwrap().unwrap())
            .collect::<Vec<_>>();
        let epoch = store.commit_epoch();
        drop(store);
        let recovered = GraphStore::open(&path, &mut catalog).unwrap();
        assert_eq!(recovered.commit_epoch(), epoch);
        assert_eq!(recovered.scan_nodes(None).count(), expected_nodes.len());
        let expected_node_ids = expected_nodes
            .iter()
            .map(|node| node.id)
            .collect::<Vec<_>>();
        for node in expected_nodes {
            assert_eq!(recovered.node_owned(node.id).unwrap(), Some(node));
        }
        assert_eq!(recovered.scan_relationships(None).count(), expected.len());
        for relationship in expected {
            assert_eq!(
                recovered.relationship_owned(relationship.id).unwrap(),
                Some(relationship)
            );
        }
        if artifact == "source-scan" {
            let crate::scan::ScanSegmentAccessPlan::Read(plan) = recovered
                .plan_published_source_scan(&ScanPredicate::Eq {
                    property: "id".into(),
                    value: Value::Int(31),
                })
            else {
                panic!("successful retry must reopen a current Source sidecar");
            };
            assert_eq!(plan.segments.len(), 1);
            assert_eq!(plan.segments[0].candidates.as_ref().unwrap().remaining(), 1);
        }
        if artifact == "projected-graph" {
            let graph = recovered
                .projected_graph_artifact(
                    "CheckpointGraph",
                    &crate::projection::ProjectedGraphDefinition {
                        node_labels: Vec::new(),
                        rel_types: Vec::new(),
                        relationship_predicates: Default::default(),
                    },
                )
                .expect("successful retry must reopen current projected arrays");
            assert_eq!(graph.nodes(), expected_node_ids);
            assert_eq!(
                graph.csr_offsets(),
                std::iter::once(0)
                    .chain([63, 64])
                    .chain(std::iter::repeat_n(65, 62))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                graph.csr_targets(),
                (1..64).chain([0, 0]).collect::<Vec<_>>()
            );
            assert_eq!(
                graph.csc_offsets(),
                std::iter::once(0).chain(2..=65).collect::<Vec<_>>()
            );
            assert_eq!(
                graph.csc_sources(),
                [1, 2]
                    .into_iter()
                    .chain(std::iter::repeat_n(0, 63))
                    .collect::<Vec<_>>()
            );
        }
        drop(recovered);
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
        std::env::temp_dir().join(format!("hawdb_store_{name}_{nanos}"))
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
        let manifest = std::fs::read_to_string(root.join("manifest.hawdb")).unwrap();
        assert!(manifest.contains("HAWDB_MANIFEST_V1\n"));
        let generation = manifest.lines().find_map(|line| {
            let (field, value) = line.split_once('\t')?;
            (field == manifest_field && value != "none").then_some(value)
        });
        root.join(format!(
            "{prefix}.{}.hawdb",
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
        let manifest_path = root.join("manifest.hawdb");
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

    include!("store/tests/graph_store_cases_1.rs");
    include!("store/tests/graph_store_cases_2.rs");
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "store/graph_checkpoint_projection_memory_tests.rs"]
mod checkpoint_projection_memory_tests;

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "store/graph_checkpoint_projection_memory_related_tests.rs"]
mod checkpoint_projection_memory_related_tests;
