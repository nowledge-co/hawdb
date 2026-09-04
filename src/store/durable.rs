//! The durable store: checkpoint publication, WAL management, the durable
//! manifest, and loading of published derived artifacts.

use super::{
    canonical_adjacency_artifact_generation_file, canonical_artifact_generation_file,
    canonical_manifest_generation_file, checkpoint_generation_file, checkpoint_publish_failpoint,
    checksum_bytes, cleanup_abandoned_checkpoint_preparations, copy_backup_file,
    decode_projected_graph_artifacts, derived_repair, doctor, elapsed_micros,
    encode_binary_wal_header, encode_binary_wal_record, encode_bool, encode_durable_text,
    encode_index_kind, encode_nullable, encode_optional_sha256, encode_optional_u64,
    encode_property_type, encode_schema_object_state,
    encode_search_projection_relational_primary_key_changes, encode_string, encode_string_vec,
    encode_table_kind, encode_u64_vec, encode_value_vec, file_checksum, frame_binary_wal_record,
    has_storage_artifacts, parse_append_segment_generation_file, parse_optional_sha256,
    parse_optional_u64, parse_relational_overflow_extent_generation_file,
    parse_relational_row_page_artifact_generation_file, parse_u64, process_crash_failpoint,
    property_projection_artifact_generation_file, property_projection_manifest_generation_file,
    property_spill_artifact_generation_file, property_spill_manifest_generation_file,
    read_durable_text, read_durable_text_bytes_with_limit, relational_checkpoint_generation_file,
    remove_generation_reclamation_candidate, remove_source_scan_artifacts,
    safe_reclaim_commit_epoch, source_scan, split_manifest_checksum,
    split_projected_graph_artifact_checksum, storage_generation_for_file, store_id_for_path,
    sync_parent_dir, validate_backup_files, validate_new_backup_destination,
    validate_search_projection_checkpoint_changes, validate_storage_version, verify_integrity,
    wal_generation_file, wal_group_sync_failpoint, BackupManifest, CheckpointPublishStage,
    ProjectedGraphArtifact, WalCursorEvent, WalEntry, WalOp, WalOpenOutcome, WalRecordCursor,
    BACKUP_MANIFEST_FILE, CANONICAL_MANIFEST_MAX_BYTES, CHECKPOINT_HEADER_V1,
    CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER, MANIFEST_FILE, MANIFEST_HEADER_V1,
    MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES, PROJECTED_GRAPHS_FILE,
    PROPERTY_PROJECTION_MANIFEST_MAX_BYTES, PROPERTY_SPILL_MANIFEST_MAX_BYTES,
    STABLE_ID_MAPPING_FILE, STORAGE_VERSION, WAL_BINARY_FILE_HEADER_BYTES,
};
use crate::error::{Result, SkeinError};
use crate::schema::{Catalog, GraphStatistics};
use skein_integrity::{integrity_digest, Sha256Digest};
use skein_storage::{
    append_generation_manifest_file, append_segment_file, available_storage_space,
    decode_relational_checkpoint_file, durable_replace_file,
    encode_relational_checkpoint_to_writer, AppendGenerationArtifacts, AppendGenerationManifest,
    AppendGenerationReader, AppendPublicationConfig, AppendSegmentArtifactMetadata,
    CanonicalAdjacencyArtifactMetadata, CanonicalAdjacencyConfig,
    CanonicalAdjacencyGenerationArtifacts, CanonicalAdjacencyReader, CanonicalAdjacencyWriter,
    CanonicalSegmentConfig, CanonicalSegmentError, CanonicalSegmentManifest,
    CanonicalSegmentReader, CanonicalSegmentWriter, DatabaseDirectoryLease, DurabilityPolicy,
    DurableCompression, FileSegmentRangeReader, GraphDescriptorKind,
    GraphDescriptorTreeArtifactMetadata, GraphDescriptorTreeBuildConfig,
    GraphDescriptorTreeGenerationArtifacts, GraphDescriptorTreePaths,
    GraphDescriptorTreeRootReader, ManifestGeneration, NodeRecord,
    PersistentPropertyProjectionConfig, PersistentPropertyProjectionDefinition,
    PersistentPropertyProjectionDescriptorTree, PersistentPropertyProjectionManifest,
    PersistentPropertyProjectionReader, PersistentPropertyProjectionRecord,
    PersistentPropertyProjectionWriter, PersistentPropertySpillDescriptorTree,
    ProjectedGraphDefinition, PropertySpillConfig, PropertySpillManifest, PropertySpillReader,
    PropertySpillWriteOptions, RelRecord, RelationalDecodeLimits, RelationalIndexArtifactMetadata,
    RelationalIndexGenerationArtifacts, RelationalOverflowArtifactMetadata,
    RelationalOverflowGenerationArtifacts, RelationalRowPageArtifactMetadata,
    RelationalRowPageGenerationArtifacts, RelationalState, ScanSegmentManifest,
    SearchProjectionGraphChange, SegmentCache, StableIdentityKey, StableIdentityMappingConfig,
    StableIdentityMappingError, StableIdentityMappingReader, StableIdentityMappingWriter,
    StableIdentityMaterializeLimits, StorageBackupReport, StorageDebtController,
    StoragePressureSignals, StorageScrubReport, StorageTelemetrySink, StoreId,
    StoreStableIdMapping, WalAppendTelemetry, WalReplayConfig, WalSyncGroupFlush,
    WalSyncGroupProgress, WalSyncGroupState,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const WAL_FREE_SPACE_PROBE_INTERVAL_BYTES: u64 = 64 * 1024 * 1024;

fn stable_identity_error(error: StableIdentityMappingError) -> SkeinError {
    SkeinError::Storage(error.to_string())
}

#[derive(Debug, Clone)]
pub(super) struct DurableStore {
    _directory_lease: Arc<DatabaseDirectoryLease>,
    pub(super) root_path: PathBuf,
    pub(super) checkpoint_path: PathBuf,
    manifest_path: PathBuf,
    projected_graphs_path: PathBuf,
    stable_id_mapping_path: PathBuf,
    pub(super) wal_path: PathBuf,
    pub(super) wal_append_file: Option<Arc<File>>,
    #[cfg(test)]
    pub(super) wal_append_open_count: usize,
    pub(super) checkpoint_encoded_len: Option<u64>,
    checkpoint_encoded_checksum: Option<u64>,
    checkpoint_encoded_sha256: Option<Sha256Digest>,
    pub(super) relational_checkpoint_encoded_len: Option<u64>,
    pub(super) relational_checkpoint_encoded_checksum: Option<u64>,
    pub(super) relational_checkpoint_encoded_sha256: Option<Sha256Digest>,
    canonical_manifest_encoded_len: Option<u64>,
    canonical_manifest_encoded_checksum: Option<u64>,
    canonical_manifest_encoded_sha256: Option<Sha256Digest>,
    canonical_adjacency_generation_artifacts: Option<CanonicalAdjacencyGenerationArtifacts>,
    property_spill_manifest_encoded_len: Option<u64>,
    property_spill_manifest_encoded_checksum: Option<u64>,
    property_spill_manifest_encoded_sha256: Option<Sha256Digest>,
    property_projection_manifest_encoded_len: Option<u64>,
    property_projection_manifest_encoded_checksum: Option<u64>,
    property_projection_manifest_encoded_sha256: Option<Sha256Digest>,
    pub(super) relational_row_generation_artifacts: Option<RelationalRowPageGenerationArtifacts>,
    pub(super) relational_overflow_generation_artifacts:
        Option<RelationalOverflowGenerationArtifacts>,
    pub(super) relational_index_generation_artifacts: Option<RelationalIndexGenerationArtifacts>,
    pub(super) append_generation_artifacts: Option<AppendGenerationArtifacts>,
    pub(super) wal_generation: u64,
    pub(super) checkpoint_epoch: u64,
    pub(super) checkpoint_commit_epoch: u64,
    pub(super) oldest_reader_commit_epoch: Option<u64>,
    pub(super) safe_reclaim_commit_epoch: u64,
    pub(super) wal_replay_start_lsn: u64,
    pub(super) next_lsn: u64,
    pub(super) wal_bytes: u64,
    pub(super) wal_tail_repair: Option<skein_storage::WalTailRepairReport>,
    /// Commit epoch recorded in binary WAL records (spec §3.4.3). Advisory:
    /// replay derives commit epochs from LSN order, exactly as before.
    pub(super) wal_commit_epoch: u64,
    pub(super) max_wal_bytes: Option<u64>,
    source_scan_commit_epoch: Option<u64>,
    source_scan_descriptor_checksum: Option<u64>,
    store_id: StoreId,
    pub(super) segment_cache: Arc<SegmentCache>,
    max_graph_manifest_open_bytes: u64,
    pub(super) canonical_segments: Option<CanonicalSegmentReader>,
    pub(super) canonical_adjacency: Option<CanonicalAdjacencyReader>,
    pub(super) persistent_property_projection: Option<PersistentPropertyProjectionReader>,
    stable_id_mapping_reader: Option<Arc<StableIdentityMappingReader>>,
    pub(super) source_scan_reader: FileSegmentRangeReader,
    durability: DurabilityPolicy,
    pub(super) read_only: bool,
    max_record_bytes: Option<usize>,
    max_batch_operations: Option<usize>,
    pub(super) telemetry: Option<Arc<dyn StorageTelemetrySink>>,
    wal_sync_group: Option<WalSyncGroupState>,
    generation_reclamation_debt: GenerationReclamationDebt,
    wal_free_space_probe: WalFreeSpaceProbeState,
}

#[derive(Debug, Clone, Default)]
struct WalFreeSpaceProbeState {
    last_available_bytes: Option<u64>,
    wal_bytes_since_probe: u64,
    #[cfg(test)]
    available_bytes_override: Option<u64>,
    #[cfg(test)]
    probe_count: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct GenerationReclamationDebt {
    pub(super) retry_required: bool,
    pub(super) pending_file_count: usize,
    pub(super) pending_bytes: u64,
}

struct StorageScrubCounters {
    checked_file_count: usize,
    checked_bytes: u64,
    sha256_verified_file_count: usize,
}

impl StorageScrubCounters {
    fn new(manifest_bytes: u64) -> Self {
        Self {
            checked_file_count: 1,
            checked_bytes: manifest_bytes,
            sha256_verified_file_count: 0,
        }
    }

    fn verify_path(
        &mut self,
        path: &Path,
        expected_len: u64,
        expected_checksum: u64,
        expected_sha256: Sha256Digest,
        artifact: &str,
    ) -> Result<()> {
        let (actual_len, actual_checksum, actual_sha256) = file_checksum(path)?;
        if actual_len != expected_len {
            return Err(SkeinError::Storage(format!(
                "{artifact} length mismatch during scrub: expected {expected_len}, got {actual_len}"
            )));
        }
        if actual_checksum != expected_checksum {
            return Err(SkeinError::Storage(format!(
                "{artifact} CRC32C mismatch during scrub: expected {expected_checksum}, got {actual_checksum}"
            )));
        }
        if actual_sha256 != expected_sha256 {
            return Err(SkeinError::Storage(format!(
                "{artifact} SHA-256 mismatch during scrub: expected {expected_sha256}, got {actual_sha256}"
            )));
        }
        self.checked_file_count = self.checked_file_count.saturating_add(1);
        self.checked_bytes = self.checked_bytes.saturating_add(actual_len);
        self.sha256_verified_file_count = self.sha256_verified_file_count.saturating_add(1);
        Ok(())
    }
}

pub(super) struct CheckpointImage<'a> {
    pub(super) catalog: &'a Catalog,
    pub(super) commit_epoch: u64,
    pub(super) next_node_id: u64,
    pub(super) next_rel_id: u64,
    pub(super) search_projection_change_log_start_epoch: u64,
    pub(super) search_projection_graph_changes: &'a [SearchProjectionGraphChange],
    pub(super) statistics: &'a GraphStatistics,
    pub(super) projected_graphs: &'a BTreeMap<String, ProjectedGraphDefinition>,
    pub(super) initial_import_source_fingerprint: Option<&'a str>,
    pub(super) relational_checkpoint: Option<DurableArtifactMetadata>,
}

#[derive(Debug)]
pub(crate) struct PreparedCheckpoint {
    pub(super) source_commit_epoch: u64,
    pub(super) source_checkpoint_epoch: u64,
    pub(super) source_next_lsn: u64,
    pub(super) generation: u64,
    pub(super) checkpoint_out_of_core: bool,
    pub(super) projected_graph_artifacts: BTreeMap<String, ProjectedGraphArtifact>,
    pub(super) publish_projected_graph_artifacts: bool,
    pub(super) source_scan_publication: Option<source_scan::SourceScanPublication>,
    pub(super) checkpoint_statistics: GraphStatistics,
    pub(super) checkpoint_relational_state: Option<RelationalState>,
    pub(super) checkpoint_append_reader: AppendGenerationReader,
    pub(super) relational_index_candidate:
        Option<super::relational_index_shadow::PreparedRelationalIndexCandidate>,
    pub(super) relational_overflow_compaction_report:
        Option<super::RelationalOverflowCompactionReport>,
    pub(super) manifest_artifacts: CheckpointManifestArtifacts,
    pub(super) staging_path: PathBuf,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DerivedArtifactBuildConfig {
    pub(super) adjacency: CanonicalAdjacencyConfig,
    pub(super) property_projection: PersistentPropertyProjectionConfig,
}

#[derive(Debug, Clone, Copy)]
struct DurableStoreOpenOptions {
    read_only: bool,
    initialize_if_empty: bool,
    load_rebuildable_artifacts: bool,
    segment_cache_capacity_bytes: u64,
    max_graph_manifest_open_bytes: u64,
    max_wal_bytes: Option<u64>,
    max_record_bytes: Option<usize>,
    max_batch_operations: Option<usize>,
    automatic_tail_repair: Option<WalReplayConfig>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct GraphManifestOpenBudget {
    max_encoded_bytes: u64,
    admitted_encoded_bytes: u64,
}

impl GraphManifestOpenBudget {
    pub(super) const fn new(max_encoded_bytes: u64) -> Self {
        Self {
            max_encoded_bytes,
            admitted_encoded_bytes: 0,
        }
    }

    fn admit(&mut self, encoded_bytes: u64, artifact: &str) -> Result<()> {
        let required = self
            .admitted_encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| {
                SkeinError::Storage("aggregate graph manifest open bytes overflow u64".to_string())
            })?;
        if required > self.max_encoded_bytes {
            return Err(SkeinError::Storage(format!(
                "{artifact} requires {required} aggregate encoded graph manifest bytes during open, exceeding configured limit {}",
                self.max_encoded_bytes
            )));
        }
        self.admitted_encoded_bytes = required;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct DurableArtifactMetadata {
    pub(super) encoded_len: u64,
    pub(super) encoded_checksum: u64,
    pub(super) encoded_sha256: Sha256Digest,
}

impl DurableArtifactMetadata {
    fn for_bytes(bytes: &[u8]) -> Self {
        let digest = integrity_digest(bytes);
        Self {
            encoded_len: bytes.len() as u64,
            encoded_checksum: digest.crc32c.as_u64(),
            encoded_sha256: digest.sha256,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CanonicalAdjacencyCheckpointArtifacts {
    pub(super) generation: CanonicalAdjacencyGenerationArtifacts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CheckpointManifestArtifacts {
    pub(super) checkpoint: DurableArtifactMetadata,
    pub(super) relational_checkpoint: Option<DurableArtifactMetadata>,
    pub(super) canonical_manifest: DurableArtifactMetadata,
    pub(super) canonical_adjacency: CanonicalAdjacencyCheckpointArtifacts,
    pub(super) property_spill_manifest: DurableArtifactMetadata,
    pub(super) property_projection_manifest: DurableArtifactMetadata,
    pub(super) relational_row: RelationalRowPageGenerationArtifacts,
    pub(super) relational_overflow: RelationalOverflowGenerationArtifacts,
    pub(super) relational_index: Option<RelationalIndexGenerationArtifacts>,
    pub(super) append: AppendGenerationArtifacts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DurableOpenMode {
    CreateIfMissing,
    ExistingOnly,
}

impl DurableStore {
    pub(super) fn open(
        path: &Path,
        durability: DurabilityPolicy,
        replay_config: WalReplayConfig,
    ) -> Result<Self> {
        if replay_config.max_graph_manifest_open_bytes == 0 {
            return Err(SkeinError::Storage(
                "max_graph_manifest_open_bytes must be non-zero".to_string(),
            ));
        }
        fs::create_dir_all(path)?;
        Self::open_existing(
            path,
            durability,
            DurableStoreOpenOptions {
                read_only: false,
                initialize_if_empty: true,
                load_rebuildable_artifacts: true,
                segment_cache_capacity_bytes: replay_config.segment_cache_capacity_bytes,
                max_graph_manifest_open_bytes: replay_config.max_graph_manifest_open_bytes,
                max_wal_bytes: replay_config.max_bytes,
                max_record_bytes: replay_config.max_record_bytes,
                max_batch_operations: replay_config.max_batch_operations,
                automatic_tail_repair: (replay_config.recovery_mode
                    == skein_storage::RecoveryMode::AutoRepairTornTail)
                    .then_some(replay_config),
            },
        )
    }

    pub(super) fn open_existing_only(
        path: &Path,
        durability: DurabilityPolicy,
        segment_cache_capacity_bytes: u64,
        max_graph_manifest_open_bytes: u64,
        max_wal_bytes: Option<u64>,
        max_record_bytes: Option<usize>,
        max_batch_operations: Option<usize>,
    ) -> Result<Self> {
        if !path.exists() {
            return Err(SkeinError::Storage(format!(
                "read-only database path does not exist: {}",
                path.display()
            )));
        }
        if !path.is_dir() {
            return Err(SkeinError::Storage(format!(
                "read-only database path is not a directory: {}",
                path.display()
            )));
        }
        Self::open_existing(
            path,
            durability,
            DurableStoreOpenOptions {
                read_only: true,
                initialize_if_empty: false,
                load_rebuildable_artifacts: true,
                automatic_tail_repair: None,
                segment_cache_capacity_bytes,
                max_graph_manifest_open_bytes,
                max_wal_bytes,
                max_record_bytes,
                max_batch_operations,
            },
        )
    }

    pub(super) fn open_for_derived_repair(
        path: &Path,
        durability: DurabilityPolicy,
        segment_cache_capacity_bytes: u64,
        max_graph_manifest_open_bytes: u64,
        max_wal_bytes: Option<u64>,
        max_record_bytes: Option<usize>,
        max_batch_operations: Option<usize>,
    ) -> Result<Self> {
        if !path.is_dir() {
            return Err(SkeinError::Storage(format!(
                "derived repair database path is not a directory: {}",
                path.display()
            )));
        }
        Self::open_existing(
            path,
            durability,
            DurableStoreOpenOptions {
                read_only: true,
                initialize_if_empty: false,
                load_rebuildable_artifacts: false,
                automatic_tail_repair: None,
                segment_cache_capacity_bytes,
                max_graph_manifest_open_bytes,
                max_wal_bytes,
                max_record_bytes,
                max_batch_operations,
            },
        )
    }

    fn open_existing(
        path: &Path,
        durability: DurabilityPolicy,
        options: DurableStoreOpenOptions,
    ) -> Result<Self> {
        let DurableStoreOpenOptions {
            read_only,
            initialize_if_empty,
            load_rebuildable_artifacts,
            segment_cache_capacity_bytes,
            max_graph_manifest_open_bytes,
            max_wal_bytes,
            max_record_bytes,
            max_batch_operations,
            automatic_tail_repair,
        } = options;
        if max_graph_manifest_open_bytes == 0 {
            return Err(SkeinError::Storage(
                "max_graph_manifest_open_bytes must be non-zero".to_string(),
            ));
        }
        let directory_lease = DatabaseDirectoryLease::acquire(path)
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        if load_rebuildable_artifacts {
            derived_repair::reject_pending_derived_artifact_repair(path)?;
        }
        let wal_tail_repair = match automatic_tail_repair {
            Some(config) => doctor::resume_automatic_wal_tail_repair_locked(path, config)?,
            None => {
                doctor::reject_pending_wal_doctor_repair(path)?;
                None
            }
        };
        let manifest_path = path.join(MANIFEST_FILE);
        let manifest = if manifest_path.exists() {
            DurableManifest::load(&manifest_path)?
        } else if has_storage_artifacts(path)? {
            return Err(SkeinError::Storage(
                "database has storage artifacts but no durable manifest".to_string(),
            ));
        } else if initialize_if_empty {
            let manifest = DurableManifest::initial_generation();
            manifest.write(&manifest_path)?;
            manifest
        } else {
            return Err(SkeinError::Storage(
                "read-only database directory has no durable manifest".to_string(),
            ));
        };
        manifest.validate()?;
        if !read_only {
            cleanup_abandoned_checkpoint_preparations(path, manifest.checkpoint_epoch)?;
        }
        let checkpoint_path = manifest.checkpoint_path(path);
        let wal_path = manifest.wal_path(path);
        let wal_bytes = fs::metadata(&wal_path)
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let segment_cache = Arc::new(SegmentCache::new(segment_cache_capacity_bytes));
        let store_id = store_id_for_path(path)?;
        let mut graph_manifest_budget = GraphManifestOpenBudget::new(max_graph_manifest_open_bytes);
        let canonical_segments = load_published_canonical_segments(
            path,
            manifest,
            Arc::clone(&segment_cache),
            store_id,
            &mut graph_manifest_budget,
        )?;
        let canonical_adjacency = load_rebuildable_artifacts
            .then(|| {
                load_published_canonical_adjacency(
                    path,
                    manifest,
                    Arc::clone(&segment_cache),
                    store_id,
                    &mut graph_manifest_budget,
                )
            })
            .transpose()?
            .flatten();
        let persistent_property_projection = load_rebuildable_artifacts
            .then(|| {
                load_published_property_projection(
                    path,
                    manifest,
                    Arc::clone(&segment_cache),
                    store_id,
                    &mut graph_manifest_budget,
                )
            })
            .transpose()?
            .flatten();
        if let (Some(canonical), Some(adjacency)) = (&canonical_segments, &canonical_adjacency)
            && canonical.manifest().relationship_count != adjacency.relationship_count()
        {
            return Err(SkeinError::Storage(
                "canonical adjacency relationship count does not match canonical segments"
                    .to_string(),
            ));
        }
        let mut source_scan_reader = FileSegmentRangeReader::new().with_cache(
            Arc::clone(&segment_cache),
            store_id,
            ManifestGeneration(manifest.checkpoint_epoch),
        );
        source_scan_reader.register(
            source_scan::SOURCE_SCAN_ARTIFACT_ID,
            path.join(source_scan::SOURCE_SCAN_PAYLOAD_FILE),
        );
        Ok(Self {
            _directory_lease: Arc::new(directory_lease),
            root_path: path.to_path_buf(),
            checkpoint_path,
            manifest_path,
            projected_graphs_path: path.join(PROJECTED_GRAPHS_FILE),
            stable_id_mapping_path: path.join(STABLE_ID_MAPPING_FILE),
            wal_path,
            wal_append_file: None,
            #[cfg(test)]
            wal_append_open_count: 0,
            checkpoint_encoded_len: manifest.checkpoint_encoded_len,
            checkpoint_encoded_checksum: manifest.checkpoint_encoded_checksum,
            checkpoint_encoded_sha256: manifest.checkpoint_encoded_sha256,
            relational_checkpoint_encoded_len: None,
            relational_checkpoint_encoded_checksum: None,
            relational_checkpoint_encoded_sha256: None,
            canonical_manifest_encoded_len: manifest.canonical_manifest_encoded_len,
            canonical_manifest_encoded_checksum: manifest.canonical_manifest_encoded_checksum,
            canonical_manifest_encoded_sha256: manifest.canonical_manifest_encoded_sha256,
            canonical_adjacency_generation_artifacts: manifest
                .canonical_adjacency_generation_artifacts,
            property_spill_manifest_encoded_len: manifest.property_spill_manifest_encoded_len,
            property_spill_manifest_encoded_checksum: manifest
                .property_spill_manifest_encoded_checksum,
            property_spill_manifest_encoded_sha256: manifest.property_spill_manifest_encoded_sha256,
            property_projection_manifest_encoded_len: manifest
                .property_projection_manifest_encoded_len,
            property_projection_manifest_encoded_checksum: manifest
                .property_projection_manifest_encoded_checksum,
            property_projection_manifest_encoded_sha256: manifest
                .property_projection_manifest_encoded_sha256,
            relational_row_generation_artifacts: manifest.relational_row_generation_artifacts,
            relational_overflow_generation_artifacts: manifest
                .relational_overflow_generation_artifacts,
            relational_index_generation_artifacts: manifest.relational_index_generation_artifacts,
            append_generation_artifacts: manifest.append_generation_artifacts,
            wal_generation: manifest.wal_generation,
            checkpoint_epoch: manifest.checkpoint_epoch,
            checkpoint_commit_epoch: manifest.checkpoint_commit_epoch,
            oldest_reader_commit_epoch: manifest.oldest_reader_commit_epoch,
            safe_reclaim_commit_epoch: manifest.safe_reclaim_commit_epoch,
            wal_replay_start_lsn: manifest.wal_replay_start_lsn,
            next_lsn: manifest.next_lsn,
            wal_bytes,
            wal_tail_repair,
            wal_commit_epoch: manifest.checkpoint_commit_epoch,
            max_wal_bytes,
            source_scan_commit_epoch: manifest.source_scan_commit_epoch,
            source_scan_descriptor_checksum: manifest.source_scan_descriptor_checksum,
            store_id,
            segment_cache,
            max_graph_manifest_open_bytes,
            canonical_segments,
            canonical_adjacency,
            persistent_property_projection,
            stable_id_mapping_reader: None,
            source_scan_reader,
            durability,
            read_only,
            max_record_bytes,
            max_batch_operations,
            telemetry: None,
            wal_sync_group: None,
            generation_reclamation_debt: GenerationReclamationDebt::default(),
            wal_free_space_probe: WalFreeSpaceProbeState::default(),
        })
    }

    pub(super) fn root_path(&self) -> &Path {
        &self.root_path
    }

    pub(super) const fn generation_reclamation_debt(&self) -> GenerationReclamationDebt {
        self.generation_reclamation_debt
    }

    pub(super) fn store_id(&self) -> StoreId {
        self.store_id
    }

    pub(super) const fn graph_manifest_open_budget_bytes(&self) -> u64 {
        self.max_graph_manifest_open_bytes
    }

    #[cfg(test)]
    pub(super) fn set_graph_manifest_open_budget_bytes(&mut self, max_bytes: u64) {
        self.max_graph_manifest_open_bytes = max_bytes;
    }

    pub(super) fn graph_manifest_encoded_bytes(&self) -> u64 {
        [
            self.canonical_manifest_encoded_len,
            self.canonical_adjacency_generation_artifacts
                .map(|binding| binding.descriptor_root_artifact.encoded_len),
            self.property_spill_manifest_encoded_len,
            self.property_projection_manifest_encoded_len,
        ]
        .into_iter()
        .flatten()
        .fold(0u64, u64::saturating_add)
    }

    fn admit_graph_manifest_artifacts(
        &self,
        artifacts: &CheckpointManifestArtifacts,
    ) -> Result<()> {
        let mut budget = GraphManifestOpenBudget::new(self.max_graph_manifest_open_bytes);
        let adjacency_root = artifacts
            .canonical_adjacency
            .generation
            .descriptor_root_artifact;
        for (artifact, metadata, format_max_bytes) in [
            (
                "canonical manifest",
                artifacts.canonical_manifest,
                CANONICAL_MANIFEST_MAX_BYTES,
            ),
            (
                "property spill manifest",
                artifacts.property_spill_manifest,
                PROPERTY_SPILL_MANIFEST_MAX_BYTES,
            ),
            (
                "canonical adjacency descriptor root",
                DurableArtifactMetadata {
                    encoded_len: adjacency_root.encoded_len,
                    encoded_checksum: u64::from(adjacency_root.encoded_crc32c),
                    encoded_sha256: adjacency_root.encoded_sha256,
                },
                GraphDescriptorTreeBuildConfig::default()
                    .max_root_bytes
                    .get() as u64,
            ),
            (
                "property projection manifest",
                artifacts.property_projection_manifest,
                PROPERTY_PROJECTION_MANIFEST_MAX_BYTES,
            ),
        ] {
            admit_graph_manifest_binding(
                metadata.encoded_len,
                format_max_bytes,
                artifact,
                &mut budget,
            )?;
        }
        Ok(())
    }

    pub(super) fn begin_wal_sync_group(&mut self) -> Result<bool> {
        if self.durability != DurabilityPolicy::SyncOnEveryWrite {
            return Ok(false);
        }
        if self.wal_sync_group.is_some() {
            return Err(SkeinError::Storage(
                "nested WAL sync groups are not allowed".to_string(),
            ));
        }
        self.wal_sync_group = Some(WalSyncGroupState::default());
        Ok(true)
    }

    pub(super) fn wal_sync_group_progress(&self) -> WalSyncGroupProgress {
        self.wal_sync_group
            .map_or_else(WalSyncGroupProgress::default, WalSyncGroupState::progress)
    }

    pub(super) const fn wal_sync_group_active(&self) -> bool {
        self.wal_sync_group.is_some()
    }

    pub(super) fn finish_wal_sync_group(&mut self) -> Result<WalSyncGroupFlush> {
        let Some(group) = self.wal_sync_group.take() else {
            return Ok(WalSyncGroupFlush::default());
        };
        if group.is_empty() {
            return Ok(WalSyncGroupFlush::default());
        }
        wal_group_sync_failpoint()?;
        let started = std::time::Instant::now();
        let file = self.wal_append_file.as_ref().ok_or_else(|| {
            SkeinError::Storage(
                "WAL sync group has entries without an open append handle".to_string(),
            )
        })?;
        file.sync_data()?;
        if group.requires_parent_sync() {
            sync_parent_dir(&self.wal_path)?;
        }
        process_crash_failpoint("after_wal_sync");
        Ok(group.into_flush(elapsed_micros(started)))
    }

    pub(super) fn wal_age_millis(&self) -> Option<u64> {
        let modified = fs::metadata(&self.wal_path).ok()?.modified().ok()?;
        let elapsed = std::time::SystemTime::now().duration_since(modified).ok()?;
        Some(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
    }

    pub(super) fn obsolete_generation_bytes(&self, oldest_reader_commit_epoch: Option<u64>) -> u64 {
        if oldest_reader_commit_epoch.is_none() {
            return 0;
        }
        let retain_from = self.checkpoint_epoch.saturating_sub(1);
        fs::read_dir(&self.root_path)
            .into_iter()
            .flatten()
            .filter_map(std::result::Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name();
                let name = name.to_str()?;
                let generation = storage_generation_for_file(name)?;
                (generation < retain_from)
                    .then(|| entry.metadata().ok().map(|metadata| metadata.len()))
                    .flatten()
            })
            .fold(0u64, u64::saturating_add)
    }

    pub(super) fn backup_to(&self, destination: &Path) -> Result<StorageBackupReport> {
        let generation = self.checkpoint_epoch;
        if self.checkpoint_encoded_len.is_none() {
            return Err(SkeinError::Storage(
                "storage must have a published checkpoint before backup".to_string(),
            ));
        }
        validate_new_backup_destination(&self.root_path, destination)?;
        fs::create_dir(destination)?;

        let result = (|| {
            let mut sources = BTreeMap::from([
                (MANIFEST_FILE.to_string(), self.manifest_path.clone()),
                (
                    checkpoint_generation_file(generation),
                    self.checkpoint_path.clone(),
                ),
                (wal_generation_file(generation), self.wal_path.clone()),
            ]);
            let relational_checkpoint_name = relational_checkpoint_generation_file(generation);
            let relational_checkpoint_path = self.root_path.join(&relational_checkpoint_name);
            if self.relational_checkpoint_encoded_len.is_some() {
                sources.insert(relational_checkpoint_name, relational_checkpoint_path);
            }
            if let Some(binding) = self.relational_index_generation_artifacts {
                let page_name =
                    skein_storage::relational_index_shadow_artifact_file(binding.generation);
                let manifest_name = skein_storage::relational_index_shadow_manifest_generation_file(
                    binding.generation,
                );
                sources.insert(page_name.clone(), self.root_path.join(page_name));
                sources.insert(manifest_name.clone(), self.root_path.join(manifest_name));
            }
            if let Some(binding) = self.relational_row_generation_artifacts {
                for name in [
                    skein_storage::relational_row_page_root_descriptor_file(binding.generation),
                    skein_storage::relational_row_page_root_key_file(binding.generation),
                    skein_storage::relational_row_page_manifest_generation_file(binding.generation),
                ] {
                    sources.insert(name.clone(), self.root_path.join(name));
                }
            }
            if let Some(binding) = self.relational_overflow_generation_artifacts {
                for name in [
                    skein_storage::relational_overflow_descriptor_file(binding.generation),
                    skein_storage::relational_overflow_manifest_generation_file(binding.generation),
                ] {
                    sources.insert(name.clone(), self.root_path.join(name));
                }
            }
            if let Some(binding) = self.append_generation_artifacts {
                let reader = AppendGenerationReader::open_bound(
                    &self.root_path,
                    binding,
                    AppendPublicationConfig::default(),
                )
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
                let manifest_name = append_generation_manifest_file(binding.generation);
                sources.insert(manifest_name.clone(), self.root_path.join(manifest_name));
                for segment in reader.segment_bindings() {
                    let name = append_segment_file(segment.generation);
                    sources.insert(name.clone(), self.root_path.join(name));
                }
            }
            let overflow_root = self.open_bound_relational_overflow()?;
            let mut overflow_extent_generations =
                BTreeSet::from([overflow_root.manifest().generation]);
            overflow_root
                .visit_descriptors(|descriptor| {
                    overflow_extent_generations.insert(descriptor.physical_generation);
                    Ok(())
                })
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
            for physical_generation in overflow_extent_generations {
                let name = skein_storage::relational_overflow_extent_file(physical_generation);
                sources.insert(name.clone(), self.root_path.join(name));
            }
            let row_root = self.open_bound_relational_row_pages(&overflow_root)?;
            let mut row_page_generations = BTreeSet::from([row_root.manifest().generation]);
            let tables = row_root
                .manifest()
                .tables
                .iter()
                .map(|table| table.table.clone())
                .collect::<Vec<_>>();
            for table in tables {
                row_root
                    .visit_table_pages(&table, |descriptor| {
                        row_page_generations.insert(descriptor.physical_generation);
                        Ok(())
                    })
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
            }
            for physical_generation in row_page_generations {
                let name = skein_storage::relational_row_page_artifact_file(physical_generation);
                sources.insert(name.clone(), self.root_path.join(name));
            }
            if self.canonical_manifest_encoded_len.is_some() {
                sources.insert(
                    canonical_artifact_generation_file(generation),
                    self.root_path
                        .join(canonical_artifact_generation_file(generation)),
                );
                sources.insert(
                    canonical_manifest_generation_file(generation),
                    self.root_path
                        .join(canonical_manifest_generation_file(generation)),
                );
                for name in [
                    skein_storage::canonical_segment_descriptor_page_file(generation),
                    skein_storage::canonical_segment_descriptor_root_file(generation),
                ] {
                    sources.insert(name.clone(), self.root_path.join(name));
                }
            }
            if self.canonical_adjacency_generation_artifacts.is_some() {
                sources.insert(
                    canonical_adjacency_artifact_generation_file(generation),
                    self.root_path
                        .join(canonical_adjacency_artifact_generation_file(generation)),
                );
                for name in [
                    skein_storage::canonical_adjacency_descriptor_page_file(generation),
                    skein_storage::canonical_adjacency_descriptor_root_file(generation),
                ] {
                    sources.insert(name.clone(), self.root_path.join(name));
                }
            }
            if self.property_spill_manifest_encoded_len.is_some() {
                sources.insert(
                    property_spill_artifact_generation_file(generation),
                    self.root_path
                        .join(property_spill_artifact_generation_file(generation)),
                );
                sources.insert(
                    property_spill_manifest_generation_file(generation),
                    self.root_path
                        .join(property_spill_manifest_generation_file(generation)),
                );
                for name in [
                    skein_storage::property_spill_descriptor_page_file(generation),
                    skein_storage::property_spill_descriptor_root_file(generation),
                ] {
                    sources.insert(name.clone(), self.root_path.join(name));
                }
            }
            if self.property_projection_manifest_encoded_len.is_some() {
                sources.insert(
                    property_projection_artifact_generation_file(generation),
                    self.root_path
                        .join(property_projection_artifact_generation_file(generation)),
                );
                sources.insert(
                    property_projection_manifest_generation_file(generation),
                    self.root_path
                        .join(property_projection_manifest_generation_file(generation)),
                );
                for name in [
                    skein_storage::property_projection_descriptor_page_file(generation),
                    skein_storage::property_projection_descriptor_root_file(generation),
                ] {
                    sources.insert(name.clone(), self.root_path.join(name));
                }
            }
            match (
                self.stable_id_mapping_path.exists(),
                self.stable_id_mapping_reader.as_ref(),
            ) {
                (true, Some(reader)) => {
                    let artifact_path = reader.artifact_path();
                    let artifact_name = artifact_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .ok_or_else(|| {
                            SkeinError::Storage(
                                "stable identity generation artifact name is not UTF-8".to_string(),
                            )
                        })?
                        .to_string();
                    sources.insert(
                        STABLE_ID_MAPPING_FILE.to_string(),
                        self.stable_id_mapping_path.clone(),
                    );
                    sources.insert(artifact_name, artifact_path.to_path_buf());
                }
                (true, None) => {
                    return Err(SkeinError::Storage(
                        "stable identity selector exists without a pinned generation".to_string(),
                    ));
                }
                (false, Some(_)) => {
                    return Err(SkeinError::Storage(
                        "pinned stable identity generation has no selector".to_string(),
                    ));
                }
                (false, None) => {}
            }
            let mut files = Vec::with_capacity(sources.len());
            for (name, source) in sources {
                files.push(copy_backup_file(&source, &destination.join(&name), &name)?);
            }
            files.sort_by(|left, right| left.name.cmp(&right.name));
            validate_backup_files(destination, &files, generation)?;
            let backup_manifest = BackupManifest::write(
                &destination.join(BACKUP_MANIFEST_FILE),
                generation,
                self.checkpoint_commit_epoch,
                files,
            )?;
            sync_parent_dir(&destination.join(BACKUP_MANIFEST_FILE))?;
            let total_bytes = backup_manifest
                .files
                .iter()
                .try_fold(0u64, |total, file| total.checked_add(file.encoded_len))
                .ok_or_else(|| SkeinError::Storage("backup byte count overflow".to_string()))?;
            Ok(StorageBackupReport {
                generation,
                checkpoint_commit_epoch: self.checkpoint_commit_epoch,
                file_count: backup_manifest.files.len(),
                total_bytes,
                manifest_checksum: backup_manifest.checksum,
            })
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(destination);
        }
        result
    }

    pub(super) fn scrub_storage(&self) -> Result<StorageScrubReport> {
        let manifest = DurableManifest::load(&self.manifest_path)?;
        let mut scrub = StorageScrubCounters::new(fs::metadata(&self.manifest_path)?.len());

        if let (
            Some(generation),
            Some(expected_len),
            Some(expected_checksum),
            Some(expected_sha256),
        ) = (
            manifest.checkpoint_generation,
            manifest.checkpoint_encoded_len,
            manifest.checkpoint_encoded_checksum,
            manifest.checkpoint_encoded_sha256,
        ) {
            scrub.verify_path(
                &self.root_path.join(checkpoint_generation_file(generation)),
                expected_len,
                expected_checksum,
                expected_sha256,
                "checkpoint",
            )?;
        }

        if let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
            self.relational_checkpoint_encoded_len,
            self.relational_checkpoint_encoded_checksum,
            self.relational_checkpoint_encoded_sha256,
        ) {
            let path = self
                .root_path
                .join(relational_checkpoint_generation_file(self.checkpoint_epoch));
            scrub.verify_path(
                &path,
                expected_len,
                expected_checksum,
                expected_sha256,
                "relational checkpoint",
            )?;
            let checkpoint =
                decode_relational_checkpoint_file(&path, RelationalDecodeLimits::checkpoint())
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
            if checkpoint.epoch != self.checkpoint_commit_epoch {
                return Err(SkeinError::Storage(format!(
                    "relational checkpoint epoch {} does not match manifest checkpoint commit epoch {}",
                    checkpoint.epoch, self.checkpoint_commit_epoch
                )));
            }
        }

        if let Some(binding) = manifest.relational_index_generation_artifacts {
            let page_path =
                self.root_path
                    .join(skein_storage::relational_index_shadow_artifact_file(
                        binding.generation,
                    ));
            scrub.verify_path(
                &page_path,
                binding.page_artifact.encoded_len,
                binding.page_artifact.encoded_crc32c,
                binding.page_artifact.encoded_sha256,
                "relational index page artifact",
            )?;
            let generation_manifest_path = self.root_path.join(
                skein_storage::relational_index_shadow_manifest_generation_file(binding.generation),
            );
            scrub.verify_path(
                &generation_manifest_path,
                binding.manifest_artifact.encoded_len,
                binding.manifest_artifact.encoded_crc32c,
                binding.manifest_artifact.encoded_sha256,
                "relational index generation manifest",
            )?;
            let reader = skein_storage::RelationalIndexShadowReader::open_generation(
                &self.root_path,
                skein_storage::RelationalIndexGenerationIdentity {
                    generation: binding.generation,
                    source_commit_epoch: binding.source_commit_epoch,
                },
                skein_storage::RelationalIndexShadowConfig::default(),
            )
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
            if reader.manifest().catalog_schema_digest != binding.catalog_schema_digest
                || reader.manifest().root_set_digest != binding.root_set_digest
            {
                return Err(SkeinError::Storage(
                    "relational index manifest digests do not match canonical binding during scrub"
                        .to_string(),
                ));
            }
        }

        if let Some(binding) = manifest.append_generation_artifacts {
            scrub.verify_path(
                &self
                    .root_path
                    .join(append_generation_manifest_file(binding.generation)),
                binding.manifest_artifact.encoded_len,
                u64::from(binding.manifest_artifact.encoded_crc32c),
                binding.manifest_artifact.encoded_sha256,
                "append generation manifest",
            )?;
            let reader = AppendGenerationReader::open_bound(
                &self.root_path,
                binding,
                AppendPublicationConfig::default(),
            )
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
            for segment in reader.segment_bindings() {
                scrub.verify_path(
                    &self.root_path.join(append_segment_file(segment.generation)),
                    segment.artifact.encoded_len,
                    u64::from(segment.artifact.encoded_crc32c),
                    segment.artifact.encoded_sha256,
                    "append segment",
                )?;
            }
            reader
                .deep_scrub()
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
        }

        let overflow_root = self.open_bound_relational_overflow()?;
        let overflow_binding = manifest
            .relational_overflow_generation_artifacts
            .expect("validated checkpoint has an overflow binding");
        scrub.verify_path(
            &self
                .root_path
                .join(skein_storage::relational_overflow_manifest_generation_file(
                    overflow_binding.generation,
                )),
            overflow_binding.manifest_artifact.encoded_len,
            u64::from(overflow_binding.manifest_artifact.encoded_crc32c),
            overflow_binding.manifest_artifact.encoded_sha256,
            "relational overflow generation manifest",
        )?;
        scrub.verify_path(
            &self
                .root_path
                .join(skein_storage::relational_overflow_extent_file(
                    overflow_binding.generation,
                )),
            overflow_root.manifest().extent_artifact.encoded_len,
            u64::from(overflow_root.manifest().extent_artifact.encoded_crc32c),
            overflow_root.manifest().extent_artifact.encoded_sha256,
            "relational overflow extent artifact",
        )?;
        scrub.verify_path(
            &self
                .root_path
                .join(skein_storage::relational_overflow_descriptor_file(
                    overflow_binding.generation,
                )),
            overflow_root.manifest().descriptor_artifact.encoded_len,
            u64::from(overflow_root.manifest().descriptor_artifact.encoded_crc32c),
            overflow_root.manifest().descriptor_artifact.encoded_sha256,
            "relational overflow descriptor artifact",
        )?;

        let row_root = self.open_bound_relational_row_pages(&overflow_root)?;
        let row_binding = manifest
            .relational_row_generation_artifacts
            .expect("validated checkpoint has a row-page binding");
        scrub.verify_path(
            &self
                .root_path
                .join(skein_storage::relational_row_page_manifest_generation_file(
                    row_binding.generation,
                )),
            row_binding.manifest_artifact.encoded_len,
            u64::from(row_binding.manifest_artifact.encoded_crc32c),
            row_binding.manifest_artifact.encoded_sha256,
            "relational row-page generation manifest",
        )?;
        for (path, metadata, artifact) in [
            (
                self.root_path
                    .join(skein_storage::relational_row_page_artifact_file(
                        row_binding.generation,
                    )),
                row_root.manifest().page_artifact,
                "relational row-page artifact",
            ),
            (
                self.root_path
                    .join(skein_storage::relational_row_page_root_descriptor_file(
                        row_binding.generation,
                    )),
                row_root.manifest().root_descriptor_artifact,
                "relational row-page descriptor artifact",
            ),
            (
                self.root_path
                    .join(skein_storage::relational_row_page_root_key_file(
                        row_binding.generation,
                    )),
                row_root.manifest().root_key_artifact,
                "relational row-page key artifact",
            ),
        ] {
            scrub.verify_path(
                &path,
                metadata.encoded_len,
                u64::from(metadata.encoded_crc32c),
                metadata.encoded_sha256,
                artifact,
            )?;
        }

        if let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
            manifest.canonical_manifest_encoded_len,
            manifest.canonical_manifest_encoded_checksum,
            manifest.canonical_manifest_encoded_sha256,
        ) {
            let generation = manifest
                .checkpoint_generation
                .expect("validated canonical metadata has a checkpoint generation");
            let manifest_path = self
                .root_path
                .join(canonical_manifest_generation_file(generation));
            scrub.verify_path(
                &manifest_path,
                expected_len,
                expected_checksum,
                expected_sha256,
                "canonical manifest",
            )?;
            let artifact = CanonicalSegmentManifest::decode(&fs::read_to_string(&manifest_path)?)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
            if artifact.generation != ManifestGeneration(generation)
                || artifact.source_commit_epoch != manifest.checkpoint_commit_epoch
            {
                return Err(SkeinError::StorageIntegrity(
                    "canonical descriptor identity does not match its checkpoint during scrub"
                        .to_string(),
                ));
            }
            let canonical_path = self
                .root_path
                .join(canonical_artifact_generation_file(generation));
            scrub.verify_path(
                &canonical_path,
                artifact.artifact_len,
                artifact.artifact_digest.0,
                artifact.artifact_sha256,
                "canonical artifact",
            )?;
            let descriptor_paths = GraphDescriptorTreePaths::new(
                self.root_path
                    .join(skein_storage::canonical_segment_descriptor_page_file(
                        generation,
                    )),
                self.root_path
                    .join(skein_storage::canonical_segment_descriptor_root_file(
                        generation,
                    )),
            );
            scrub.verify_path(
                &descriptor_paths.root_manifest,
                artifact.descriptor_root_artifact.encoded_len,
                u64::from(artifact.descriptor_root_artifact.encoded_crc32c),
                artifact.descriptor_root_artifact.encoded_sha256,
                "canonical segment descriptor root",
            )?;
            let descriptor_config = GraphDescriptorTreeBuildConfig::default();
            let root_reader = GraphDescriptorTreeRootReader::open_bound(
                descriptor_paths.clone(),
                artifact.descriptor_generation_artifacts(),
                descriptor_config,
            )
            .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
            if root_reader.root().descriptor_count != artifact.segment_count {
                return Err(SkeinError::StorageIntegrity(
                    "canonical descriptor count does not match its manifest during scrub"
                        .to_string(),
                ));
            }
            scrub.verify_path(
                &descriptor_paths.page_artifact,
                root_reader.root().page_artifact_len,
                root_reader.root().page_artifact_crc32c.as_u64(),
                root_reader.root().page_artifact_sha256,
                "canonical segment descriptor pages",
            )?;
            let reader = self.canonical_segments.as_ref().ok_or_else(|| {
                SkeinError::StorageIntegrity(
                    "canonical manifest is selected without an open canonical reader during scrub"
                        .to_string(),
                )
            })?;
            if reader.path() != canonical_path || reader.manifest() != &artifact {
                return Err(SkeinError::StorageIntegrity(
                    "open canonical reader identity drifted from the selected manifest during scrub"
                        .to_string(),
                ));
            }
            reader
                .deep_scrub()
                .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
        }

        if let Some(binding) = manifest.canonical_adjacency_generation_artifacts {
            let descriptor_config = GraphDescriptorTreeBuildConfig::default();
            let descriptor_paths = GraphDescriptorTreePaths::new(
                self.root_path
                    .join(skein_storage::canonical_adjacency_descriptor_page_file(
                        binding.generation,
                    )),
                self.root_path
                    .join(skein_storage::canonical_adjacency_descriptor_root_file(
                        binding.generation,
                    )),
            );
            scrub.verify_path(
                &descriptor_paths.root_manifest,
                binding.descriptor_root_artifact.encoded_len,
                u64::from(binding.descriptor_root_artifact.encoded_crc32c),
                binding.descriptor_root_artifact.encoded_sha256,
                "canonical adjacency descriptor root",
            )?;
            let root_reader = GraphDescriptorTreeRootReader::open_bound(
                descriptor_paths.clone(),
                GraphDescriptorTreeGenerationArtifacts {
                    kind: GraphDescriptorKind::CanonicalAdjacency,
                    generation: binding.generation,
                    source_commit_epoch: binding.source_commit_epoch,
                    root_artifact: binding.descriptor_root_artifact,
                },
                descriptor_config,
            )
            .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
            scrub.verify_path(
                &descriptor_paths.page_artifact,
                root_reader.root().page_artifact_len,
                root_reader.root().page_artifact_crc32c.as_u64(),
                root_reader.root().page_artifact_sha256,
                "canonical adjacency descriptor pages",
            )?;
            let adjacency_path = self
                .root_path
                .join(canonical_adjacency_artifact_generation_file(
                    binding.generation,
                ));
            scrub.verify_path(
                &adjacency_path,
                binding.adjacency_artifact.encoded_len,
                binding.adjacency_artifact.encoded_crc32c,
                binding.adjacency_artifact.encoded_sha256,
                "canonical adjacency artifact",
            )?;
            let config = CanonicalAdjacencyConfig::default();
            let max_block_bytes = NonZeroU64::new(
                config
                    .target_block_bytes
                    .get()
                    .max(config.max_record_bytes.get().saturating_add(1024)),
            )
            .expect("canonical adjacency maximum block size is non-zero");
            CanonicalAdjacencyReader::open_demand_paged(
                adjacency_path,
                binding,
                root_reader,
                descriptor_config,
                Arc::clone(&self.segment_cache),
                self.store_id,
                max_block_bytes,
            )
            .and_then(|reader| reader.deep_scrub())
            .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
        }

        if let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
            manifest.property_spill_manifest_encoded_len,
            manifest.property_spill_manifest_encoded_checksum,
            manifest.property_spill_manifest_encoded_sha256,
        ) {
            let generation = manifest
                .checkpoint_generation
                .expect("validated property spill metadata has a checkpoint generation");
            let manifest_path = self
                .root_path
                .join(property_spill_manifest_generation_file(generation));
            scrub.verify_path(
                &manifest_path,
                expected_len,
                expected_checksum,
                expected_sha256,
                "property spill manifest",
            )?;
            let artifact = PropertySpillManifest::decode(&fs::read_to_string(&manifest_path)?)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
            if artifact.generation != ManifestGeneration(generation)
                || artifact.source_commit_epoch != manifest.checkpoint_commit_epoch
            {
                return Err(SkeinError::StorageIntegrity(
                    "property spill identity does not match its checkpoint".to_string(),
                ));
            }
            scrub.verify_path(
                &self
                    .root_path
                    .join(property_spill_artifact_generation_file(generation)),
                artifact.artifact_len,
                artifact.artifact_digest.0,
                artifact.artifact_sha256,
                "property spill artifact",
            )?;
            let descriptor_paths = GraphDescriptorTreePaths::new(
                self.root_path
                    .join(skein_storage::property_spill_descriptor_page_file(
                        generation,
                    )),
                self.root_path
                    .join(skein_storage::property_spill_descriptor_root_file(
                        generation,
                    )),
            );
            scrub.verify_path(
                &descriptor_paths.root_manifest,
                artifact.descriptor_root_artifact.encoded_len,
                u64::from(artifact.descriptor_root_artifact.encoded_crc32c),
                artifact.descriptor_root_artifact.encoded_sha256,
                "property spill descriptor root",
            )?;
            let descriptor_root = GraphDescriptorTreeRootReader::open_bound(
                descriptor_paths.clone(),
                artifact.descriptor_generation_artifacts(),
                GraphDescriptorTreeBuildConfig::default(),
            )
            .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
            if descriptor_root.root().descriptor_count != artifact.block_count {
                return Err(SkeinError::StorageIntegrity(
                    "property spill descriptor count does not match its manifest".to_string(),
                ));
            }
            scrub.verify_path(
                &descriptor_paths.page_artifact,
                descriptor_root.root().page_artifact_len,
                descriptor_root.root().page_artifact_crc32c.as_u64(),
                descriptor_root.root().page_artifact_sha256,
                "property spill descriptor pages",
            )?;
            let selected = self
                .canonical_segments
                .as_ref()
                .and_then(CanonicalSegmentReader::property_spill_manifest)
                .ok_or_else(|| {
                    SkeinError::StorageIntegrity(
                        "property spill manifest is selected without an open spill reader during scrub"
                            .to_string(),
                    )
                })?;
            if selected != &artifact {
                return Err(SkeinError::StorageIntegrity(
                    "open property spill reader identity drifted from the selected manifest during scrub"
                        .to_string(),
                ));
            }
        }

        if let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
            manifest.property_projection_manifest_encoded_len,
            manifest.property_projection_manifest_encoded_checksum,
            manifest.property_projection_manifest_encoded_sha256,
        ) {
            let generation = manifest
                .checkpoint_generation
                .expect("validated property projection metadata has a checkpoint generation");
            let manifest_path = self
                .root_path
                .join(property_projection_manifest_generation_file(generation));
            scrub.verify_path(
                &manifest_path,
                expected_len,
                expected_checksum,
                expected_sha256,
                "property projection manifest",
            )?;
            let artifact =
                PersistentPropertyProjectionManifest::decode(&fs::read_to_string(&manifest_path)?)
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
            scrub.verify_path(
                &self
                    .root_path
                    .join(property_projection_artifact_generation_file(generation)),
                artifact.artifact_len,
                artifact.artifact_digest.0,
                artifact.artifact_sha256,
                "property projection artifact",
            )?;
            let descriptor_paths = GraphDescriptorTreePaths::new(
                self.root_path
                    .join(skein_storage::property_projection_descriptor_page_file(
                        generation,
                    )),
                self.root_path
                    .join(skein_storage::property_projection_descriptor_root_file(
                        generation,
                    )),
            );
            scrub.verify_path(
                &descriptor_paths.root_manifest,
                artifact.descriptor_root_artifact.encoded_len,
                u64::from(artifact.descriptor_root_artifact.encoded_crc32c),
                artifact.descriptor_root_artifact.encoded_sha256,
                "property projection descriptor root",
            )?;
            let descriptor_root = GraphDescriptorTreeRootReader::open_bound(
                descriptor_paths.clone(),
                artifact.descriptor_generation_artifacts(),
                GraphDescriptorTreeBuildConfig::default(),
            )
            .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
            scrub.verify_path(
                &descriptor_paths.page_artifact,
                descriptor_root.root().page_artifact_len,
                descriptor_root.root().page_artifact_crc32c.as_u64(),
                descriptor_root.root().page_artifact_sha256,
                "property projection descriptor pages",
            )?;
            let reader = self
                .persistent_property_projection
                .as_ref()
                .ok_or_else(|| {
                    SkeinError::Storage(
                        "property projection publication exists without a selected reader during scrub"
                            .to_string(),
                    )
                })?;
            if reader.manifest() != &artifact {
                return Err(SkeinError::Storage(
                    "selected property projection reader does not match the durable manifest"
                        .to_string(),
                ));
            }
            reader
                .deep_scrub()
                .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
        }

        match (
            self.stable_id_mapping_path.exists(),
            self.stable_id_mapping_reader.as_ref(),
        ) {
            (true, Some(reader)) => {
                let report = reader.deep_scrub().map_err(stable_identity_error)?;
                scrub.checked_file_count = scrub.checked_file_count.saturating_add(2);
                scrub.checked_bytes = scrub.checked_bytes.saturating_add(report.checked_bytes);
                scrub.sha256_verified_file_count =
                    scrub.sha256_verified_file_count.saturating_add(2);
            }
            (true, None) => {
                return Err(SkeinError::Storage(
                    "stable identity mapping exists without a selected reader during scrub"
                        .to_string(),
                ));
            }
            (false, Some(_)) => {
                return Err(SkeinError::Storage(
                    "selected stable identity mapping is missing during scrub".to_string(),
                ));
            }
            (false, None) => {}
        }

        let mut overflow_extent_generations = BTreeSet::new();
        let mut older_overflow_bytes = 0u64;
        overflow_root
            .visit_descriptors(|descriptor| {
                overflow_extent_generations.insert(descriptor.physical_generation);
                overflow_root.hydrate(
                    &descriptor.reference,
                    &mut skein_storage::RelationalHydrationBudget::default(),
                    None,
                )?;
                if descriptor.physical_generation != overflow_binding.generation {
                    older_overflow_bytes = older_overflow_bytes
                        .checked_add(descriptor.envelope_bytes)
                        .ok_or_else(|| {
                            skein_storage::RelationalOverflowPublicationError::Admission(
                                "overflow scrub byte count overflow".to_string(),
                            )
                        })?;
                }
                Ok(())
            })
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let older_overflow_files = overflow_extent_generations
            .iter()
            .filter(|generation| **generation != overflow_binding.generation)
            .count();
        scrub.checked_file_count = scrub
            .checked_file_count
            .saturating_add(older_overflow_files);
        scrub.checked_bytes = scrub.checked_bytes.saturating_add(older_overflow_bytes);

        let mut row_page_generations = BTreeSet::new();
        let mut older_row_page_bytes = 0u64;
        let tables = row_root
            .manifest()
            .tables
            .iter()
            .map(|table| table.table.clone())
            .collect::<Vec<_>>();
        for table in tables {
            row_root
                .visit_table_pages(&table, |descriptor| {
                    row_page_generations.insert(descriptor.physical_generation);
                    row_root.read_page(descriptor)?;
                    if descriptor.physical_generation != row_binding.generation {
                        older_row_page_bytes = older_row_page_bytes
                            .checked_add(row_root.manifest().page_bytes)
                            .ok_or_else(|| {
                                skein_storage::RelationalRowPagePublicationError::Admission(
                                    "row-page scrub byte count overflow".to_string(),
                                )
                            })?;
                    }
                    Ok(())
                })
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
        }
        let older_row_page_files = row_page_generations
            .iter()
            .filter(|generation| **generation != row_binding.generation)
            .count();
        scrub.checked_file_count = scrub
            .checked_file_count
            .saturating_add(older_row_page_files);
        scrub.checked_bytes = scrub.checked_bytes.saturating_add(older_row_page_bytes);

        let (wal_record_count, wal_bytes) = self.scrub_wal()?;
        if self.wal_path.exists() {
            scrub.checked_file_count = scrub.checked_file_count.saturating_add(1);
            scrub.checked_bytes = scrub.checked_bytes.saturating_add(wal_bytes);
        }
        Ok(StorageScrubReport {
            generation: manifest.wal_generation,
            checked_file_count: scrub.checked_file_count,
            checked_bytes: scrub.checked_bytes,
            sha256_verified_file_count: scrub.sha256_verified_file_count,
            wal_record_count,
            wal_bytes,
        })
    }

    fn scrub_wal(&self) -> Result<(usize, u64)> {
        if !self.wal_path.exists() {
            if self.checkpoint_epoch > 0 {
                return Err(SkeinError::Storage(format!(
                    "manifest WAL generation {} is missing during scrub",
                    self.wal_generation
                )));
            }
            return Ok((0, 0));
        }
        let wal_bytes = fs::metadata(&self.wal_path)?.len();
        let mut cursor = match WalRecordCursor::open(&self.wal_path, self.max_record_bytes)? {
            WalOpenOutcome::Cursor(cursor) => cursor,
            WalOpenOutcome::MissingHeader => {
                return Err(SkeinError::Storage(format!(
                    "WAL generation {} is missing its header during scrub",
                    self.wal_generation
                )));
            }
            WalOpenOutcome::HeaderTorn { .. } => {
                return Err(SkeinError::Storage(
                    "WAL scrub rejected a torn tail; use explicit doctor repair if discarding the incomplete record is acceptable"
                        .to_string(),
                ));
            }
            WalOpenOutcome::HeaderCorrupt { reason } => {
                return Err(SkeinError::Storage(reason));
            }
        };
        if cursor.generation() != self.wal_generation
            || cursor.start_lsn() != self.wal_replay_start_lsn
        {
            return Err(SkeinError::Storage(
                "WAL scrub found a header that does not match the durable manifest".to_string(),
            ));
        }
        let mut expected_lsn = self.wal_replay_start_lsn;
        let mut record_count = 0usize;
        loop {
            let entry = match cursor.next()? {
                WalCursorEvent::Eof => break,
                WalCursorEvent::TornTail { .. } => {
                    return Err(SkeinError::Storage(
                        "WAL scrub rejected a torn tail; use explicit doctor repair if discarding the incomplete record is acceptable"
                            .to_string(),
                    ));
                }
                WalCursorEvent::Corrupt { reason, .. } => {
                    return Err(SkeinError::Storage(format!(
                        "WAL scrub found a corrupt record: {reason}"
                    )));
                }
                WalCursorEvent::Entry { entry, .. } => entry,
            };
            if entry.lsn != expected_lsn {
                return Err(SkeinError::Storage(format!(
                    "WAL scrub found an LSN sequence mismatch: expected {expected_lsn}, got {}",
                    entry.lsn
                )));
            }
            expected_lsn = expected_lsn
                .checked_add(1)
                .ok_or_else(|| SkeinError::Storage("WAL LSN overflow during scrub".to_string()))?;
            record_count = record_count.saturating_add(1);
        }
        if expected_lsn != self.next_lsn {
            return Err(SkeinError::Storage(format!(
                "WAL scrub ended at next LSN {expected_lsn}, but the open store expects {}",
                self.next_lsn
            )));
        }
        Ok((record_count, wal_bytes))
    }

    pub(super) fn read_checkpoint_text(&self, config: WalReplayConfig) -> Result<String> {
        let metadata = fs::metadata(&self.checkpoint_path)?;
        if config
            .max_checkpoint_encoded_bytes
            .is_some_and(|limit| metadata.len() > limit)
        {
            return Err(SkeinError::Storage(format!(
                "checkpoint encoded byte limit exceeded: max_checkpoint_encoded_bytes={}",
                config.max_checkpoint_encoded_bytes.unwrap_or_default()
            )));
        }
        let bytes = fs::read(&self.checkpoint_path)?;
        let expected_len = self.checkpoint_encoded_len.ok_or_else(|| {
            SkeinError::Storage("checkpoint is missing its encoded length".to_string())
        })?;
        let expected_checksum = self.checkpoint_encoded_checksum.ok_or_else(|| {
            SkeinError::Storage("checkpoint is missing its encoded checksum".to_string())
        })?;
        let expected_sha256 = self.checkpoint_encoded_sha256.ok_or_else(|| {
            SkeinError::Storage("checkpoint is missing its encoded SHA-256".to_string())
        })?;
        verify_integrity(
            &bytes,
            expected_len,
            expected_checksum,
            expected_sha256,
            "checkpoint",
        )?;
        read_durable_text_bytes_with_limit(
            &bytes,
            "checkpoint",
            config.max_checkpoint_decoded_bytes,
        )
    }

    pub(super) fn load_source_scan_manifest(
        &self,
        _graph_epoch: u64,
    ) -> Result<Option<ScanSegmentManifest>> {
        let (Some(source_scan_epoch), Some(source_scan_descriptor_checksum)) = (
            self.source_scan_commit_epoch,
            self.source_scan_descriptor_checksum,
        ) else {
            return Ok(None);
        };
        match source_scan::load(
            &self.root_path,
            source_scan_epoch,
            source_scan_descriptor_checksum,
        ) {
            Ok(manifest) => Ok(manifest),
            Err(_) if !self.read_only => {
                remove_source_scan_artifacts(&self.root_path)?;
                Ok(None)
            }
            Err(_) => Ok(None),
        }
    }

    pub(super) fn append_single(
        &mut self,
        op: WalOp,
        pressure_signals: StoragePressureSignals,
    ) -> Result<()> {
        self.append_entry(op, 1, pressure_signals)
    }

    pub(super) fn append_batch(
        &mut self,
        ops: Vec<WalOp>,
        pressure_signals: StoragePressureSignals,
    ) -> Result<()> {
        let operation_count = ops.len();
        if self
            .max_batch_operations
            .is_some_and(|limit| operation_count > limit)
        {
            return Err(SkeinError::Storage(format!(
                "WAL batch operation limit exceeded before append: max_wal_batch_operations={}",
                self.max_batch_operations.unwrap_or_default()
            )));
        }
        self.append_entry(WalOp::Batch(ops), operation_count, pressure_signals)
    }

    fn append_entry(
        &mut self,
        op: WalOp,
        operation_count: usize,
        pressure_signals: StoragePressureSignals,
    ) -> Result<()> {
        let entry = WalEntry {
            lsn: self.next_lsn,
            op,
        };
        let payload = encode_binary_wal_record(&entry, self.wal_commit_epoch.saturating_add(1))?;
        if self
            .max_record_bytes
            .is_some_and(|limit| payload.len() > limit)
        {
            return Err(SkeinError::Storage(format!(
                "WAL record byte limit exceeded before append: max_wal_record_bytes={}",
                self.max_record_bytes.unwrap_or_default()
            )));
        }
        let header_bytes = encode_binary_wal_header(self.wal_generation, self.wal_replay_start_lsn);
        let position = self
            .wal_bytes
            .saturating_sub(WAL_BINARY_FILE_HEADER_BYTES as u64);
        let record_bytes = frame_binary_wal_record(self.wal_generation, &payload, position);
        let started = std::time::Instant::now();
        let mut byte_count = record_bytes.len() as u64;
        if self.wal_bytes == 0 {
            byte_count = byte_count.saturating_add(header_bytes.len() as u64);
        }
        self.ensure_wal_admission(
            self.wal_bytes.saturating_add(byte_count),
            byte_count,
            pressure_signals,
        )?;
        process_crash_failpoint("before_wal_append");
        let sync_deferred = self.wal_sync_group.is_some();
        let result = match self.take_wal_append() {
            Err(error) => Err(error),
            Ok((file, created)) => {
                let write_result: Result<()> = (|| {
                    let mut writer = file.as_ref();
                    if self.wal_bytes == 0 {
                        writer.write_all(&header_bytes)?;
                    }
                    #[cfg(test)]
                    {
                        if std::env::var(super::PROCESS_CRASH_POINT_ENV).as_deref()
                            == Ok("during_wal_append")
                        {
                            writer.write_all(&record_bytes[..record_bytes.len() / 2])?;
                            process_crash_failpoint("during_wal_append");
                        }
                        if matches!(
                            super::WAL_APPEND_FAILURE.get(),
                            Some(
                                super::WalAppendFailure::PartialWrite
                                    | super::WalAppendFailure::Rollback
                            )
                        ) {
                            writer.write_all(&record_bytes[..record_bytes.len() / 2])?;
                            if super::WAL_APPEND_FAILURE.get()
                                == Some(super::WalAppendFailure::PartialWrite)
                            {
                                super::WAL_APPEND_FAILURE.take();
                            }
                            return Err(
                                std::io::Error::from(std::io::ErrorKind::StorageFull).into()
                            );
                        }
                    }
                    writer.write_all(&record_bytes)?;
                    Ok(())
                })();
                let append_result = match write_result {
                    Err(error) => match self.rollback_failed_wal_write(created) {
                        Ok(()) => {
                            // A group may still need this handle to acknowledge
                            // earlier entries even when no later write retries.
                            if self.wal_bytes > 0 {
                                self.wal_append_file = Some(Arc::clone(&file));
                            }
                            Err(SkeinError::Storage(format!(
                                "WAL write failed and was rolled back to byte {}: {error}",
                                self.wal_bytes
                            )))
                        }
                        Err(rollback_error) => Err(SkeinError::StorageIntegrity(format!(
                            "WAL write failed: {error}; rollback to byte {} failed: {rollback_error}; close and recover the database",
                            self.wal_bytes
                        ))),
                    },
                    Ok(()) => {
                        process_crash_failpoint("after_wal_append");
                        let sync_result = self.finish_wal_append(file.as_ref(), created);
                        if sync_result.is_ok() && !sync_deferred {
                            process_crash_failpoint("after_wal_sync");
                        }
                        sync_result.map_err(|error| SkeinError::StorageIntegrity(format!(
                            "WAL append outcome is uncertain after writing the complete record: {error}"
                        )))
                    }
                };
                match append_result {
                    Ok(fsync_micros) => {
                        self.wal_append_file = Some(file);
                        Ok(fsync_micros)
                    }
                    Err(error) => Err(error),
                }
            }
        };
        if let Some(telemetry) = &self.telemetry {
            telemetry.record_wal_append(WalAppendTelemetry {
                success: result.is_ok(),
                elapsed_micros: elapsed_micros(started),
                operation_count,
                byte_count,
                fsync_micros: result.as_ref().copied().unwrap_or_default(),
                generation: self.wal_generation,
            });
        }
        if result.is_ok() {
            self.next_lsn += 1;
            self.wal_commit_epoch = self.wal_commit_epoch.saturating_add(1);
            self.wal_bytes = self.wal_bytes.saturating_add(byte_count);
            self.wal_free_space_probe.wal_bytes_since_probe = self
                .wal_free_space_probe
                .wal_bytes_since_probe
                .saturating_add(byte_count);
            if let Some(group) = &mut self.wal_sync_group {
                group.record_entry(byte_count);
            }
        }
        result.map(|_| ())
    }

    fn rollback_failed_wal_write(&mut self, created: bool) -> Result<()> {
        #[cfg(test)]
        if super::WAL_APPEND_FAILURE.take() == Some(super::WalAppendFailure::Rollback) {
            return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into());
        }
        // Windows append-only handles do not grant the access required to resize.
        let file = OpenOptions::new().write(true).open(&self.wal_path)?;
        if file.metadata()?.len() < self.wal_bytes {
            return Err(SkeinError::Storage(
                "WAL lost previously appended bytes before rollback".to_string(),
            ));
        }
        file.set_len(self.wal_bytes)?;
        file.sync_all()?;
        if created {
            if self.wal_bytes == 0 {
                drop(file);
                fs::remove_file(&self.wal_path)?;
            }
            sync_parent_dir(&self.wal_path)?;
        }
        self.wal_free_space_probe = WalFreeSpaceProbeState::default();
        Ok(())
    }

    fn ensure_wal_admission(
        &mut self,
        projected_wal_bytes: u64,
        pending_wal_bytes: u64,
        mut signals: StoragePressureSignals,
    ) -> Result<()> {
        let available_free_space_bytes = self
            .available_space_for_wal_admission()?
            .saturating_sub(pending_wal_bytes);
        let reclamation = self.generation_reclamation_debt();
        signals.wal_bytes = projected_wal_bytes;
        signals.max_wal_bytes = self.max_wal_bytes;
        signals.generation_reclamation_retry_required = reclamation.retry_required;
        signals.generation_reclamation_pending_files = reclamation.pending_file_count;
        signals.generation_reclamation_pending_bytes = reclamation.pending_bytes;
        signals.oldest_reader_commit_epoch = self.oldest_reader_commit_epoch;
        signals.obsolete_generation_bytes =
            self.obsolete_generation_bytes(self.oldest_reader_commit_epoch);
        signals.estimated_checkpoint_temporary_bytes = signals
            .estimated_checkpoint_temporary_bytes
            .saturating_add(pending_wal_bytes.saturating_mul(CHECKPOINT_TEMPORARY_SPACE_MULTIPLIER))
            .max(MIN_CHECKPOINT_TEMPORARY_SPACE_BYTES);
        signals.available_free_space_bytes = Some(available_free_space_bytes);
        let pressure = StorageDebtController.evaluate(signals);
        if pressure.state.admits_mutation() {
            return Ok(());
        }
        let reasons = pressure
            .reason_codes
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let recovery = if pressure
            .reason_codes
            .contains(&skein_storage::StoragePressureReasonCode::IntegrityPoisoned)
        {
            "close and reopen the database before retrying"
        } else if pressure
            .reason_codes
            .contains(&skein_storage::StoragePressureReasonCode::FreeSpaceReserve)
        {
            "free storage space before retrying"
        } else {
            "checkpoint the database before retrying"
        };
        Err(SkeinError::Storage(format!(
            "WAL append rejected by storage pressure: state={}, projected_wal_bytes={projected_wal_bytes}, max_wal_bytes={}, available_free_space_bytes={}, estimated_checkpoint_temporary_bytes={}, reasons={reasons}; {recovery}",
            pressure.state.as_str(),
            self.max_wal_bytes.unwrap_or_default(),
            pressure.available_free_space_bytes.unwrap_or_default(),
            pressure.estimated_checkpoint_temporary_bytes,
        )))
    }

    fn available_space_for_wal_admission(&mut self) -> Result<u64> {
        let probe_required = self.wal_free_space_probe.last_available_bytes.is_none()
            || self.wal_free_space_probe.wal_bytes_since_probe
                >= WAL_FREE_SPACE_PROBE_INTERVAL_BYTES;
        if probe_required {
            #[cfg(test)]
            let available = self
                .wal_free_space_probe
                .available_bytes_override
                .or_else(|| available_storage_space(&self.root_path));
            #[cfg(not(test))]
            let available = available_storage_space(&self.root_path);
            let available = available.ok_or_else(|| {
                SkeinError::Storage(
                    "WAL append rejected because filesystem free space could not be inspected"
                        .to_string(),
                )
            })?;
            self.wal_free_space_probe.last_available_bytes = Some(available);
            self.wal_free_space_probe.wal_bytes_since_probe = 0;
            #[cfg(test)]
            {
                self.wal_free_space_probe.probe_count =
                    self.wal_free_space_probe.probe_count.saturating_add(1);
            }
        }
        Ok(self
            .wal_free_space_probe
            .last_available_bytes
            .unwrap_or_default()
            .saturating_sub(self.wal_free_space_probe.wal_bytes_since_probe))
    }

    #[cfg(test)]
    pub(super) fn set_wal_available_space_override(&mut self, available_bytes: u64) {
        self.wal_free_space_probe.available_bytes_override = Some(available_bytes);
        self.wal_free_space_probe.last_available_bytes = None;
        self.wal_free_space_probe.wal_bytes_since_probe = 0;
    }

    #[cfg(test)]
    pub(super) fn wal_free_space_probe_count(&self) -> u64 {
        self.wal_free_space_probe.probe_count
    }

    fn take_wal_append(&mut self) -> Result<(Arc<File>, bool)> {
        if let Some(file) = self.wal_append_file.take() {
            return Ok((file, false));
        }
        let open_existing = || OpenOptions::new().append(true).open(&self.wal_path);
        let (file, created) = if self.wal_bytes == 0 {
            match OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(&self.wal_path)
            {
                Ok(file) => (file, true),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    (open_existing()?, false)
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            (open_existing()?, false)
        };
        #[cfg(test)]
        {
            self.wal_append_open_count = self.wal_append_open_count.saturating_add(1);
        }
        Ok((Arc::new(file), created))
    }

    fn finish_wal_append(&mut self, file: &File, created: bool) -> Result<u64> {
        #[cfg(test)]
        if super::WAL_APPEND_FAILURE.take() == Some(super::WalAppendFailure::Sync) {
            return Err(std::io::Error::other("injected WAL sync failure").into());
        }
        let mut writer = file;
        writer.flush()?;
        if let Some(group) = &mut self.wal_sync_group {
            if created {
                group.record_wal_created();
            }
            return Ok(0);
        }
        let mut fsync_micros = 0;
        if self.durability == DurabilityPolicy::SyncOnEveryWrite {
            let started = std::time::Instant::now();
            file.sync_data()?;
            if created {
                sync_parent_dir(&self.wal_path)?;
            }
            fsync_micros = elapsed_micros(started);
        }
        Ok(fsync_micros)
    }

    pub(super) fn write_canonical_segments<N, R>(
        &self,
        nodes: N,
        relationships: R,
        generation: u64,
        source_commit_epoch: u64,
    ) -> Result<(DurableArtifactMetadata, DurableArtifactMetadata)>
    where
        N: IntoIterator<Item = std::result::Result<NodeRecord, CanonicalSegmentError>>,
        R: IntoIterator<Item = std::result::Result<RelRecord, CanonicalSegmentError>>,
    {
        let artifact_path = self
            .root_path
            .join(canonical_artifact_generation_file(generation));
        let property_artifact_path = self
            .root_path
            .join(property_spill_artifact_generation_file(generation));
        let property_descriptor_tree = PersistentPropertySpillDescriptorTree::new(
            GraphDescriptorTreePaths::new(
                self.root_path
                    .join(skein_storage::property_spill_descriptor_page_file(
                        generation,
                    )),
                self.root_path
                    .join(skein_storage::property_spill_descriptor_root_file(
                        generation,
                    )),
            ),
            GraphDescriptorTreeBuildConfig::default(),
        );
        let (canonical_manifest, property_spill_output) =
            CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
                .write_fallible_with_property_spills(
                    &artifact_path,
                    ManifestGeneration(generation),
                    nodes,
                    relationships,
                    PropertySpillWriteOptions {
                        artifact_path: &property_artifact_path,
                        source_commit_epoch,
                        config: PropertySpillConfig::default(),
                        descriptor_tree: property_descriptor_tree,
                    },
                )
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let property_spill_manifest = property_spill_output.manifest;
        let encoded = canonical_manifest
            .encode()
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let metadata = DurableArtifactMetadata::for_bytes(encoded.as_bytes());
        let manifest_path = self
            .root_path
            .join(canonical_manifest_generation_file(generation));
        let tmp_path = manifest_path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(encoded.as_bytes())?;
            file.sync_all()?;
        }
        durable_replace_file(&tmp_path, &manifest_path)?;
        let property_encoded = property_spill_manifest
            .encode()
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let property_manifest_path = self
            .root_path
            .join(property_spill_manifest_generation_file(generation));
        let property_tmp_path = property_manifest_path.with_extension("skein.tmp");
        {
            let mut file = File::create(&property_tmp_path)?;
            file.write_all(property_encoded.as_bytes())?;
            file.sync_all()?;
        }
        durable_replace_file(&property_tmp_path, &property_manifest_path)?;
        Ok((
            metadata,
            DurableArtifactMetadata::for_bytes(property_encoded.as_bytes()),
        ))
    }

    pub(super) fn write_canonical_adjacency<R>(
        &self,
        relationships: R,
        generation: u64,
        source_commit_epoch: u64,
        config: CanonicalAdjacencyConfig,
    ) -> Result<CanonicalAdjacencyCheckpointArtifacts>
    where
        R: IntoIterator<
            Item = std::result::Result<RelRecord, skein_storage::CanonicalAdjacencyError>,
        >,
    {
        let artifact_path = self
            .root_path
            .join(canonical_adjacency_artifact_generation_file(generation));
        let descriptor_paths = GraphDescriptorTreePaths::new(
            self.root_path
                .join(skein_storage::canonical_adjacency_descriptor_page_file(
                    generation,
                )),
            self.root_path
                .join(skein_storage::canonical_adjacency_descriptor_root_file(
                    generation,
                )),
        );
        let descriptor_config = GraphDescriptorTreeBuildConfig {
            max_page_artifact_bytes: config.max_spill_bytes,
            max_intermediate_bytes: config.max_spill_bytes,
            ..GraphDescriptorTreeBuildConfig::default()
        };
        let output = CanonicalAdjacencyWriter::new(config)
            .write_fallible_with_descriptor_tree(
                &artifact_path,
                descriptor_paths,
                ManifestGeneration(generation),
                source_commit_epoch,
                descriptor_config,
                relationships,
            )
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let descriptor_tree = output.descriptor_tree.as_ref().ok_or_else(|| {
            SkeinError::Storage(
                "canonical adjacency checkpoint omitted its descriptor root".to_string(),
            )
        })?;
        let expected_descriptor_count = output
            .report
            .sparse_block_count
            .checked_add(output.report.dense_block_count)
            .ok_or_else(|| {
                SkeinError::Storage("canonical adjacency descriptor count overflow".to_string())
            })?;
        if descriptor_tree.root.generation != generation
            || descriptor_tree.root.source_commit_epoch != source_commit_epoch
            || descriptor_tree.root.descriptor_count != expected_descriptor_count
        {
            return Err(SkeinError::Storage(
                "canonical adjacency descriptor root identity is inconsistent".to_string(),
            ));
        }
        let generation_artifacts = output.generation_artifacts().ok_or_else(|| {
            SkeinError::Storage(
                "canonical adjacency checkpoint omitted its generation binding".to_string(),
            )
        })?;
        Ok(CanonicalAdjacencyCheckpointArtifacts {
            generation: generation_artifacts,
        })
    }

    pub(super) fn write_persistent_property_projection<N>(
        &self,
        definitions: Vec<PersistentPropertyProjectionDefinition>,
        nodes: N,
        generation: u64,
        source_commit_epoch: u64,
        config: PersistentPropertyProjectionConfig,
    ) -> Result<DurableArtifactMetadata>
    where
        N: IntoIterator<
            Item = std::result::Result<
                PersistentPropertyProjectionRecord,
                skein_storage::PersistentPropertyProjectionError,
            >,
        >,
    {
        let artifact_path = self
            .root_path
            .join(property_projection_artifact_generation_file(generation));
        let descriptor_paths = GraphDescriptorTreePaths::new(
            self.root_path
                .join(skein_storage::property_projection_descriptor_page_file(
                    generation,
                )),
            self.root_path
                .join(skein_storage::property_projection_descriptor_root_file(
                    generation,
                )),
        );
        let output = PersistentPropertyProjectionWriter::new(config)
            .write_fallible(
                &artifact_path,
                ManifestGeneration(generation),
                source_commit_epoch,
                definitions,
                nodes,
                PersistentPropertyProjectionDescriptorTree::new(
                    descriptor_paths,
                    GraphDescriptorTreeBuildConfig::default(),
                ),
            )
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let descriptor_tree = &output.descriptor_tree;
        if descriptor_tree.root.kind != GraphDescriptorKind::PropertyProjection
            || descriptor_tree.root.generation != generation
            || descriptor_tree.root.source_commit_epoch != source_commit_epoch
            || descriptor_tree.root.descriptor_count != output.report.block_count
        {
            return Err(SkeinError::Storage(
                "property projection descriptor root identity is inconsistent".to_string(),
            ));
        }
        let encoded = output
            .manifest
            .encode()
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let metadata = DurableArtifactMetadata::for_bytes(encoded.as_bytes());
        let manifest_path = self
            .root_path
            .join(property_projection_manifest_generation_file(generation));
        let tmp_path = manifest_path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(encoded.as_bytes())?;
            file.sync_all()?;
        }
        durable_replace_file(&tmp_path, &manifest_path)?;
        Ok(metadata)
    }

    pub(super) fn write_relational_checkpoint(
        &self,
        state: &RelationalState,
        commit_epoch: u64,
        generation: u64,
    ) -> Result<Option<DurableArtifactMetadata>> {
        let path = self
            .root_path
            .join(relational_checkpoint_generation_file(generation));
        // Canonical metadata-only state deliberately omits database-sized row
        // and overflow residency. Its durable authority is the bound row and
        // overflow roots written by the same checkpoint publication. Encoding
        // it as a legacy full-row checkpoint would either require whole-store
        // hydration or produce an incomplete artifact whose retained overflow
        // segments appear unreachable.
        if state.is_empty() || state.canonical_row_metadata_only() {
            match fs::remove_file(&path) {
                Ok(()) => sync_parent_dir(&path)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            return Ok(None);
        }
        let max_bytes = RelationalDecodeLimits::checkpoint().max_record_bytes;
        let tmp_path = path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            encode_relational_checkpoint_to_writer(&mut file, commit_epoch, state, max_bytes)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
            file.sync_all()?;
        }
        let (encoded_len, encoded_checksum, encoded_sha256) = file_checksum(&tmp_path)?;
        let metadata = DurableArtifactMetadata {
            encoded_len,
            encoded_checksum,
            encoded_sha256,
        };
        durable_replace_file(&tmp_path, &path)?;
        Ok(Some(metadata))
    }

    pub(super) fn write_checkpoint(
        &self,
        image: CheckpointImage<'_>,
        generation: u64,
    ) -> Result<DurableArtifactMetadata> {
        validate_search_projection_checkpoint_changes(
            image.search_projection_change_log_start_epoch,
            image.commit_epoch,
            image.search_projection_graph_changes,
        )?;
        let mut body = String::new();
        body.push_str(&format!("{CHECKPOINT_HEADER_V1}\n"));
        body.push_str(&format!("version\t{STORAGE_VERSION}\n"));
        body.push_str(&format!("generation\t{generation}\n"));
        body.push_str(&format!("commit_epoch\t{}\n", image.commit_epoch));
        if let Some(relational) = image.relational_checkpoint {
            body.push_str(&format!(
                "relational_checkpoint_encoded_len\t{}\n",
                relational.encoded_len
            ));
            body.push_str(&format!(
                "relational_checkpoint_encoded_checksum\t{}\n",
                relational.encoded_checksum
            ));
            body.push_str(&format!(
                "relational_checkpoint_encoded_sha256\t{}\n",
                relational.encoded_sha256
            ));
        }
        body.push_str(&format!("next_node_id\t{}\n", image.next_node_id));
        body.push_str(&format!("next_rel_id\t{}\n", image.next_rel_id));
        body.push_str("canonical_records\ttrue\n");
        body.push_str(&format!(
            "search_projection_change_log_start_epoch\t{}\n",
            image.search_projection_change_log_start_epoch
        ));
        if let Some(source_fingerprint) = image.initial_import_source_fingerprint {
            body.push_str(&format!(
                "initial_import_source_fingerprint\t{}\n",
                encode_string(source_fingerprint)
            ));
        }
        for change in image.search_projection_graph_changes {
            let (relational_kind, relational_changes) =
                encode_search_projection_relational_primary_key_changes(
                    &change.relational_primary_key_changes,
                )?;
            body.push_str(&format!(
                "search_projection_change\t{}\t{}\t{}\t{}\t{}\n",
                change.commit_epoch,
                encode_u64_vec(change.upsert_node_ids.iter().copied()),
                encode_string_vec(&change.delete_document_ids),
                relational_kind,
                relational_changes,
            ));
        }
        for label in image.catalog.labels() {
            if !label.name.is_empty() {
                body.push_str(&format!(
                    "label\t{}\t{}\n",
                    label.id.0,
                    encode_string(&label.name)
                ));
            }
        }
        for rel_type in image.catalog.rel_types() {
            if !rel_type.name.is_empty() {
                body.push_str(&format!(
                    "rel_type\t{}\t{}\n",
                    rel_type.id.0,
                    encode_string(&rel_type.name)
                ));
            }
        }
        for index in image.catalog.property_indexes() {
            body.push_str(&format!(
                "property_index\t{}\t{}\t{}\t{}\n",
                index.id.0,
                index.label_id.0,
                encode_string(&index.property),
                encode_index_kind(index.kind)
            ));
        }
        for index in image.catalog.composite_property_indexes() {
            body.push_str(&format!(
                "composite_property_index\t{}\t{}\t{}\n",
                index.id.0,
                index.label_id.0,
                encode_string_vec(&index.properties)
            ));
        }
        for table in image.catalog.table_descriptors() {
            body.push_str(&format!(
                "table\t{}\t{}\t{}\t{}\n",
                table.id.0,
                encode_table_kind(table.kind),
                encode_string(&table.name),
                encode_schema_object_state(table.state)
            ));
        }
        for property in image.catalog.property_descriptors() {
            body.push_str(&format!(
                "property\t{}\t{}\t{}\t{}\t{}\t{}\n",
                property.id.0,
                property.table_id.0,
                encode_string(&property.name),
                encode_property_type(property.value_type),
                encode_nullable(property.nullable),
                encode_schema_object_state(property.state)
            ));
        }
        for constraint in image.catalog.unique_constraints() {
            let crate::schema::ConstraintSubject::Node(label_id) = constraint.subject else {
                continue;
            };
            body.push_str(&format!(
                "unique_constraint\t{}\t{}\t{}\n",
                constraint.id.0,
                label_id.0,
                encode_string(&constraint.property)
            ));
        }
        for constraint in image.catalog.node_property_exists_constraints() {
            let crate::schema::ConstraintSubject::Node(label_id) = constraint.subject else {
                continue;
            };
            body.push_str(&format!(
                "node_property_exists_constraint\t{}\t{}\t{}\n",
                constraint.id.0,
                label_id.0,
                encode_string(&constraint.property)
            ));
        }
        for constraint in image.catalog.relationship_property_exists_constraints() {
            let crate::schema::ConstraintSubject::Relationship(rel_type_id) = constraint.subject
            else {
                continue;
            };
            body.push_str(&format!(
                "relationship_property_exists_constraint\t{}\t{}\t{}\n",
                constraint.id.0,
                rel_type_id.0,
                encode_string(&constraint.property)
            ));
        }
        for constraint in image.catalog.relationship_unique_constraints() {
            let crate::schema::ConstraintSubject::Relationship(rel_type_id) = constraint.subject
            else {
                continue;
            };
            body.push_str(&format!(
                "relationship_unique_constraint\t{}\t{}\t{}\n",
                constraint.id.0,
                rel_type_id.0,
                encode_string(&constraint.property)
            ));
        }
        let statistics = image.statistics;
        body.push_str(&format!(
            "stat_commit_epoch\t{}\n",
            statistics.computed_at_commit_epoch
        ));
        body.push_str(&format!(
            "stat_advanced_complete\t{}\n",
            statistics.advanced_statistics_complete
        ));
        body.push_str(&format!(
            "stat_histogram_sample_limit\t{}\n",
            statistics.histogram_sample_limit
        ));
        body.push_str(&format!("stat_node_count\t{}\n", statistics.node_count));
        body.push_str(&format!(
            "stat_relationship_count\t{}\n",
            statistics.relationship_count
        ));
        for (label_id, count) in &statistics.label_counts {
            body.push_str(&format!("stat_label_count\t{}\t{}\n", label_id.0, count));
        }
        for (rel_type_id, count) in &statistics.rel_type_counts {
            body.push_str(&format!(
                "stat_rel_type_count\t{}\t{}\n",
                rel_type_id.0, count
            ));
        }
        for (rel_type_id, count) in &statistics.rel_type_source_counts {
            body.push_str(&format!(
                "stat_rel_type_source_count\t{}\t{}\n",
                rel_type_id.0, count
            ));
        }
        for (rel_type_id, count) in &statistics.rel_type_target_counts {
            body.push_str(&format!(
                "stat_rel_type_target_count\t{}\t{}\n",
                rel_type_id.0, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id), count) in &statistics.path_counts {
            body.push_str(&format!(
                "stat_path_count\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id), count) in
            &statistics.path_source_distinct_counts
        {
            body.push_str(&format!(
                "stat_path_source_distinct_count\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id), count) in
            &statistics.path_target_distinct_counts
        {
            body.push_str(&format!(
                "stat_path_target_distinct_count\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id, hops), count) in
            &statistics.bounded_path_counts
        {
            body.push_str(&format!(
                "stat_bounded_path_count\t{}\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id, hops), count) in
            &statistics.bounded_path_source_distinct_counts
        {
            body.push_str(&format!(
                "stat_bounded_path_source_distinct_count\t{}\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
            ));
        }
        for ((source_label_id, rel_type_id, target_label_id, hops), count) in
            &statistics.bounded_path_target_distinct_counts
        {
            body.push_str(&format!(
                "stat_bounded_path_target_distinct_count\t{}\t{}\t{}\t{}\t{}\n",
                source_label_id.0, rel_type_id.0, target_label_id.0, hops, count
            ));
        }
        for (index_id, sample) in &statistics.index_samples {
            body.push_str(&format!(
                "stat_index_sample\t{}\t{}\t{}\t{}\t{}\n",
                index_id.0,
                sample.index_size,
                sample.unique_values,
                sample.sample_size,
                sample.updates_since_sample
            ));
        }
        for ((label_id, property), count) in &statistics.property_distinct_counts {
            body.push_str(&format!(
                "stat_property_distinct_count\t{}\t{}\t{}\n",
                label_id.0,
                encode_string(property),
                count
            ));
        }
        for ((rel_type_id, property), count) in &statistics.rel_property_distinct_counts {
            body.push_str(&format!(
                "stat_rel_property_distinct_count\t{}\t{}\t{}\n",
                rel_type_id.0,
                encode_string(property),
                count
            ));
        }
        for ((rel_type_id, property), values) in &statistics.rel_property_histograms {
            body.push_str(&format!(
                "stat_rel_property_histogram\t{}\t{}\t{}\n",
                rel_type_id.0,
                encode_string(property),
                encode_value_vec(values)
            ));
        }
        for ((rel_type_id, property), sampled) in &statistics.sampled_rel_property_histograms {
            body.push_str(&format!(
                "stat_rel_property_histogram_sampled\t{}\t{}\t{}\n",
                rel_type_id.0,
                encode_string(property),
                encode_bool(*sampled)
            ));
        }
        for ((label_id, property), values) in &statistics.property_histograms {
            body.push_str(&format!(
                "stat_property_histogram\t{}\t{}\t{}\n",
                label_id.0,
                encode_string(property),
                encode_value_vec(values)
            ));
        }
        for ((label_id, property), sampled) in &statistics.sampled_property_histograms {
            body.push_str(&format!(
                "stat_property_histogram_sampled\t{}\t{}\t{}\n",
                label_id.0,
                encode_string(property),
                encode_bool(*sampled)
            ));
        }
        for (name, definition) in image.projected_graphs {
            body.push_str(&format!(
                "project_graph\t{}\t{}\t{}\n",
                encode_string(name),
                encode_string_vec(&definition.node_labels),
                encode_string_vec(&definition.rel_types)
            ));
        }
        let checksum = checksum_bytes(body.as_bytes());
        let data = format!("{body}checksum\t{checksum}\n");
        let checkpoint_path = self.root_path.join(checkpoint_generation_file(generation));
        let tmp_path = checkpoint_path.with_extension("skein.tmp");
        let encoded = encode_durable_text(&data, DurableCompression::default())?;
        let metadata = DurableArtifactMetadata::for_bytes(&encoded);
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&encoded)?;
            file.sync_all()?;
        }
        durable_replace_file(&tmp_path, &checkpoint_path)?;
        Ok(metadata)
    }

    pub(super) fn write_projected_graph_artifacts(&self, body: &str) -> Result<()> {
        self.write_projected_graph_artifacts_to(&self.projected_graphs_path, body)
    }

    pub(super) fn write_projected_graph_artifacts_to(&self, path: &Path, body: &str) -> Result<()> {
        let checksum = checksum_bytes(body.as_bytes());
        let data = format!("{body}checksum\t{checksum}\n");
        let tmp_path = path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            let encoded = encode_durable_text(&data, DurableCompression::default())?;
            file.write_all(&encoded)?;
            file.sync_all()?;
        }
        durable_replace_file(&tmp_path, path)?;
        Ok(())
    }

    pub(super) fn prepare_checkpoint_staging(&self, generation: u64) -> Result<PathBuf> {
        let staging_path = self
            .root_path
            .join(format!(".checkpoint.{generation}.prepare"));
        if staging_path.exists() {
            fs::remove_dir_all(&staging_path)?;
        }
        fs::create_dir(&staging_path)?;
        Ok(staging_path)
    }

    pub(super) fn publish_checkpoint_sidecars(
        &self,
        staging_path: &Path,
        publish_projected_graph_artifacts: bool,
        source_scan_publication: Option<source_scan::SourceScanPublication>,
    ) -> Result<()> {
        if publish_projected_graph_artifacts {
            durable_replace_file(
                &staging_path.join(PROJECTED_GRAPHS_FILE),
                &self.projected_graphs_path,
            )?;
        } else {
            self.remove_projected_graph_artifacts()?;
        }
        if source_scan_publication.is_some() {
            for file in [
                source_scan::SOURCE_SCAN_PAYLOAD_FILE,
                source_scan::SOURCE_SCAN_DESCRIPTOR_FILE,
            ] {
                durable_replace_file(&staging_path.join(file), &self.root_path.join(file))?;
            }
        } else {
            remove_source_scan_artifacts(&self.root_path)?;
        }
        match fs::remove_dir(staging_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn discard_prepared_checkpoint(
        &self,
        generation: u64,
        staging_path: &Path,
    ) -> Result<()> {
        for file in [
            checkpoint_generation_file(generation),
            relational_checkpoint_generation_file(generation),
            wal_generation_file(generation),
            canonical_artifact_generation_file(generation),
            canonical_manifest_generation_file(generation),
            skein_storage::canonical_segment_descriptor_page_file(generation),
            skein_storage::canonical_segment_descriptor_root_file(generation),
            canonical_adjacency_artifact_generation_file(generation),
            skein_storage::canonical_adjacency_descriptor_page_file(generation),
            skein_storage::canonical_adjacency_descriptor_root_file(generation),
            property_spill_artifact_generation_file(generation),
            property_spill_manifest_generation_file(generation),
            skein_storage::property_spill_descriptor_page_file(generation),
            skein_storage::property_spill_descriptor_root_file(generation),
            property_projection_artifact_generation_file(generation),
            property_projection_manifest_generation_file(generation),
            skein_storage::property_projection_descriptor_page_file(generation),
            skein_storage::property_projection_descriptor_root_file(generation),
            skein_storage::relational_index_shadow_artifact_file(generation),
            skein_storage::relational_index_shadow_manifest_generation_file(generation),
            skein_storage::relational_row_page_artifact_file(generation),
            skein_storage::relational_row_page_root_descriptor_file(generation),
            skein_storage::relational_row_page_root_key_file(generation),
            skein_storage::relational_row_page_manifest_generation_file(generation),
            skein_storage::relational_overflow_extent_file(generation),
            skein_storage::relational_overflow_descriptor_file(generation),
            skein_storage::relational_overflow_manifest_generation_file(generation),
        ] {
            match fs::remove_file(self.root_path.join(file)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        match fs::remove_dir_all(staging_path) {
            Ok(()) => sync_parent_dir(&self.manifest_path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn remove_projected_graph_artifacts(&self) -> Result<()> {
        match fs::remove_file(&self.projected_graphs_path) {
            Ok(()) => sync_parent_dir(&self.projected_graphs_path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn load_projected_graph_artifacts(
        &self,
    ) -> Result<BTreeMap<String, ProjectedGraphArtifact>> {
        if !self.projected_graphs_path.exists() {
            return Ok(BTreeMap::new());
        }
        let artifacts = read_durable_text(&self.projected_graphs_path, "projected graph artifact")
            .and_then(|text| {
                let (body, checksum) = split_projected_graph_artifact_checksum(&text)?;
                let actual = checksum_bytes(body.as_bytes());
                if checksum != actual {
                    return Err(SkeinError::Storage(format!(
                        "projected graph artifact checksum mismatch: expected {checksum}, got {actual}"
                    )));
                }
                decode_projected_graph_artifacts(body).map(|(_, artifacts)| artifacts)
            });
        match artifacts {
            Ok(artifacts) => Ok(artifacts),
            Err(_) => {
                if !self.read_only {
                    fs::remove_file(&self.projected_graphs_path)?;
                    sync_parent_dir(&self.projected_graphs_path)?;
                }
                Ok(BTreeMap::new())
            }
        }
    }

    pub(super) fn write_stable_id_mapping(
        &mut self,
        mapping: &StoreStableIdMapping,
        covered_commit_epoch: u64,
    ) -> Result<()> {
        let entries = mapping
            .node_stable_ids
            .iter()
            .map(|(id, value)| (StableIdentityKey::node(id.0), value))
            .chain(
                mapping
                    .relationship_stable_ids
                    .iter()
                    .map(|(id, value)| (StableIdentityKey::relationship(id.0), value)),
            );
        StableIdentityMappingWriter::publish(
            &self.stable_id_mapping_path,
            covered_commit_epoch,
            entries,
            StableIdentityMappingConfig::default(),
        )
        .map_err(stable_identity_error)?;
        self.open_stable_id_mapping_reader()
    }

    pub(super) fn load_stable_id_mapping(&mut self) -> Result<()> {
        if !self.stable_id_mapping_path.exists() {
            self.stable_id_mapping_reader = None;
            return Ok(());
        }
        self.open_stable_id_mapping_reader()
    }

    pub(super) fn materialize_stable_id_mapping(&self) -> Result<StoreStableIdMapping> {
        self.stable_id_mapping_reader.as_ref().map_or_else(
            || Ok(StoreStableIdMapping::default()),
            |reader| {
                reader
                    .materialize(StableIdentityMaterializeLimits::default())
                    .map(|(mapping, _)| mapping)
                    .map_err(stable_identity_error)
            },
        )
    }

    fn open_stable_id_mapping_reader(&mut self) -> Result<()> {
        let reader = StableIdentityMappingReader::open_with_cache(
            &self.stable_id_mapping_path,
            StableIdentityMappingConfig::default(),
            Arc::clone(&self.segment_cache),
            self.store_id,
        )
        .map_err(stable_identity_error)?;
        self.stable_id_mapping_reader = Some(Arc::new(reader));
        Ok(())
    }

    pub(super) fn open_bound_relational_overflow(
        &self,
    ) -> Result<skein_storage::RelationalOverflowRootReader> {
        let binding = self
            .relational_overflow_generation_artifacts
            .ok_or_else(|| {
                SkeinError::Storage(
                    "published checkpoint has no relational overflow generation binding"
                        .to_string(),
                )
            })?;
        let reader = skein_storage::RelationalOverflowRootReader::open_bound_generation(
            &self.root_path,
            binding,
            skein_storage::RelationalOverflowPublicationConfig::default(),
        )
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
        Ok(reader)
    }

    pub(super) fn open_bound_relational_row_pages(
        &self,
        overflow_root: &skein_storage::RelationalOverflowRootReader,
    ) -> Result<skein_storage::RelationalRowPageRootReader> {
        let binding = self.relational_row_generation_artifacts.ok_or_else(|| {
            SkeinError::Storage(
                "published checkpoint has no relational row-page generation binding".to_string(),
            )
        })?;
        let reader = skein_storage::RelationalRowPageRootReader::open_bound_generation(
            &self.root_path,
            binding,
            skein_storage::RelationalRowPagePublicationConfig::default(),
        )
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let manifest = reader.manifest();
        if manifest.source_commit_epoch != binding.source_commit_epoch
            || manifest.root_set_digest != binding.root_set_digest
        {
            return Err(SkeinError::Storage(
                "relational row-page generation identity differs from canonical binding"
                    .to_string(),
            ));
        }
        if manifest.overflow_root.is_none() {
            return Err(SkeinError::Storage(
                "canonical relational row-page generation has no overflow binding".to_string(),
            ));
        }
        reader
            .validate_overflow_root(overflow_root)
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        Ok(reader)
    }

    pub(super) fn prepare_wal_generation(&self, generation: u64) -> Result<()> {
        // New WAL generations always use the binary format; an existing
        // text database therefore upgrades at its next checkpoint.
        let wal_path = self.root_path.join(wal_generation_file(generation));
        let tmp_path = wal_path.with_extension("skein.tmp");
        let header = encode_binary_wal_header(generation, self.next_lsn);
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&header)?;
            file.sync_all()?;
        }
        durable_replace_file(&tmp_path, &wal_path)?;
        Ok(())
    }

    pub(super) fn publish_checkpoint_manifest(
        &mut self,
        generation: u64,
        artifacts: CheckpointManifestArtifacts,
        checkpoint_commit_epoch: u64,
        oldest_reader_commit_epoch: Option<u64>,
        source_scan_publication: Option<source_scan::SourceScanPublication>,
    ) -> Result<()> {
        self.admit_graph_manifest_artifacts(&artifacts)?;
        let safe_reclaim_commit_epoch =
            safe_reclaim_commit_epoch(checkpoint_commit_epoch, oldest_reader_commit_epoch);
        let source_scan_commit_epoch = source_scan_publication.map(|value| value.graph_epoch());
        let source_scan_descriptor_checksum =
            source_scan_publication.map(|value| value.descriptor_checksum());
        let CheckpointManifestArtifacts {
            checkpoint,
            relational_checkpoint,
            canonical_manifest,
            canonical_adjacency,
            property_spill_manifest,
            property_projection_manifest,
            relational_row,
            relational_overflow,
            relational_index,
            append,
        } = artifacts;
        let manifest = DurableManifest {
            checkpoint_generation: Some(generation),
            checkpoint_encoded_len: Some(checkpoint.encoded_len),
            checkpoint_encoded_checksum: Some(checkpoint.encoded_checksum),
            checkpoint_encoded_sha256: Some(checkpoint.encoded_sha256),
            canonical_manifest_encoded_len: Some(canonical_manifest.encoded_len),
            canonical_manifest_encoded_checksum: Some(canonical_manifest.encoded_checksum),
            canonical_manifest_encoded_sha256: Some(canonical_manifest.encoded_sha256),
            canonical_adjacency_generation_artifacts: Some(canonical_adjacency.generation),
            property_spill_manifest_encoded_len: Some(property_spill_manifest.encoded_len),
            property_spill_manifest_encoded_checksum: Some(
                property_spill_manifest.encoded_checksum,
            ),
            property_spill_manifest_encoded_sha256: Some(property_spill_manifest.encoded_sha256),
            property_projection_manifest_encoded_len: Some(
                property_projection_manifest.encoded_len,
            ),
            property_projection_manifest_encoded_checksum: Some(
                property_projection_manifest.encoded_checksum,
            ),
            property_projection_manifest_encoded_sha256: Some(
                property_projection_manifest.encoded_sha256,
            ),
            relational_row_generation_artifacts: Some(relational_row),
            relational_overflow_generation_artifacts: Some(relational_overflow),
            relational_index_generation_artifacts: relational_index,
            append_generation_artifacts: Some(append),
            wal_generation: generation,
            checkpoint_epoch: generation,
            checkpoint_commit_epoch,
            oldest_reader_commit_epoch,
            safe_reclaim_commit_epoch,
            wal_replay_start_lsn: self.next_lsn,
            next_lsn: self.next_lsn,
            source_scan_commit_epoch,
            source_scan_descriptor_checksum,
        };
        manifest.validate()?;
        manifest.write(&self.manifest_path)?;
        checkpoint_publish_failpoint(CheckpointPublishStage::ManifestPublished)?;

        self.wal_append_file = None;
        self.checkpoint_path = manifest.checkpoint_path(&self.root_path);
        self.wal_path = manifest.wal_path(&self.root_path);
        self.checkpoint_encoded_len = manifest.checkpoint_encoded_len;
        self.checkpoint_encoded_checksum = manifest.checkpoint_encoded_checksum;
        self.checkpoint_encoded_sha256 = manifest.checkpoint_encoded_sha256;
        self.relational_checkpoint_encoded_len =
            relational_checkpoint.map(|artifact| artifact.encoded_len);
        self.relational_checkpoint_encoded_checksum =
            relational_checkpoint.map(|artifact| artifact.encoded_checksum);
        self.relational_checkpoint_encoded_sha256 =
            relational_checkpoint.map(|artifact| artifact.encoded_sha256);
        self.canonical_manifest_encoded_len = manifest.canonical_manifest_encoded_len;
        self.canonical_manifest_encoded_checksum = manifest.canonical_manifest_encoded_checksum;
        self.canonical_manifest_encoded_sha256 = manifest.canonical_manifest_encoded_sha256;
        self.canonical_adjacency_generation_artifacts =
            manifest.canonical_adjacency_generation_artifacts;
        self.property_spill_manifest_encoded_len = manifest.property_spill_manifest_encoded_len;
        self.property_spill_manifest_encoded_checksum =
            manifest.property_spill_manifest_encoded_checksum;
        self.property_spill_manifest_encoded_sha256 =
            manifest.property_spill_manifest_encoded_sha256;
        self.property_projection_manifest_encoded_len =
            manifest.property_projection_manifest_encoded_len;
        self.property_projection_manifest_encoded_checksum =
            manifest.property_projection_manifest_encoded_checksum;
        self.property_projection_manifest_encoded_sha256 =
            manifest.property_projection_manifest_encoded_sha256;
        self.relational_row_generation_artifacts = manifest.relational_row_generation_artifacts;
        self.relational_overflow_generation_artifacts =
            manifest.relational_overflow_generation_artifacts;
        self.relational_index_generation_artifacts = manifest.relational_index_generation_artifacts;
        self.append_generation_artifacts = manifest.append_generation_artifacts;
        self.wal_generation = manifest.wal_generation;
        self.checkpoint_epoch = manifest.checkpoint_epoch;
        self.checkpoint_commit_epoch = manifest.checkpoint_commit_epoch;
        self.oldest_reader_commit_epoch = manifest.oldest_reader_commit_epoch;
        self.safe_reclaim_commit_epoch = manifest.safe_reclaim_commit_epoch;
        self.wal_replay_start_lsn = manifest.wal_replay_start_lsn;
        self.wal_bytes = fs::metadata(&self.wal_path)?.len();
        self.wal_commit_epoch = manifest.checkpoint_commit_epoch;
        self.wal_free_space_probe.last_available_bytes = None;
        self.wal_free_space_probe.wal_bytes_since_probe = 0;
        self.source_scan_commit_epoch = manifest.source_scan_commit_epoch;
        self.source_scan_descriptor_checksum = manifest.source_scan_descriptor_checksum;
        let mut graph_manifest_budget =
            GraphManifestOpenBudget::new(self.max_graph_manifest_open_bytes);
        self.canonical_segments = load_published_canonical_segments(
            &self.root_path,
            manifest,
            Arc::clone(&self.segment_cache),
            self.store_id,
            &mut graph_manifest_budget,
        )?;
        self.canonical_adjacency = load_published_canonical_adjacency(
            &self.root_path,
            manifest,
            Arc::clone(&self.segment_cache),
            self.store_id,
            &mut graph_manifest_budget,
        )?;
        self.persistent_property_projection = load_published_property_projection(
            &self.root_path,
            manifest,
            Arc::clone(&self.segment_cache),
            self.store_id,
            &mut graph_manifest_budget,
        )?;
        if let (Some(canonical), Some(adjacency)) =
            (&self.canonical_segments, &self.canonical_adjacency)
            && canonical.manifest().relationship_count != adjacency.relationship_count()
        {
            return Err(SkeinError::Storage(
                "canonical adjacency relationship count does not match canonical segments"
                    .to_string(),
            ));
        }
        self.source_scan_reader = FileSegmentRangeReader::new().with_cache(
            Arc::clone(&self.segment_cache),
            self.store_id,
            ManifestGeneration(generation),
        );
        self.source_scan_reader.register(
            source_scan::SOURCE_SCAN_ARTIFACT_ID,
            self.root_path.join(source_scan::SOURCE_SCAN_PAYLOAD_FILE),
        );
        Ok(())
    }

    pub(super) fn reclaim_old_generations(
        &mut self,
        current_generation: u64,
        pinned_reader_generations: Option<&BTreeSet<u64>>,
    ) {
        self.generation_reclamation_debt =
            self.try_reclaim_old_generations(current_generation, pinned_reader_generations);
    }

    fn try_reclaim_old_generations(
        &self,
        current_generation: u64,
        pinned_reader_generations: Option<&BTreeSet<u64>>,
    ) -> GenerationReclamationDebt {
        let mut debt = GenerationReclamationDebt::default();
        if self.oldest_reader_commit_epoch.is_some() && pinned_reader_generations.is_none() {
            return debt;
        }

        let mut retained_generations = pinned_reader_generations.cloned().unwrap_or_default();
        retained_generations.insert(current_generation);
        if current_generation > 1 {
            retained_generations.insert(current_generation - 1);
        }
        let (retained_row_page_generations, retained_overflow_extent_generations) =
            match self.retained_relational_physical_generations(&retained_generations) {
                Ok(generations) => generations,
                Err(_) => {
                    debt.retry_required = true;
                    return debt;
                }
            };
        let retained_append_segment_generations =
            match self.retained_append_physical_generations(&retained_generations) {
                Ok(generations) => generations,
                Err(_) => {
                    debt.retry_required = true;
                    return debt;
                }
            };
        let entries = match fs::read_dir(&self.root_path) {
            Ok(entries) => entries,
            Err(_) => {
                debt.retry_required = true;
                return debt;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    debt.retry_required = true;
                    continue;
                }
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let generation = storage_generation_for_file(name);
            if generation.is_some_and(|generation| {
                generation < current_generation && !retained_generations.contains(&generation)
            }) {
                if parse_relational_row_page_artifact_generation_file(name)
                    .is_some_and(|generation| retained_row_page_generations.contains(&generation))
                    || parse_relational_overflow_extent_generation_file(name).is_some_and(
                        |generation| retained_overflow_extent_generations.contains(&generation),
                    )
                    || parse_append_segment_generation_file(name).is_some_and(|generation| {
                        retained_append_segment_generations.contains(&generation)
                    })
                {
                    continue;
                }
                let pending_bytes = entry.metadata().map_or(0, |metadata| metadata.len());
                if remove_generation_reclamation_candidate(&entry.path()).is_err() {
                    debt.retry_required = true;
                    debt.pending_file_count = debt.pending_file_count.saturating_add(1);
                    debt.pending_bytes = debt.pending_bytes.saturating_add(pending_bytes);
                }
            }
        }
        if sync_parent_dir(&self.manifest_path).is_err() {
            debt.retry_required = true;
        }
        debt
    }

    fn retained_relational_physical_generations(
        &self,
        retained_generations: &BTreeSet<u64>,
    ) -> Result<(BTreeSet<u64>, BTreeSet<u64>)> {
        let mut row_page_generations = BTreeSet::new();
        let mut overflow_extent_generations = BTreeSet::new();

        for &generation in retained_generations {
            let overflow_manifest =
                self.root_path
                    .join(skein_storage::relational_overflow_manifest_generation_file(
                        generation,
                    ));
            if overflow_manifest.exists() {
                let overflow = skein_storage::RelationalOverflowRootReader::open_generation(
                    &self.root_path,
                    generation,
                    skein_storage::RelationalOverflowPublicationConfig::default(),
                )
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
                overflow
                    .visit_descriptors(|descriptor| {
                        overflow_extent_generations.insert(descriptor.physical_generation);
                        Ok(())
                    })
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
            }

            let row_manifest =
                self.root_path
                    .join(skein_storage::relational_row_page_manifest_generation_file(
                        generation,
                    ));
            if row_manifest.exists() {
                let rows = skein_storage::RelationalRowPageRootReader::open_generation(
                    &self.root_path,
                    generation,
                    skein_storage::RelationalRowPagePublicationConfig::default(),
                )
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
                let tables = rows
                    .manifest()
                    .tables
                    .iter()
                    .map(|table| table.table.clone())
                    .collect::<Vec<_>>();
                for table in tables {
                    rows.visit_table_pages(&table, |descriptor| {
                        row_page_generations.insert(descriptor.physical_generation);
                        Ok(())
                    })
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
                }
            }
        }

        Ok((row_page_generations, overflow_extent_generations))
    }

    fn retained_append_physical_generations(
        &self,
        retained_generations: &BTreeSet<u64>,
    ) -> Result<BTreeSet<u64>> {
        let mut segment_generations = BTreeSet::new();
        for &generation in retained_generations {
            let path = self
                .root_path
                .join(append_generation_manifest_file(generation));
            if !path.exists() {
                continue;
            }
            let manifest = AppendGenerationManifest::read_generation(
                &self.root_path,
                generation,
                AppendPublicationConfig::default(),
            )
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
            segment_generations.extend(manifest.segments.iter().map(|segment| segment.generation));
        }
        Ok(segment_generations)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct DurableManifest {
    pub(super) checkpoint_generation: Option<u64>,
    pub(super) checkpoint_encoded_len: Option<u64>,
    pub(super) checkpoint_encoded_checksum: Option<u64>,
    pub(super) checkpoint_encoded_sha256: Option<Sha256Digest>,
    pub(super) canonical_manifest_encoded_len: Option<u64>,
    pub(super) canonical_manifest_encoded_checksum: Option<u64>,
    pub(super) canonical_manifest_encoded_sha256: Option<Sha256Digest>,
    pub(super) canonical_adjacency_generation_artifacts:
        Option<CanonicalAdjacencyGenerationArtifacts>,
    pub(super) property_spill_manifest_encoded_len: Option<u64>,
    pub(super) property_spill_manifest_encoded_checksum: Option<u64>,
    pub(super) property_spill_manifest_encoded_sha256: Option<Sha256Digest>,
    pub(super) property_projection_manifest_encoded_len: Option<u64>,
    pub(super) property_projection_manifest_encoded_checksum: Option<u64>,
    pub(super) property_projection_manifest_encoded_sha256: Option<Sha256Digest>,
    pub(super) relational_row_generation_artifacts: Option<RelationalRowPageGenerationArtifacts>,
    pub(super) relational_overflow_generation_artifacts:
        Option<RelationalOverflowGenerationArtifacts>,
    pub(super) relational_index_generation_artifacts: Option<RelationalIndexGenerationArtifacts>,
    pub(super) append_generation_artifacts: Option<AppendGenerationArtifacts>,
    pub(super) wal_generation: u64,
    pub(super) checkpoint_epoch: u64,
    pub(super) checkpoint_commit_epoch: u64,
    oldest_reader_commit_epoch: Option<u64>,
    safe_reclaim_commit_epoch: u64,
    pub(super) wal_replay_start_lsn: u64,
    next_lsn: u64,
    source_scan_commit_epoch: Option<u64>,
    source_scan_descriptor_checksum: Option<u64>,
}

#[derive(Default)]
struct RelationalIndexManifestFields {
    generation: Option<u64>,
    source_commit_epoch: Option<u64>,
    catalog_schema_digest: Option<Sha256Digest>,
    root_set_digest: Option<Sha256Digest>,
    page_encoded_len: Option<u64>,
    page_encoded_checksum: Option<u64>,
    page_encoded_sha256: Option<Sha256Digest>,
    manifest_encoded_len: Option<u64>,
    manifest_encoded_checksum: Option<u64>,
    manifest_encoded_sha256: Option<Sha256Digest>,
}

#[derive(Default)]
struct RelationalRootManifestFields {
    generation: Option<u64>,
    source_commit_epoch: Option<u64>,
    root_set_digest: Option<Sha256Digest>,
    manifest_encoded_len: Option<u64>,
    manifest_encoded_checksum: Option<u64>,
    manifest_encoded_sha256: Option<Sha256Digest>,
}

#[derive(Default)]
struct CanonicalAdjacencyGenerationFields {
    generation: Option<u64>,
    source_commit_epoch: Option<u64>,
    relationship_count: Option<u64>,
    entry_count: Option<u64>,
    artifact_encoded_len: Option<u64>,
    artifact_encoded_crc32c: Option<u64>,
    artifact_encoded_sha256: Option<Sha256Digest>,
    descriptor_root_encoded_len: Option<u64>,
    descriptor_root_encoded_crc32c: Option<u64>,
    descriptor_root_encoded_sha256: Option<Sha256Digest>,
}

impl CanonicalAdjacencyGenerationFields {
    fn finish(self) -> Result<Option<CanonicalAdjacencyGenerationArtifacts>> {
        let presence = [
            self.generation.is_some(),
            self.source_commit_epoch.is_some(),
            self.relationship_count.is_some(),
            self.entry_count.is_some(),
            self.artifact_encoded_len.is_some(),
            self.artifact_encoded_crc32c.is_some(),
            self.artifact_encoded_sha256.is_some(),
            self.descriptor_root_encoded_len.is_some(),
            self.descriptor_root_encoded_crc32c.is_some(),
            self.descriptor_root_encoded_sha256.is_some(),
        ];
        if presence.iter().all(|present| !present) {
            return Ok(None);
        }
        if !presence.iter().all(|present| *present) {
            return Err(SkeinError::Storage(
                "manifest canonical adjacency generation binding is incomplete".to_string(),
            ));
        }
        Ok(Some(CanonicalAdjacencyGenerationArtifacts {
            generation: self.generation.expect("complete binding has generation"),
            source_commit_epoch: self
                .source_commit_epoch
                .expect("complete binding has source commit epoch"),
            relationship_count: self
                .relationship_count
                .expect("complete binding has relationship count"),
            entry_count: self.entry_count.expect("complete binding has entry count"),
            adjacency_artifact: CanonicalAdjacencyArtifactMetadata {
                encoded_len: self
                    .artifact_encoded_len
                    .expect("complete binding has adjacency artifact length"),
                encoded_crc32c: self
                    .artifact_encoded_crc32c
                    .expect("complete binding has adjacency artifact CRC32C"),
                encoded_sha256: self
                    .artifact_encoded_sha256
                    .expect("complete binding has adjacency artifact SHA-256"),
            },
            descriptor_root_artifact: GraphDescriptorTreeArtifactMetadata {
                encoded_len: self
                    .descriptor_root_encoded_len
                    .expect("complete binding has descriptor root length"),
                encoded_crc32c: u32::try_from(
                    self.descriptor_root_encoded_crc32c
                        .expect("complete binding has descriptor root CRC32C"),
                )
                .map_err(|_| {
                    SkeinError::Storage(
                        "canonical adjacency descriptor root checksum exceeds CRC32C range"
                            .to_string(),
                    )
                })?,
                encoded_sha256: self
                    .descriptor_root_encoded_sha256
                    .expect("complete binding has descriptor root SHA-256"),
            },
        }))
    }
}

impl RelationalRootManifestFields {
    fn presence(&self) -> [bool; 6] {
        [
            self.generation.is_some(),
            self.source_commit_epoch.is_some(),
            self.root_set_digest.is_some(),
            self.manifest_encoded_len.is_some(),
            self.manifest_encoded_checksum.is_some(),
            self.manifest_encoded_sha256.is_some(),
        ]
    }

    fn require_complete(&self, artifact: &str) -> Result<bool> {
        let presence = self.presence();
        if presence.iter().all(|present| !present) {
            return Ok(false);
        }
        if !presence.iter().all(|present| *present) {
            return Err(SkeinError::Storage(format!(
                "manifest {artifact} generation binding is incomplete"
            )));
        }
        Ok(true)
    }

    fn finish_row(self) -> Result<Option<RelationalRowPageGenerationArtifacts>> {
        if !self.require_complete("relational row-page")? {
            return Ok(None);
        }
        Ok(Some(RelationalRowPageGenerationArtifacts {
            generation: self.generation.expect("complete binding has generation"),
            source_commit_epoch: self
                .source_commit_epoch
                .expect("complete binding has source commit epoch"),
            root_set_digest: self
                .root_set_digest
                .expect("complete binding has root-set digest"),
            manifest_artifact: RelationalRowPageArtifactMetadata {
                encoded_len: self
                    .manifest_encoded_len
                    .expect("complete binding has manifest length"),
                encoded_crc32c: u32::try_from(
                    self.manifest_encoded_checksum
                        .expect("complete binding has manifest checksum"),
                )
                .map_err(|_| {
                    SkeinError::Storage(
                        "relational row-page manifest checksum exceeds CRC32C range".to_string(),
                    )
                })?,
                encoded_sha256: self
                    .manifest_encoded_sha256
                    .expect("complete binding has manifest SHA-256"),
            },
        }))
    }

    fn finish_append(self) -> Result<Option<AppendGenerationArtifacts>> {
        if !self.require_complete("append")? {
            return Ok(None);
        }
        Ok(Some(AppendGenerationArtifacts {
            generation: self.generation.expect("complete binding has generation"),
            source_commit_epoch: self
                .source_commit_epoch
                .expect("complete binding has source commit epoch"),
            root_set_digest: self
                .root_set_digest
                .expect("complete binding has root-set digest"),
            manifest_artifact: AppendSegmentArtifactMetadata {
                encoded_len: self
                    .manifest_encoded_len
                    .expect("complete binding has manifest length"),
                encoded_crc32c: u32::try_from(
                    self.manifest_encoded_checksum
                        .expect("complete binding has manifest checksum"),
                )
                .map_err(|_| {
                    SkeinError::Storage("append manifest checksum exceeds CRC32C range".to_string())
                })?,
                encoded_sha256: self
                    .manifest_encoded_sha256
                    .expect("complete binding has manifest SHA-256"),
            },
        }))
    }

    fn finish_overflow(self) -> Result<Option<RelationalOverflowGenerationArtifacts>> {
        if !self.require_complete("relational overflow")? {
            return Ok(None);
        }
        Ok(Some(RelationalOverflowGenerationArtifacts {
            generation: self.generation.expect("complete binding has generation"),
            source_commit_epoch: self
                .source_commit_epoch
                .expect("complete binding has source commit epoch"),
            root_set_digest: self
                .root_set_digest
                .expect("complete binding has root-set digest"),
            manifest_artifact: RelationalOverflowArtifactMetadata {
                encoded_len: self
                    .manifest_encoded_len
                    .expect("complete binding has manifest length"),
                encoded_crc32c: u32::try_from(
                    self.manifest_encoded_checksum
                        .expect("complete binding has manifest checksum"),
                )
                .map_err(|_| {
                    SkeinError::Storage(
                        "relational overflow manifest checksum exceeds CRC32C range".to_string(),
                    )
                })?,
                encoded_sha256: self
                    .manifest_encoded_sha256
                    .expect("complete binding has manifest SHA-256"),
            },
        }))
    }
}

impl RelationalIndexManifestFields {
    fn finish(self) -> Result<Option<RelationalIndexGenerationArtifacts>> {
        let presence = [
            self.generation.is_some(),
            self.source_commit_epoch.is_some(),
            self.catalog_schema_digest.is_some(),
            self.root_set_digest.is_some(),
            self.page_encoded_len.is_some(),
            self.page_encoded_checksum.is_some(),
            self.page_encoded_sha256.is_some(),
            self.manifest_encoded_len.is_some(),
            self.manifest_encoded_checksum.is_some(),
            self.manifest_encoded_sha256.is_some(),
        ];
        if presence.iter().all(|present| !present) {
            return Ok(None);
        }
        if !presence.iter().all(|present| *present) {
            return Err(SkeinError::Storage(
                "manifest relational index generation binding is incomplete".to_string(),
            ));
        }
        Ok(Some(RelationalIndexGenerationArtifacts {
            generation: self.generation.expect("complete binding has generation"),
            source_commit_epoch: self
                .source_commit_epoch
                .expect("complete binding has source commit epoch"),
            catalog_schema_digest: self
                .catalog_schema_digest
                .expect("complete binding has catalog schema digest"),
            root_set_digest: self
                .root_set_digest
                .expect("complete binding has root-set digest"),
            page_artifact: RelationalIndexArtifactMetadata {
                encoded_len: self
                    .page_encoded_len
                    .expect("complete binding has page length"),
                encoded_crc32c: self
                    .page_encoded_checksum
                    .expect("complete binding has page checksum"),
                encoded_sha256: self
                    .page_encoded_sha256
                    .expect("complete binding has page SHA-256"),
            },
            manifest_artifact: RelationalIndexArtifactMetadata {
                encoded_len: self
                    .manifest_encoded_len
                    .expect("complete binding has manifest length"),
                encoded_crc32c: self
                    .manifest_encoded_checksum
                    .expect("complete binding has manifest checksum"),
                encoded_sha256: self
                    .manifest_encoded_sha256
                    .expect("complete binding has manifest SHA-256"),
            },
        }))
    }
}

pub(super) fn artifact_metadata_presence_consistent(
    encoded_len: Option<u64>,
    encoded_checksum: Option<u64>,
    encoded_sha256: Option<Sha256Digest>,
) -> bool {
    let present = encoded_len.is_some();
    encoded_checksum.is_some() == present && encoded_sha256.is_some() == present
}

impl Default for DurableManifest {
    fn default() -> Self {
        Self::initial_generation()
    }
}

impl DurableManifest {
    const fn initial_generation() -> Self {
        Self {
            checkpoint_generation: None,
            checkpoint_encoded_len: None,
            checkpoint_encoded_checksum: None,
            checkpoint_encoded_sha256: None,
            canonical_manifest_encoded_len: None,
            canonical_manifest_encoded_checksum: None,
            canonical_manifest_encoded_sha256: None,
            canonical_adjacency_generation_artifacts: None,
            property_spill_manifest_encoded_len: None,
            property_spill_manifest_encoded_checksum: None,
            property_spill_manifest_encoded_sha256: None,
            property_projection_manifest_encoded_len: None,
            property_projection_manifest_encoded_checksum: None,
            property_projection_manifest_encoded_sha256: None,
            relational_row_generation_artifacts: None,
            relational_overflow_generation_artifacts: None,
            relational_index_generation_artifacts: None,
            append_generation_artifacts: None,
            wal_generation: 0,
            checkpoint_epoch: 0,
            checkpoint_commit_epoch: 0,
            oldest_reader_commit_epoch: None,
            safe_reclaim_commit_epoch: 0,
            wal_replay_start_lsn: 1,
            next_lsn: 1,
            source_scan_commit_epoch: None,
            source_scan_descriptor_checksum: None,
        }
    }

    pub(super) fn checkpoint_path(self, root: &Path) -> PathBuf {
        root.join(checkpoint_generation_file(
            self.checkpoint_generation.unwrap_or(self.checkpoint_epoch),
        ))
    }

    pub(super) fn wal_path(self, root: &Path) -> PathBuf {
        root.join(wal_generation_file(self.wal_generation))
    }

    pub(super) fn validate(self) -> Result<()> {
        if self.wal_replay_start_lsn == 0 || self.next_lsn == 0 {
            return Err(SkeinError::Storage(
                "manifest WAL LSN values must be non-zero".to_string(),
            ));
        }
        if self.next_lsn < self.wal_replay_start_lsn {
            return Err(SkeinError::Storage(format!(
                "manifest next LSN {} precedes replay start LSN {}",
                self.next_lsn, self.wal_replay_start_lsn
            )));
        }
        if !artifact_metadata_presence_consistent(
            self.canonical_manifest_encoded_len,
            self.canonical_manifest_encoded_checksum,
            self.canonical_manifest_encoded_sha256,
        ) {
            return Err(SkeinError::Storage(
                "manifest canonical segment metadata is incomplete".to_string(),
            ));
        }
        if let Some(binding) = self.canonical_adjacency_generation_artifacts {
            if binding.generation == 0
                || binding.generation != self.checkpoint_epoch
                || binding.source_commit_epoch != self.checkpoint_commit_epoch
            {
                return Err(SkeinError::Storage(format!(
                    "manifest canonical adjacency generation/epoch {}/{} does not match checkpoint {}/{}",
                    binding.generation,
                    binding.source_commit_epoch,
                    self.checkpoint_epoch,
                    self.checkpoint_commit_epoch
                )));
            }
            if binding.adjacency_artifact.encoded_len == 0
                || binding.descriptor_root_artifact.encoded_len == 0
            {
                return Err(SkeinError::Storage(
                    "manifest canonical adjacency artifacts must not be empty".to_string(),
                ));
            }
            if binding.entry_count
                != binding.relationship_count.checked_mul(2).ok_or_else(|| {
                    SkeinError::Storage(
                        "manifest canonical adjacency relationship count overflow".to_string(),
                    )
                })?
            {
                return Err(SkeinError::Storage(
                    "manifest canonical adjacency entry count must be twice its relationship count"
                        .to_string(),
                ));
            }
        }
        if self.canonical_adjacency_generation_artifacts.is_some()
            && self.canonical_manifest_encoded_len.is_none()
        {
            return Err(SkeinError::Storage(
                "manifest canonical adjacency requires canonical segments".to_string(),
            ));
        }
        if self.canonical_manifest_encoded_len.is_some()
            && self.canonical_adjacency_generation_artifacts.is_none()
        {
            return Err(SkeinError::Storage(
                "manifest canonical segments require canonical adjacency".to_string(),
            ));
        }
        if !artifact_metadata_presence_consistent(
            self.property_spill_manifest_encoded_len,
            self.property_spill_manifest_encoded_checksum,
            self.property_spill_manifest_encoded_sha256,
        ) {
            return Err(SkeinError::Storage(
                "manifest property spill metadata is incomplete".to_string(),
            ));
        }
        if self.property_spill_manifest_encoded_len.is_some()
            && self.canonical_manifest_encoded_len.is_none()
        {
            return Err(SkeinError::Storage(
                "manifest property spills require canonical segments".to_string(),
            ));
        }
        if !artifact_metadata_presence_consistent(
            self.property_projection_manifest_encoded_len,
            self.property_projection_manifest_encoded_checksum,
            self.property_projection_manifest_encoded_sha256,
        ) {
            return Err(SkeinError::Storage(
                "manifest property projection metadata is incomplete".to_string(),
            ));
        }
        if self.property_projection_manifest_encoded_len.is_some()
            && self.canonical_manifest_encoded_len.is_none()
        {
            return Err(SkeinError::Storage(
                "manifest property projections require canonical segments".to_string(),
            ));
        }
        for (artifact, binding) in [
            (
                "relational row-page",
                self.relational_row_generation_artifacts.map(|binding| {
                    (
                        binding.generation,
                        binding.source_commit_epoch,
                        binding.manifest_artifact.encoded_len,
                    )
                }),
            ),
            (
                "relational overflow",
                self.relational_overflow_generation_artifacts
                    .map(|binding| {
                        (
                            binding.generation,
                            binding.source_commit_epoch,
                            binding.manifest_artifact.encoded_len,
                        )
                    }),
            ),
            (
                "append",
                self.append_generation_artifacts.map(|binding| {
                    (
                        binding.generation,
                        binding.source_commit_epoch,
                        binding.manifest_artifact.encoded_len,
                    )
                }),
            ),
        ] {
            if let Some((generation, source_commit_epoch, manifest_bytes)) = binding {
                if generation == 0 {
                    return Err(SkeinError::Storage(format!(
                        "manifest {artifact} generation must be non-zero"
                    )));
                }
                if generation != self.checkpoint_epoch
                    || source_commit_epoch != self.checkpoint_commit_epoch
                {
                    return Err(SkeinError::Storage(format!(
                        "manifest {artifact} generation/epoch {generation}/{source_commit_epoch} does not match checkpoint {}/{}",
                        self.checkpoint_epoch, self.checkpoint_commit_epoch
                    )));
                }
                if manifest_bytes == 0 {
                    return Err(SkeinError::Storage(format!(
                        "manifest {artifact} generation manifest must not be empty"
                    )));
                }
            }
        }
        if let Some(binding) = self.relational_index_generation_artifacts {
            if binding.generation == 0 {
                return Err(SkeinError::Storage(
                    "manifest relational index generation must be non-zero".to_string(),
                ));
            }
            if binding.generation != self.checkpoint_epoch
                || binding.source_commit_epoch != self.checkpoint_commit_epoch
            {
                return Err(SkeinError::Storage(format!(
                    "manifest relational index generation/epoch {}/{} does not match checkpoint {}/{}",
                    binding.generation,
                    binding.source_commit_epoch,
                    self.checkpoint_epoch,
                    self.checkpoint_commit_epoch,
                )));
            }
            if binding.manifest_artifact.encoded_len == 0 {
                return Err(SkeinError::Storage(
                    "manifest relational index generation manifest must not be empty".to_string(),
                ));
            }
        }
        if self.wal_generation != self.checkpoint_epoch {
            return Err(SkeinError::Storage(format!(
                "manifest WAL generation {} does not match checkpoint epoch {}",
                self.wal_generation, self.checkpoint_epoch
            )));
        }
        match self.checkpoint_generation {
            Some(generation) => {
                if generation != self.checkpoint_epoch {
                    return Err(SkeinError::Storage(format!(
                            "manifest checkpoint generation {generation} does not match checkpoint epoch {}",
                            self.checkpoint_epoch
                        )));
                }
                if self.checkpoint_encoded_len.is_none()
                    || self.checkpoint_encoded_checksum.is_none()
                    || self.checkpoint_encoded_sha256.is_none()
                {
                    return Err(SkeinError::Storage(
                        "manifest checkpoint artifact metadata is incomplete".to_string(),
                    ));
                }
                if self.relational_row_generation_artifacts.is_none()
                    || self.relational_overflow_generation_artifacts.is_none()
                {
                    return Err(SkeinError::Storage(
                        "published checkpoint must bind relational row-page and overflow generations"
                            .to_string(),
                    ));
                }
            }
            None => {
                if self.checkpoint_epoch != 0
                    || self.checkpoint_commit_epoch != 0
                    || self.checkpoint_encoded_len.is_some()
                    || self.checkpoint_encoded_checksum.is_some()
                    || self.checkpoint_encoded_sha256.is_some()
                    || self.canonical_manifest_encoded_len.is_some()
                    || self.canonical_manifest_encoded_checksum.is_some()
                    || self.canonical_manifest_encoded_sha256.is_some()
                    || self.canonical_adjacency_generation_artifacts.is_some()
                    || self.property_spill_manifest_encoded_len.is_some()
                    || self.property_spill_manifest_encoded_checksum.is_some()
                    || self.property_spill_manifest_encoded_sha256.is_some()
                    || self.property_projection_manifest_encoded_len.is_some()
                    || self.property_projection_manifest_encoded_checksum.is_some()
                    || self.property_projection_manifest_encoded_sha256.is_some()
                    || self.relational_row_generation_artifacts.is_some()
                    || self.relational_overflow_generation_artifacts.is_some()
                    || self.relational_index_generation_artifacts.is_some()
                    || self.append_generation_artifacts.is_some()
                {
                    return Err(SkeinError::Storage(
                        "manifest without a checkpoint must describe generation zero".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)?;
        let (body, checksum) = split_manifest_checksum(&text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "manifest checksum mismatch: expected {checksum}, got {actual}"
            )));
        }
        let mut lines = body.lines();
        if lines.next() != Some(MANIFEST_HEADER_V1) {
            return Err(SkeinError::Storage(
                "manifest is missing the V1 format header".to_string(),
            ));
        }
        let mut manifest = Self::initial_generation();
        let mut canonical_adjacency = CanonicalAdjacencyGenerationFields::default();
        let mut relational_row = RelationalRootManifestFields::default();
        let mut relational_overflow = RelationalRootManifestFields::default();
        let mut append = RelationalRootManifestFields::default();
        let mut relational_index = RelationalIndexManifestFields::default();
        let mut seen_fields = BTreeSet::new();
        for line in lines {
            let fields = line.split('\t').collect::<Vec<_>>();
            if fields == [""] {
                continue;
            }
            let field = fields[0];
            if !seen_fields.insert(field) {
                return Err(SkeinError::Storage(format!(
                    "manifest has duplicate field: {field}"
                )));
            }
            match fields.as_slice() {
                ["version", version] => validate_storage_version(version)?,
                ["checkpoint_generation", raw] => {
                    manifest.checkpoint_generation =
                        parse_optional_u64(raw, "checkpoint generation")?;
                }
                ["checkpoint_encoded_len", raw] => {
                    manifest.checkpoint_encoded_len =
                        parse_optional_u64(raw, "checkpoint encoded length")?;
                }
                ["checkpoint_encoded_checksum", raw] => {
                    manifest.checkpoint_encoded_checksum =
                        parse_optional_u64(raw, "checkpoint encoded checksum")?;
                }
                ["checkpoint_encoded_sha256", raw] => {
                    manifest.checkpoint_encoded_sha256 =
                        parse_optional_sha256(raw, "checkpoint encoded SHA-256")?;
                }
                ["canonical_manifest_encoded_len", raw] => {
                    manifest.canonical_manifest_encoded_len =
                        parse_optional_u64(raw, "canonical manifest encoded length")?;
                }
                ["canonical_manifest_encoded_checksum", raw] => {
                    manifest.canonical_manifest_encoded_checksum =
                        parse_optional_u64(raw, "canonical manifest encoded checksum")?;
                }
                ["canonical_manifest_encoded_sha256", raw] => {
                    manifest.canonical_manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "canonical manifest encoded SHA-256")?;
                }
                ["canonical_adjacency_generation", raw] => {
                    canonical_adjacency.generation =
                        parse_optional_u64(raw, "canonical adjacency generation")?;
                }
                ["canonical_adjacency_source_commit_epoch", raw] => {
                    canonical_adjacency.source_commit_epoch =
                        parse_optional_u64(raw, "canonical adjacency source commit epoch")?;
                }
                ["canonical_adjacency_relationship_count", raw] => {
                    canonical_adjacency.relationship_count =
                        parse_optional_u64(raw, "canonical adjacency relationship count")?;
                }
                ["canonical_adjacency_entry_count", raw] => {
                    canonical_adjacency.entry_count =
                        parse_optional_u64(raw, "canonical adjacency entry count")?;
                }
                ["canonical_adjacency_artifact_encoded_len", raw] => {
                    canonical_adjacency.artifact_encoded_len =
                        parse_optional_u64(raw, "canonical adjacency artifact encoded length")?;
                }
                ["canonical_adjacency_artifact_encoded_crc32c", raw] => {
                    canonical_adjacency.artifact_encoded_crc32c =
                        parse_optional_u64(raw, "canonical adjacency artifact CRC32C")?;
                }
                ["canonical_adjacency_artifact_encoded_sha256", raw] => {
                    canonical_adjacency.artifact_encoded_sha256 =
                        parse_optional_sha256(raw, "canonical adjacency artifact SHA-256")?;
                }
                ["canonical_adjacency_descriptor_root_encoded_len", raw] => {
                    canonical_adjacency.descriptor_root_encoded_len = parse_optional_u64(
                        raw,
                        "canonical adjacency descriptor root encoded length",
                    )?;
                }
                ["canonical_adjacency_descriptor_root_encoded_crc32c", raw] => {
                    canonical_adjacency.descriptor_root_encoded_crc32c =
                        parse_optional_u64(raw, "canonical adjacency descriptor root CRC32C")?;
                }
                ["canonical_adjacency_descriptor_root_encoded_sha256", raw] => {
                    canonical_adjacency.descriptor_root_encoded_sha256 =
                        parse_optional_sha256(raw, "canonical adjacency descriptor root SHA-256")?;
                }
                ["property_spill_manifest_encoded_len", raw] => {
                    manifest.property_spill_manifest_encoded_len =
                        parse_optional_u64(raw, "property spill manifest encoded length")?;
                }
                ["property_spill_manifest_encoded_checksum", raw] => {
                    manifest.property_spill_manifest_encoded_checksum =
                        parse_optional_u64(raw, "property spill manifest encoded checksum")?;
                }
                ["property_spill_manifest_encoded_sha256", raw] => {
                    manifest.property_spill_manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "property spill manifest encoded SHA-256")?;
                }
                ["property_projection_manifest_encoded_len", raw] => {
                    manifest.property_projection_manifest_encoded_len =
                        parse_optional_u64(raw, "property projection manifest encoded length")?;
                }
                ["property_projection_manifest_encoded_checksum", raw] => {
                    manifest.property_projection_manifest_encoded_checksum =
                        parse_optional_u64(raw, "property projection manifest encoded checksum")?;
                }
                ["property_projection_manifest_encoded_sha256", raw] => {
                    manifest.property_projection_manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "property projection manifest encoded SHA-256")?;
                }
                ["relational_row_generation", raw] => {
                    relational_row.generation =
                        parse_optional_u64(raw, "relational row-page generation")?;
                }
                ["relational_row_source_commit_epoch", raw] => {
                    relational_row.source_commit_epoch =
                        parse_optional_u64(raw, "relational row-page source commit epoch")?;
                }
                ["relational_row_root_set_sha256", raw] => {
                    relational_row.root_set_digest =
                        parse_optional_sha256(raw, "relational row-page root-set SHA-256")?;
                }
                ["relational_row_manifest_encoded_len", raw] => {
                    relational_row.manifest_encoded_len =
                        parse_optional_u64(raw, "relational row-page manifest encoded length")?;
                }
                ["relational_row_manifest_encoded_checksum", raw] => {
                    relational_row.manifest_encoded_checksum =
                        parse_optional_u64(raw, "relational row-page manifest encoded checksum")?;
                }
                ["relational_row_manifest_encoded_sha256", raw] => {
                    relational_row.manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "relational row-page manifest encoded SHA-256")?;
                }
                ["relational_overflow_generation", raw] => {
                    relational_overflow.generation =
                        parse_optional_u64(raw, "relational overflow generation")?;
                }
                ["relational_overflow_source_commit_epoch", raw] => {
                    relational_overflow.source_commit_epoch =
                        parse_optional_u64(raw, "relational overflow source commit epoch")?;
                }
                ["relational_overflow_root_set_sha256", raw] => {
                    relational_overflow.root_set_digest =
                        parse_optional_sha256(raw, "relational overflow root-set SHA-256")?;
                }
                ["relational_overflow_manifest_encoded_len", raw] => {
                    relational_overflow.manifest_encoded_len =
                        parse_optional_u64(raw, "relational overflow manifest encoded length")?;
                }
                ["relational_overflow_manifest_encoded_checksum", raw] => {
                    relational_overflow.manifest_encoded_checksum =
                        parse_optional_u64(raw, "relational overflow manifest encoded checksum")?;
                }
                ["relational_overflow_manifest_encoded_sha256", raw] => {
                    relational_overflow.manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "relational overflow manifest encoded SHA-256")?;
                }
                ["append_generation", raw] => {
                    append.generation = parse_optional_u64(raw, "append generation")?;
                }
                ["append_source_commit_epoch", raw] => {
                    append.source_commit_epoch =
                        parse_optional_u64(raw, "append source commit epoch")?;
                }
                ["append_root_set_sha256", raw] => {
                    append.root_set_digest = parse_optional_sha256(raw, "append root-set SHA-256")?;
                }
                ["append_manifest_encoded_len", raw] => {
                    append.manifest_encoded_len =
                        parse_optional_u64(raw, "append manifest encoded length")?;
                }
                ["append_manifest_encoded_checksum", raw] => {
                    append.manifest_encoded_checksum =
                        parse_optional_u64(raw, "append manifest encoded checksum")?;
                }
                ["append_manifest_encoded_sha256", raw] => {
                    append.manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "append manifest encoded SHA-256")?;
                }
                ["relational_index_generation", raw] => {
                    relational_index.generation =
                        parse_optional_u64(raw, "relational index generation")?;
                }
                ["relational_index_source_commit_epoch", raw] => {
                    relational_index.source_commit_epoch =
                        parse_optional_u64(raw, "relational index source commit epoch")?;
                }
                ["relational_index_catalog_schema_sha256", raw] => {
                    relational_index.catalog_schema_digest =
                        parse_optional_sha256(raw, "relational index catalog schema SHA-256")?;
                }
                ["relational_index_root_set_sha256", raw] => {
                    relational_index.root_set_digest =
                        parse_optional_sha256(raw, "relational index root-set SHA-256")?;
                }
                ["relational_index_page_encoded_len", raw] => {
                    relational_index.page_encoded_len =
                        parse_optional_u64(raw, "relational index page encoded length")?;
                }
                ["relational_index_page_encoded_checksum", raw] => {
                    relational_index.page_encoded_checksum =
                        parse_optional_u64(raw, "relational index page encoded checksum")?;
                }
                ["relational_index_page_encoded_sha256", raw] => {
                    relational_index.page_encoded_sha256 =
                        parse_optional_sha256(raw, "relational index page encoded SHA-256")?;
                }
                ["relational_index_manifest_encoded_len", raw] => {
                    relational_index.manifest_encoded_len =
                        parse_optional_u64(raw, "relational index manifest encoded length")?;
                }
                ["relational_index_manifest_encoded_checksum", raw] => {
                    relational_index.manifest_encoded_checksum =
                        parse_optional_u64(raw, "relational index manifest encoded checksum")?;
                }
                ["relational_index_manifest_encoded_sha256", raw] => {
                    relational_index.manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "relational index manifest encoded SHA-256")?;
                }
                ["wal_generation", raw] => {
                    manifest.wal_generation = parse_u64(raw, "WAL generation")?;
                }
                ["checkpoint_epoch", raw] => {
                    manifest.checkpoint_epoch = parse_u64(raw, "checkpoint epoch")?;
                }
                ["checkpoint_commit_epoch", raw] => {
                    manifest.checkpoint_commit_epoch = parse_u64(raw, "checkpoint commit epoch")?;
                }
                ["oldest_reader_commit_epoch", raw] => {
                    manifest.oldest_reader_commit_epoch =
                        parse_optional_u64(raw, "oldest reader commit epoch")?;
                }
                ["safe_reclaim_commit_epoch", raw] => {
                    manifest.safe_reclaim_commit_epoch =
                        parse_u64(raw, "safe reclaim commit epoch")?;
                }
                ["wal_replay_start_lsn", raw] => {
                    manifest.wal_replay_start_lsn = parse_u64(raw, "wal replay start lsn")?;
                }
                ["next_lsn", raw] => {
                    manifest.next_lsn = parse_u64(raw, "manifest next lsn")?;
                }
                ["source_scan_commit_epoch", raw] => {
                    manifest.source_scan_commit_epoch =
                        parse_optional_u64(raw, "source scan commit epoch")?;
                }
                ["source_scan_descriptor_checksum", raw] => {
                    manifest.source_scan_descriptor_checksum =
                        parse_optional_u64(raw, "source scan descriptor checksum")?;
                }
                _ => {
                    return Err(SkeinError::Storage(format!(
                        "invalid manifest line: {line}"
                    )));
                }
            }
        }
        for required in [
            "version",
            "checkpoint_generation",
            "checkpoint_encoded_len",
            "checkpoint_encoded_checksum",
            "checkpoint_encoded_sha256",
            "canonical_manifest_encoded_len",
            "canonical_manifest_encoded_checksum",
            "canonical_manifest_encoded_sha256",
            "canonical_adjacency_generation",
            "canonical_adjacency_source_commit_epoch",
            "canonical_adjacency_relationship_count",
            "canonical_adjacency_entry_count",
            "canonical_adjacency_artifact_encoded_len",
            "canonical_adjacency_artifact_encoded_crc32c",
            "canonical_adjacency_artifact_encoded_sha256",
            "canonical_adjacency_descriptor_root_encoded_len",
            "canonical_adjacency_descriptor_root_encoded_crc32c",
            "canonical_adjacency_descriptor_root_encoded_sha256",
            "property_spill_manifest_encoded_len",
            "property_spill_manifest_encoded_checksum",
            "property_spill_manifest_encoded_sha256",
            "property_projection_manifest_encoded_len",
            "property_projection_manifest_encoded_checksum",
            "property_projection_manifest_encoded_sha256",
            "relational_row_generation",
            "relational_row_source_commit_epoch",
            "relational_row_root_set_sha256",
            "relational_row_manifest_encoded_len",
            "relational_row_manifest_encoded_checksum",
            "relational_row_manifest_encoded_sha256",
            "relational_overflow_generation",
            "relational_overflow_source_commit_epoch",
            "relational_overflow_root_set_sha256",
            "relational_overflow_manifest_encoded_len",
            "relational_overflow_manifest_encoded_checksum",
            "relational_overflow_manifest_encoded_sha256",
            "relational_index_generation",
            "relational_index_source_commit_epoch",
            "relational_index_catalog_schema_sha256",
            "relational_index_root_set_sha256",
            "relational_index_page_encoded_len",
            "relational_index_page_encoded_checksum",
            "relational_index_page_encoded_sha256",
            "relational_index_manifest_encoded_len",
            "relational_index_manifest_encoded_checksum",
            "relational_index_manifest_encoded_sha256",
            "wal_generation",
            "checkpoint_epoch",
            "checkpoint_commit_epoch",
            "oldest_reader_commit_epoch",
            "safe_reclaim_commit_epoch",
            "wal_replay_start_lsn",
            "next_lsn",
            "source_scan_commit_epoch",
            "source_scan_descriptor_checksum",
        ] {
            if !seen_fields.contains(required) {
                return Err(SkeinError::Storage(format!(
                    "manifest is missing required field: {required}"
                )));
            }
        }
        manifest.relational_row_generation_artifacts = relational_row.finish_row()?;
        manifest.canonical_adjacency_generation_artifacts = canonical_adjacency.finish()?;
        manifest.relational_overflow_generation_artifacts =
            relational_overflow.finish_overflow()?;
        manifest.relational_index_generation_artifacts = relational_index.finish()?;
        manifest.append_generation_artifacts = append.finish_append()?;
        if manifest.safe_reclaim_commit_epoch == 0 && manifest.checkpoint_commit_epoch > 0 {
            manifest.safe_reclaim_commit_epoch = safe_reclaim_commit_epoch(
                manifest.checkpoint_commit_epoch,
                manifest.oldest_reader_commit_epoch,
            );
        }
        manifest.validate()?;
        Ok(manifest)
    }

    fn write(&self, path: &Path) -> Result<()> {
        let mut body = String::new();
        body.push_str(&format!("{MANIFEST_HEADER_V1}\n"));
        body.push_str(&format!("version\t{STORAGE_VERSION}\n"));
        body.push_str(&format!(
            "checkpoint_generation\t{}\n",
            encode_optional_u64(self.checkpoint_generation)
        ));
        body.push_str(&format!(
            "checkpoint_encoded_len\t{}\n",
            encode_optional_u64(self.checkpoint_encoded_len)
        ));
        body.push_str(&format!(
            "checkpoint_encoded_checksum\t{}\n",
            encode_optional_u64(self.checkpoint_encoded_checksum)
        ));
        body.push_str(&format!(
            "checkpoint_encoded_sha256\t{}\n",
            encode_optional_sha256(self.checkpoint_encoded_sha256)
        ));
        body.push_str(&format!(
            "canonical_manifest_encoded_len\t{}\n",
            encode_optional_u64(self.canonical_manifest_encoded_len)
        ));
        body.push_str(&format!(
            "canonical_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(self.canonical_manifest_encoded_checksum)
        ));
        body.push_str(&format!(
            "canonical_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(self.canonical_manifest_encoded_sha256)
        ));
        let canonical_adjacency = self.canonical_adjacency_generation_artifacts;
        body.push_str(&format!(
            "canonical_adjacency_generation\t{}\n",
            encode_optional_u64(canonical_adjacency.map(|binding| binding.generation))
        ));
        body.push_str(&format!(
            "canonical_adjacency_source_commit_epoch\t{}\n",
            encode_optional_u64(canonical_adjacency.map(|binding| binding.source_commit_epoch))
        ));
        body.push_str(&format!(
            "canonical_adjacency_relationship_count\t{}\n",
            encode_optional_u64(canonical_adjacency.map(|binding| binding.relationship_count))
        ));
        body.push_str(&format!(
            "canonical_adjacency_entry_count\t{}\n",
            encode_optional_u64(canonical_adjacency.map(|binding| binding.entry_count))
        ));
        body.push_str(&format!(
            "canonical_adjacency_artifact_encoded_len\t{}\n",
            encode_optional_u64(
                canonical_adjacency.map(|binding| binding.adjacency_artifact.encoded_len)
            )
        ));
        body.push_str(&format!(
            "canonical_adjacency_artifact_encoded_crc32c\t{}\n",
            encode_optional_u64(
                canonical_adjacency.map(|binding| binding.adjacency_artifact.encoded_crc32c)
            )
        ));
        body.push_str(&format!(
            "canonical_adjacency_artifact_encoded_sha256\t{}\n",
            encode_optional_sha256(
                canonical_adjacency.map(|binding| binding.adjacency_artifact.encoded_sha256)
            )
        ));
        body.push_str(&format!(
            "canonical_adjacency_descriptor_root_encoded_len\t{}\n",
            encode_optional_u64(
                canonical_adjacency.map(|binding| binding.descriptor_root_artifact.encoded_len)
            )
        ));
        body.push_str(&format!(
            "canonical_adjacency_descriptor_root_encoded_crc32c\t{}\n",
            encode_optional_u64(
                canonical_adjacency
                    .map(|binding| { u64::from(binding.descriptor_root_artifact.encoded_crc32c) })
            )
        ));
        body.push_str(&format!(
            "canonical_adjacency_descriptor_root_encoded_sha256\t{}\n",
            encode_optional_sha256(
                canonical_adjacency.map(|binding| binding.descriptor_root_artifact.encoded_sha256)
            )
        ));
        body.push_str(&format!(
            "property_spill_manifest_encoded_len\t{}\n",
            encode_optional_u64(self.property_spill_manifest_encoded_len)
        ));
        body.push_str(&format!(
            "property_spill_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(self.property_spill_manifest_encoded_checksum)
        ));
        body.push_str(&format!(
            "property_spill_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(self.property_spill_manifest_encoded_sha256)
        ));
        body.push_str(&format!(
            "property_projection_manifest_encoded_len\t{}\n",
            encode_optional_u64(self.property_projection_manifest_encoded_len)
        ));
        body.push_str(&format!(
            "property_projection_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(self.property_projection_manifest_encoded_checksum)
        ));
        body.push_str(&format!(
            "property_projection_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(self.property_projection_manifest_encoded_sha256)
        ));
        let relational_row = self.relational_row_generation_artifacts;
        body.push_str(&format!(
            "relational_row_generation\t{}\n",
            encode_optional_u64(relational_row.map(|binding| binding.generation))
        ));
        body.push_str(&format!(
            "relational_row_source_commit_epoch\t{}\n",
            encode_optional_u64(relational_row.map(|binding| binding.source_commit_epoch))
        ));
        body.push_str(&format!(
            "relational_row_root_set_sha256\t{}\n",
            encode_optional_sha256(relational_row.map(|binding| binding.root_set_digest))
        ));
        body.push_str(&format!(
            "relational_row_manifest_encoded_len\t{}\n",
            encode_optional_u64(
                relational_row.map(|binding| binding.manifest_artifact.encoded_len)
            )
        ));
        body.push_str(&format!(
            "relational_row_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(
                relational_row.map(|binding| u64::from(binding.manifest_artifact.encoded_crc32c))
            )
        ));
        body.push_str(&format!(
            "relational_row_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(
                relational_row.map(|binding| binding.manifest_artifact.encoded_sha256)
            )
        ));
        let relational_overflow = self.relational_overflow_generation_artifacts;
        body.push_str(&format!(
            "relational_overflow_generation\t{}\n",
            encode_optional_u64(relational_overflow.map(|binding| binding.generation))
        ));
        body.push_str(&format!(
            "relational_overflow_source_commit_epoch\t{}\n",
            encode_optional_u64(relational_overflow.map(|binding| binding.source_commit_epoch))
        ));
        body.push_str(&format!(
            "relational_overflow_root_set_sha256\t{}\n",
            encode_optional_sha256(relational_overflow.map(|binding| binding.root_set_digest))
        ));
        body.push_str(&format!(
            "relational_overflow_manifest_encoded_len\t{}\n",
            encode_optional_u64(
                relational_overflow.map(|binding| binding.manifest_artifact.encoded_len)
            )
        ));
        body.push_str(&format!(
            "relational_overflow_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(
                relational_overflow
                    .map(|binding| u64::from(binding.manifest_artifact.encoded_crc32c))
            )
        ));
        body.push_str(&format!(
            "relational_overflow_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(
                relational_overflow.map(|binding| binding.manifest_artifact.encoded_sha256)
            )
        ));
        let append = self.append_generation_artifacts;
        body.push_str(&format!(
            "append_generation\t{}\n",
            encode_optional_u64(append.map(|binding| binding.generation))
        ));
        body.push_str(&format!(
            "append_source_commit_epoch\t{}\n",
            encode_optional_u64(append.map(|binding| binding.source_commit_epoch))
        ));
        body.push_str(&format!(
            "append_root_set_sha256\t{}\n",
            encode_optional_sha256(append.map(|binding| binding.root_set_digest))
        ));
        body.push_str(&format!(
            "append_manifest_encoded_len\t{}\n",
            encode_optional_u64(append.map(|binding| binding.manifest_artifact.encoded_len))
        ));
        body.push_str(&format!(
            "append_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(
                append.map(|binding| u64::from(binding.manifest_artifact.encoded_crc32c))
            )
        ));
        body.push_str(&format!(
            "append_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(append.map(|binding| binding.manifest_artifact.encoded_sha256))
        ));
        let relational_index = self.relational_index_generation_artifacts;
        body.push_str(&format!(
            "relational_index_generation\t{}\n",
            encode_optional_u64(relational_index.map(|binding| binding.generation))
        ));
        body.push_str(&format!(
            "relational_index_source_commit_epoch\t{}\n",
            encode_optional_u64(relational_index.map(|binding| binding.source_commit_epoch))
        ));
        body.push_str(&format!(
            "relational_index_catalog_schema_sha256\t{}\n",
            encode_optional_sha256(relational_index.map(|binding| binding.catalog_schema_digest))
        ));
        body.push_str(&format!(
            "relational_index_root_set_sha256\t{}\n",
            encode_optional_sha256(relational_index.map(|binding| binding.root_set_digest))
        ));
        body.push_str(&format!(
            "relational_index_page_encoded_len\t{}\n",
            encode_optional_u64(relational_index.map(|binding| binding.page_artifact.encoded_len))
        ));
        body.push_str(&format!(
            "relational_index_page_encoded_checksum\t{}\n",
            encode_optional_u64(
                relational_index.map(|binding| binding.page_artifact.encoded_crc32c)
            )
        ));
        body.push_str(&format!(
            "relational_index_page_encoded_sha256\t{}\n",
            encode_optional_sha256(
                relational_index.map(|binding| binding.page_artifact.encoded_sha256)
            )
        ));
        body.push_str(&format!(
            "relational_index_manifest_encoded_len\t{}\n",
            encode_optional_u64(
                relational_index.map(|binding| binding.manifest_artifact.encoded_len)
            )
        ));
        body.push_str(&format!(
            "relational_index_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(
                relational_index.map(|binding| binding.manifest_artifact.encoded_crc32c)
            )
        ));
        body.push_str(&format!(
            "relational_index_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(
                relational_index.map(|binding| binding.manifest_artifact.encoded_sha256)
            )
        ));
        body.push_str(&format!("wal_generation\t{}\n", self.wal_generation));
        body.push_str(&format!("checkpoint_epoch\t{}\n", self.checkpoint_epoch));
        body.push_str(&format!(
            "checkpoint_commit_epoch\t{}\n",
            self.checkpoint_commit_epoch
        ));
        body.push_str(&format!(
            "oldest_reader_commit_epoch\t{}\n",
            encode_optional_u64(self.oldest_reader_commit_epoch)
        ));
        body.push_str(&format!(
            "safe_reclaim_commit_epoch\t{}\n",
            self.safe_reclaim_commit_epoch
        ));
        body.push_str(&format!(
            "wal_replay_start_lsn\t{}\n",
            self.wal_replay_start_lsn
        ));
        body.push_str(&format!("next_lsn\t{}\n", self.next_lsn));
        body.push_str(&format!(
            "source_scan_commit_epoch\t{}\n",
            encode_optional_u64(self.source_scan_commit_epoch)
        ));
        body.push_str(&format!(
            "source_scan_descriptor_checksum\t{}\n",
            encode_optional_u64(self.source_scan_descriptor_checksum)
        ));
        let checksum = checksum_bytes(body.as_bytes());
        let data = format!("{body}checksum\t{checksum}\n");
        let tmp_path = path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(data.as_bytes())?;
            file.sync_all()?;
        }
        durable_replace_file(&tmp_path, path)?;
        Ok(())
    }
}

fn admit_graph_manifest_binding(
    expected_len: u64,
    format_max_bytes: u64,
    artifact: &str,
    open_budget: &mut GraphManifestOpenBudget,
) -> Result<()> {
    if expected_len > format_max_bytes {
        return Err(SkeinError::Storage(format!(
            "{artifact} exceeds format limit {format_max_bytes} bytes"
        )));
    }
    open_budget.admit(expected_len, artifact)
}

fn read_bound_graph_manifest(
    path: &Path,
    expected_len: u64,
    expected_checksum: u64,
    expected_sha256: Sha256Digest,
    format_max_bytes: u64,
    artifact: &str,
    open_budget: &mut GraphManifestOpenBudget,
) -> Result<Vec<u8>> {
    admit_graph_manifest_binding(expected_len, format_max_bytes, artifact, open_budget)?;
    let read_limit = expected_len
        .checked_add(1)
        .ok_or_else(|| SkeinError::Storage(format!("{artifact} read limit overflows u64")))?;
    let file = File::open(path)?;
    let actual_len = file.metadata()?.len();
    if actual_len > expected_len {
        return Err(SkeinError::Storage(format!(
            "{artifact} contains {actual_len} bytes, exceeding its admitted bound {expected_len}"
        )));
    }
    let capacity = usize::try_from(actual_len)
        .map_err(|_| SkeinError::Storage(format!("{artifact} length does not fit usize")))?;
    let mut encoded = Vec::with_capacity(capacity);
    file.take(read_limit).read_to_end(&mut encoded)?;
    if encoded.len() as u64 > expected_len {
        return Err(SkeinError::Storage(format!(
            "{artifact} grew beyond its admitted bound {expected_len} during open"
        )));
    }
    verify_integrity(
        &encoded,
        expected_len,
        expected_checksum,
        expected_sha256,
        artifact,
    )?;
    Ok(encoded)
}

fn load_published_canonical_segments(
    root: &Path,
    durable_manifest: DurableManifest,
    cache: Arc<SegmentCache>,
    store_id: StoreId,
    open_budget: &mut GraphManifestOpenBudget,
) -> Result<Option<CanonicalSegmentReader>> {
    let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
        durable_manifest.canonical_manifest_encoded_len,
        durable_manifest.canonical_manifest_encoded_checksum,
        durable_manifest.canonical_manifest_encoded_sha256,
    ) else {
        return Ok(None);
    };
    let generation = durable_manifest.checkpoint_generation.ok_or_else(|| {
        SkeinError::Storage(
            "canonical manifest metadata requires a checkpoint generation".to_string(),
        )
    })?;
    let manifest_path = root.join(canonical_manifest_generation_file(generation));
    let encoded = read_bound_graph_manifest(
        &manifest_path,
        expected_len,
        expected_checksum,
        expected_sha256,
        CANONICAL_MANIFEST_MAX_BYTES,
        "canonical manifest",
        open_budget,
    )?;
    let text = std::str::from_utf8(&encoded).map_err(|error| {
        SkeinError::Storage(format!("canonical manifest is not UTF-8: {error}"))
    })?;
    let canonical_manifest = CanonicalSegmentManifest::decode(text)
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
    if canonical_manifest.generation != ManifestGeneration(generation) {
        return Err(SkeinError::Storage(format!(
            "canonical manifest generation {} does not match durable generation {generation}",
            canonical_manifest.generation.0
        )));
    }
    if canonical_manifest.source_commit_epoch != durable_manifest.checkpoint_commit_epoch {
        return Err(SkeinError::Storage(format!(
            "canonical descriptor source epoch {} does not match durable checkpoint epoch {}",
            canonical_manifest.source_commit_epoch, durable_manifest.checkpoint_commit_epoch
        )));
    }
    let config = CanonicalSegmentConfig::default();
    let max_segment_bytes = NonZeroU64::new(
        config
            .target_segment_bytes
            .get()
            .max(config.max_record_bytes.get().saturating_add(64)),
    )
    .expect("canonical segment maximum is non-zero");
    let property_spills = load_published_property_spills(
        root,
        durable_manifest,
        Arc::clone(&cache),
        store_id,
        open_budget,
    )?;
    match property_spills {
        Some(property_spills) => CanonicalSegmentReader::open_with_property_spills(
            root.join(canonical_artifact_generation_file(generation)),
            canonical_manifest,
            cache,
            store_id,
            max_segment_bytes,
            property_spills,
        ),
        None => CanonicalSegmentReader::open(
            root.join(canonical_artifact_generation_file(generation)),
            canonical_manifest,
            cache,
            store_id,
            max_segment_bytes,
        ),
    }
    .map(Some)
    .map_err(|error| SkeinError::Storage(error.to_string()))
}

fn load_published_property_spills(
    root: &Path,
    durable_manifest: DurableManifest,
    cache: Arc<SegmentCache>,
    store_id: StoreId,
    open_budget: &mut GraphManifestOpenBudget,
) -> Result<Option<PropertySpillReader>> {
    let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
        durable_manifest.property_spill_manifest_encoded_len,
        durable_manifest.property_spill_manifest_encoded_checksum,
        durable_manifest.property_spill_manifest_encoded_sha256,
    ) else {
        return Ok(None);
    };
    let generation = durable_manifest.checkpoint_generation.ok_or_else(|| {
        SkeinError::Storage("property spill metadata requires a checkpoint generation".to_string())
    })?;
    let manifest_path = root.join(property_spill_manifest_generation_file(generation));
    let encoded = read_bound_graph_manifest(
        &manifest_path,
        expected_len,
        expected_checksum,
        expected_sha256,
        PROPERTY_SPILL_MANIFEST_MAX_BYTES,
        "property spill manifest",
        open_budget,
    )?;
    let text = std::str::from_utf8(&encoded).map_err(|error| {
        SkeinError::Storage(format!("property spill manifest is not UTF-8: {error}"))
    })?;
    let manifest = PropertySpillManifest::decode(text)
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
    if manifest.generation != ManifestGeneration(generation) {
        return Err(SkeinError::Storage(format!(
            "property spill generation {} does not match durable generation {generation}",
            manifest.generation.0
        )));
    }
    if manifest.source_commit_epoch != durable_manifest.checkpoint_commit_epoch {
        return Err(SkeinError::Storage(format!(
            "property spill source epoch {} does not match durable checkpoint epoch {}",
            manifest.source_commit_epoch, durable_manifest.checkpoint_commit_epoch
        )));
    }
    let descriptor_tree = PersistentPropertySpillDescriptorTree::new(
        GraphDescriptorTreePaths::new(
            root.join(skein_storage::property_spill_descriptor_page_file(
                generation,
            )),
            root.join(skein_storage::property_spill_descriptor_root_file(
                generation,
            )),
        ),
        GraphDescriptorTreeBuildConfig::default(),
    );
    let config = PropertySpillConfig::default();
    let max_block_bytes = NonZeroU64::new(
        config
            .target_block_bytes
            .get()
            .max(config.max_value_bytes.get().saturating_add(1024)),
    )
    .expect("property spill maximum block size is non-zero");
    PropertySpillReader::open(
        root.join(property_spill_artifact_generation_file(generation)),
        manifest,
        descriptor_tree,
        cache,
        store_id,
        max_block_bytes,
    )
    .map(Some)
    .map_err(|error| SkeinError::Storage(error.to_string()))
}

pub(super) fn load_published_property_projection(
    root: &Path,
    durable_manifest: DurableManifest,
    cache: Arc<SegmentCache>,
    store_id: StoreId,
    open_budget: &mut GraphManifestOpenBudget,
) -> Result<Option<PersistentPropertyProjectionReader>> {
    let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
        durable_manifest.property_projection_manifest_encoded_len,
        durable_manifest.property_projection_manifest_encoded_checksum,
        durable_manifest.property_projection_manifest_encoded_sha256,
    ) else {
        return Ok(None);
    };
    let generation = durable_manifest.checkpoint_generation.ok_or_else(|| {
        SkeinError::Storage(
            "property projection metadata requires a checkpoint generation".to_string(),
        )
    })?;
    let manifest_path = root.join(property_projection_manifest_generation_file(generation));
    let encoded = read_bound_graph_manifest(
        &manifest_path,
        expected_len,
        expected_checksum,
        expected_sha256,
        PROPERTY_PROJECTION_MANIFEST_MAX_BYTES,
        "property projection manifest",
        open_budget,
    )?;
    let text = std::str::from_utf8(&encoded).map_err(|error| {
        SkeinError::Storage(format!(
            "property projection manifest is not UTF-8: {error}"
        ))
    })?;
    let manifest = PersistentPropertyProjectionManifest::decode(text)
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
    if manifest.generation != ManifestGeneration(generation)
        || manifest.source_commit_epoch != durable_manifest.checkpoint_commit_epoch
    {
        return Err(SkeinError::Storage(
            "property projection generation or source epoch does not match the durable checkpoint"
                .to_string(),
        ));
    }
    let config = PersistentPropertyProjectionConfig::default();
    let max_block_bytes = NonZeroU64::new(
        config
            .target_block_bytes
            .get()
            .max(config.max_index_key_bytes.get().saturating_add(1024)),
    )
    .expect("property projection maximum block size is non-zero");
    PersistentPropertyProjectionReader::open(
        root.join(property_projection_artifact_generation_file(generation)),
        manifest,
        PersistentPropertyProjectionDescriptorTree::new(
            GraphDescriptorTreePaths::new(
                root.join(skein_storage::property_projection_descriptor_page_file(
                    generation,
                )),
                root.join(skein_storage::property_projection_descriptor_root_file(
                    generation,
                )),
            ),
            GraphDescriptorTreeBuildConfig::default(),
        ),
        cache,
        store_id,
        max_block_bytes,
    )
    .map(Some)
    .map_err(|error| SkeinError::Storage(error.to_string()))
}

pub(super) fn load_published_canonical_adjacency(
    root: &Path,
    durable_manifest: DurableManifest,
    cache: Arc<SegmentCache>,
    store_id: StoreId,
    open_budget: &mut GraphManifestOpenBudget,
) -> Result<Option<CanonicalAdjacencyReader>> {
    let Some(binding) = durable_manifest.canonical_adjacency_generation_artifacts else {
        return Ok(None);
    };
    let generation = durable_manifest.checkpoint_generation.ok_or_else(|| {
        SkeinError::Storage(
            "canonical adjacency metadata requires a checkpoint generation".to_string(),
        )
    })?;
    if binding.generation != generation {
        return Err(SkeinError::Storage(format!(
            "canonical adjacency generation {} does not match durable generation {generation}",
            binding.generation
        )));
    }
    open_budget.admit(
        binding.descriptor_root_artifact.encoded_len,
        "canonical adjacency descriptor root",
    )?;
    let descriptor_config = GraphDescriptorTreeBuildConfig::default();
    let descriptor_paths = GraphDescriptorTreePaths::new(
        root.join(skein_storage::canonical_adjacency_descriptor_page_file(
            generation,
        )),
        root.join(skein_storage::canonical_adjacency_descriptor_root_file(
            generation,
        )),
    );
    let root_reader = GraphDescriptorTreeRootReader::open_bound(
        descriptor_paths,
        GraphDescriptorTreeGenerationArtifacts {
            kind: GraphDescriptorKind::CanonicalAdjacency,
            generation: binding.generation,
            source_commit_epoch: binding.source_commit_epoch,
            root_artifact: binding.descriptor_root_artifact,
        },
        descriptor_config,
    )
    .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))?;
    let config = CanonicalAdjacencyConfig::default();
    let max_block_bytes = NonZeroU64::new(
        config
            .target_block_bytes
            .get()
            .max(config.max_record_bytes.get().saturating_add(1024)),
    )
    .expect("canonical adjacency maximum block size is non-zero");
    CanonicalAdjacencyReader::open_demand_paged(
        root.join(canonical_adjacency_artifact_generation_file(generation)),
        binding,
        root_reader,
        descriptor_config,
        cache,
        store_id,
        max_block_bytes,
    )
    .map(Some)
    .map_err(|error| SkeinError::StorageIntegrity(error.to_string()))
}
