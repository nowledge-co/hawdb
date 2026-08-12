//! The durable store: checkpoint publication, WAL management, the durable
//! manifest, and loading of published derived artifacts.

use super::{
    canonical_adjacency_artifact_generation_file, canonical_adjacency_manifest_generation_file,
    canonical_artifact_generation_file, canonical_manifest_generation_file,
    checkpoint_generation_file, checkpoint_publish_failpoint, checksum_bytes,
    cleanup_abandoned_checkpoint_preparations, copy_backup_file, decode_projected_graph_artifacts,
    decode_stable_id_mapping, derived_repair, doctor, elapsed_micros, encode_binary_wal_header,
    encode_binary_wal_record, encode_bool, encode_durable_text, encode_index_kind, encode_nullable,
    encode_optional_sha256, encode_optional_u64, encode_property_type, encode_schema_object_state,
    encode_stable_id_mapping, encode_string, encode_string_vec, encode_table_kind, encode_u64_vec,
    encode_value_vec, encode_wal_header, file_checksum, frame_binary_wal_record,
    has_storage_artifacts, parse_optional_sha256, parse_optional_u64, parse_u64,
    process_crash_failpoint, property_projection_artifact_generation_file,
    property_projection_manifest_generation_file, property_spill_artifact_generation_file,
    property_spill_manifest_generation_file, read_durable_text, read_durable_text_bytes_with_limit,
    relational_checkpoint_generation_file, remove_source_scan_artifacts, safe_reclaim_commit_epoch,
    sniff_wal_format, source_scan, split_manifest_checksum,
    split_projected_graph_artifact_checksum, split_stable_id_mapping_checksum,
    storage_generation_for_file, store_id_for_path, sync_parent_dir, validate_backup_files,
    validate_new_backup_destination, validate_search_projection_checkpoint_changes,
    validate_storage_version, verify_integrity, wal_generation_file, wal_group_sync_failpoint,
    CheckpointPublishStage, ProjectedGraphArtifact, WalCursorEvent, WalEntry, WalFileFormat, WalOp,
    WalOpenOutcome, WalRecordCursor, BACKUP_MANIFEST_FILE, CANONICAL_ADJACENCY_MANIFEST_MAX_BYTES,
    CANONICAL_MANIFEST_MAX_BYTES, CHECKPOINT_HEADER_V1, MANIFEST_FILE, MANIFEST_HEADER_V1,
    PROJECTED_GRAPHS_FILE, PROPERTY_PROJECTION_MANIFEST_MAX_BYTES,
    PROPERTY_SPILL_MANIFEST_MAX_BYTES, STABLE_ID_MAPPING_FILE, STORAGE_VERSION,
    WAL_BINARY_FILE_HEADER_BYTES,
};
use crate::error::{Result, SkeinError};
use crate::schema::{Catalog, GraphStatistics};
use crate::telemetry::{KernelTelemetry, KernelTelemetryOperation, TelemetrySink};
use crate::value::Value;
use skein_integrity::{integrity_digest, Sha256Digest};
use skein_storage::{
    decode_relational_checkpoint_file, durable_replace_file,
    encode_relational_checkpoint_to_writer, CanonicalAdjacencyConfig, CanonicalAdjacencyManifest,
    CanonicalAdjacencyReader, CanonicalAdjacencyWriter, CanonicalSegmentConfig,
    CanonicalSegmentError, CanonicalSegmentManifest, CanonicalSegmentReader,
    CanonicalSegmentWriter, DatabaseDirectoryLease, DurabilityPolicy, DurableCompression,
    FileSegmentRangeReader, ManifestGeneration, NodeId, NodeRecord,
    PersistentPropertyProjectionConfig, PersistentPropertyProjectionDefinition,
    PersistentPropertyProjectionManifest, PersistentPropertyProjectionReader,
    PersistentPropertyProjectionWriter, ProjectedGraphDefinition, PropertySpillConfig,
    PropertySpillManifest, PropertySpillReader, RelId, RelRecord, RelationalDecodeLimits,
    RelationalState, ScanSegmentManifest, SearchProjectionGraphChange, SegmentCache,
    StorageBackupReport, StorageDebtController, StoragePressureSignals, StorageScrubReport,
    StoreId, StoreStableIdMapping, WalReplayConfig, WalSyncGroupFlush, WalSyncGroupProgress,
    WalSyncGroupState,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(super) struct DurableStore {
    _directory_lease: Arc<DatabaseDirectoryLease>,
    pub(super) root_path: PathBuf,
    pub(super) checkpoint_path: PathBuf,
    manifest_path: PathBuf,
    projected_graphs_path: PathBuf,
    stable_id_mapping_path: PathBuf,
    pub(super) wal_path: PathBuf,
    pub(super) checkpoint_encoded_len: Option<u64>,
    checkpoint_encoded_checksum: Option<u64>,
    checkpoint_encoded_sha256: Option<Sha256Digest>,
    pub(super) relational_checkpoint_encoded_len: Option<u64>,
    pub(super) relational_checkpoint_encoded_checksum: Option<u64>,
    pub(super) relational_checkpoint_encoded_sha256: Option<Sha256Digest>,
    canonical_manifest_encoded_len: Option<u64>,
    canonical_manifest_encoded_checksum: Option<u64>,
    canonical_manifest_encoded_sha256: Option<Sha256Digest>,
    canonical_adjacency_manifest_encoded_len: Option<u64>,
    canonical_adjacency_manifest_encoded_checksum: Option<u64>,
    canonical_adjacency_manifest_encoded_sha256: Option<Sha256Digest>,
    property_spill_manifest_encoded_len: Option<u64>,
    property_spill_manifest_encoded_checksum: Option<u64>,
    property_spill_manifest_encoded_sha256: Option<Sha256Digest>,
    property_projection_manifest_encoded_len: Option<u64>,
    property_projection_manifest_encoded_checksum: Option<u64>,
    property_projection_manifest_encoded_sha256: Option<Sha256Digest>,
    pub(super) wal_generation: u64,
    pub(super) checkpoint_epoch: u64,
    pub(super) checkpoint_commit_epoch: u64,
    pub(super) oldest_reader_commit_epoch: Option<u64>,
    pub(super) safe_reclaim_commit_epoch: u64,
    pub(super) wal_replay_start_lsn: u64,
    pub(super) next_lsn: u64,
    pub(super) wal_bytes: u64,
    /// Encoding of the active WAL generation file. Existing text (V1)
    /// generations keep appending text records; every new generation is
    /// binary, so a database upgrades at its next checkpoint rotation.
    pub(super) wal_format: WalFileFormat,
    /// Commit epoch recorded in binary WAL records (spec §3.4.3). Advisory:
    /// replay derives commit epochs from LSN order, exactly as before.
    pub(super) wal_commit_epoch: u64,
    pub(super) max_wal_bytes: Option<u64>,
    source_scan_commit_epoch: Option<u64>,
    source_scan_descriptor_checksum: Option<u64>,
    store_id: StoreId,
    pub(super) segment_cache: Arc<SegmentCache>,
    pub(super) canonical_segments: Option<CanonicalSegmentReader>,
    pub(super) canonical_adjacency: Option<CanonicalAdjacencyReader>,
    pub(super) persistent_property_projection: Option<PersistentPropertyProjectionReader>,
    pub(super) source_scan_reader: FileSegmentRangeReader,
    durability: DurabilityPolicy,
    pub(super) read_only: bool,
    max_record_bytes: Option<usize>,
    max_batch_operations: Option<usize>,
    pub(super) telemetry: Option<Arc<dyn TelemetrySink>>,
    wal_sync_group: Option<WalSyncGroupState>,
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
    max_wal_bytes: Option<u64>,
    max_record_bytes: Option<usize>,
    max_batch_operations: Option<usize>,
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
pub(super) struct CheckpointManifestArtifacts {
    pub(super) checkpoint: DurableArtifactMetadata,
    pub(super) relational_checkpoint: Option<DurableArtifactMetadata>,
    pub(super) canonical_manifest: DurableArtifactMetadata,
    pub(super) canonical_adjacency_manifest: DurableArtifactMetadata,
    pub(super) property_spill_manifest: DurableArtifactMetadata,
    pub(super) property_projection_manifest: DurableArtifactMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BackupFileEntry {
    pub(super) name: String,
    pub(super) encoded_len: u64,
    pub(super) encoded_checksum: u64,
    pub(super) sha256: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BackupManifest {
    pub(super) generation: u64,
    pub(super) checkpoint_commit_epoch: u64,
    pub(super) files: Vec<BackupFileEntry>,
    pub(super) checksum: u64,
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
        segment_cache_capacity_bytes: u64,
        max_wal_bytes: Option<u64>,
        max_record_bytes: Option<usize>,
        max_batch_operations: Option<usize>,
    ) -> Result<Self> {
        fs::create_dir_all(path)?;
        Self::open_existing(
            path,
            durability,
            DurableStoreOpenOptions {
                read_only: false,
                initialize_if_empty: true,
                load_rebuildable_artifacts: true,
                segment_cache_capacity_bytes,
                max_wal_bytes,
                max_record_bytes,
                max_batch_operations,
            },
        )
    }

    pub(super) fn open_existing_only(
        path: &Path,
        durability: DurabilityPolicy,
        segment_cache_capacity_bytes: u64,
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
                segment_cache_capacity_bytes,
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
                segment_cache_capacity_bytes,
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
            max_wal_bytes,
            max_record_bytes,
            max_batch_operations,
        } = options;
        let directory_lease = DatabaseDirectoryLease::acquire(path)
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        doctor::reject_pending_wal_doctor_repair(path)?;
        if load_rebuildable_artifacts {
            derived_repair::reject_pending_derived_artifact_repair(path)?;
        }
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
        let wal_format = sniff_wal_format(&wal_path)?;
        let segment_cache = Arc::new(SegmentCache::new(segment_cache_capacity_bytes));
        let store_id = store_id_for_path(path)?;
        let canonical_segments = load_published_canonical_segments(
            path,
            manifest,
            Arc::clone(&segment_cache),
            store_id,
        )?;
        let canonical_adjacency = load_rebuildable_artifacts
            .then(|| {
                load_published_canonical_adjacency(
                    path,
                    manifest,
                    Arc::clone(&segment_cache),
                    store_id,
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
                )
            })
            .transpose()?
            .flatten();
        if let (Some(canonical), Some(adjacency)) = (&canonical_segments, &canonical_adjacency)
            && canonical.manifest().relationship_count != adjacency.manifest().relationship_count
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
            checkpoint_encoded_len: manifest.checkpoint_encoded_len,
            checkpoint_encoded_checksum: manifest.checkpoint_encoded_checksum,
            checkpoint_encoded_sha256: manifest.checkpoint_encoded_sha256,
            relational_checkpoint_encoded_len: None,
            relational_checkpoint_encoded_checksum: None,
            relational_checkpoint_encoded_sha256: None,
            canonical_manifest_encoded_len: manifest.canonical_manifest_encoded_len,
            canonical_manifest_encoded_checksum: manifest.canonical_manifest_encoded_checksum,
            canonical_manifest_encoded_sha256: manifest.canonical_manifest_encoded_sha256,
            canonical_adjacency_manifest_encoded_len: manifest
                .canonical_adjacency_manifest_encoded_len,
            canonical_adjacency_manifest_encoded_checksum: manifest
                .canonical_adjacency_manifest_encoded_checksum,
            canonical_adjacency_manifest_encoded_sha256: manifest
                .canonical_adjacency_manifest_encoded_sha256,
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
            wal_generation: manifest.wal_generation,
            checkpoint_epoch: manifest.checkpoint_epoch,
            checkpoint_commit_epoch: manifest.checkpoint_commit_epoch,
            oldest_reader_commit_epoch: manifest.oldest_reader_commit_epoch,
            safe_reclaim_commit_epoch: manifest.safe_reclaim_commit_epoch,
            wal_replay_start_lsn: manifest.wal_replay_start_lsn,
            next_lsn: manifest.next_lsn,
            wal_bytes,
            wal_format,
            wal_commit_epoch: manifest.checkpoint_commit_epoch,
            max_wal_bytes,
            source_scan_commit_epoch: manifest.source_scan_commit_epoch,
            source_scan_descriptor_checksum: manifest.source_scan_descriptor_checksum,
            store_id,
            segment_cache,
            canonical_segments,
            canonical_adjacency,
            persistent_property_projection,
            source_scan_reader,
            durability,
            read_only,
            max_record_bytes,
            max_batch_operations,
            telemetry: None,
            wal_sync_group: None,
        })
    }

    pub(super) fn root_path(&self) -> &Path {
        &self.root_path
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

    pub(super) fn finish_wal_sync_group(&mut self) -> Result<WalSyncGroupFlush> {
        let Some(group) = self.wal_sync_group.take() else {
            return Ok(WalSyncGroupFlush::default());
        };
        if group.is_empty() {
            return Ok(WalSyncGroupFlush::default());
        }
        wal_group_sync_failpoint()?;
        let started = std::time::Instant::now();
        let file = OpenOptions::new().write(true).open(&self.wal_path)?;
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
            let mut sources = vec![
                (MANIFEST_FILE.to_string(), self.manifest_path.clone()),
                (
                    checkpoint_generation_file(generation),
                    self.checkpoint_path.clone(),
                ),
                (wal_generation_file(generation), self.wal_path.clone()),
            ];
            let relational_checkpoint_name = relational_checkpoint_generation_file(generation);
            let relational_checkpoint_path = self.root_path.join(&relational_checkpoint_name);
            if self.relational_checkpoint_encoded_len.is_some() {
                sources.push((relational_checkpoint_name, relational_checkpoint_path));
            }
            if self.canonical_manifest_encoded_len.is_some() {
                sources.push((
                    canonical_artifact_generation_file(generation),
                    self.root_path
                        .join(canonical_artifact_generation_file(generation)),
                ));
                sources.push((
                    canonical_manifest_generation_file(generation),
                    self.root_path
                        .join(canonical_manifest_generation_file(generation)),
                ));
            }
            if self.canonical_adjacency_manifest_encoded_len.is_some() {
                sources.push((
                    canonical_adjacency_artifact_generation_file(generation),
                    self.root_path
                        .join(canonical_adjacency_artifact_generation_file(generation)),
                ));
                sources.push((
                    canonical_adjacency_manifest_generation_file(generation),
                    self.root_path
                        .join(canonical_adjacency_manifest_generation_file(generation)),
                ));
            }
            if self.property_spill_manifest_encoded_len.is_some() {
                sources.push((
                    property_spill_artifact_generation_file(generation),
                    self.root_path
                        .join(property_spill_artifact_generation_file(generation)),
                ));
                sources.push((
                    property_spill_manifest_generation_file(generation),
                    self.root_path
                        .join(property_spill_manifest_generation_file(generation)),
                ));
            }
            if self.property_projection_manifest_encoded_len.is_some() {
                sources.push((
                    property_projection_artifact_generation_file(generation),
                    self.root_path
                        .join(property_projection_artifact_generation_file(generation)),
                ));
                sources.push((
                    property_projection_manifest_generation_file(generation),
                    self.root_path
                        .join(property_projection_manifest_generation_file(generation)),
                ));
            }
            let mut files = Vec::with_capacity(sources.len().saturating_add(1));
            for (name, source) in sources {
                files.push(copy_backup_file(&source, &destination.join(&name), &name)?);
            }
            if self.stable_id_mapping_path.exists() {
                files.push(copy_backup_file(
                    &self.stable_id_mapping_path,
                    &destination.join(STABLE_ID_MAPPING_FILE),
                    STABLE_ID_MAPPING_FILE,
                )?);
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
        let mut checked_file_count = 1usize;
        let mut checked_bytes = fs::metadata(&self.manifest_path)?.len();
        let mut sha256_verified_file_count = 0usize;

        let mut verify_path = |path: &Path,
                               expected_len: u64,
                               expected_checksum: u64,
                               expected_sha256: Sha256Digest,
                               artifact: &str|
         -> Result<()> {
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
            checked_file_count = checked_file_count.saturating_add(1);
            checked_bytes = checked_bytes.saturating_add(actual_len);
            sha256_verified_file_count = sha256_verified_file_count.saturating_add(1);
            Ok(())
        };

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
            verify_path(
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
            verify_path(
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
            verify_path(
                &manifest_path,
                expected_len,
                expected_checksum,
                expected_sha256,
                "canonical manifest",
            )?;
            let artifact = CanonicalSegmentManifest::decode(&fs::read_to_string(&manifest_path)?)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
            verify_path(
                &self
                    .root_path
                    .join(canonical_artifact_generation_file(generation)),
                artifact.artifact_len,
                artifact.artifact_digest.0,
                artifact.artifact_sha256,
                "canonical artifact",
            )?;
        }

        if let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
            manifest.canonical_adjacency_manifest_encoded_len,
            manifest.canonical_adjacency_manifest_encoded_checksum,
            manifest.canonical_adjacency_manifest_encoded_sha256,
        ) {
            let generation = manifest
                .checkpoint_generation
                .expect("validated adjacency metadata has a checkpoint generation");
            let manifest_path = self
                .root_path
                .join(canonical_adjacency_manifest_generation_file(generation));
            verify_path(
                &manifest_path,
                expected_len,
                expected_checksum,
                expected_sha256,
                "canonical adjacency manifest",
            )?;
            let artifact = CanonicalAdjacencyManifest::decode(&fs::read_to_string(&manifest_path)?)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
            verify_path(
                &self
                    .root_path
                    .join(canonical_adjacency_artifact_generation_file(generation)),
                artifact.artifact_len,
                artifact.artifact_digest.0,
                artifact.artifact_sha256,
                "canonical adjacency artifact",
            )?;
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
            verify_path(
                &manifest_path,
                expected_len,
                expected_checksum,
                expected_sha256,
                "property spill manifest",
            )?;
            let artifact = PropertySpillManifest::decode(&fs::read_to_string(&manifest_path)?)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
            verify_path(
                &self
                    .root_path
                    .join(property_spill_artifact_generation_file(generation)),
                artifact.artifact_len,
                artifact.artifact_digest.0,
                artifact.artifact_sha256,
                "property spill artifact",
            )?;
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
            verify_path(
                &manifest_path,
                expected_len,
                expected_checksum,
                expected_sha256,
                "property projection manifest",
            )?;
            let artifact =
                PersistentPropertyProjectionManifest::decode(&fs::read_to_string(&manifest_path)?)
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
            verify_path(
                &self
                    .root_path
                    .join(property_projection_artifact_generation_file(generation)),
                artifact.artifact_len,
                artifact.artifact_digest.0,
                artifact.artifact_sha256,
                "property projection artifact",
            )?;
        }

        let (wal_record_count, wal_bytes) = self.scrub_wal()?;
        if self.wal_path.exists() {
            checked_file_count = checked_file_count.saturating_add(1);
            checked_bytes = checked_bytes.saturating_add(wal_bytes);
        }
        Ok(StorageScrubReport {
            generation: manifest.wal_generation,
            checked_file_count,
            checked_bytes,
            sha256_verified_file_count,
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

    pub(super) fn append_create_node(
        &mut self,
        id: NodeId,
        label: &str,
        properties: &BTreeMap<String, Value>,
    ) -> Result<()> {
        self.append_entry(
            WalOp::CreateNode {
                id,
                label: label.to_string(),
                properties: properties.clone(),
            },
            1,
        )
    }

    pub(super) fn append_create_relationship(
        &mut self,
        id: RelId,
        source: NodeId,
        target: NodeId,
        rel_type: &str,
        properties: &BTreeMap<String, Value>,
    ) -> Result<()> {
        self.append_entry(
            WalOp::CreateRelationship {
                id,
                source,
                target,
                rel_type: rel_type.to_string(),
                properties: properties.clone(),
            },
            1,
        )
    }

    pub(super) fn append_batch(&mut self, ops: Vec<WalOp>) -> Result<()> {
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
        self.append_entry(WalOp::Batch(ops), operation_count)
    }

    pub(super) fn append_project_graph(
        &mut self,
        name: &str,
        definition: &ProjectedGraphDefinition,
    ) -> Result<()> {
        self.append_entry(
            WalOp::ProjectGraph {
                name: name.to_string(),
                node_labels: definition.node_labels.clone(),
                rel_types: definition.rel_types.clone(),
            },
            1,
        )
    }

    fn append_entry(&mut self, op: WalOp, operation_count: usize) -> Result<()> {
        let entry = WalEntry {
            lsn: self.next_lsn,
            op,
        };
        // Header bytes are written when the record starts a fresh file
        // (text keeps its original created-file trigger); record bytes are
        // the framed record itself.
        let (header_bytes, record_bytes) = match self.wal_format {
            WalFileFormat::TextV1 => {
                let encoded_entry = entry.encode();
                if self
                    .max_record_bytes
                    .is_some_and(|limit| encoded_entry.len().saturating_add(1) > limit)
                {
                    return Err(SkeinError::Storage(format!(
                        "WAL record byte limit exceeded before append: max_wal_record_bytes={}",
                        self.max_record_bytes.unwrap_or_default()
                    )));
                }
                let mut header =
                    encode_wal_header(self.wal_generation, self.wal_replay_start_lsn).into_bytes();
                header.push(b'\n');
                let mut record = encoded_entry.into_bytes();
                record.push(b'\n');
                (header, record)
            }
            WalFileFormat::BinaryV2 => {
                let payload =
                    encode_binary_wal_record(&entry, self.wal_commit_epoch.saturating_add(1));
                if self
                    .max_record_bytes
                    .is_some_and(|limit| payload.len() > limit)
                {
                    return Err(SkeinError::Storage(format!(
                        "WAL record byte limit exceeded before append: max_wal_record_bytes={}",
                        self.max_record_bytes.unwrap_or_default()
                    )));
                }
                let header =
                    encode_binary_wal_header(self.wal_generation, self.wal_replay_start_lsn);
                let position = self
                    .wal_bytes
                    .saturating_sub(WAL_BINARY_FILE_HEADER_BYTES as u64);
                (
                    header,
                    frame_binary_wal_record(self.wal_generation, &payload, position),
                )
            }
        };
        let started = std::time::Instant::now();
        let mut byte_count = record_bytes.len() as u64;
        if self.wal_bytes == 0 {
            byte_count = byte_count.saturating_add(header_bytes.len() as u64);
        }
        self.ensure_wal_admission(self.wal_bytes.saturating_add(byte_count))?;
        process_crash_failpoint("before_wal_append");
        let sync_deferred = self.wal_sync_group.is_some();
        let result = (|| {
            let (mut file, created) = self.open_wal_append()?;
            let write_header = match self.wal_format {
                WalFileFormat::TextV1 => created,
                WalFileFormat::BinaryV2 => self.wal_bytes == 0,
            };
            if write_header {
                file.write_all(&header_bytes)?;
            }
            file.write_all(&record_bytes)?;
            process_crash_failpoint("after_wal_append");
            let fsync_micros = self.finish_wal_append(&mut file, created)?;
            if !sync_deferred {
                process_crash_failpoint("after_wal_sync");
            }
            Ok(fsync_micros)
        })();
        if let Some(telemetry) = &self.telemetry {
            telemetry.record_kernel(KernelTelemetry {
                operation: KernelTelemetryOperation::WalAppend,
                success: result.is_ok(),
                elapsed_micros: elapsed_micros(started),
                item_count: operation_count,
                byte_count,
                fsync_micros: result.as_ref().copied().unwrap_or_default(),
                generation: Some(self.wal_generation),
            });
        }
        if result.is_ok() {
            self.next_lsn += 1;
            self.wal_commit_epoch = self.wal_commit_epoch.saturating_add(1);
            self.wal_bytes = self.wal_bytes.saturating_add(byte_count);
            if let Some(group) = &mut self.wal_sync_group {
                group.record_entry(byte_count);
            }
        } else if let Ok(metadata) = fs::metadata(&self.wal_path) {
            self.wal_bytes = metadata.len();
        }
        result.map(|_| ())
    }

    fn ensure_wal_admission(&self, projected_wal_bytes: u64) -> Result<()> {
        let pressure = StorageDebtController.evaluate(StoragePressureSignals {
            wal_bytes: projected_wal_bytes,
            max_wal_bytes: self.max_wal_bytes,
            ..StoragePressureSignals::default()
        });
        if pressure.state.admits_mutation() {
            return Ok(());
        }
        let reasons = pressure
            .reason_codes
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>()
            .join(",");
        Err(SkeinError::Storage(format!(
            "WAL append rejected by storage pressure: state={}, projected_wal_bytes={projected_wal_bytes}, max_wal_bytes={}, reasons={reasons}; checkpoint the database before retrying",
            pressure.state.as_str(),
            self.max_wal_bytes.unwrap_or_default()
        )))
    }

    fn open_wal_append(&self) -> Result<(File, bool)> {
        let created = !self.wal_path.exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)?;
        Ok((file, created))
    }

    fn finish_wal_append(&mut self, file: &mut File, created: bool) -> Result<u64> {
        file.flush()?;
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
        let (canonical_manifest, property_spill_manifest) =
            CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
                .write_fallible_with_property_spills(
                    &artifact_path,
                    &property_artifact_path,
                    ManifestGeneration(generation),
                    nodes,
                    relationships,
                    PropertySpillConfig::default(),
                )
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
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
        config: CanonicalAdjacencyConfig,
    ) -> Result<DurableArtifactMetadata>
    where
        R: IntoIterator<
            Item = std::result::Result<RelRecord, skein_storage::CanonicalAdjacencyError>,
        >,
    {
        let artifact_path = self
            .root_path
            .join(canonical_adjacency_artifact_generation_file(generation));
        let output = CanonicalAdjacencyWriter::new(config)
            .write_fallible(
                &artifact_path,
                ManifestGeneration(generation),
                relationships,
            )
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let encoded = output
            .manifest
            .encode()
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let metadata = DurableArtifactMetadata::for_bytes(encoded.as_bytes());
        let manifest_path = self
            .root_path
            .join(canonical_adjacency_manifest_generation_file(generation));
        let tmp_path = manifest_path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(encoded.as_bytes())?;
            file.sync_all()?;
        }
        durable_replace_file(&tmp_path, &manifest_path)?;
        Ok(metadata)
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
                NodeRecord,
                skein_storage::PersistentPropertyProjectionError,
            >,
        >,
    {
        let artifact_path = self
            .root_path
            .join(property_projection_artifact_generation_file(generation));
        let output = PersistentPropertyProjectionWriter::new(config)
            .write_fallible(
                &artifact_path,
                ManifestGeneration(generation),
                source_commit_epoch,
                definitions,
                nodes,
            )
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
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
        if state.is_empty() {
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
            body.push_str(&format!(
                "search_projection_change\t{}\t{}\t{}\n",
                change.commit_epoch,
                encode_u64_vec(change.upsert_node_ids.iter().copied()),
                encode_string_vec(&change.delete_document_ids)
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
            canonical_adjacency_artifact_generation_file(generation),
            canonical_adjacency_manifest_generation_file(generation),
            property_spill_artifact_generation_file(generation),
            property_spill_manifest_generation_file(generation),
            property_projection_artifact_generation_file(generation),
            property_projection_manifest_generation_file(generation),
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

    pub(super) fn write_stable_id_mapping(&self, mapping: &StoreStableIdMapping) -> Result<()> {
        let body = encode_stable_id_mapping(mapping);
        let checksum = checksum_bytes(body.as_bytes());
        let data = format!("{body}checksum\t{checksum}\n");
        let tmp_path = self.stable_id_mapping_path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            let encoded = encode_durable_text(&data, DurableCompression::default())?;
            file.write_all(&encoded)?;
            file.sync_all()?;
        }
        durable_replace_file(&tmp_path, &self.stable_id_mapping_path)?;
        Ok(())
    }

    pub(super) fn load_stable_id_mapping(&self) -> Result<StoreStableIdMapping> {
        if !self.stable_id_mapping_path.exists() {
            return Ok(StoreStableIdMapping::default());
        }
        let text = read_durable_text(&self.stable_id_mapping_path, "stable id mapping")?;
        let (body, checksum) = split_stable_id_mapping_checksum(&text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "stable id mapping checksum mismatch: expected {checksum}, got {actual}"
            )));
        }
        decode_stable_id_mapping(body)
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
        let safe_reclaim_commit_epoch =
            safe_reclaim_commit_epoch(checkpoint_commit_epoch, oldest_reader_commit_epoch);
        let source_scan_commit_epoch = source_scan_publication.map(|value| value.graph_epoch());
        let source_scan_descriptor_checksum =
            source_scan_publication.map(|value| value.descriptor_checksum());
        let CheckpointManifestArtifacts {
            checkpoint,
            relational_checkpoint,
            canonical_manifest,
            canonical_adjacency_manifest,
            property_spill_manifest,
            property_projection_manifest,
        } = artifacts;
        let manifest = DurableManifest {
            checkpoint_generation: Some(generation),
            checkpoint_encoded_len: Some(checkpoint.encoded_len),
            checkpoint_encoded_checksum: Some(checkpoint.encoded_checksum),
            checkpoint_encoded_sha256: Some(checkpoint.encoded_sha256),
            canonical_manifest_encoded_len: Some(canonical_manifest.encoded_len),
            canonical_manifest_encoded_checksum: Some(canonical_manifest.encoded_checksum),
            canonical_manifest_encoded_sha256: Some(canonical_manifest.encoded_sha256),
            canonical_adjacency_manifest_encoded_len: Some(
                canonical_adjacency_manifest.encoded_len,
            ),
            canonical_adjacency_manifest_encoded_checksum: Some(
                canonical_adjacency_manifest.encoded_checksum,
            ),
            canonical_adjacency_manifest_encoded_sha256: Some(
                canonical_adjacency_manifest.encoded_sha256,
            ),
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
        self.canonical_adjacency_manifest_encoded_len =
            manifest.canonical_adjacency_manifest_encoded_len;
        self.canonical_adjacency_manifest_encoded_checksum =
            manifest.canonical_adjacency_manifest_encoded_checksum;
        self.canonical_adjacency_manifest_encoded_sha256 =
            manifest.canonical_adjacency_manifest_encoded_sha256;
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
        self.wal_generation = manifest.wal_generation;
        self.checkpoint_epoch = manifest.checkpoint_epoch;
        self.checkpoint_commit_epoch = manifest.checkpoint_commit_epoch;
        self.oldest_reader_commit_epoch = manifest.oldest_reader_commit_epoch;
        self.safe_reclaim_commit_epoch = manifest.safe_reclaim_commit_epoch;
        self.wal_replay_start_lsn = manifest.wal_replay_start_lsn;
        self.wal_bytes = fs::metadata(&self.wal_path)?.len();
        self.wal_format = WalFileFormat::BinaryV2;
        self.wal_commit_epoch = manifest.checkpoint_commit_epoch;
        self.source_scan_commit_epoch = manifest.source_scan_commit_epoch;
        self.source_scan_descriptor_checksum = manifest.source_scan_descriptor_checksum;
        self.canonical_segments = load_published_canonical_segments(
            &self.root_path,
            manifest,
            Arc::clone(&self.segment_cache),
            self.store_id,
        )?;
        self.canonical_adjacency = load_published_canonical_adjacency(
            &self.root_path,
            manifest,
            Arc::clone(&self.segment_cache),
            self.store_id,
        )?;
        self.persistent_property_projection = load_published_property_projection(
            &self.root_path,
            manifest,
            Arc::clone(&self.segment_cache),
            self.store_id,
        )?;
        if let (Some(canonical), Some(adjacency)) =
            (&self.canonical_segments, &self.canonical_adjacency)
            && canonical.manifest().relationship_count != adjacency.manifest().relationship_count
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
        self.reclaim_old_generations(generation)
    }

    fn reclaim_old_generations(&self, current_generation: u64) -> Result<()> {
        if self.oldest_reader_commit_epoch.is_some() {
            return Ok(());
        }
        let retain_from = current_generation.saturating_sub(1);
        for entry in fs::read_dir(&self.root_path)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let generation = storage_generation_for_file(name);
            if generation.is_some_and(|generation| generation < retain_from) {
                fs::remove_file(entry.path())?;
            }
        }
        sync_parent_dir(&self.manifest_path)
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
    pub(super) canonical_adjacency_manifest_encoded_len: Option<u64>,
    pub(super) canonical_adjacency_manifest_encoded_checksum: Option<u64>,
    pub(super) canonical_adjacency_manifest_encoded_sha256: Option<Sha256Digest>,
    pub(super) property_spill_manifest_encoded_len: Option<u64>,
    pub(super) property_spill_manifest_encoded_checksum: Option<u64>,
    pub(super) property_spill_manifest_encoded_sha256: Option<Sha256Digest>,
    pub(super) property_projection_manifest_encoded_len: Option<u64>,
    pub(super) property_projection_manifest_encoded_checksum: Option<u64>,
    pub(super) property_projection_manifest_encoded_sha256: Option<Sha256Digest>,
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
            canonical_adjacency_manifest_encoded_len: None,
            canonical_adjacency_manifest_encoded_checksum: None,
            canonical_adjacency_manifest_encoded_sha256: None,
            property_spill_manifest_encoded_len: None,
            property_spill_manifest_encoded_checksum: None,
            property_spill_manifest_encoded_sha256: None,
            property_projection_manifest_encoded_len: None,
            property_projection_manifest_encoded_checksum: None,
            property_projection_manifest_encoded_sha256: None,
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
        if !artifact_metadata_presence_consistent(
            self.canonical_adjacency_manifest_encoded_len,
            self.canonical_adjacency_manifest_encoded_checksum,
            self.canonical_adjacency_manifest_encoded_sha256,
        ) {
            return Err(SkeinError::Storage(
                "manifest canonical adjacency metadata is incomplete".to_string(),
            ));
        }
        if self.canonical_adjacency_manifest_encoded_len.is_some()
            && self.canonical_manifest_encoded_len.is_none()
        {
            return Err(SkeinError::Storage(
                "manifest canonical adjacency requires canonical segments".to_string(),
            ));
        }
        if self.canonical_manifest_encoded_len.is_some()
            && self.canonical_adjacency_manifest_encoded_len.is_none()
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
                    || self.canonical_adjacency_manifest_encoded_len.is_some()
                    || self.canonical_adjacency_manifest_encoded_checksum.is_some()
                    || self.canonical_adjacency_manifest_encoded_sha256.is_some()
                    || self.property_spill_manifest_encoded_len.is_some()
                    || self.property_spill_manifest_encoded_checksum.is_some()
                    || self.property_spill_manifest_encoded_sha256.is_some()
                    || self.property_projection_manifest_encoded_len.is_some()
                    || self.property_projection_manifest_encoded_checksum.is_some()
                    || self.property_projection_manifest_encoded_sha256.is_some()
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
                ["canonical_adjacency_manifest_encoded_len", raw] => {
                    manifest.canonical_adjacency_manifest_encoded_len =
                        parse_optional_u64(raw, "canonical adjacency manifest encoded length")?;
                }
                ["canonical_adjacency_manifest_encoded_checksum", raw] => {
                    manifest.canonical_adjacency_manifest_encoded_checksum =
                        parse_optional_u64(raw, "canonical adjacency manifest encoded checksum")?;
                }
                ["canonical_adjacency_manifest_encoded_sha256", raw] => {
                    manifest.canonical_adjacency_manifest_encoded_sha256 =
                        parse_optional_sha256(raw, "canonical adjacency manifest encoded SHA-256")?;
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
            "canonical_adjacency_manifest_encoded_len",
            "canonical_adjacency_manifest_encoded_checksum",
            "canonical_adjacency_manifest_encoded_sha256",
            "property_spill_manifest_encoded_len",
            "property_spill_manifest_encoded_checksum",
            "property_spill_manifest_encoded_sha256",
            "property_projection_manifest_encoded_len",
            "property_projection_manifest_encoded_checksum",
            "property_projection_manifest_encoded_sha256",
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
        body.push_str(&format!(
            "canonical_adjacency_manifest_encoded_len\t{}\n",
            encode_optional_u64(self.canonical_adjacency_manifest_encoded_len)
        ));
        body.push_str(&format!(
            "canonical_adjacency_manifest_encoded_checksum\t{}\n",
            encode_optional_u64(self.canonical_adjacency_manifest_encoded_checksum)
        ));
        body.push_str(&format!(
            "canonical_adjacency_manifest_encoded_sha256\t{}\n",
            encode_optional_sha256(self.canonical_adjacency_manifest_encoded_sha256)
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

fn load_published_canonical_segments(
    root: &Path,
    durable_manifest: DurableManifest,
    cache: Arc<SegmentCache>,
    store_id: StoreId,
) -> Result<Option<CanonicalSegmentReader>> {
    let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
        durable_manifest.canonical_manifest_encoded_len,
        durable_manifest.canonical_manifest_encoded_checksum,
        durable_manifest.canonical_manifest_encoded_sha256,
    ) else {
        return Ok(None);
    };
    if expected_len > CANONICAL_MANIFEST_MAX_BYTES {
        return Err(SkeinError::Storage(format!(
            "canonical manifest exceeds {CANONICAL_MANIFEST_MAX_BYTES} bytes"
        )));
    }
    let generation = durable_manifest.checkpoint_generation.ok_or_else(|| {
        SkeinError::Storage(
            "canonical manifest metadata requires a checkpoint generation".to_string(),
        )
    })?;
    let manifest_path = root.join(canonical_manifest_generation_file(generation));
    let encoded = fs::read(&manifest_path)?;
    verify_integrity(
        &encoded,
        expected_len,
        expected_checksum,
        expected_sha256,
        "canonical manifest",
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
    let config = CanonicalSegmentConfig::default();
    let max_segment_bytes = NonZeroU64::new(
        config
            .target_segment_bytes
            .get()
            .max(config.max_record_bytes.get().saturating_add(64)),
    )
    .expect("canonical segment maximum is non-zero");
    let property_spills =
        load_published_property_spills(root, durable_manifest, Arc::clone(&cache), store_id)?;
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
) -> Result<Option<PropertySpillReader>> {
    let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
        durable_manifest.property_spill_manifest_encoded_len,
        durable_manifest.property_spill_manifest_encoded_checksum,
        durable_manifest.property_spill_manifest_encoded_sha256,
    ) else {
        return Ok(None);
    };
    if expected_len > PROPERTY_SPILL_MANIFEST_MAX_BYTES {
        return Err(SkeinError::Storage(format!(
            "property spill manifest exceeds {PROPERTY_SPILL_MANIFEST_MAX_BYTES} bytes"
        )));
    }
    let generation = durable_manifest.checkpoint_generation.ok_or_else(|| {
        SkeinError::Storage("property spill metadata requires a checkpoint generation".to_string())
    })?;
    let manifest_path = root.join(property_spill_manifest_generation_file(generation));
    let encoded = fs::read(&manifest_path)?;
    verify_integrity(
        &encoded,
        expected_len,
        expected_checksum,
        expected_sha256,
        "property spill manifest",
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
) -> Result<Option<PersistentPropertyProjectionReader>> {
    let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
        durable_manifest.property_projection_manifest_encoded_len,
        durable_manifest.property_projection_manifest_encoded_checksum,
        durable_manifest.property_projection_manifest_encoded_sha256,
    ) else {
        return Ok(None);
    };
    if expected_len > PROPERTY_PROJECTION_MANIFEST_MAX_BYTES {
        return Err(SkeinError::Storage(format!(
            "property projection manifest exceeds {PROPERTY_PROJECTION_MANIFEST_MAX_BYTES} bytes"
        )));
    }
    let generation = durable_manifest.checkpoint_generation.ok_or_else(|| {
        SkeinError::Storage(
            "property projection metadata requires a checkpoint generation".to_string(),
        )
    })?;
    let manifest_path = root.join(property_projection_manifest_generation_file(generation));
    let encoded = fs::read(&manifest_path)?;
    verify_integrity(
        &encoded,
        expected_len,
        expected_checksum,
        expected_sha256,
        "property projection manifest",
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
) -> Result<Option<CanonicalAdjacencyReader>> {
    let (Some(expected_len), Some(expected_checksum), Some(expected_sha256)) = (
        durable_manifest.canonical_adjacency_manifest_encoded_len,
        durable_manifest.canonical_adjacency_manifest_encoded_checksum,
        durable_manifest.canonical_adjacency_manifest_encoded_sha256,
    ) else {
        return Ok(None);
    };
    if expected_len > CANONICAL_ADJACENCY_MANIFEST_MAX_BYTES {
        return Err(SkeinError::Storage(format!(
            "canonical adjacency manifest exceeds {CANONICAL_ADJACENCY_MANIFEST_MAX_BYTES} bytes"
        )));
    }
    let generation = durable_manifest.checkpoint_generation.ok_or_else(|| {
        SkeinError::Storage(
            "canonical adjacency metadata requires a checkpoint generation".to_string(),
        )
    })?;
    let manifest_path = root.join(canonical_adjacency_manifest_generation_file(generation));
    let encoded = fs::read(&manifest_path)?;
    verify_integrity(
        &encoded,
        expected_len,
        expected_checksum,
        expected_sha256,
        "canonical adjacency manifest",
    )?;
    let text = std::str::from_utf8(&encoded).map_err(|error| {
        SkeinError::Storage(format!(
            "canonical adjacency manifest is not UTF-8: {error}"
        ))
    })?;
    let manifest = CanonicalAdjacencyManifest::decode(text)
        .map_err(|error| SkeinError::Storage(error.to_string()))?;
    if manifest.generation != ManifestGeneration(generation) {
        return Err(SkeinError::Storage(format!(
            "canonical adjacency generation {} does not match durable generation {generation}",
            manifest.generation.0
        )));
    }
    let config = CanonicalAdjacencyConfig::default();
    let max_block_bytes = NonZeroU64::new(
        config
            .target_block_bytes
            .get()
            .max(config.max_record_bytes.get().saturating_add(1024)),
    )
    .expect("canonical adjacency maximum block size is non-zero");
    CanonicalAdjacencyReader::open(
        root.join(canonical_adjacency_artifact_generation_file(generation)),
        manifest,
        cache,
        store_id,
        max_block_bytes,
    )
    .map(Some)
    .map_err(|error| SkeinError::Storage(error.to_string()))
}
