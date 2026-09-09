//! Durable store ownership, open admission and shared checkpoint state.
//!
//! Private child modules own artifact I/O, backup, scrub, WAL, checkpoint
//! publication, manifest validation and physical-generation reclamation.

#[path = "durable/artifacts.rs"]
mod artifacts;
#[path = "durable/backup.rs"]
mod backup;
#[path = "durable/checkpoint.rs"]
mod checkpoint;
#[path = "durable/manifest.rs"]
mod manifest;
#[path = "durable/reclamation.rs"]
mod reclamation;
#[path = "durable/scrub.rs"]
mod scrub;
#[path = "durable/wal.rs"]
mod wal;

use artifacts::{admit_graph_manifest_binding, load_published_canonical_segments};
pub(super) use artifacts::{
    load_published_canonical_adjacency, load_published_property_projection,
};
pub(super) use manifest::{artifact_metadata_presence_consistent, DurableManifest};

use super::{
    cleanup_abandoned_checkpoint_preparations, derived_repair, doctor, has_storage_artifacts,
    source_scan, store_id_for_path, ProjectedGraphArtifact, CANONICAL_MANIFEST_MAX_BYTES,
    MANIFEST_FILE, PROJECTED_GRAPHS_FILE, PROPERTY_PROJECTION_MANIFEST_MAX_BYTES,
    PROPERTY_SPILL_MANIFEST_MAX_BYTES, STABLE_ID_MAPPING_FILE,
};
use crate::error::{Result, SkeinError};
use crate::schema::{Catalog, GraphStatistics};
use skein_integrity::{integrity_digest, Sha256Digest};
use skein_storage::{
    AppendGenerationArtifacts, AppendGenerationReader, CanonicalAdjacencyConfig,
    CanonicalAdjacencyGenerationArtifacts, CanonicalAdjacencyReader, CanonicalSegmentReader,
    DatabaseDirectoryLease, DurabilityPolicy, FileSegmentRangeReader,
    GraphDescriptorTreeBuildConfig, ManifestGeneration, PersistentPropertyProjectionConfig,
    PersistentPropertyProjectionReader, ProjectedGraphDefinition,
    RelationalIndexGenerationArtifacts, RelationalOverflowGenerationArtifacts,
    RelationalRowPageGenerationArtifacts, RelationalState, SearchProjectionGraphChange,
    SegmentCache, StableIdentityMappingError, StableIdentityMappingReader, StorageTelemetrySink,
    StoreId, WalReplayConfig, WalSyncGroupState,
};
use std::collections::BTreeMap;
use std::fs::{self, File};
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
    pub(super) relational_row_compaction_report: Option<super::RelationalRowPageCompactionReport>,
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
}
