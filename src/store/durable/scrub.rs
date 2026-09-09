//! Strong verification of selected checkpoint artifacts and the WAL.

use super::{stable_identity_error, DurableManifest, DurableStore};
use crate::error::{Result, SkeinError};
use crate::store::{
    canonical_adjacency_artifact_generation_file, canonical_artifact_generation_file,
    canonical_manifest_generation_file, checkpoint_generation_file, file_checksum,
    property_projection_artifact_generation_file, property_projection_manifest_generation_file,
    property_spill_artifact_generation_file, property_spill_manifest_generation_file,
    relational_checkpoint_generation_file, WalCursorEvent, WalOpenOutcome, WalRecordCursor,
};
use skein_integrity::Sha256Digest;
use skein_storage::{
    append_generation_manifest_file, append_segment_file, decode_relational_checkpoint_file,
    AppendGenerationReader, AppendPublicationConfig, CanonicalAdjacencyConfig,
    CanonicalAdjacencyReader, CanonicalSegmentManifest, CanonicalSegmentReader,
    GraphDescriptorKind, GraphDescriptorTreeBuildConfig, GraphDescriptorTreeGenerationArtifacts,
    GraphDescriptorTreePaths, GraphDescriptorTreeRootReader, ManifestGeneration,
    PersistentPropertyProjectionManifest, PropertySpillManifest, RelationalDecodeLimits,
    StorageScrubReport,
};
use std::collections::BTreeSet;
use std::fs::{self};
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

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

impl DurableStore {
    pub(in crate::store) fn scrub_storage(&self) -> Result<StorageScrubReport> {
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

        row_root
            .scrub_physical_pages()
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        let older_generations = row_root
            .manifest()
            .physical_generations
            .iter()
            .filter(|entry| entry.generation != row_binding.generation);
        let older_row_page_files = older_generations.clone().count();
        let older_row_page_bytes = older_generations
            .map(|entry| entry.live_pages * row_root.manifest().page_bytes)
            .sum::<u64>();
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
}
