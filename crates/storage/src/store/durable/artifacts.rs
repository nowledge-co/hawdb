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

//! Generation-bound artifact writers and bounded published readers.

use super::{
    stable_identity_error, CanonicalAdjacencyCheckpointArtifacts, DurableArtifactMetadata,
    DurableManifest, DurableStore, GraphManifestOpenBudget,
};
use crate::error::{HawDBError, Result};
use crate::file_io::{self as fs, File};
use crate::store::{
    canonical_adjacency_artifact_generation_file, canonical_artifact_generation_file,
    canonical_manifest_generation_file, checksum_bytes, decode_projected_graph_artifacts,
    property_projection_artifact_generation_file, property_projection_manifest_generation_file,
    property_spill_artifact_generation_file, property_spill_manifest_generation_file,
    read_durable_text, remove_source_scan_artifacts, source_scan,
    split_projected_graph_artifact_checksum, sync_parent_dir, ProjectedGraphArtifact,
    CANONICAL_MANIFEST_MAX_BYTES, PROPERTY_PROJECTION_MANIFEST_MAX_BYTES,
    PROPERTY_SPILL_MANIFEST_MAX_BYTES,
};
use hawdb_storage::{
    cache::{ManifestGeneration, SegmentCache, StoreId},
    canonical::{
        CanonicalSegmentConfig, CanonicalSegmentError, CanonicalSegmentManifest,
        CanonicalSegmentReader, CanonicalSegmentWriter,
    },
    canonical_adjacency::{
        CanonicalAdjacencyConfig, CanonicalAdjacencyReader, CanonicalAdjacencyWriter,
    },
    config::DurableCompression,
    durability::durable_replace_file,
    graph_descriptor_page::GraphDescriptorKind,
    graph_descriptor_tree::{
        GraphDescriptorTreeBuildConfig, GraphDescriptorTreeGenerationArtifacts,
        GraphDescriptorTreePaths, GraphDescriptorTreeRootReader,
    },
    projection::StoreStableIdMapping,
    property_projection::{
        PersistentPropertyProjectionConfig, PersistentPropertyProjectionDefinition,
        PersistentPropertyProjectionDescriptorTree, PersistentPropertyProjectionManifest,
        PersistentPropertyProjectionReader, PersistentPropertyProjectionRecord,
        PersistentPropertyProjectionWriter,
    },
    property_spill::{
        PersistentPropertySpillDescriptorTree, PropertySpillConfig, PropertySpillManifest,
        PropertySpillReader, PropertySpillWriteOptions,
    },
    scan::ScanSegmentManifest,
    stable_identity::{
        StableIdentityKey, StableIdentityMappingConfig, StableIdentityMappingReader,
        StableIdentityMappingWriter, StableIdentityMaterializeLimits,
    },
    NodeRecord, RelRecord,
};
use std::collections::BTreeMap;
use std::io::Write;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(super) struct CheckpointMetadataTemporaryPath(pub(super) Option<PathBuf>);

impl Drop for CheckpointMetadataTemporaryPath {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = fs::remove_file(path);
        }
    }
}

pub(super) fn publish_checkpoint_metadata(
    path: &Path,
    encoded: &[u8],
    work: &crate::background::CheckpointWorkContext,
) -> Result<()> {
    let tmp_path = path.with_extension("hawdb.tmp");
    let mut temporary_path = CheckpointMetadataTemporaryPath(None);
    let mut file = {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(&tmp_path)?;
        temporary_path.0 = Some(tmp_path.clone());
        unit.finish();
        file
    };
    for block in encoded.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
        file.write_all(block)?;
        unit.finish();
    }
    {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
        file.sync_all()?;
        unit.finish();
    }
    drop(file);
    {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
        durable_replace_file(&tmp_path, path)?;
        temporary_path.0 = None;
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(())
}

impl DurableStore {
    pub(in crate::store) fn load_source_scan_manifest(
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

    pub(in crate::store) fn write_canonical_segments<N, R>(
        &self,
        nodes: N,
        relationships: R,
        generation: u64,
        source_commit_epoch: u64,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<(DurableArtifactMetadata, DurableArtifactMetadata)>
    where
        N: IntoIterator<Item = std::result::Result<Option<NodeRecord>, CanonicalSegmentError>>,
        R: IntoIterator<Item = std::result::Result<Option<RelRecord>, CanonicalSegmentError>>,
    {
        let artifact_path = self
            .root_path
            .join(canonical_artifact_generation_file(generation));
        let property_artifact_path = self
            .root_path
            .join(property_spill_artifact_generation_file(generation));
        let property_descriptor_tree = PersistentPropertySpillDescriptorTree::new(
            GraphDescriptorTreePaths::new(
                self.root_path.join(
                    hawdb_storage::property_spill::property_spill_descriptor_page_file(generation),
                ),
                self.root_path.join(
                    hawdb_storage::property_spill::property_spill_descriptor_root_file(generation),
                ),
            ),
            GraphDescriptorTreeBuildConfig::default(),
        );
        let (canonical_manifest, property_spill_output) =
            CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
                .with_work_context(work.clone())
                .write_fallible_with_property_spills_steps(
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
                .map_err(HawDBError::from_storage_error)?;
        let property_spill_manifest = property_spill_output.manifest;
        let encoded = canonical_manifest
            .encode_with_work_context(work)
            .map_err(HawDBError::from_storage_error)?;
        let integrity = work
            .integrity(encoded.as_bytes())
            .map_err(HawDBError::from_storage_error)?;
        let metadata = DurableArtifactMetadata {
            encoded_len: encoded.len() as u64,
            encoded_checksum: integrity.crc32c.as_u64(),
            encoded_sha256: integrity.sha256,
        };
        let manifest_path = self
            .root_path
            .join(canonical_manifest_generation_file(generation));
        publish_checkpoint_metadata(&manifest_path, encoded.as_bytes(), work)?;
        let property_encoded = property_spill_manifest
            .encode()
            .map_err(HawDBError::from_storage_error)?;
        let property_manifest_path = self
            .root_path
            .join(property_spill_manifest_generation_file(generation));
        publish_checkpoint_metadata(&property_manifest_path, property_encoded.as_bytes(), work)?;
        Ok((
            metadata,
            DurableArtifactMetadata::for_bytes(property_encoded.as_bytes()),
        ))
    }

    pub(in crate::store) fn write_canonical_adjacency<R>(
        &self,
        relationships: R,
        generation: u64,
        source_commit_epoch: u64,
        config: CanonicalAdjacencyConfig,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<CanonicalAdjacencyCheckpointArtifacts>
    where
        R: IntoIterator<
            Item = std::result::Result<
                Option<RelRecord>,
                hawdb_storage::canonical_adjacency::CanonicalAdjacencyError,
            >,
        >,
    {
        let artifact_path = self
            .root_path
            .join(canonical_adjacency_artifact_generation_file(generation));
        let descriptor_paths = GraphDescriptorTreePaths::new(
            self.root_path.join(
                hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(
                    generation,
                ),
            ),
            self.root_path.join(
                hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(
                    generation,
                ),
            ),
        );
        let descriptor_config = GraphDescriptorTreeBuildConfig {
            max_page_artifact_bytes: config.max_spill_bytes,
            max_intermediate_bytes: config.max_spill_bytes,
            ..GraphDescriptorTreeBuildConfig::default()
        };
        let output = CanonicalAdjacencyWriter::new(config)
            .with_work_context(work.clone())
            .write_fallible_with_descriptor_tree_steps(
                &artifact_path,
                descriptor_paths,
                ManifestGeneration(generation),
                source_commit_epoch,
                descriptor_config,
                relationships,
            )
            .map_err(HawDBError::from_storage_error)?;
        let descriptor_tree = output.descriptor_tree.as_ref().ok_or_else(|| {
            HawDBError::Storage(
                "canonical adjacency checkpoint omitted its descriptor root".to_string(),
            )
        })?;
        let expected_descriptor_count = output
            .report
            .sparse_block_count
            .checked_add(output.report.dense_block_count)
            .ok_or_else(|| {
                HawDBError::Storage("canonical adjacency descriptor count overflow".to_string())
            })?;
        if descriptor_tree.root.generation != generation
            || descriptor_tree.root.source_commit_epoch != source_commit_epoch
            || descriptor_tree.root.descriptor_count != expected_descriptor_count
        {
            return Err(HawDBError::Storage(
                "canonical adjacency descriptor root identity is inconsistent".to_string(),
            ));
        }
        let generation_artifacts = output.generation_artifacts().ok_or_else(|| {
            HawDBError::Storage(
                "canonical adjacency checkpoint omitted its generation binding".to_string(),
            )
        })?;
        Ok(CanonicalAdjacencyCheckpointArtifacts {
            generation: generation_artifacts,
        })
    }

    pub(in crate::store) fn write_persistent_property_projection<N>(
        &self,
        definitions: Vec<PersistentPropertyProjectionDefinition>,
        nodes: N,
        generation: u64,
        source_commit_epoch: u64,
        config: PersistentPropertyProjectionConfig,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<DurableArtifactMetadata>
    where
        N: IntoIterator<
            Item = std::result::Result<
                Option<PersistentPropertyProjectionRecord>,
                hawdb_storage::property_projection::PersistentPropertyProjectionError,
            >,
        >,
    {
        let artifact_path = self
            .root_path
            .join(property_projection_artifact_generation_file(generation));
        let descriptor_paths = GraphDescriptorTreePaths::new(
            self.root_path.join(
                hawdb_storage::property_projection::property_projection_descriptor_page_file(
                    generation,
                ),
            ),
            self.root_path.join(
                hawdb_storage::property_projection::property_projection_descriptor_root_file(
                    generation,
                ),
            ),
        );
        let output = PersistentPropertyProjectionWriter::new(config)
            .with_work_context(work.clone())
            .write_fallible_steps(
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
            .map_err(HawDBError::from_storage_error)?;
        let descriptor_tree = &output.descriptor_tree;
        if descriptor_tree.root.kind != GraphDescriptorKind::PropertyProjection
            || descriptor_tree.root.generation != generation
            || descriptor_tree.root.source_commit_epoch != source_commit_epoch
            || descriptor_tree.root.descriptor_count != output.report.block_count
        {
            return Err(HawDBError::Storage(
                "property projection descriptor root identity is inconsistent".to_string(),
            ));
        }
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let encoded = output
            .manifest
            .encode()
            .map_err(HawDBError::from_storage_error)?;
        unit.finish();
        let integrity = work
            .integrity(encoded.as_bytes())
            .map_err(HawDBError::from_storage_error)?;
        let metadata = DurableArtifactMetadata {
            encoded_len: encoded.len() as u64,
            encoded_checksum: integrity.crc32c.as_u64(),
            encoded_sha256: integrity.sha256,
        };
        let manifest_path = self
            .root_path
            .join(property_projection_manifest_generation_file(generation));
        publish_checkpoint_metadata(&manifest_path, encoded.as_bytes(), work)?;
        Ok(metadata)
    }

    pub(in crate::store) fn write_projected_graph_artifacts(&self, body: &str) -> Result<()> {
        self.write_projected_graph_artifacts_to(&self.projected_graphs_path, body)
    }

    pub(in crate::store) fn write_projected_graph_artifacts_to(
        &self,
        path: &Path,
        body: &str,
    ) -> Result<()> {
        self.write_projected_graph_artifacts_to_with_work_context(
            path,
            body,
            &crate::background::CheckpointWorkContext::default(),
        )
    }

    pub(in crate::store) fn write_projected_graph_artifacts_to_with_work_context(
        &self,
        path: &Path,
        body: &str,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<()> {
        let checksum = work
            .checksum(body.as_bytes())
            .map_err(HawDBError::from_storage_error)?;
        let data = format!("{body}checksum\t{checksum}\n");
        let encoded = crate::text::envelope::encode_durable_text_with_work_context(
            &data,
            DurableCompression::default(),
            work,
        )?;
        publish_checkpoint_metadata(path, &encoded, work)
    }

    pub(super) fn remove_projected_graph_artifacts(&self) -> Result<()> {
        match fs::remove_file(&self.projected_graphs_path) {
            Ok(()) => sync_parent_dir(&self.projected_graphs_path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(in crate::store) fn load_projected_graph_artifacts(
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
                    return Err(HawDBError::Storage(format!(
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

    pub(in crate::store) fn write_stable_id_mapping(
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

    pub(in crate::store) fn load_stable_id_mapping(&mut self) -> Result<()> {
        if !self.stable_id_mapping_path.exists() {
            self.stable_id_mapping_reader = None;
            return Ok(());
        }
        self.open_stable_id_mapping_reader()
    }

    pub(in crate::store) fn materialize_stable_id_mapping(&self) -> Result<StoreStableIdMapping> {
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

    pub(in crate::store) fn open_bound_relational_overflow(
        &self,
    ) -> Result<hawdb_storage::relational::RelationalOverflowRootReader> {
        let binding = self
            .relational_overflow_generation_artifacts
            .ok_or_else(|| {
                HawDBError::Storage(
                    "published checkpoint has no relational overflow generation binding"
                        .to_string(),
                )
            })?;
        let reader =
            hawdb_storage::relational::RelationalOverflowRootReader::open_bound_generation(
                &self.root_path,
                binding,
                hawdb_storage::relational::RelationalOverflowPublicationConfig::default(),
            )
            .map_err(HawDBError::from_storage_error)?;
        Ok(reader)
    }

    pub(in crate::store) fn open_bound_relational_row_pages(
        &self,
        overflow_root: &hawdb_storage::relational::RelationalOverflowRootReader,
    ) -> Result<hawdb_storage::relational::RelationalRowPageRootReader> {
        let binding = self.relational_row_generation_artifacts.ok_or_else(|| {
            HawDBError::Storage(
                "published checkpoint has no relational row-page generation binding".to_string(),
            )
        })?;
        let reader = hawdb_storage::relational::RelationalRowPageRootReader::open_bound_generation(
            &self.root_path,
            binding,
            hawdb_storage::relational::RelationalRowPagePublicationConfig::default(),
        )
        .map_err(HawDBError::from_storage_error)?;
        let manifest = reader.manifest();
        if manifest.source_commit_epoch != binding.source_commit_epoch
            || manifest.root_set_digest != binding.root_set_digest
        {
            return Err(HawDBError::Storage(
                "relational row-page generation identity differs from canonical binding"
                    .to_string(),
            ));
        }
        if manifest.overflow_root.is_none() {
            return Err(HawDBError::Storage(
                "canonical relational row-page generation has no overflow binding".to_string(),
            ));
        }
        reader
            .validate_overflow_root(overflow_root)
            .map_err(HawDBError::from_storage_error)?;
        Ok(reader)
    }
}

pub(super) use hawdb_storage::artifact_binding::admit_graph_manifest_binding;
use hawdb_storage::artifact_binding::read_bound_graph_manifest;

pub(super) fn load_published_canonical_segments(
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
        HawDBError::Storage(
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
        HawDBError::Storage(format!("canonical manifest is not UTF-8: {error}"))
    })?;
    let canonical_manifest =
        CanonicalSegmentManifest::decode(text).map_err(HawDBError::from_storage_error)?;
    if canonical_manifest.generation != ManifestGeneration(generation) {
        return Err(HawDBError::Storage(format!(
            "canonical manifest generation {} does not match durable generation {generation}",
            canonical_manifest.generation.0
        )));
    }
    if canonical_manifest.source_commit_epoch != durable_manifest.checkpoint_commit_epoch {
        return Err(HawDBError::Storage(format!(
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
    .map_err(HawDBError::from_storage_error)
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
        HawDBError::Storage("property spill metadata requires a checkpoint generation".to_string())
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
        HawDBError::Storage(format!("property spill manifest is not UTF-8: {error}"))
    })?;
    let manifest = PropertySpillManifest::decode(text).map_err(HawDBError::from_storage_error)?;
    if manifest.generation != ManifestGeneration(generation) {
        return Err(HawDBError::Storage(format!(
            "property spill generation {} does not match durable generation {generation}",
            manifest.generation.0
        )));
    }
    if manifest.source_commit_epoch != durable_manifest.checkpoint_commit_epoch {
        return Err(HawDBError::Storage(format!(
            "property spill source epoch {} does not match durable checkpoint epoch {}",
            manifest.source_commit_epoch, durable_manifest.checkpoint_commit_epoch
        )));
    }
    let descriptor_tree = PersistentPropertySpillDescriptorTree::new(
        GraphDescriptorTreePaths::new(
            root.join(
                hawdb_storage::property_spill::property_spill_descriptor_page_file(generation),
            ),
            root.join(
                hawdb_storage::property_spill::property_spill_descriptor_root_file(generation),
            ),
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
    .map_err(HawDBError::from_storage_error)
}

pub(in crate::store) fn load_published_property_projection(
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
        HawDBError::Storage(
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
        HawDBError::Storage(format!(
            "property projection manifest is not UTF-8: {error}"
        ))
    })?;
    let manifest = PersistentPropertyProjectionManifest::decode(text)
        .map_err(HawDBError::from_storage_error)?;
    if manifest.generation != ManifestGeneration(generation)
        || manifest.source_commit_epoch != durable_manifest.checkpoint_commit_epoch
    {
        return Err(HawDBError::Storage(
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
                root.join(
                    hawdb_storage::property_projection::property_projection_descriptor_page_file(
                        generation,
                    ),
                ),
                root.join(
                    hawdb_storage::property_projection::property_projection_descriptor_root_file(
                        generation,
                    ),
                ),
            ),
            GraphDescriptorTreeBuildConfig::default(),
        ),
        cache,
        store_id,
        max_block_bytes,
    )
    .map(Some)
    .map_err(HawDBError::from_storage_error)
}

pub(in crate::store) fn load_published_canonical_adjacency(
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
        HawDBError::Storage(
            "canonical adjacency metadata requires a checkpoint generation".to_string(),
        )
    })?;
    if binding.generation != generation {
        return Err(HawDBError::Storage(format!(
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
        root.join(
            hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(
                generation,
            ),
        ),
        root.join(
            hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(
                generation,
            ),
        ),
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
    .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
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
    .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))
}
