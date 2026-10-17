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

//! Canonical record and manifest progress for one owned checkpoint generation.
use super::*;
use crate::canonical::{CanonicalPreparation, CheckpointCanonicalManifest};
use crate::graph_descriptor_tree::{GraphDescriptorTreeBuildConfig, GraphDescriptorTreePaths};
use crate::graph_overlay::CheckpointOverlayCursor;
use crate::property_spill::{
    PersistentPropertySpillDescriptorTree, PropertySpillConfig, PropertySpillManifest,
    PropertySpillWriteOptions,
};
use crate::store::durable::DurableArtifactMetadata;

pub(super) struct CanonicalPreparationState {
    writer: CanonicalPreparation,
    nodes: CheckpointOverlayCursor<NodeRecord, crate::cow::CowNodeMap>,
    relationships: CheckpointOverlayCursor<RelRecord>,
    phase: u8,
    manifests: Option<(CheckpointCanonicalManifest, PropertySpillManifest)>,
    encoded: Option<crate::background::CheckpointText>,
    metadata: Option<DurableArtifactMetadata>,
    canonical_published: bool,
    property_encoded: Option<String>,
    property_published: bool,
    generation: u64,
}

impl std::fmt::Debug for CanonicalPreparationState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CanonicalPreparationState")
            .field("generation", &self.generation)
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}

impl CanonicalPreparationState {
    pub(super) fn new(source: &GraphStore, durable: &DurableStore, generation: u64) -> Self {
        let path = durable
            .root_path()
            .join(canonical_artifact_generation_file(generation));
        let spill_path = durable
            .root_path()
            .join(property_spill_artifact_generation_file(generation));
        let writer = CanonicalPreparation::checkpoint(
            &path,
            ManifestGeneration(generation),
            PropertySpillWriteOptions {
                artifact_path: &spill_path,
                source_commit_epoch: source.commit_epoch,
                config: PropertySpillConfig::default(),
                descriptor_tree: PersistentPropertySpillDescriptorTree::new(
                    GraphDescriptorTreePaths::new(
                        durable.root_path().join(
                            crate::property_spill::property_spill_descriptor_page_file(generation),
                        ),
                        durable.root_path().join(
                            crate::property_spill::property_spill_descriptor_root_file(generation),
                        ),
                    ),
                    GraphDescriptorTreeBuildConfig::default(),
                ),
            },
        );
        Self {
            writer,
            nodes: CheckpointOverlayCursor::new(
                source.canonical_base.as_ref(),
                &source.nodes,
                &source.node_tombstones,
            ),
            relationships: CheckpointOverlayCursor::new(
                source.canonical_base.as_ref(),
                &source.relationships,
                &source.relationship_tombstones,
            ),
            phase: 0,
            manifests: None,
            encoded: None,
            metadata: None,
            canonical_published: false,
            property_encoded: None,
            property_published: false,
            generation,
        }
    }

    pub(super) fn advance(
        &mut self,
        durable: &DurableStore,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<(DurableArtifactMetadata, DurableArtifactMetadata)> {
        if self.phase == 0 {
            while let Some(node) = self
                .nodes
                .peek(work)
                .map_err(CanonicalSegmentError::into_checkpoint_read_error)?
            {
                self.writer
                    .append_node(node, work)
                    .map_err(HawDBError::from_storage_error)?;
                self.nodes.accept();
            }
            self.writer
                .finish_nodes(work)
                .map_err(HawDBError::from_storage_error)?;
            self.phase = 1;
        }
        if self.phase == 1 {
            while let Some(relationship) = self
                .relationships
                .peek(work)
                .map_err(CanonicalSegmentError::into_checkpoint_read_error)?
            {
                self.writer
                    .append_relationship(relationship, work)
                    .map_err(HawDBError::from_storage_error)?;
                self.relationships.accept();
            }
            self.phase = 2;
        }
        if self.manifests.is_none() {
            let (canonical, spill) = self
                .writer
                .finish(work)
                .map_err(HawDBError::from_storage_error)?;
            self.manifests = Some((canonical, spill.manifest));
        }
        let (canonical, property) = self
            .manifests
            .as_ref()
            .expect("completed canonical manifests");
        if self.encoded.is_none() {
            self.encoded = Some(
                canonical
                    .encode_with_work_context(work)
                    .map_err(HawDBError::from_storage_error)?,
            );
        }
        let encoded = self.encoded.as_ref().expect("encoded canonical manifest");
        if self.metadata.is_none() {
            let integrity = work
                .integrity(encoded.as_bytes())
                .map_err(HawDBError::from_storage_error)?;
            self.metadata = Some(DurableArtifactMetadata {
                encoded_len: encoded.len() as u64,
                encoded_checksum: integrity.crc32c.as_u64(),
                encoded_sha256: integrity.sha256,
            });
        }
        if !self.canonical_published {
            durable::artifacts::publish_checkpoint_metadata(
                &durable
                    .root_path()
                    .join(canonical_manifest_generation_file(self.generation)),
                encoded.as_bytes(),
                work,
            )?;
            self.canonical_published = true;
        }
        if self.property_encoded.is_none() {
            self.property_encoded =
                Some(property.encode().map_err(HawDBError::from_storage_error)?);
        }
        let property_encoded = self
            .property_encoded
            .as_ref()
            .expect("encoded property manifest");
        if !self.property_published {
            durable::artifacts::publish_checkpoint_metadata(
                &durable
                    .root_path()
                    .join(property_spill_manifest_generation_file(self.generation)),
                property_encoded.as_bytes(),
                work,
            )?;
            self.property_published = true;
        }
        Ok((
            self.metadata.expect("canonical metadata"),
            DurableArtifactMetadata::for_bytes(property_encoded.as_bytes()),
        ))
    }
}
