// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::{
    CanonicalAdjacencyCheckpointArtifacts, DerivedArtifactBuildConfig, DurableArtifactMetadata,
    DurableStore,
};
use crate::store::Catalog;

#[derive(Clone, Copy)]
pub(in crate::store) struct GraphBundleCheckpointArtifacts {
    pub(in crate::store) canonical_manifest: DurableArtifactMetadata,
    pub(in crate::store) property_spill_manifest: DurableArtifactMetadata,
    pub(in crate::store) adjacency: CanonicalAdjacencyCheckpointArtifacts,
    pub(in crate::store) property_projection_manifest: DurableArtifactMetadata,
}

impl DurableStore {
    pub(in crate::store) fn graph_generation(&self) -> u64 {
        self.canonical_adjacency_generation_artifacts
            .map_or(self.checkpoint_epoch, |binding| binding.generation)
    }

    pub(in crate::store) fn reusable_graph_bundle(
        &self,
        catalog: &Catalog,
        graph_change_commit_epoch: u64,
        config: DerivedArtifactBuildConfig,
    ) -> Option<GraphBundleCheckpointArtifacts> {
        // Compare against the last checkpoint, not the bundle's older source
        // epoch: reopening conservatively advances the graph-change fence.
        // Full graph Catalog equality also catches direct non-WAL schema edits.
        if self.checkpoint_epoch == 0
            || graph_change_commit_epoch > self.checkpoint_commit_epoch
            || self.checkpoint_graph_catalog.as_ref()? != catalog
            || config.adjacency != Default::default()
            || config.property_projection != Default::default()
            || self.canonical_segments.is_none()
            || self.canonical_adjacency.is_none()
            || self.persistent_property_projection.is_none()
        {
            return None;
        }
        Some(GraphBundleCheckpointArtifacts {
            canonical_manifest: DurableArtifactMetadata {
                encoded_len: self.canonical_manifest_encoded_len?,
                encoded_checksum: self.canonical_manifest_encoded_checksum?,
                encoded_sha256: self.canonical_manifest_encoded_sha256?,
            },
            property_spill_manifest: DurableArtifactMetadata {
                encoded_len: self.property_spill_manifest_encoded_len?,
                encoded_checksum: self.property_spill_manifest_encoded_checksum?,
                encoded_sha256: self.property_spill_manifest_encoded_sha256?,
            },
            adjacency: CanonicalAdjacencyCheckpointArtifacts {
                generation: self.canonical_adjacency_generation_artifacts?,
            },
            property_projection_manifest: DurableArtifactMetadata {
                encoded_len: self.property_projection_manifest_encoded_len?,
                encoded_checksum: self.property_projection_manifest_encoded_checksum?,
                encoded_sha256: self.property_projection_manifest_encoded_sha256?,
            },
        })
    }
}
