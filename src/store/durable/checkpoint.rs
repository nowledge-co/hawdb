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

//! Checkpoint encoding, staged sidecars and manifest selection.

use super::{
    load_published_canonical_adjacency, load_published_canonical_segments,
    load_published_property_projection, CheckpointImage, CheckpointManifestArtifacts,
    DurableArtifactMetadata, DurableManifest, DurableStore, GraphManifestOpenBudget,
};
use crate::error::{HawDBError, Result};
use crate::store::{
    canonical_adjacency_artifact_generation_file, canonical_artifact_generation_file,
    canonical_manifest_generation_file, checkpoint_generation_file, checkpoint_publish_failpoint,
    checksum_bytes, encode_durable_text, file_checksum,
    property_projection_artifact_generation_file, property_projection_manifest_generation_file,
    property_spill_artifact_generation_file, property_spill_manifest_generation_file,
    read_durable_text_bytes_with_limit, relational_checkpoint_generation_file,
    remove_source_scan_artifacts, safe_reclaim_commit_epoch, source_scan, sync_parent_dir,
    verify_integrity, wal_generation_file, CheckpointPublishStage, PROJECTED_GRAPHS_FILE,
};
use hawdb_storage::{
    durable_replace_file, encode_relational_checkpoint_to_writer, DurableCompression,
    FileSegmentRangeReader, ManifestGeneration, RelationalDecodeLimits, RelationalState,
    WalReplayConfig,
};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

impl DurableStore {
    pub(in crate::store) fn read_checkpoint_text(&self, config: WalReplayConfig) -> Result<String> {
        let metadata = fs::metadata(&self.checkpoint_path)?;
        if config
            .max_checkpoint_encoded_bytes
            .is_some_and(|limit| metadata.len() > limit)
        {
            return Err(HawDBError::Storage(format!(
                "checkpoint encoded byte limit exceeded: max_checkpoint_encoded_bytes={}",
                config.max_checkpoint_encoded_bytes.unwrap_or_default()
            )));
        }
        let bytes = fs::read(&self.checkpoint_path)?;
        let expected_len = self.checkpoint_encoded_len.ok_or_else(|| {
            HawDBError::Storage("checkpoint is missing its encoded length".to_string())
        })?;
        let expected_checksum = self.checkpoint_encoded_checksum.ok_or_else(|| {
            HawDBError::Storage("checkpoint is missing its encoded checksum".to_string())
        })?;
        let expected_sha256 = self.checkpoint_encoded_sha256.ok_or_else(|| {
            HawDBError::Storage("checkpoint is missing its encoded SHA-256".to_string())
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

    pub(in crate::store) fn write_relational_checkpoint(
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
        let tmp_path = path.with_extension("hawdb.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            encode_relational_checkpoint_to_writer(&mut file, commit_epoch, state, max_bytes)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
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

    pub(in crate::store) fn write_checkpoint(
        &self,
        image: CheckpointImage<'_>,
        generation: u64,
    ) -> Result<DurableArtifactMetadata> {
        let data = hawdb_storage::checkpoint::encode_checkpoint_body(&image, generation)?;
        let checksum = checksum_bytes(data.as_bytes());
        let data = format!("{data}checksum\t{checksum}\n");
        let checkpoint_path = self.root_path.join(checkpoint_generation_file(generation));
        let tmp_path = checkpoint_path.with_extension("hawdb.tmp");
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

    pub(in crate::store) fn prepare_checkpoint_staging(&self, generation: u64) -> Result<PathBuf> {
        let staging_path = self
            .root_path
            .join(format!(".checkpoint.{generation}.prepare"));
        if staging_path.exists() {
            fs::remove_dir_all(&staging_path)?;
        }
        fs::create_dir(&staging_path)?;
        Ok(staging_path)
    }

    pub(in crate::store) fn publish_checkpoint_sidecars(
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

    pub(in crate::store) fn discard_prepared_checkpoint(
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
            hawdb_storage::canonical_segment_descriptor_page_file(generation),
            hawdb_storage::canonical_segment_descriptor_root_file(generation),
            canonical_adjacency_artifact_generation_file(generation),
            hawdb_storage::canonical_adjacency_descriptor_page_file(generation),
            hawdb_storage::canonical_adjacency_descriptor_root_file(generation),
            property_spill_artifact_generation_file(generation),
            property_spill_manifest_generation_file(generation),
            hawdb_storage::property_spill_descriptor_page_file(generation),
            hawdb_storage::property_spill_descriptor_root_file(generation),
            property_projection_artifact_generation_file(generation),
            property_projection_manifest_generation_file(generation),
            hawdb_storage::property_projection_descriptor_page_file(generation),
            hawdb_storage::property_projection_descriptor_root_file(generation),
            hawdb_storage::relational_index_shadow_artifact_file(generation),
            hawdb_storage::relational_index_shadow_manifest_generation_file(generation),
            hawdb_storage::relational_row_page_artifact_file(generation),
            hawdb_storage::relational_row_page_root_descriptor_file(generation),
            hawdb_storage::relational_row_page_root_key_file(generation),
            hawdb_storage::relational_row_page_manifest_generation_file(generation),
            hawdb_storage::relational_overflow_extent_file(generation),
            hawdb_storage::relational_overflow_descriptor_file(generation),
            hawdb_storage::relational_overflow_manifest_generation_file(generation),
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

    pub(in crate::store) fn publish_checkpoint_manifest(
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
            return Err(HawDBError::Storage(
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
}
