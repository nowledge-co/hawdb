//! Checkpoint encoding, staged sidecars and manifest selection.

use super::{
    load_published_canonical_adjacency, load_published_canonical_segments,
    load_published_property_projection, CheckpointImage, CheckpointManifestArtifacts,
    DurableArtifactMetadata, DurableManifest, DurableStore, GraphManifestOpenBudget,
};
use crate::error::{Result, SkeinError};
use crate::store::{
    canonical_adjacency_artifact_generation_file, canonical_artifact_generation_file,
    canonical_manifest_generation_file, checkpoint_generation_file, checkpoint_publish_failpoint,
    checksum_bytes, encode_bool, encode_durable_text, encode_index_kind, encode_nullable,
    encode_property_type, encode_schema_object_state,
    encode_search_projection_relational_primary_key_changes, encode_string, encode_string_vec,
    encode_table_kind, encode_u64_vec, encode_value_vec, file_checksum,
    property_projection_artifact_generation_file, property_projection_manifest_generation_file,
    property_spill_artifact_generation_file, property_spill_manifest_generation_file,
    read_durable_text_bytes_with_limit, relational_checkpoint_generation_file,
    remove_source_scan_artifacts, safe_reclaim_commit_epoch, source_scan, sync_parent_dir,
    validate_search_projection_checkpoint_changes, verify_integrity, wal_generation_file,
    CheckpointPublishStage, CHECKPOINT_HEADER_V1, PROJECTED_GRAPHS_FILE, STORAGE_VERSION,
};
use skein_storage::{
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

    pub(in crate::store) fn write_checkpoint(
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
}
