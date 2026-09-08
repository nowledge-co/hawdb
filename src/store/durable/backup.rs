//! Checkpoint-bound backup closure and destination publication.

use super::DurableStore;
use crate::error::{Result, SkeinError};
use crate::store::{
    canonical_adjacency_artifact_generation_file, canonical_artifact_generation_file,
    canonical_manifest_generation_file, checkpoint_generation_file, copy_backup_file,
    property_projection_artifact_generation_file, property_projection_manifest_generation_file,
    property_spill_artifact_generation_file, property_spill_manifest_generation_file,
    relational_checkpoint_generation_file, sync_parent_dir, validate_backup_files,
    validate_new_backup_destination, wal_generation_file, BackupManifest, BACKUP_MANIFEST_FILE,
    MANIFEST_FILE, STABLE_ID_MAPPING_FILE,
};
use skein_storage::{
    append_generation_manifest_file, append_segment_file, AppendGenerationReader,
    AppendPublicationConfig, StorageBackupReport,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self};
use std::path::Path;

impl DurableStore {
    pub(in crate::store) fn backup_to(&self, destination: &Path) -> Result<StorageBackupReport> {
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
}
