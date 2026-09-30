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

//! Checkpoint-bound backup closure and destination publication.

use super::DurableStore;
use crate::checkpoint_closure::{
    CheckpointArtifactFamily, CheckpointArtifactInput, CheckpointClosurePlan,
};
use crate::durable_manifest::DurableManifest;
use crate::error::{HawDBError, Result};
use crate::immutable_object::{ObjectKind, ObjectReference};
use crate::store::{
    canonical_adjacency_artifact_generation_file, canonical_artifact_generation_file,
    canonical_manifest_generation_file, checkpoint_generation_file, copy_backup_file,
    property_projection_artifact_generation_file, property_projection_manifest_generation_file,
    property_spill_artifact_generation_file, property_spill_manifest_generation_file,
    relational_checkpoint_generation_file, sync_parent_dir, validate_backup_files,
    validate_new_backup_destination, wal_generation_file, BackupManifest, BACKUP_MANIFEST_FILE,
    MANIFEST_FILE, STABLE_ID_MAPPING_FILE,
};
use hawdb_storage::{
    append_table::{
        append_generation_manifest_file, append_segment_file, AppendGenerationReader,
        AppendPublicationConfig,
    },
    backup::StorageBackupReport,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self};
use std::path::{Path, PathBuf};

impl DurableStore {
    /// Builds the complete validated checkpoint closure through bound family
    /// readers; it never scans the storage directory.
    pub(in crate::store) fn checkpoint_closure_plan(
        &self,
        manifest: DurableManifest,
    ) -> Result<CheckpointClosurePlan> {
        let generation = manifest.checkpoint_generation.ok_or_else(|| {
            HawDBError::Storage("checkpoint closure requires a checkpoint generation".into())
        })?;
        let mut plan =
            CheckpointClosurePlan::new(manifest.manifest_artifact_inputs(&self.root_path)?);
        let input = |path: PathBuf| -> Result<CheckpointArtifactInput> {
            let bytes = fs::read(&path).map_err(|error| {
                HawDBError::Storage(format!(
                    "read checkpoint closure artifact {}: {error}",
                    path.display()
                ))
            })?;
            Ok(CheckpointArtifactInput {
                path,
                reference: ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &bytes),
            })
        };
        let map_paths = |names: Vec<String>| {
            names
                .iter()
                .map(|name| input(self.root_path.join(name)))
                .collect::<Result<Vec<_>>>()
        };
        let add =
            |plan: &mut CheckpointClosurePlan, family, inputs: Vec<CheckpointArtifactInput>| {
                plan.add_family_artifacts(family, inputs)
                    .map_err(|error| HawDBError::Storage(error.to_string()))
            };
        if manifest.canonical_manifest_encoded_len.is_some() {
            add(
                &mut plan,
                CheckpointArtifactFamily::Canonical,
                map_paths(vec![
                    canonical_artifact_generation_file(generation),
                    hawdb_storage::canonical::canonical_segment_descriptor_page_file(generation),
                    hawdb_storage::canonical::canonical_segment_descriptor_root_file(generation),
                ])?,
            )?;
        } else {
            plan.mark_family_empty(CheckpointArtifactFamily::Canonical)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
        }
        if manifest.canonical_adjacency_generation_artifacts.is_some() {
            add(
                &mut plan,
                CheckpointArtifactFamily::Adjacency,
                map_paths(vec![
                    hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(
                        generation,
                    ),
                ])?,
            )?;
        } else {
            plan.mark_family_empty(CheckpointArtifactFamily::Adjacency)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
        }
        if manifest.property_spill_manifest_encoded_len.is_some() {
            add(
                &mut plan,
                CheckpointArtifactFamily::PropertySpill,
                map_paths(vec![
                    property_spill_artifact_generation_file(generation),
                    hawdb_storage::property_spill::property_spill_descriptor_page_file(generation),
                    hawdb_storage::property_spill::property_spill_descriptor_root_file(generation),
                ])?,
            )?;
        } else {
            plan.mark_family_empty(CheckpointArtifactFamily::PropertySpill)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
        }
        if manifest.property_projection_manifest_encoded_len.is_some() {
            add(
                &mut plan,
                CheckpointArtifactFamily::PropertyProjection,
                map_paths(vec![
                    property_projection_artifact_generation_file(generation),
                    hawdb_storage::property_projection::property_projection_descriptor_page_file(
                        generation,
                    ),
                    hawdb_storage::property_projection::property_projection_descriptor_root_file(
                        generation,
                    ),
                ])?,
            )?;
        } else {
            plan.mark_family_empty(CheckpointArtifactFamily::PropertyProjection)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
        }
        if let Some(binding) = manifest.relational_overflow_generation_artifacts {
            let overflow = self.open_bound_relational_overflow()?;
            let mut generations = BTreeSet::from([overflow.manifest().generation]);
            overflow
                .visit_descriptors(|descriptor| {
                    generations.insert(descriptor.physical_generation);
                    Ok(())
                })
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
            let mut inputs = vec![input(self.root_path.join(
                hawdb_storage::relational::relational_overflow_descriptor_file(binding.generation),
            ))?];
            inputs.extend(
                generations
                    .into_iter()
                    .map(|generation| {
                        input(self.root_path.join(
                            hawdb_storage::relational::relational_overflow_extent_file(generation),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            add(
                &mut plan,
                CheckpointArtifactFamily::RelationalOverflow,
                inputs,
            )?;
        } else {
            plan.mark_family_empty(CheckpointArtifactFamily::RelationalOverflow)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
        }
        if let Some(binding) = manifest.relational_row_generation_artifacts {
            let overflow = self.open_bound_relational_overflow()?;
            let rows = self.open_bound_relational_row_pages(&overflow)?;
            let mut generations = BTreeSet::from([rows.manifest().generation]);
            for table in rows
                .manifest()
                .tables
                .iter()
                .map(|table| table.table.clone())
            {
                rows.visit_table_pages(&table, |descriptor| {
                    generations.insert(descriptor.physical_generation);
                    Ok(())
                })
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
            }
            let mut inputs = vec![
                input(self.root_path.join(
                    hawdb_storage::relational::relational_row_page_root_descriptor_file(
                        binding.generation,
                    ),
                ))?,
                input(self.root_path.join(
                    hawdb_storage::relational::relational_row_page_root_key_file(
                        binding.generation,
                    ),
                ))?,
            ];
            inputs.extend(
                generations
                    .into_iter()
                    .map(|generation| {
                        input(self.root_path.join(
                            hawdb_storage::relational::relational_row_page_artifact_file(
                                generation,
                            ),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
            add(&mut plan, CheckpointArtifactFamily::RelationalRow, inputs)?;
        } else {
            plan.mark_family_empty(CheckpointArtifactFamily::RelationalRow)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
        }
        if let Some(binding) = manifest.relational_index_generation_artifacts {
            add(
                &mut plan,
                CheckpointArtifactFamily::RelationalIndex,
                vec![input(self.root_path.join(
                    hawdb_storage::relational::relational_index_shadow_artifact_file(
                        binding.generation,
                    ),
                ))?],
            )?;
        } else {
            plan.mark_family_empty(CheckpointArtifactFamily::RelationalIndex)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
        }
        if let Some(binding) = manifest.append_generation_artifacts {
            let reader = AppendGenerationReader::open_bound(
                &self.root_path,
                binding,
                AppendPublicationConfig::default(),
            )
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
            let inputs = reader
                .segment_bindings()
                .iter()
                .map(|segment| input(self.root_path.join(append_segment_file(segment.generation))))
                .collect::<Result<Vec<_>>>()?;
            add(&mut plan, CheckpointArtifactFamily::Append, inputs)?;
        } else {
            plan.mark_family_empty(CheckpointArtifactFamily::Append)
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
        }
        Ok(plan)
    }

    pub(in crate::store) fn backup_to(&self, destination: &Path) -> Result<StorageBackupReport> {
        let generation = self.checkpoint_epoch;
        if self.checkpoint_encoded_len.is_none() {
            return Err(HawDBError::Storage(
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
                let page_name = hawdb_storage::relational::relational_index_shadow_artifact_file(
                    binding.generation,
                );
                let manifest_name =
                    hawdb_storage::relational::relational_index_shadow_manifest_generation_file(
                        binding.generation,
                    );
                sources.insert(page_name.clone(), self.root_path.join(page_name));
                sources.insert(manifest_name.clone(), self.root_path.join(manifest_name));
            }
            if let Some(binding) = self.relational_row_generation_artifacts {
                for name in [
                    hawdb_storage::relational::relational_row_page_root_descriptor_file(
                        binding.generation,
                    ),
                    hawdb_storage::relational::relational_row_page_root_key_file(
                        binding.generation,
                    ),
                    hawdb_storage::relational::relational_row_page_manifest_generation_file(
                        binding.generation,
                    ),
                ] {
                    sources.insert(name.clone(), self.root_path.join(name));
                }
            }
            if let Some(binding) = self.relational_overflow_generation_artifacts {
                for name in [
                    hawdb_storage::relational::relational_overflow_descriptor_file(
                        binding.generation,
                    ),
                    hawdb_storage::relational::relational_overflow_manifest_generation_file(
                        binding.generation,
                    ),
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
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
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
                .map_err(|error| HawDBError::Storage(error.to_string()))?;
            for physical_generation in overflow_extent_generations {
                let name =
                    hawdb_storage::relational::relational_overflow_extent_file(physical_generation);
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
                    .map_err(|error| HawDBError::Storage(error.to_string()))?;
            }
            for physical_generation in row_page_generations {
                let name = hawdb_storage::relational::relational_row_page_artifact_file(
                    physical_generation,
                );
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
                    hawdb_storage::canonical::canonical_segment_descriptor_page_file(generation),
                    hawdb_storage::canonical::canonical_segment_descriptor_root_file(generation),
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
                    hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(
                        generation,
                    ),
                    hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_root_file(
                        generation,
                    ),
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
                    hawdb_storage::property_spill::property_spill_descriptor_page_file(generation),
                    hawdb_storage::property_spill::property_spill_descriptor_root_file(generation),
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
                    hawdb_storage::property_projection::property_projection_descriptor_page_file(
                        generation,
                    ),
                    hawdb_storage::property_projection::property_projection_descriptor_root_file(
                        generation,
                    ),
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
                            HawDBError::Storage(
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
                    return Err(HawDBError::Storage(
                        "stable identity selector exists without a pinned generation".to_string(),
                    ));
                }
                (false, Some(_)) => {
                    return Err(HawDBError::Storage(
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
                .ok_or_else(|| HawDBError::Storage("backup byte count overflow".to_string()))?;
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
