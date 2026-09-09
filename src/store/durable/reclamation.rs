//! Reader-safe physical-generation retention and retryable reclamation.

use super::{DurableStore, GenerationReclamationDebt};
use crate::error::{Result, SkeinError};
use crate::store::{
    parse_append_segment_generation_file, parse_relational_overflow_extent_generation_file,
    parse_relational_row_page_artifact_generation_file, remove_generation_reclamation_candidate,
    storage_generation_for_file, sync_parent_dir,
};
use skein_storage::{
    append_generation_manifest_file, AppendGenerationManifest, AppendPublicationConfig,
};
use std::collections::BTreeSet;
use std::fs::{self};

impl DurableStore {
    pub(in crate::store) fn obsolete_generation_bytes(
        &self,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> u64 {
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

    pub(in crate::store) fn reclaim_old_generations(
        &mut self,
        current_generation: u64,
        pinned_reader_generations: Option<&BTreeSet<u64>>,
    ) {
        self.generation_reclamation_debt =
            self.try_reclaim_old_generations(current_generation, pinned_reader_generations);
    }

    fn try_reclaim_old_generations(
        &self,
        current_generation: u64,
        pinned_reader_generations: Option<&BTreeSet<u64>>,
    ) -> GenerationReclamationDebt {
        let mut debt = GenerationReclamationDebt::default();
        if self.oldest_reader_commit_epoch.is_some() && pinned_reader_generations.is_none() {
            return debt;
        }

        let mut retained_generations = pinned_reader_generations.cloned().unwrap_or_default();
        retained_generations.insert(current_generation);
        if current_generation > 1 {
            retained_generations.insert(current_generation - 1);
        }
        let (retained_row_page_generations, retained_overflow_extent_generations) =
            match self.retained_relational_physical_generations(&retained_generations) {
                Ok(generations) => generations,
                Err(_) => {
                    debt.retry_required = true;
                    return debt;
                }
            };
        let retained_append_segment_generations =
            match self.retained_append_physical_generations(&retained_generations) {
                Ok(generations) => generations,
                Err(_) => {
                    debt.retry_required = true;
                    return debt;
                }
            };
        let entries = match fs::read_dir(&self.root_path) {
            Ok(entries) => entries,
            Err(_) => {
                debt.retry_required = true;
                return debt;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    debt.retry_required = true;
                    continue;
                }
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let generation = storage_generation_for_file(name);
            if generation.is_some_and(|generation| {
                generation < current_generation && !retained_generations.contains(&generation)
            }) {
                if parse_relational_row_page_artifact_generation_file(name)
                    .is_some_and(|generation| retained_row_page_generations.contains(&generation))
                    || parse_relational_overflow_extent_generation_file(name).is_some_and(
                        |generation| retained_overflow_extent_generations.contains(&generation),
                    )
                    || parse_append_segment_generation_file(name).is_some_and(|generation| {
                        retained_append_segment_generations.contains(&generation)
                    })
                {
                    continue;
                }
                let pending_bytes = entry.metadata().map_or(0, |metadata| metadata.len());
                if remove_generation_reclamation_candidate(&entry.path()).is_err() {
                    debt.retry_required = true;
                    debt.pending_file_count = debt.pending_file_count.saturating_add(1);
                    debt.pending_bytes = debt.pending_bytes.saturating_add(pending_bytes);
                }
            }
        }
        if sync_parent_dir(&self.manifest_path).is_err() {
            debt.retry_required = true;
        }
        debt
    }

    fn retained_relational_physical_generations(
        &self,
        retained_generations: &BTreeSet<u64>,
    ) -> Result<(BTreeSet<u64>, BTreeSet<u64>)> {
        let mut row_page_generations = BTreeSet::new();
        let mut overflow_extent_generations = BTreeSet::new();

        for &generation in retained_generations {
            let overflow_manifest =
                self.root_path
                    .join(skein_storage::relational_overflow_manifest_generation_file(
                        generation,
                    ));
            if overflow_manifest.exists() {
                let overflow = skein_storage::RelationalOverflowRootReader::open_generation(
                    &self.root_path,
                    generation,
                    skein_storage::RelationalOverflowPublicationConfig::default(),
                )
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
                overflow
                    .visit_descriptors(|descriptor| {
                        overflow_extent_generations.insert(descriptor.physical_generation);
                        Ok(())
                    })
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
            }

            let row_manifest =
                self.root_path
                    .join(skein_storage::relational_row_page_manifest_generation_file(
                        generation,
                    ));
            if row_manifest.exists() {
                let rows = skein_storage::RelationalRowPageRootReader::open_generation(
                    &self.root_path,
                    generation,
                    skein_storage::RelationalRowPagePublicationConfig::default(),
                )
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
                let tables = rows
                    .manifest()
                    .tables
                    .iter()
                    .map(|table| table.table.clone())
                    .collect::<Vec<_>>();
                for table in tables {
                    rows.visit_table_pages(&table, |descriptor| {
                        row_page_generations.insert(descriptor.physical_generation);
                        Ok(())
                    })
                    .map_err(|error| SkeinError::Storage(error.to_string()))?;
                }
            }
        }

        Ok((row_page_generations, overflow_extent_generations))
    }

    fn retained_append_physical_generations(
        &self,
        retained_generations: &BTreeSet<u64>,
    ) -> Result<BTreeSet<u64>> {
        let mut segment_generations = BTreeSet::new();
        for &generation in retained_generations {
            let path = self
                .root_path
                .join(append_generation_manifest_file(generation));
            if !path.exists() {
                continue;
            }
            let manifest = AppendGenerationManifest::read_generation(
                &self.root_path,
                generation,
                AppendPublicationConfig::default(),
            )
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
            segment_generations.extend(manifest.segments.iter().map(|segment| segment.generation));
        }
        Ok(segment_generations)
    }
}
