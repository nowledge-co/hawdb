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

//! Reader-safe physical-generation retention and retryable reclamation.
//!
//! Two directories hold reclaimable generations. The durable data root holds
//! checkpoint artifacts and the data-root WAL of every generation. An admitted
//! branch also owns private WAL files beside its `branch.head`
//! (`branches/<id>/wal.<generation>.hawdb`); the durable head selects exactly
//! one of them. Both are swept by the same post-publication pass and report
//! failures through the same retryable debt receipt.

use super::{DurableStore, GenerationReclamationDebt, ObjectReclamationDebt};
use crate::artifact_files::parse_generation_file;
use crate::error::{HawDBError, Result};
use crate::file_io as fs;
use crate::store::{
    parse_append_segment_generation_file, parse_relational_overflow_extent_generation_file,
    parse_relational_row_page_artifact_generation_file, read_branch_head_for_wal_sweep,
    remove_generation_reclamation_candidate, storage_generation_for_file,
    sync_branch_directory_for_wal_sweep, sync_parent_dir,
};
use hawdb_storage::append_table::{
    append_generation_manifest_file, AppendGenerationManifest, AppendPublicationConfig,
};
use hawdb_storage::ownership::DatabaseDirectoryLease;
use std::collections::BTreeSet;

fn object_debt_from_report(
    report: &crate::branch_reclamation::BranchReclamationReport,
    previous: ObjectReclamationDebt,
) -> ObjectReclamationDebt {
    if report.deferred_for_active_leases {
        // Nothing was inventoried; keep the last measurement.
        return ObjectReclamationDebt {
            retry_required: true,
            ..previous
        };
    }
    ObjectReclamationDebt {
        retry_required: report.retry_required || report.pending_objects > 0,
        pending_objects: report.pending_objects,
        pending_bytes: report.pending_bytes,
        store_objects: report.object_store_objects,
        store_bytes: report.object_store_bytes,
        skipped_objects: report
            .skipped_corrupt_objects
            .saturating_add(report.skipped_quarantine_objects),
        skipped_bytes: report.skipped_bytes,
        corrupt_object: report.corrupt_object,
        quarantine_unverified: report.skipped_quarantine_objects > 0,
    }
}

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

    /// Post-publication maintenance: data-root generations, superseded
    /// branch WALs and, given the branch owner lease, unreachable project
    /// immutable objects (#1736). Never changes the publication outcome.
    pub(in crate::store) fn reclaim_old_generations(
        &mut self,
        current_generation: u64,
        previous_checkpoint_generation: u64,
        pinned_reader_generations: Option<&BTreeSet<u64>>,
        owner: Option<&DatabaseDirectoryLease>,
    ) {
        let mut debt = self.try_reclaim_old_generations(
            current_generation,
            previous_checkpoint_generation,
            pinned_reader_generations,
        );
        debt.objects = match owner {
            Some(owner) => self.try_reclaim_unreachable_objects(owner),
            None => self.generation_reclamation_debt().objects,
        };
        // Filesystem work stays outside this lock. Publish the complete debt
        // receipt atomically to all views of this admitted runtime.
        *self
            .generation_reclamation_debt
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = debt;
    }

    /// Retries only the immutable-object sweep, for a checkpoint request that
    /// found nothing new to publish while object debt remains.
    pub(in crate::store) fn retry_object_reclamation(&self, owner: &DatabaseDirectoryLease) {
        if !self.generation_reclamation_debt().objects.retry_required {
            return;
        }
        let objects = self.try_reclaim_unreachable_objects(owner);
        self.generation_reclamation_debt
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .objects = objects;
    }

    /// Records the outcome of an explicit project reclamation pass.
    pub(in crate::store) fn record_object_reclamation(
        &self,
        report: &crate::branch_reclamation::BranchReclamationReport,
    ) {
        let mut debt = self
            .generation_reclamation_debt
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        debt.objects = object_debt_from_report(report, debt.objects);
    }

    /// One automatic, bounded pass over the project immutable-object store
    /// after this runtime published a head (#1736).
    ///
    /// Liveness is the complete project mark of explicit reclamation: every
    /// non-deleted catalog branch's durable head, every recorded creation
    /// base root (children and siblings start from a sealed parent root), and
    /// every root registered by a live owner (this runtime's head, reader
    /// snapshots, prepared checkpoint candidates and handoffs, derived jobs).
    /// An unregistered active owner defers the pass. The catalog metadata
    /// lock that serializes every root publication and registration is held
    /// from catalog read through the last removal; if it is busy, the pass
    /// is deferred rather than waiting behind another publication. Every
    /// head and the catalog are synchronized before the first removal. Any
    /// deferral or failure keeps all objects and is recorded as retryable
    /// debt for the next checkpoint or reclamation call.
    fn try_reclaim_unreachable_objects(
        &self,
        owner: &DatabaseDirectoryLease,
    ) -> ObjectReclamationDebt {
        let previous = self.generation_reclamation_debt().objects;
        if self.read_only {
            return previous;
        }
        let Some(branch) = &self.branch_runtime else {
            return previous;
        };
        let retry = ObjectReclamationDebt {
            retry_required: true,
            ..previous
        };
        let Ok(_descriptor_operation) = self.file_descriptor_context().internal_operation() else {
            return retry;
        };
        match crate::branch_reclamation::reclaim_owned_project(
            branch.catalog_path(),
            branch.immutable_store_root(),
            owner,
            &branch.head,
            crate::branch_reclamation::BranchReclamationLimits::default(),
            crate::branch_reclamation::ReclamationTrigger::Automatic,
        ) {
            Ok(report) => object_debt_from_report(&report, previous),
            Err(_) => retry,
        }
    }

    fn try_reclaim_old_generations(
        &self,
        current_generation: u64,
        previous_checkpoint_generation: u64,
        pinned_reader_generations: Option<&BTreeSet<u64>>,
    ) -> GenerationReclamationDebt {
        let mut debt = GenerationReclamationDebt::default();
        let Ok(_descriptor_operation) = self.file_descriptor_context().internal_operation() else {
            debt.retry_required = true;
            return debt;
        };
        if self.oldest_reader_commit_epoch.is_some() && pinned_reader_generations.is_none() {
            // An open reader with an unknown pin set may still read any older
            // data-root generation, so that scan waits for a known pin set.
            // Readers never open a branch-private WAL, so its sweep proceeds.
            self.reclaim_superseded_branch_wals(None, &mut debt);
            return debt;
        }

        let mut retained_generations = pinned_reader_generations.cloned().unwrap_or_default();
        retained_generations.insert(current_generation);
        // WAL rotations share the namespace and may leave gaps between
        // checkpoints. Preserve the actual preceding recovery generation.
        if previous_checkpoint_generation > 0 {
            retained_generations.insert(previous_checkpoint_generation);
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
        self.reclaim_superseded_branch_wals(pinned_reader_generations, &mut debt);
        debt
    }

    /// Removes branch-private WAL files that the durable branch head no longer
    /// selects (#1727).
    ///
    /// Recovery and admission open only `wal.<head.active_wal.generation>`
    /// beside the head; every older record is reachable through the sealed
    /// root (sealed WAL objects or the checkpoint it references). Child and
    /// sibling branches are created from a sealed parent root and own their
    /// own directory, so they never read this one. The writable runtime holds
    /// the exclusive branch directory lease, so no reader admission of this
    /// branch can race the sweep.
    ///
    /// Safety ordering: this runs only after the head selecting the newer WAL
    /// was published, and it re-reads that head from disk and synchronizes the
    /// branch directory before the first removal. A head adopted at admission
    /// after a crash may still have an unsynchronized rename; it is made
    /// durable here before anything it superseded is removed. Generations at
    /// or above the selected one (prepared successors, in-flight checkpoint
    /// candidates, interrupted-publication evidence) and reader-pinned
    /// generations are never removed. An interrupted-publication name below
    /// the selected generation can no longer be selected (head generations
    /// and their WAL generations only advance), so it is reclaimed like a
    /// data-root generation below the selected checkpoint. A failure leaves
    /// the file in place and is recorded as retryable debt; the next
    /// reclamation pass retries it.
    ///
    /// Retention is deliberately separate from the data-root set: the current
    /// and previous checkpoint generations number data-root artifacts, not
    /// branch WALs, so they are not consulted here. Reader pins are physical
    /// generations in the shared namespace. Readers never open a branch WAL,
    /// so a pin that matches a branch WAL number only delays its removal.
    fn reclaim_superseded_branch_wals(
        &self,
        pinned_reader_generations: Option<&BTreeSet<u64>>,
        debt: &mut GenerationReclamationDebt,
    ) {
        if self.read_only {
            return;
        }
        let Some(branch) = &self.branch_runtime else {
            return;
        };
        let Some(directory) = branch.head_path().parent() else {
            debt.retry_required = true;
            return;
        };
        let selected_generation = branch.head.active_wal.generation;
        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(_) => {
                debt.retry_required = true;
                return;
            }
        };
        let mut superseded = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    debt.retry_required = true;
                    continue;
                }
            };
            let name = entry.file_name();
            let Some(generation) = name
                .to_str()
                .and_then(|name| parse_generation_file(name, "wal."))
            else {
                continue;
            };
            if generation >= selected_generation
                || pinned_reader_generations.is_some_and(|pins| pins.contains(&generation))
            {
                continue;
            }
            let Ok(metadata) = entry.metadata() else {
                debt.retry_required = true;
                continue;
            };
            if metadata.is_file() {
                superseded.push((entry.path(), metadata.len()));
            }
        }
        if superseded.is_empty() {
            return;
        }
        let record_pending = |debt: &mut GenerationReclamationDebt, bytes: u64| {
            debt.retry_required = true;
            debt.pending_file_count = debt.pending_file_count.saturating_add(1);
            debt.pending_bytes = debt.pending_bytes.saturating_add(bytes);
        };
        let selected = read_branch_head_for_wal_sweep(branch.head_path())
            .is_ok_and(|head| head == branch.head);
        if !selected || sync_branch_directory_for_wal_sweep(directory).is_err() {
            for (_, bytes) in superseded {
                record_pending(debt, bytes);
            }
            return;
        }
        for (path, bytes) in superseded {
            if remove_generation_reclamation_candidate(&path).is_err() {
                record_pending(debt, bytes);
            }
        }
        if crate::durability::sync_directory(directory).is_err() {
            debt.retry_required = true;
        }
    }

    fn retained_relational_physical_generations(
        &self,
        retained_generations: &BTreeSet<u64>,
    ) -> Result<(BTreeSet<u64>, BTreeSet<u64>)> {
        let mut row_page_generations = BTreeSet::new();
        let mut overflow_extent_generations = BTreeSet::new();

        for &generation in retained_generations {
            let overflow_manifest = self.root_path.join(
                hawdb_storage::relational::relational_overflow_manifest_generation_file(generation),
            );
            if overflow_manifest.exists() {
                let overflow =
                    hawdb_storage::relational::RelationalOverflowRootReader::open_generation(
                        &self.root_path,
                        generation,
                        hawdb_storage::relational::RelationalOverflowPublicationConfig::default(),
                    )
                    .map_err(HawDBError::from_storage_error)?;
                overflow
                    .visit_descriptors(|descriptor| {
                        overflow_extent_generations.insert(descriptor.physical_generation);
                        Ok(())
                    })
                    .map_err(HawDBError::from_storage_error)?;
            }

            let row_manifest = self.root_path.join(
                hawdb_storage::relational::relational_row_page_manifest_generation_file(generation),
            );
            if row_manifest.exists() {
                let rows = hawdb_storage::relational::RelationalRowPageRootReader::open_generation(
                    &self.root_path,
                    generation,
                    hawdb_storage::relational::RelationalRowPagePublicationConfig::default(),
                )
                .map_err(HawDBError::from_storage_error)?;
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
                    .map_err(HawDBError::from_storage_error)?;
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
            .map_err(HawDBError::from_storage_error)?;
            segment_generations.extend(manifest.segments.iter().map(|segment| segment.generation));
        }
        Ok(segment_generations)
    }
}
