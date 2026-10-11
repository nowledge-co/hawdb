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

//! A pinned checkpoint base with incremental, validated post-snapshot replay.

use super::durable::CheckpointReplayBoundary;
use super::immutable_root::PreparedCheckpointBranchRoot;
use super::*;
use crate::relational::RelationalRecoverySourceBuilder;
use std::io::Write;

#[cfg(test)]
#[path = "graph_checkpoint_candidate/tests.rs"]
mod checkpoint_wal_frame_tests;

#[cfg(test)]
#[path = "graph_checkpoint_candidate/unchanged_relational_tests.rs"]
mod unchanged_relational_tests;

#[cfg(test)]
#[path = "graph_checkpoint_candidate/unchanged_relational_related_tests.rs"]
mod unchanged_relational_related_tests;

#[cfg(test)]
#[path = "graph_checkpoint_candidate/resume_tests.rs"]
mod resume_tests;

#[cfg(test)]
#[path = "graph_checkpoint_candidate/verbatim_tests.rs"]
mod verbatim_tests;

#[cfg(test)]
#[path = "graph_checkpoint_candidate/unit_qos_tests.rs"]
mod unit_qos_tests;

#[cfg(test)]
#[path = "graph_checkpoint_candidate/incremental_memory_tests.rs"]
mod incremental_memory_tests;

/// Identity of the complete foreground prefix from which a worker publishes.
/// Opaque fields prevent a facade caller from manufacturing a partial receipt.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointSourceIdentity {
    store_id: crate::cache::StoreId,
    commit_epoch: u64,
    checkpoint_generation: u64,
    wal_generation: u64,
    next_lsn: u64,
    wal_bytes: u64,
    branch_head: Option<crate::branch_head::BranchHead>,
}

impl CheckpointSourceIdentity {
    pub(super) fn can_advance_to(self, next: Self) -> bool {
        self.store_id == next.store_id
            && self.checkpoint_generation == next.checkpoint_generation
            && self.wal_generation == next.wal_generation
            && self.branch_head == next.branch_head
            && next.wal_bytes >= self.wal_bytes
            && next
                .next_lsn
                .checked_sub(self.next_lsn)
                .is_some_and(|entries| {
                    next.commit_epoch.checked_sub(self.commit_epoch) == Some(entries)
                })
    }
}

/// Constant-time WAL/delta scheduling signals. Reading these does not enumerate
/// generations, walk graph records, or probe the filesystem.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointDebtSnapshot {
    pub commit_epoch: u64,
    pub checkpoint_commit_epoch: u64,
    pub wal_generation: u64,
    pub wal_bytes: u64,
    pub wal_age_millis: u64,
    pub max_wal_bytes: Option<u64>,
    pub max_wal_record_bytes: Option<usize>,
    pub delta_bytes: u64,
    pub max_delta_bytes: Option<u64>,
    pub read_only: bool,
    pub wal_sync_group_active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BootstrapStage {
    Prepared,
    Mounted,
    BranchPrepared,
    Ready,
}

/// One unpublished checkpoint and its complete private replay state. A caller
/// must serialize candidates and manual checkpoint/seal work for this writer.
/// Catch-up runs on captured sources outside the writer critical section.
#[doc(hidden)]
pub struct CheckpointCandidate {
    bootstrap_stage: BootstrapStage,
    bootstrap_manifest: Option<super::durable::DurableManifest>,
    // Keep unit admission across fresh catch-up tasks without retaining the
    // cancelled task or an execution permit for every record in the dataset.
    scheduler: Option<hawdb_qos::LocalQosScheduler>,
    prepared: Option<PreparedCheckpoint>,
    store: Option<GraphStore>,
    catalog: Option<Catalog>,
    source_wal_path: PathBuf,
    source_head: Option<crate::branch_head::BranchHead>,
    source_store_id: crate::cache::StoreId,
    source_capture_started: std::time::Instant,
    captured_wal_bytes: u64,
    captured_next_lsn: u64,
    candidate_wal_bytes: u64,
    // Only a completely applied record advances candidate_wal_bytes. A
    // failed physical write may leave bytes through this attempted boundary;
    // a fresh admitted task must truncate them before continuing.
    wal_write_attempt_end: Option<u64>,
    wal_sync_pending: bool,
    replay_interrupted: bool,
    branch_root: Option<PreparedCheckpointBranchRoot>,
    owns_branch_wal: bool,
    recovery_source: Option<RelationalRecoverySourceBuilder>,
    recovery_selectors: [Option<crate::relational::PreparedRelationalRecoverySelector>; 2],
    replay_finalized: bool,
    relational_replay_required: bool,
    failed: bool,
    retain_artifacts: bool,
    // Destruction of old COW maps may scale with the dataset. The job drops
    // this owner after releasing the writer publication barrier.
    retired_store: Option<GraphStore>,
    retired_recovery_builders: Option<(
        Option<crate::relational::RelationalRowDeltaBuilder>,
        Option<crate::relational::RelationalIndexRecoveryBuilder>,
    )>,
}

impl std::fmt::Debug for CheckpointCandidate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CheckpointCandidate")
            .field("commit_epoch", &self.commit_epoch())
            .field("captured_next_lsn", &self.captured_next_lsn)
            .field("captured_wal_bytes", &self.captured_wal_bytes)
            .field("candidate_wal_bytes", &self.candidate_wal_bytes)
            .field(
                "relational_replay_required",
                &self.relational_replay_required,
            )
            .field("failed", &self.failed)
            .field("retain_artifacts", &self.retain_artifacts)
            .finish_non_exhaustive()
    }
}

impl GraphStore {
    /// Provisional reservation estimate for the current whole-candidate builder.
    /// This is not a complete bound for source/candidate/retained allocations.
    /// It is deliberately not clipped to available host memory; explicit
    /// allocation leases and bounded work still require full qualification.
    #[doc(hidden)]
    pub fn checkpoint_candidate_admission_bytes(&self) -> Result<u64> {
        self.checkpoint_candidate_admission_bytes_with_visit(&mut || {
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap_or_else(|never| match never {})
    }

    /// Plans the existing conservative reservation through borrowed size steps.
    /// Every graph/property/value and relational cell step can defer or cancel;
    /// no complete record/value array is allocated during this scan.
    #[doc(hidden)]
    pub fn checkpoint_candidate_admission_bytes_with_work_context(
        &self,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<u64> {
        self.checkpoint_candidate_admission_bytes_with_visit(&mut || {
            let unit = work.start_unit()?;
            unit.finish();
            Ok::<_, crate::background::CheckpointWorkError>(())
        })
        .map_err(HawDBError::from_storage_error)?
    }

    fn checkpoint_candidate_admission_bytes_with_visit<E>(
        &self,
        visit: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<Result<u64>, E> {
        visit()?;
        let mut logical = self
            .canonical_base
            .as_ref()
            .map_or(0, |reader| reader.manifest().artifact_len);
        for node in self.nodes.values() {
            logical = logical.saturating_add(estimated_node_record_bytes_with_visit(node, visit)?);
        }
        for relationship in self.relationships.values() {
            logical = logical.saturating_add(estimated_relationship_record_bytes_with_visit(
                relationship,
                visit,
            )?);
        }
        let (resident_rows, checkpoint_rows) = self
            .relational_state
            .estimated_checkpoint_admission_bytes_with_visit(visit)?;
        Ok(logical
            .checked_add(resident_rows)
            .and_then(|bytes| bytes.checked_add(checkpoint_rows))
            .and_then(|bytes| bytes.checked_mul(16))
            .and_then(|bytes| bytes.checked_add(128 * 1024 * 1024))
            .ok_or_else(|| {
                HawDBError::Storage("checkpoint candidate admission size overflow".into())
            }))
    }

    #[doc(hidden)]
    pub fn checkpoint_source_identity(&self) -> Option<CheckpointSourceIdentity> {
        let durable = self.durable.as_ref()?;
        Some(CheckpointSourceIdentity {
            store_id: durable.store_id(),
            commit_epoch: self.commit_epoch,
            checkpoint_generation: durable.checkpoint_epoch,
            wal_generation: durable.wal_generation,
            next_lsn: durable.next_lsn,
            wal_bytes: durable.wal_bytes,
            branch_head: self.admitted_branch_head().copied(),
        })
    }

    #[doc(hidden)]
    pub fn checkpoint_debt_snapshot(&self) -> Option<CheckpointDebtSnapshot> {
        let durable = self.durable.as_ref()?;
        Some(CheckpointDebtSnapshot {
            commit_epoch: self.commit_epoch,
            checkpoint_commit_epoch: durable.checkpoint_commit_epoch,
            wal_generation: durable.wal_generation,
            wal_bytes: durable.wal_bytes,
            wal_age_millis: if self.commit_epoch > durable.checkpoint_commit_epoch {
                durable.wal_age_millis().unwrap_or_default()
            } else {
                0
            },
            max_wal_bytes: durable.max_wal_bytes,
            max_wal_record_bytes: durable.max_wal_record_bytes(),
            delta_bytes: if self.canonical_base_out_of_core {
                self.estimated_delta_resident_bytes()
            } else {
                0
            },
            max_delta_bytes: if self.canonical_base_out_of_core {
                self.max_out_of_core_delta_bytes
            } else {
                None
            },
            read_only: durable.read_only,
            wal_sync_group_active: durable.wal_sync_group_active(),
        })
    }

    /// Receives a runtime whose durable selector was published by the sole
    /// checkpoint worker while frontend writes held the same publication gate.
    /// Returns the retired frontend so its destruction can run off the gate.
    #[doc(hidden)]
    pub fn adopt_selected_checkpoint(
        &mut self,
        mut selected: Self,
        expected_source: CheckpointSourceIdentity,
    ) -> Result<Self> {
        self.ensure_usable()?;
        selected.ensure_usable()?;
        let valid = self.checkpoint_source_identity() == Some(expected_source)
            && !self.wal_sync_group_active()
            && selected.durable.as_ref().is_some_and(|durable| {
                !durable.read_only
                    && !durable.wal_sync_group_active()
                    && durable.store_id() == expected_source.store_id
                    && durable.checkpoint_epoch > expected_source.checkpoint_generation
                    && durable.wal_generation > expected_source.wal_generation
                    && durable.next_lsn == expected_source.next_lsn
                    && selected.commit_epoch == expected_source.commit_epoch
            });
        if !valid {
            // A selected checkpoint is already disk authority. Never continue
            // appending to an old WAL after a failed frontend handoff.
            self.integrity_poisoned.store(true, AtomicOrdering::Release);
            return Err(HawDBError::StorageIntegrity(
                "selected checkpoint does not match the complete foreground prefix".into(),
            ));
        }
        selected.inherit_live_checkpoint_ownership(self);
        Ok(std::mem::replace(self, selected))
    }

    fn inherit_live_checkpoint_ownership(&mut self, source: &mut Self) {
        self.checkpoint_capture_started = source.checkpoint_capture_started;
        self.version_index = source.version_index.clone();
        self.version_snapshot_pins = source.version_snapshot_pins.clone();
        self.version_snapshot_pin = source.version_snapshot_pin.take();
        self.post_wal_apply_poisoned = Arc::clone(&source.post_wal_apply_poisoned);
        self.integrity_poisoned = Arc::clone(&source.integrity_poisoned);
        self.search_projection_graph_changes = source.search_projection_graph_changes.clone();
        self.search_projection_change_log_start_epoch =
            source.search_projection_change_log_start_epoch;
        self.search_projection_change_log_retained_bytes =
            source.search_projection_change_log_retained_bytes;
        self.checkpoint_statistics = source.checkpoint_statistics.clone();
        self.advanced_statistics_dirty = source.advanced_statistics_dirty;
        self.runtime_governor = source.runtime_governor.clone();
        self.projection_generations = source.projection_generations.take();
        self.snapshot_file_context = source.snapshot_file_context.clone();
        if let (Some(next), Some(current)) = (&mut self.durable, &source.durable) {
            next.telemetry = current.telemetry.clone();
        }
    }

    /// Prepares and mounts a private base from this pinned checkpoint source.
    /// The old manifest, branch head and active WAL remain authoritative.
    #[doc(hidden)]
    pub fn prepare_checkpoint_candidate(
        &self,
        catalog: &Catalog,
    ) -> Result<Option<CheckpointCandidate>> {
        self.prepare_checkpoint_candidate_with_work_context(
            catalog,
            &crate::background::CheckpointWorkContext::default(),
        )
    }

    #[doc(hidden)]
    pub fn prepare_checkpoint_candidate_with_work_context(
        &self,
        catalog: &Catalog,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<Option<CheckpointCandidate>> {
        let Some(mut preparation) = self.begin_checkpoint_preparation(catalog)? else {
            return Ok(None);
        };
        preparation.prepare_candidate_with_work_context(work)
    }

    pub(super) fn checkpoint_candidate_from_prepared(
        &self,
        catalog: &Catalog,
        prepared: PreparedCheckpoint,
        work: &crate::background::CheckpointWorkContext,
    ) -> CheckpointCandidate {
        let durable = self
            .durable
            .as_ref()
            .expect("prepared checkpoint is durable");
        CheckpointCandidate {
            bootstrap_stage: BootstrapStage::Prepared,
            bootstrap_manifest: None,
            scheduler: work.scheduler(),
            source_wal_path: durable.wal_path.clone(),
            source_head: self.admitted_branch_head().copied(),
            source_store_id: durable.store_id(),
            source_capture_started: self
                .checkpoint_capture_started
                .unwrap_or_else(std::time::Instant::now),
            captured_wal_bytes: prepared.source_wal_bytes,
            captured_next_lsn: prepared.source_next_lsn,
            candidate_wal_bytes: WAL_BINARY_FILE_HEADER_BYTES as u64,
            wal_write_attempt_end: None,
            wal_sync_pending: false,
            replay_interrupted: false,
            recovery_source: Some(RelationalRecoverySourceBuilder::new(
                prepared.generation,
                prepared.source_next_lsn,
            )),
            replay_finalized: false,
            relational_replay_required: false,
            recovery_selectors: [None, None],
            store: Some(self.checkpoint_source()),
            catalog: Some(catalog.clone()),
            prepared: Some(prepared),
            branch_root: None,
            owns_branch_wal: false,
            failed: false,
            retain_artifacts: false,
            retired_store: None,
            retired_recovery_builders: None,
        }
    }

    /// Selects an already caught-up candidate. The caller holds the writer
    /// publication barrier. A stale final capture is rejected without erasing
    /// the candidate, so another incremental catch-up pass can make progress.
    #[doc(hidden)]
    pub fn publish_checkpoint_candidate(
        &mut self,
        candidate: &mut CheckpointCandidate,
        oldest_reader_commit_epoch: Option<u64>,
        pinned_reader_generations: &BTreeSet<u64>,
    ) -> Result<Catalog> {
        self.publish_checkpoint_candidate_inner(
            candidate,
            oldest_reader_commit_epoch,
            Some(pinned_reader_generations),
        )
    }

    /// The automatic worker finalizes replay before entering the publication
    /// gate and retires generations after leaving it. This entry never walks
    /// obsolete generations or prunes a database-sized version index.
    #[doc(hidden)]
    pub fn publish_checkpoint_candidate_deferred_reclamation(
        &mut self,
        candidate: &mut CheckpointCandidate,
        oldest_reader_commit_epoch: Option<u64>,
    ) -> Result<Catalog> {
        if !candidate.replay_finalized {
            return Err(HawDBError::Storage(
                "automatic checkpoint replay must be finalized before publication".into(),
            ));
        }
        self.publish_checkpoint_candidate_inner(candidate, oldest_reader_commit_epoch, None)
    }

    fn publish_checkpoint_candidate_inner(
        &mut self,
        candidate: &mut CheckpointCandidate,
        oldest_reader_commit_epoch: Option<u64>,
        pinned_reader_generations: Option<&BTreeSet<u64>>,
    ) -> Result<Catalog> {
        self.ensure_usable()?;
        candidate.validate_source(self)?;
        let next = candidate
            .store
            .as_ref()
            .expect("validated candidate owns its runtime");
        if fs::metadata(&next.durable.as_ref().expect("durable candidate").wal_path)?.len()
            != candidate.candidate_wal_bytes
        {
            candidate.failed = true;
            return Err(HawDBError::StorageIntegrity(
                "checkpoint candidate WAL no longer covers its replayed commits".into(),
            ));
        }
        if next.commit_epoch != self.commit_epoch
            || candidate.captured_next_lsn
                != self.durable.as_ref().expect("durable source").next_lsn
            || candidate.captured_wal_bytes
                != self.durable.as_ref().expect("durable source").wal_bytes
        {
            return Err(HawDBError::Storage(
                "checkpoint candidate needs another captured suffix before publication".into(),
            ));
        }
        candidate.finish_catch_up()?;
        let prepared = candidate
            .prepared
            .as_ref()
            .expect("validated candidate owns its base");
        let generation = prepared.generation;
        let previous_generation = prepared.source_checkpoint_epoch;
        let next = candidate
            .store
            .as_mut()
            .expect("validated candidate owns its runtime");
        let replayed = usize::try_from(candidate.captured_next_lsn - prepared.source_next_lsn)
            .map_err(|_| HawDBError::Storage("checkpoint suffix entry count overflow".into()))?;
        // Private replay/reader setup has completed. Selector publication now
        // makes this candidate potentially authoritative; retain evidence if
        // any of these publications become uncertain.
        candidate.retain_artifacts = true;
        let publication: Result<()> = (|| {
            for selector in candidate.recovery_selectors.iter_mut().flatten() {
                selector.publish().map_err(|error| {
                    HawDBError::Storage(format!("publish checkpoint recovery selector: {error}"))
                })?;
            }
            if let Some(report) = next.relational_row_pages.recovery_report.as_mut() {
                report.events[3] =
                    crate::relational::RelationalRowDeltaPublicationPhase::LatestManifestPublished;
            }
            let durable = next.durable.as_mut().expect("candidate is durable");
            let is_branch = candidate.branch_root.is_some();
            let manifest = durable.checkpoint_manifest(
                generation,
                prepared.manifest_artifacts,
                prepared.source_commit_epoch,
                if is_branch {
                    None
                } else {
                    oldest_reader_commit_epoch
                },
                prepared.source_scan_publication,
                CheckpointReplayBoundary {
                    start_lsn: prepared.source_next_lsn,
                    next_lsn: if is_branch {
                        prepared.source_next_lsn
                    } else {
                        candidate.captured_next_lsn
                    },
                    commit_epoch: if is_branch {
                        prepared.source_commit_epoch
                    } else {
                        next.commit_epoch
                    },
                },
            )?;
            durable.publish_checkpoint_sidecars(
                &prepared.staging_path,
                prepared.publish_projected_graph_artifacts,
                prepared.source_scan_publication,
            )?;
            manifest.write(durable.manifest_path())?;
            checkpoint_publish_failpoint(CheckpointPublishStage::ManifestPublished)?;
            // The immutable branch root was prepared outside the writer; its
            // manifest cannot be rewritten with final reader metrics because
            // that would select a different content-addressed root.
            durable.oldest_reader_commit_epoch = oldest_reader_commit_epoch;
            durable.safe_reclaim_commit_epoch =
                safe_reclaim_commit_epoch(prepared.source_commit_epoch, oldest_reader_commit_epoch);
            if let Some(root) = candidate.branch_root.take() {
                next.publish_checkpoint_branch_root(root)?;
                checkpoint_publish_failpoint(CheckpointPublishStage::BranchHeadPublished)?;
            }
            Ok(())
        })();
        if let Err(error) = publication {
            self.integrity_poisoned.store(true, AtomicOrdering::Release);
            candidate.failed = true;
            return Err(HawDBError::StorageIntegrity(format!(
                "checkpoint candidate publication requires recovery: {error}"
            )));
        }
        let mut next = candidate
            .store
            .take()
            .expect("published candidate owns its runtime");
        // These maps may contain many sealed run descriptors. Move their
        // ownership out of the serving store; the job drops them off the gate.
        candidate.retired_recovery_builders = Some((
            next.relational_row_pages.recovery_builder.take(),
            next.relational_index_shadow.recovery_builder.take(),
        ));
        // Recovery reconstructs values, not foreground conflict history or
        // consumer acknowledgments. Preserve the exact live ownership state.
        next.inherit_live_checkpoint_ownership(self);
        if let Some(durable) = &mut next.durable {
            durable.wal_uncheckpointed_since = if replayed == 0 {
                None
            } else {
                // S is now checkpointed. Its original debt may be arbitrarily
                // old and must not survive as the age of the post-S suffix.
                // Capture time is a conservative floor for the first suffix
                // commit; later appends never move that floor forward.
                Some(candidate.source_capture_started)
            };
        }
        let catalog = candidate
            .catalog
            .take()
            .expect("published candidate owns its catalog");
        let old = std::mem::replace(self, next);
        candidate.retired_store = Some(old);
        if let Some(pinned_reader_generations) = pinned_reader_generations {
            if let Some(durable) = &mut self.durable {
                durable.reclaim_old_generations(
                    generation,
                    previous_generation,
                    Some(pinned_reader_generations),
                );
            }
            self.reclaim_version_history();
        }
        Ok(catalog)
    }
}

impl CheckpointCandidate {
    pub(super) fn bootstrap_with_work_context(
        &mut self,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<()> {
        if self.bootstrap_stage == BootstrapStage::Prepared {
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
            let store = self.store.as_mut().expect("candidate owns a runtime");
            // Private replay errors must not poison the still-authoritative writer.
            // The original shared flags are restored on successful selection.
            store.post_wal_apply_poisoned = Arc::new(AtomicBool::new(false));
            store.integrity_poisoned = Arc::new(AtomicBool::new(false));
            let prepared = self.prepared.as_mut().expect("candidate owns its base");
            let durable = store.durable.as_mut().expect("candidate is durable");
            let manifest = durable.checkpoint_manifest(
                prepared.generation,
                prepared.manifest_artifacts,
                prepared.source_commit_epoch,
                None,
                prepared.source_scan_publication,
                CheckpointReplayBoundary {
                    start_lsn: prepared.source_next_lsn,
                    next_lsn: prepared.source_next_lsn,
                    commit_epoch: prepared.source_commit_epoch,
                },
            )?;
            durable.adopt_checkpoint_manifest(
                manifest,
                prepared.manifest_artifacts.relational_checkpoint,
                prepared.source_commit_epoch,
            )?;
            store.source_scan_manifest = CowSegment::default();
            store.adopt_prepared_checkpoint_state(prepared)?;
            store.mount_relational_index_shadow_for_recovery();
            store.validate_authoritative_relational_index_open()?;
            self.bootstrap_manifest = Some(manifest);
            self.bootstrap_stage = BootstrapStage::Mounted;
        }
        if self.bootstrap_stage == BootstrapStage::Mounted {
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
            let store = self.store.as_mut().expect("candidate owns a runtime");
            let manifest = self.bootstrap_manifest.expect("mounted candidate manifest");
            self.branch_root = store.prepare_checkpoint_branch_root(manifest)?;
            self.bootstrap_stage = BootstrapStage::BranchPrepared;
        }
        if self.bootstrap_stage == BootstrapStage::BranchPrepared {
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
            let store = self.store.as_mut().expect("candidate owns a runtime");
            let prepared = self.prepared.as_ref().expect("candidate owns its base");
            if let Some(head_path) = store
                .durable
                .as_ref()
                .and_then(|durable| durable.branch_runtime.as_ref())
                .map(|branch| branch.head_path().to_path_buf())
            {
                let directory = head_path.parent().ok_or_else(|| {
                    HawDBError::StorageIntegrity("checkpoint branch has no directory".into())
                })?;
                let path = directory.join(wal_generation_file(prepared.generation));
                let header =
                    encode_binary_wal_header(prepared.generation, prepared.source_next_lsn);
                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                // Ownership begins only after exclusive creation. Never remove an
                // earlier interrupted candidate with the same generation spelling.
                self.owns_branch_wal = true;
                store
                    .durable
                    .as_mut()
                    .expect("candidate is durable")
                    .wal_path = path;
                file.write_all(&header)?;
                file.sync_all()?;
                drop(file);
                crate::durability::sync_directory_ancestors(directory)?;
            }
            self.bootstrap_manifest = None;
            self.bootstrap_stage = BootstrapStage::Ready;
        }
        work.checkpoint().map_err(HawDBError::from_storage_error)
    }

    /// Whether a stopped private attempt can continue against this complete
    /// source. Publication uncertainty and partial mutation never qualify.
    /// This does not admit work or modify recovery dependencies.
    #[doc(hidden)]
    pub fn can_continue_from(&self, source: &GraphStore) -> bool {
        !self.retain_artifacts
            && source.ensure_usable().is_ok()
            && self.validate_source(source).is_ok()
    }

    pub fn commit_epoch(&self) -> u64 {
        self.store.as_ref().map_or(0, GraphStore::commit_epoch)
    }

    /// Bytes in the validated captured suffix still missing from this candidate.
    /// This does not read the WAL or bound the cost of applying its operations.
    #[doc(hidden)]
    pub fn remaining_catch_up_bytes(&self, source: &GraphStore) -> Result<u64> {
        self.validate_source(source)?;
        let durable = source
            .durable
            .as_ref()
            .expect("validated source is durable");
        durable
            .wal_bytes
            .checked_sub(self.captured_wal_bytes)
            .ok_or_else(|| {
                HawDBError::StorageIntegrity("checkpoint suffix byte bound regressed".into())
            })
    }

    /// Seals this complete prefix and pins its serving readers outside the
    /// writer gate. Later catch-up extends the same pinned checkpoint base;
    /// its builders and source digest remain available for another seal.
    pub fn finish_catch_up(&mut self) -> Result<()> {
        if self.failed {
            return Err(HawDBError::Storage(
                "checkpoint candidate has failed".into(),
            ));
        }
        if self.replay_interrupted || self.wal_sync_pending || self.wal_write_attempt_end.is_some()
        {
            return Err(HawDBError::Storage(
                "checkpoint replay must resume and synchronize before finalization".into(),
            ));
        }
        if self.replay_finalized {
            return Ok(());
        }
        let result = (|| {
            let prepared = self.prepared.as_ref().ok_or_else(|| {
                HawDBError::Storage("checkpoint candidate has no prepared base".into())
            })?;
            let source = self.recovery_source.as_ref().ok_or_else(|| {
                HawDBError::StorageIntegrity("checkpoint replay source is missing".into())
            })?;
            let replayed = usize::try_from(self.captured_next_lsn - prepared.source_next_lsn)
                .map_err(|_| {
                    HawDBError::Storage("checkpoint suffix entry count overflow".into())
                })?;
            let next = self.store.as_mut().ok_or_else(|| {
                HawDBError::Storage("checkpoint candidate has no private runtime".into())
            })?;
            if self.relational_replay_required {
                self.recovery_selectors = next.finish_private_wal_recovery(source, replayed)?;
            } else {
                // The validated WAL is still durable and complete. Its
                // non-relational commits advanced the pinned read views with
                // empty live publications, leaving the base roots unchanged.
                // No empty recovery manifest needs to be synchronized for
                // every captured prefix. A later relational operation switches
                // this same candidate to the ordinary recovery builder path.
                if replayed != 0 {
                    source
                        .prefix_identity()
                        .map_err(|reason| HawDBError::StorageIntegrity(reason.to_string()))?;
                }
                self.recovery_selectors = [None, None];
            }
            next.validate_authoritative_relational_index_open()?;
            next.open_relational_row_snapshot_reader()?;
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        } else {
            self.replay_finalized = true;
        }
        result
    }

    /// Retires eligible old generations after selector publication and after
    /// releasing the writer gate. Physical reader pins are freshly collected
    /// by the owner; logical conflict history remains with the live frontend.
    pub fn reclaim_published_generations(
        &self,
        selected: &mut GraphStore,
        pinned_reader_generations: &BTreeSet<u64>,
    ) -> Result<()> {
        let (durable, generation, previous) = self.reclamation_runtime(selected)?;
        durable.reclaim_old_generations(generation, previous, Some(pinned_reader_generations));
        Ok(())
    }

    /// Records postponed cleanup on the live runtime without scanning files.
    /// Existing measured debt is preserved until the next admitted scan.
    pub fn defer_published_generation_reclamation(&self, selected: &mut GraphStore) -> Result<()> {
        let (durable, _, _) = self.reclamation_runtime(selected)?;
        durable.defer_generation_reclamation();
        Ok(())
    }

    fn reclamation_runtime<'a>(
        &self,
        selected: &'a mut GraphStore,
    ) -> Result<(&'a mut DurableStore, u64, u64)> {
        let prepared = self.prepared.as_ref().ok_or_else(|| {
            HawDBError::Storage("checkpoint candidate has no publication identity".into())
        })?;
        let durable = selected.durable.as_mut().ok_or_else(|| {
            HawDBError::Storage("checkpoint reclamation requires a durable runtime".into())
        })?;
        if self.store.is_some()
            || durable.store_id() != self.source_store_id
            || durable.checkpoint_epoch != prepared.generation
        {
            return Err(HawDBError::Storage(
                "checkpoint reclamation must use the selected generation".into(),
            ));
        }
        Ok((
            durable,
            prepared.generation,
            prepared.source_checkpoint_epoch,
        ))
    }

    fn validate_source(&self, source: &GraphStore) -> Result<()> {
        if self.failed || self.prepared.is_none() || self.store.is_none() {
            return Err(HawDBError::Storage(
                "checkpoint candidate is no longer reusable".into(),
            ));
        }
        let prepared = self
            .prepared
            .as_ref()
            .expect("validated candidate owns its base");
        let durable = source
            .durable
            .as_ref()
            .ok_or_else(|| HawDBError::Storage("checkpoint suffix source is not durable".into()))?;
        if durable.read_only
            || durable.store_id() != self.source_store_id
            || durable.wal_sync_group_active()
            || durable.wal_path != self.source_wal_path
            || source.admitted_branch_head().copied() != self.source_head
            || durable.checkpoint_epoch != prepared.source_checkpoint_epoch
            || durable.wal_generation != prepared.source_wal_generation
            || durable.wal_bytes < self.captured_wal_bytes
            || durable.next_lsn < self.captured_next_lsn
            || source.commit_epoch < self.commit_epoch()
            || durable.next_lsn - self.captured_next_lsn
                != source.commit_epoch - self.commit_epoch()
        {
            return Err(HawDBError::Storage(
                "checkpoint suffix source identity changed".into(),
            ));
        }
        Ok(())
    }

    /// Replays only the newly captured complete interval. Earlier prefixes and
    /// checkpoint artifacts are never rebuilt by a successful catch-up pass.
    pub fn catch_up(&mut self, source: &GraphStore) -> Result<CheckpointWalTail> {
        self.catch_up_with_task_context(source, &RuntimeTaskContext::default())
    }

    /// The automatic owner passes its admitted task here. Cancellation is
    /// checked between complete WAL records and physical read/write waves use
    /// that task's existing I/O reservation. No second governor is acquired.
    pub fn catch_up_with_task_context(
        &mut self,
        source: &GraphStore,
        task: &RuntimeTaskContext,
    ) -> Result<CheckpointWalTail> {
        let mut work = crate::background::CheckpointWorkContext::new(task.clone());
        if let Some(scheduler) = &self.scheduler {
            work = work.with_scheduler(scheduler.clone());
        }
        // Existing callers retain their original operation diagnostics. The
        // automatic owner uses the classified entry below to choose retries.
        self.catch_up_checked(source, &work)
    }

    /// Preserve resource/cancellation denials separately from storage failures
    /// for an owner deciding whether to retain or retire a private candidate.
    #[doc(hidden)]
    pub fn catch_up_with_work_context(
        &mut self,
        source: &GraphStore,
        work: &crate::background::CheckpointWorkContext,
    ) -> std::result::Result<
        CheckpointWalTail,
        crate::background::CheckpointOperationError<HawDBError>,
    > {
        work.classify(|work| self.catch_up_checked(source, work))
    }

    fn catch_up_checked(
        &mut self,
        source: &GraphStore,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<CheckpointWalTail> {
        source.ensure_usable()?;
        replay_checkpoint(work)?;
        self.validate_source(source)?;
        let durable = source.durable.as_ref().expect("validated durable source");
        if durable.next_lsn == self.captured_next_lsn
            && !self.replay_interrupted
            && !self.wal_sync_pending
            && self.wal_write_attempt_end.is_none()
        {
            return Ok(CheckpointWalTail {
                captured_commit_epoch: source.commit_epoch,
                captured_next_lsn: self.captured_next_lsn,
                captured_wal_generation: durable.wal_generation,
                captured_wal_bytes: self.captured_wal_bytes,
                candidate_wal_bytes: self.candidate_wal_bytes,
                entries: self.captured_next_lsn
                    - self
                        .prepared
                        .as_ref()
                        .expect("validated base")
                        .source_next_lsn,
            });
        }
        self.replay_finalized = false;
        self.recovery_selectors = [None, None];
        self.replay_interrupted = true;
        let result = self.catch_up_inner(source, work);
        // Resource/cancellation/I/O errors before mutation retain the last
        // complete prefix. The apply path marks failures after mutation starts;
        // integrity faults are terminal even when no mutation has occurred.
        if matches!(result, Err(HawDBError::StorageIntegrity(_))) {
            self.failed = true;
        }
        if result.is_ok() {
            self.replay_interrupted = false;
        }
        result
    }

    fn catch_up_inner(
        &mut self,
        source: &GraphStore,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<CheckpointWalTail> {
        let original = source.durable.as_ref().expect("validated durable source");
        let prepared = self
            .prepared
            .as_ref()
            .expect("validated candidate owns its base");
        let store = self
            .store
            .as_mut()
            .expect("validated candidate owns its runtime");
        let catalog = self
            .catalog
            .as_mut()
            .expect("validated candidate owns its catalog");
        let durable = store.durable.as_ref().expect("candidate is durable");
        let path = durable.wal_path.clone();
        let open_wave = replay_io_wave(work)?;
        let mut output = fs::OpenOptions::new().append(true).open(&path)?;
        let actual_bytes = output.metadata()?.len();
        match self.wal_write_attempt_end {
            Some(attempted_end)
                if actual_bytes >= self.candidate_wal_bytes && actual_bytes <= attempted_end =>
            {
                if actual_bytes != self.candidate_wal_bytes {
                    replay_checkpoint(work)?;
                    self.wal_sync_pending = true;
                    output.set_len(self.candidate_wal_bytes)?;
                }
                self.wal_write_attempt_end = None;
            }
            None if actual_bytes == self.candidate_wal_bytes => {}
            _ => {
                return Err(HawDBError::StorageIntegrity(
                    "private checkpoint WAL length changed".into(),
                ));
            }
        }
        drop(open_wave);
        let mut bytes = self.candidate_wal_bytes;
        let mut expected_lsn = self.captured_next_lsn;
        if expected_lsn < original.next_lsn {
            let cursor = crate::wal::CheckpointWalRecordCursor::open_range(
                &original.wal_path,
                original.checkpoint_tail_record_limit(),
                original.wal_generation,
                original.wal_replay_start_lsn,
                self.captured_wal_bytes
                    .max(WAL_BINARY_FILE_HEADER_BYTES as u64),
                original.wal_bytes,
                work,
            );
            source.poison_on_storage_error(&cursor);
            let mut cursor = cursor?;
            loop {
                replay_checkpoint(work)?;
                let event = cursor.next();
                source.poison_on_storage_error(&event);
                let (
                    entry,
                    commit_epoch,
                    payload,
                    payload_len,
                    payload_sha256,
                    start_offset,
                    encoded_len,
                ) = match event? {
                    crate::wal::CheckpointWalCursorEvent::Entry {
                        entry,
                        commit_epoch,
                        payload,
                        payload_len,
                        payload_sha256,
                        start_offset,
                        encoded_len,
                    } => (
                        entry,
                        commit_epoch,
                        payload,
                        payload_len,
                        payload_sha256,
                        start_offset,
                        encoded_len,
                    ),
                    crate::wal::CheckpointWalCursorEvent::Eof => break,
                    crate::wal::CheckpointWalCursorEvent::Corrupt { reason, offset }
                    | crate::wal::CheckpointWalCursorEvent::TornTail {
                        reason,
                        valid_prefix_len: offset,
                    } => {
                        source
                            .integrity_poisoned
                            .store(true, AtomicOrdering::Release);
                        return Err(HawDBError::StorageIntegrity(format!(
                            "checkpoint suffix is incomplete at byte {offset}: {reason}"
                        )));
                    }
                };
                if entry.lsn != expected_lsn || expected_lsn >= original.next_lsn {
                    source
                        .integrity_poisoned
                        .store(true, AtomicOrdering::Release);
                    return Err(HawDBError::StorageIntegrity(
                        "checkpoint suffix LSN is not contiguous".into(),
                    ));
                }
                let source_record_end = start_offset.checked_add(encoded_len).ok_or_else(|| {
                    HawDBError::StorageIntegrity("checkpoint source WAL offset overflow".into())
                })?;
                let from = self
                    .captured_wal_bytes
                    .max(WAL_BINARY_FILE_HEADER_BYTES as u64);
                let trailer = crate::wal::frame::WAL_BLOCK_BYTES as u64
                    - (from - WAL_BINARY_FILE_HEADER_BYTES as u64)
                        % crate::wal::frame::WAL_BLOCK_BYTES as u64;
                let expected_start =
                    if trailer < crate::wal::frame::WAL_FRAGMENT_HEADER_BYTES as u64 {
                        from.saturating_add(trailer)
                    } else {
                        from
                    };
                if start_offset != expected_start
                    || encoded_len == 0
                    || source_record_end > original.wal_bytes
                {
                    source
                        .integrity_poisoned
                        .store(true, AtomicOrdering::Release);
                    return Err(HawDBError::StorageIntegrity(
                        "checkpoint suffix byte interval is not contiguous".into(),
                    ));
                }
                let next_lsn = expected_lsn.checked_add(1).ok_or_else(|| {
                    HawDBError::StorageIntegrity("checkpoint suffix LSN overflow".into())
                })?;
                // An observed corrupt source prefix remains an integrity
                // failure even when cancellation arrives during this read.
                let epoch = store.commit_epoch.checked_add(1).ok_or_else(|| {
                    HawDBError::StorageIntegrity("checkpoint suffix epoch overflow".into())
                })?;
                if commit_epoch != epoch {
                    source
                        .integrity_poisoned
                        .store(true, AtomicOrdering::Release);
                    return Err(HawDBError::StorageIntegrity(
                        "checkpoint suffix commit epoch is not contiguous".into(),
                    ));
                }
                replay_checkpoint(work)?;
                let relational_replay_required = self.relational_replay_required
                    || wal_op_changes_relational_state(&entry.op, work)?;
                let unchanged_views = if relational_replay_required {
                    None
                } else {
                    let rows = store.stage_relational_row_live_publication(epoch, None);
                    store.require_relational_row_live_publication(epoch, &rows)?;
                    let indexes = store.stage_relational_index_live_publication(epoch, None);
                    if let Some(Err(unavailable)) = &indexes {
                        return Err(HawDBError::StorageIntegrity(format!(
                            "unchanged checkpoint index prefix cannot advance: {}",
                            unavailable.reason
                        )));
                    }
                    Some((rows, indexes))
                };
                // C-S = N-L plus each recorded epoch/LSN proves the envelope
                // already has the candidate's exact history. Preserve its raw
                // bytes; only generation-bound physical framing changes.
                let mut framed = crate::wal::frame::CheckpointWalFrameStream::new(
                    prepared.generation,
                    &payload,
                    bytes - WAL_BINARY_FILE_HEADER_BYTES as u64,
                    work,
                )
                .map_err(HawDBError::from_storage_error)?;
                let record_end =
                    bytes
                        .checked_add(framed.encoded_len() as u64)
                        .ok_or_else(|| {
                            HawDBError::Storage("checkpoint suffix bytes overflow".into())
                        })?;
                if original
                    .max_wal_bytes
                    .is_some_and(|limit| record_end > limit)
                {
                    return Err(HawDBError::Storage(
                        "checkpoint suffix exceeds its WAL budget".into(),
                    ));
                }
                while let Some(fragment) = framed.next().map_err(HawDBError::from_storage_error)? {
                    replay_checkpoint(work)?;
                    let write_wave = replay_io_wave(work)?;
                    let end = bytes
                        .checked_add(fragment.encoded_len() as u64)
                        .ok_or_else(|| {
                            HawDBError::Storage("checkpoint fragment bytes overflow".into())
                        })?;
                    self.wal_write_attempt_end = Some(end);
                    self.wal_sync_pending = true;
                    fragment.write_to(&mut output)?;
                    bytes = end;
                    drop(write_wave);
                    // Preserve replay cancellation diagnostics after actual I/O,
                    // before the stream checks for another physical fragment.
                    replay_checkpoint(work)?;
                }
                replay_checkpoint(work)?;
                drop(payload);
                // The rolling digest and all three replay counters move only
                // with a whole schema/data transaction. No database-sized
                // rollback snapshot is taken for a partially applied record.
                entry.replay_into_with_boundary(store, catalog, work, &mut self.failed)?;
                if let Some((rows, indexes)) = unchanged_views {
                    store.publish_relational_row_live_view(rows);
                    store.publish_relational_index_live_view(indexes);
                }
                self.recovery_source
                    .as_mut()
                    .expect("candidate recovery is not finalized")
                    .record(expected_lsn, payload_len, payload_sha256)
                    .map_err(|reason| HawDBError::StorageIntegrity(reason.into()))?;
                expected_lsn = next_lsn;
                let durable = store.durable.as_mut().expect("candidate is durable");
                durable.next_lsn = next_lsn;
                durable.wal_commit_epoch = store.commit_epoch;
                durable.wal_bytes = record_end;
                self.captured_wal_bytes = source_record_end;
                self.captured_next_lsn = next_lsn;
                self.candidate_wal_bytes = record_end;
                self.relational_replay_required = relational_replay_required;
                self.wal_write_attempt_end = None;
                self.failed = false;
            }
        }
        if expected_lsn != original.next_lsn || store.commit_epoch != source.commit_epoch {
            source
                .integrity_poisoned
                .store(true, AtomicOrdering::Release);
            return Err(HawDBError::StorageIntegrity(
                "checkpoint suffix lost a captured commit".into(),
            ));
        }
        let sync_wave = replay_io_wave(work)?;
        if output.metadata()?.len() != bytes {
            return Err(HawDBError::StorageIntegrity(
                "checkpoint WAL writes did not retain every reframed record".into(),
            ));
        }
        output.sync_all()?;
        self.wal_sync_pending = false;
        drop(sync_wave);
        replay_checkpoint(work)?;
        let durable = store.durable.as_mut().expect("candidate is durable");
        durable.next_lsn = expected_lsn;
        durable.wal_commit_epoch = store.commit_epoch;
        durable.wal_bytes = bytes;
        self.captured_wal_bytes = original.wal_bytes;
        self.captured_next_lsn = expected_lsn;
        self.candidate_wal_bytes = bytes;
        Ok(CheckpointWalTail {
            captured_commit_epoch: source.commit_epoch,
            captured_next_lsn: expected_lsn,
            captured_wal_generation: original.wal_generation,
            captured_wal_bytes: original.wal_bytes,
            candidate_wal_bytes: bytes,
            entries: expected_lsn - prepared.source_next_lsn,
        })
    }
}

fn wal_op_changes_relational_state(
    operation: &WalOp,
    work: &crate::background::CheckpointWorkContext,
) -> Result<bool> {
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    let changes = match operation {
        WalOp::Relational { .. } | WalOp::RelationalSnapshot { .. } => true,
        WalOp::Batch(operations) => {
            unit.finish();
            for operation in operations {
                if wal_op_changes_relational_state(operation, work)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        WalOp::CreateNodeLabel { .. }
        | WalOp::CreateRelationshipType { .. }
        | WalOp::CreateNodeTable { .. }
        | WalOp::CreateRelationshipTable { .. }
        | WalOp::CreateProperty { .. }
        | WalOp::AlterTableState { .. }
        | WalOp::AlterPropertyState { .. }
        | WalOp::GcTableDescriptor { .. }
        | WalOp::GcPropertyDescriptor { .. }
        | WalOp::CreateIndex { .. }
        | WalOp::CreateCompositeIndex { .. }
        | WalOp::CreateRangeIndex { .. }
        | WalOp::CreateFullTextIndex { .. }
        | WalOp::CreateUniqueConstraint { .. }
        | WalOp::CreateNodePropertyExistsConstraint { .. }
        | WalOp::CreateRelationshipUniqueConstraint { .. }
        | WalOp::CreateRelationshipPropertyExistsConstraint { .. }
        | WalOp::CreateNode { .. }
        | WalOp::CreateRelationship { .. }
        | WalOp::SetNodeProperty { .. }
        | WalOp::SetRelationshipProperty { .. }
        | WalOp::DeleteNode { .. }
        | WalOp::DeleteRelationship { .. }
        | WalOp::ProjectGraph { .. }
        | WalOp::MarkInitialImportSource { .. }
        | WalOp::Append { .. } => false,
    };
    unit.finish();
    Ok(changes)
}

fn replay_checkpoint(work: &crate::background::CheckpointWorkContext) -> Result<()> {
    work.checkpoint().map_err(|error| match error {
        crate::background::CheckpointWorkError::Stopped(reason) => {
            HawDBError::Execution(format!("checkpoint WAL replay stopped: {reason}"))
        }
        error => HawDBError::from_storage_error(error),
    })
}

fn replay_io_wave(
    work: &crate::background::CheckpointWorkContext,
) -> Result<Option<Box<dyn hawdb_core::RuntimeIoWavePermit>>> {
    work.io_wave().map_err(|error| match error {
        crate::background::CheckpointWorkError::Io(reason) => {
            HawDBError::Execution(format!("checkpoint WAL replay I/O stopped: {reason}"))
        }
        error => HawDBError::from_storage_error(error),
    })
}

impl Drop for CheckpointCandidate {
    fn drop(&mut self) {
        if self.retain_artifacts {
            return;
        }
        if let (Some(prepared), Some(durable)) = (
            &self.prepared,
            self.store.as_ref().and_then(|store| store.durable.as_ref()),
        ) {
            if self.owns_branch_wal {
                let _ = fs::remove_file(&durable.wal_path);
            }
            let _ =
                durable.discard_prepared_checkpoint(prepared.generation, &prepared.staging_path);
        }
    }
}
