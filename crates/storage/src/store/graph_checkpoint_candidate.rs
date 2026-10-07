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

/// One unpublished checkpoint and its complete private replay state. A caller
/// must serialize candidates and manual checkpoint/seal work for this writer.
/// Catch-up runs on captured sources outside the writer critical section.
#[doc(hidden)]
pub struct CheckpointCandidate {
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
    branch_root: Option<PreparedCheckpointBranchRoot>,
    owns_branch_wal: bool,
    recovery_source: Option<RelationalRecoverySourceBuilder>,
    recovery_selectors: [Option<crate::relational::PreparedRelationalRecoverySelector>; 2],
    replay_finalized: bool,
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
        self.estimated_logical_record_bytes()
            .checked_add(self.relational_state.estimated_materialized_row_bytes())
            .and_then(|bytes| bytes.checked_add(self.relational_state.estimated_checkpoint_bytes()))
            .and_then(|bytes| bytes.checked_mul(16))
            .and_then(|bytes| bytes.checked_add(128 * 1024 * 1024))
            .ok_or_else(|| {
                HawDBError::Storage("checkpoint candidate admission size overflow".into())
            })
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
        self.ensure_usable()?;
        let Some(prepared) = self.prepare_checkpoint_with_work_context(catalog, work)? else {
            return Ok(None);
        };
        let durable = self
            .durable
            .as_ref()
            .expect("prepared checkpoint is durable");
        let mut candidate = CheckpointCandidate {
            source_wal_path: durable.wal_path.clone(),
            source_head: self.admitted_branch_head().copied(),
            source_store_id: durable.store_id(),
            source_capture_started: self
                .checkpoint_capture_started
                .unwrap_or_else(std::time::Instant::now),
            captured_wal_bytes: prepared.source_wal_bytes,
            captured_next_lsn: prepared.source_next_lsn,
            candidate_wal_bytes: WAL_BINARY_FILE_HEADER_BYTES as u64,
            recovery_source: Some(RelationalRecoverySourceBuilder::new(
                prepared.generation,
                prepared.source_next_lsn,
            )),
            replay_finalized: false,
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
        };
        let store = candidate.store.as_mut().expect("candidate owns a runtime");
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        // Private replay errors must not poison the still-authoritative writer.
        // The original shared flags are restored on successful selection.
        store.post_wal_apply_poisoned = Arc::new(AtomicBool::new(false));
        store.integrity_poisoned = Arc::new(AtomicBool::new(false));
        let prepared = candidate
            .prepared
            .as_mut()
            .expect("candidate owns its base");
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
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        candidate.branch_root = store.prepare_checkpoint_branch_root(manifest)?;
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
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
            let header = encode_binary_wal_header(prepared.generation, prepared.source_next_lsn);
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            // Ownership begins only after exclusive creation. Never remove an
            // earlier interrupted candidate with the same generation spelling.
            candidate.owns_branch_wal = true;
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
        Ok(Some(candidate))
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
    pub fn commit_epoch(&self) -> u64 {
        self.store.as_ref().map_or(0, GraphStore::commit_epoch)
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
            self.recovery_selectors = next.finish_private_wal_recovery(source, replayed)?;
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
        durable.reclaim_old_generations(
            prepared.generation,
            prepared.source_checkpoint_epoch,
            Some(pinned_reader_generations),
        );
        Ok(())
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
        source.ensure_usable()?;
        replay_checkpoint(task)?;
        self.validate_source(source)?;
        let durable = source.durable.as_ref().expect("validated durable source");
        if durable.next_lsn == self.captured_next_lsn {
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
        let result = self.catch_up_inner(source, task);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn catch_up_inner(
        &mut self,
        source: &GraphStore,
        task: &RuntimeTaskContext,
    ) -> Result<CheckpointWalTail> {
        let work = crate::background::CheckpointWorkContext::new(task.clone());
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
        let open_wave = replay_io_wave(task)?;
        let mut output = fs::OpenOptions::new().append(true).open(&path)?;
        if output.metadata()?.len() != self.candidate_wal_bytes {
            return Err(HawDBError::StorageIntegrity(
                "private checkpoint WAL length changed".into(),
            ));
        }
        drop(open_wave);
        let mut bytes = self.candidate_wal_bytes;
        let mut expected_lsn = self.captured_next_lsn;
        if expected_lsn < original.next_lsn {
            let open_wave = replay_io_wave(task)?;
            let cursor = WalRecordCursor::open_range(
                &original.wal_path,
                original.checkpoint_tail_record_limit(),
                original.wal_generation,
                original.wal_replay_start_lsn,
                self.captured_wal_bytes
                    .max(WAL_BINARY_FILE_HEADER_BYTES as u64),
                original.wal_bytes,
            );
            source.poison_on_storage_error(&cursor);
            let mut cursor = cursor?;
            drop(open_wave);
            loop {
                replay_checkpoint(task)?;
                let read_wave = replay_io_wave(task)?;
                let event = cursor.next();
                drop(read_wave);
                source.poison_on_storage_error(&event);
                let entry = match event? {
                    WalCursorEvent::Entry { entry, .. } => entry,
                    WalCursorEvent::Eof => break,
                    WalCursorEvent::Corrupt { reason, .. }
                    | WalCursorEvent::TornTail { reason, .. } => {
                        source
                            .integrity_poisoned
                            .store(true, AtomicOrdering::Release);
                        return Err(HawDBError::StorageIntegrity(format!(
                            "checkpoint suffix is incomplete: {reason}"
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
                // An observed corrupt source prefix remains an integrity
                // failure even when cancellation arrives during this read.
                replay_checkpoint(task)?;
                let epoch = store.commit_epoch.checked_add(1).ok_or_else(|| {
                    HawDBError::StorageIntegrity("checkpoint suffix epoch overflow".into())
                })?;
                let payload = encode_binary_wal_record(&entry, epoch)?;
                let framed = crate::wal::frame::frame_binary_wal_record_with_work_context(
                    prepared.generation,
                    &payload,
                    bytes - WAL_BINARY_FILE_HEADER_BYTES as u64,
                    &work,
                )
                .map_err(HawDBError::from_storage_error)?;
                bytes = bytes.checked_add(framed.len() as u64).ok_or_else(|| {
                    HawDBError::Storage("checkpoint suffix bytes overflow".into())
                })?;
                if original.max_wal_bytes.is_some_and(|limit| bytes > limit) {
                    return Err(HawDBError::Storage(
                        "checkpoint suffix exceeds its WAL budget".into(),
                    ));
                }
                for block in framed.chunks(64 * 1024) {
                    replay_checkpoint(task)?;
                    let write_wave = replay_io_wave(task)?;
                    output.write_all(block)?;
                    drop(write_wave);
                }
                drop(framed);
                replay_checkpoint(task)?;
                let digest = hawdb_integrity::integrity_digest(&payload);
                self.recovery_source
                    .as_mut()
                    .expect("candidate recovery is not finalized")
                    .record(entry.lsn, payload.len() as u64, digest.sha256)
                    .map_err(|reason| HawDBError::StorageIntegrity(reason.into()))?;
                store.apply_replayed_wal_transaction(catalog, entry.op)?;
                expected_lsn = expected_lsn.checked_add(1).ok_or_else(|| {
                    HawDBError::StorageIntegrity("checkpoint suffix LSN overflow".into())
                })?;
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
        let sync_wave = replay_io_wave(task)?;
        if output.metadata()?.len() != bytes {
            return Err(HawDBError::StorageIntegrity(
                "checkpoint WAL writes did not retain every reframed record".into(),
            ));
        }
        output.sync_all()?;
        drop(sync_wave);
        replay_checkpoint(task)?;
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

fn replay_checkpoint(task: &RuntimeTaskContext) -> Result<()> {
    task.checkpoint()
        .map_err(|reason| HawDBError::Execution(format!("checkpoint WAL replay stopped: {reason}")))
}

fn replay_io_wave(
    task: &RuntimeTaskContext,
) -> Result<Option<Box<dyn hawdb_core::RuntimeIoWavePermit>>> {
    task.acquire_io_wave(NonZeroUsize::MIN).map_err(|reason| {
        HawDBError::Execution(format!("checkpoint WAL replay I/O stopped: {reason}"))
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
