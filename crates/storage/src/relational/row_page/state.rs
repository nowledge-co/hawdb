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

//! Shadow recovery state for canonical relational row-page roots.

use super::delta::{RelationalRowDeltaBuilder, RelationalRowDeltaConfig, RelationalRowDeltaReport};
use super::recovery::RelationalRowPageRecoveryStatus;
use crate::cache::SegmentCache;
use crate::relational_row_workspace::RelationalMonotonicAppendMetrics;
use crate::{
    cache::StoreId,
    relational::{
        RelationalOverflowRootReader, RelationalRowChangeCapture, RelationalRowChangeCaptureLimits,
        RelationalRowPageLiveError, RelationalRowPageReadView, RelationalRowPageReadViewIdentity,
        RelationalState,
    },
    residency::RelationalRowStorageResidencyReport,
};
use std::sync::Arc;

#[doc(hidden)]
#[derive(Debug, Default)]
pub struct RelationalRowPageState {
    pub recovery_builder: Option<RelationalRowDeltaBuilder>,
    pub read_view: Option<Arc<RelationalRowPageReadView>>,
    pub serving_resources: Option<Arc<RelationalRowPageServingResources>>,
    pub live_limits: RelationalRowChangeCaptureLimits,
    pub delta_config: RelationalRowDeltaConfig,
    pub recovery_report: Option<RelationalRowDeltaReport>,
    pub recovery_status: RelationalRowPageRecoveryStatus,
    pub schema_checkpoint_required: bool,
    /// The last resource or budget failure of the required schema
    /// checkpoint. While set, commits do not retry the checkpoint; an
    /// explicit, automatic or recovery checkpoint does (#1593).
    pub schema_checkpoint_deferred: Option<String>,
    /// Incremental planning state of the pending schema checkpoint, valid
    /// while its view is at the current commit epoch.
    pub schema_rewrite: Option<RelationalRowSchemaRewrite>,
    /// Set while every schema change of the pending schema checkpoint
    /// rewrote only tables that were empty in its commit, and those tables
    /// are still empty. Commits then leave
    /// the checkpoint to the next ordinary one and advance `schema_rewrite`
    /// instead, so it stays incremental; the WAL stays authoritative until it
    /// publishes (#1942).
    pub schema_checkpoint_can_wait: bool,
    /// Set when a read-only handle recovered commits after the checkpoint
    /// that no writer has published as a row delta. Building that delta
    /// would write under the database root, so relational row reads refuse
    /// with a typed read-only error instead (#2029).
    pub read_only_unpublished_row_delta: bool,
    /// The checkpoint read view advanced by each replayed commit during
    /// read-only recovery, held until recovery finishes (#2029).
    pub read_only_replay_view: Option<Arc<RelationalRowPageReadView>>,
    pub monotonic_append_fast_path_enabled: bool,
    pub monotonic_append_metrics: Arc<RelationalMonotonicAppendMetrics>,
}

#[doc(hidden)]
#[derive(Debug)]
pub struct RelationalRowPageServingResources {
    pub base_overflow: Arc<RelationalOverflowRootReader>,
    pub cache: Arc<SegmentCache>,
    pub store_id: StoreId,
}

impl RelationalRowPageState {
    pub fn current_read_view(&self, commit_epoch: u64) -> Option<&Arc<RelationalRowPageReadView>> {
        self.read_view
            .as_ref()
            .filter(|view| view.identity().visible_commit_epoch == commit_epoch)
    }

    pub fn residency_report(
        &self,
        commit_epoch: u64,
        state: &RelationalState,
    ) -> RelationalRowStorageResidencyReport {
        let mut report = RelationalRowStorageResidencyReport {
            materialized_rows_resident: state.materialized_rows_resident(),
            checkpoint_state_metadata_only: state.canonical_row_metadata_only(),
            materialized_row_count: state.materialized_row_count(),
            materialized_row_bytes: state.estimated_materialized_row_bytes(),
            logical_row_count: state.total_row_count(),
            recovery_delta_checkpoint_runs: self.delta_config.checkpoint_runs.get(),
            monotonic_append_attempts: self.monotonic_append_metrics.attempts(),
            monotonic_append_hits: self.monotonic_append_metrics.hits(),
            monotonic_append_fallbacks: self.monotonic_append_metrics.fallbacks(),
            monotonic_append_proven_absent_primary_keys: self
                .monotonic_append_metrics
                .proven_absent_primary_keys(),
            schema_checkpoint_pending: self.schema_checkpoint_required,
            schema_checkpoint_pending_reason: self
                .schema_checkpoint_deferred
                .clone()
                .filter(|_| self.schema_checkpoint_required),
            ..RelationalRowStorageResidencyReport::default()
        };
        let Some(view) = self.current_read_view(commit_epoch) else {
            return report;
        };
        let Some(resources) = self.serving_resources.as_ref() else {
            return report;
        };
        let identity = view.identity();
        let base = view.base().manifest();
        let overflow = resources.base_overflow.manifest();
        let recovery = view.recovery_delta().map(|delta| delta.manifest());
        report.serving = true;
        report.base_generation = Some(identity.base_generation);
        report.recovery_delta_generation = identity.delta_generation;
        report.base_commit_epoch = Some(identity.base_commit_epoch);
        report.visible_commit_epoch = Some(identity.visible_commit_epoch);
        report.root_page_count = base.root_page_count;
        report.physical_generation_count = base.physical_generations.len();
        report.allocated_page_count = base
            .physical_generations
            .iter()
            .map(|entry| entry.allocated_pages)
            .sum();
        report.live_page_bytes = base
            .physical_generations
            .iter()
            .map(|entry| entry.live_bytes)
            .sum();
        report.allocated_page_bytes = base
            .physical_generations
            .iter()
            .map(|entry| entry.allocated_bytes)
            .sum();
        report.page_artifact_bytes = base.page_artifact.encoded_len;
        report.root_descriptor_artifact_bytes = base.root_descriptor_artifact.encoded_len;
        report.root_key_artifact_bytes = base.root_key_artifact.encoded_len;
        report.overflow_extent_count = overflow.extent_count;
        report.overflow_extent_artifact_bytes = overflow.extent_artifact.encoded_len;
        report.overflow_descriptor_artifact_bytes = overflow.descriptor_artifact.encoded_len;
        report.recovery_delta_runs = recovery.map_or(0, |manifest| manifest.run_count());
        report.recovery_delta_checkpoint_recommended = recovery.is_some_and(|manifest| {
            self.delta_config
                .checkpoint_recommended(manifest.run_count())
        });
        report.recovery_delta_entries = recovery.map_or(0, |manifest| manifest.total_entries());
        report.recovery_delta_artifact_bytes =
            recovery.map_or(0, |manifest| manifest.artifact_bytes());
        report.live_batches = view.live_batch_count();
        report.live_entries = view.live_entry_count();
        report.live_encoded_bytes = view.live_encoded_bytes();
        report.live_resident_bytes = view.live_resident_bytes();
        report
    }

    pub fn snapshot_at_epoch(&self, commit_epoch: u64) -> Self {
        Self {
            recovery_builder: None,
            read_view: self
                .read_view
                .as_ref()
                .filter(|view| view.identity().visible_commit_epoch == commit_epoch)
                .cloned(),
            serving_resources: self.serving_resources.clone(),
            live_limits: self.live_limits,
            delta_config: self.delta_config,
            recovery_report: self.recovery_report.clone(),
            recovery_status: self.recovery_status.clone(),
            schema_checkpoint_required: self.schema_checkpoint_required,
            schema_checkpoint_deferred: self.schema_checkpoint_deferred.clone(),
            schema_rewrite: self
                .schema_rewrite
                .clone()
                .filter(|rewrite| rewrite.view.identity().visible_commit_epoch == commit_epoch),
            schema_checkpoint_can_wait: self.schema_checkpoint_can_wait,
            read_only_unpublished_row_delta: self.read_only_unpublished_row_delta,
            read_only_replay_view: None,
            monotonic_append_fast_path_enabled: self.monotonic_append_fast_path_enabled,
            monotonic_append_metrics: Arc::clone(&self.monotonic_append_metrics),
        }
    }

    pub fn base_identity(&self) -> (Option<u64>, Option<u64>) {
        match &self.recovery_status {
            RelationalRowPageRecoveryStatus::CheckpointReady {
                generation,
                source_commit_epoch,
                ..
            } => (Some(*generation), Some(*source_commit_epoch)),
            RelationalRowPageRecoveryStatus::WalRecovered {
                base_generation,
                base_commit_epoch,
                ..
            } => (Some(*base_generation), Some(*base_commit_epoch)),
            RelationalRowPageRecoveryStatus::LiveCurrent {
                base_generation,
                base_commit_epoch,
                ..
            } => (Some(*base_generation), Some(*base_commit_epoch)),
            RelationalRowPageRecoveryStatus::LiveUnavailable {
                base_generation,
                base_commit_epoch,
                ..
            } => (Some(*base_generation), Some(*base_commit_epoch)),
            RelationalRowPageRecoveryStatus::Stale {
                generation,
                source_commit_epoch,
                ..
            } => (Some(*generation), Some(*source_commit_epoch)),
            RelationalRowPageRecoveryStatus::Unavailable {
                base_generation,
                base_commit_epoch,
                ..
            } => (*base_generation, *base_commit_epoch),
            RelationalRowPageRecoveryStatus::Missing => (None, None),
        }
    }

    /// The pending schema rewrite that commits advance instead of
    /// checkpointing (#1942): present only while it can wait and its view is
    /// at `commit_epoch`.
    pub fn current_waiting_schema_rewrite(
        &self,
        commit_epoch: u64,
    ) -> Option<&RelationalRowSchemaRewrite> {
        self.schema_rewrite.as_ref().filter(|rewrite| {
            self.schema_checkpoint_required
                && self.schema_checkpoint_can_wait
                && rewrite.view.identity().visible_commit_epoch == commit_epoch
        })
    }

    pub fn stage_live_publication(
        &self,
        current_epoch: u64,
        next_epoch: u64,
        capture: Option<RelationalRowChangeCapture>,
    ) -> Option<Result<Arc<RelationalRowPageReadView>, RelationalRowLiveUnavailable>> {
        let Some(view) = self.current_read_view(current_epoch) else {
            return self.stage_waiting_schema_rewrite(current_epoch, next_epoch, capture);
        };
        if let Some(RelationalRowChangeCapture::RequiresCheckpoint { tables, unaffected }) =
            capture.as_ref()
        {
            // Advance the view by the commit's changes outside the rewritten
            // tables. The schema checkpoint plans those tables incrementally
            // from it; it never serves reads (#1593).
            let schema_rewrite_view = unaffected.as_ref().and_then(|unaffected| {
                view.advance(
                    next_epoch,
                    Some(RelationalRowChangeCapture::Captured {
                        changes: unaffected.changes.clone(),
                        encoded_bytes: unaffected.encoded_bytes,
                    }),
                    self.live_limits,
                )
                .ok()
                .map(Arc::new)
            });
            return Some(Err(RelationalRowLiveUnavailable {
                identity: view.identity(),
                failed_commit_epoch: next_epoch,
                error: RelationalRowPageLiveError::RequiresCheckpoint {
                    tables: tables.clone(),
                },
                schema_rewrite: schema_rewrite_view.map(|view| RelationalRowSchemaRewrite {
                    view,
                    tables: tables.iter().cloned().collect(),
                }),
            }));
        }
        Some(
            view.advance(next_epoch, capture, self.live_limits)
                .map(Arc::new)
                .map_err(|error| RelationalRowLiveUnavailable {
                    identity: view.identity(),
                    failed_commit_epoch: next_epoch,
                    error,
                    schema_rewrite: None,
                }),
        )
    }

    /// Advances a waiting schema rewrite by one commit, exactly as the live
    /// view it replaces would advance: the same capture, limits and errors.
    /// A further schema change adds its tables; a row change to a pending
    /// table makes the commit require the checkpoint. A commit that does not
    /// fit is rejected before WAL append, as with a live view (#1942).
    fn stage_waiting_schema_rewrite(
        &self,
        current_epoch: u64,
        next_epoch: u64,
        capture: Option<RelationalRowChangeCapture>,
    ) -> Option<Result<Arc<RelationalRowPageReadView>, RelationalRowLiveUnavailable>> {
        let rewrite = self.current_waiting_schema_rewrite(current_epoch)?;
        let identity = rewrite.view.identity();
        let mut tables = rewrite.tables.clone();
        // The error names the tables this commit's schema change rewrites,
        // as for a live view; the rewrite carries every pending table.
        let mut commit_tables = Vec::new();
        let unavailable = |commit_tables: Vec<String>,
                           error: Option<RelationalRowPageLiveError>,
                           rewrite: Option<RelationalRowSchemaRewrite>| {
            Some(Err(RelationalRowLiveUnavailable {
                identity,
                failed_commit_epoch: next_epoch,
                error: error.unwrap_or(RelationalRowPageLiveError::RequiresCheckpoint {
                    tables: commit_tables,
                }),
                schema_rewrite: rewrite,
            }))
        };
        let changes = match capture {
            None => Vec::new(),
            Some(RelationalRowChangeCapture::Captured { changes, .. }) => changes,
            Some(RelationalRowChangeCapture::RequiresCheckpoint {
                tables: changed,
                unaffected,
            }) => {
                tables.extend(changed.iter().cloned());
                commit_tables = changed;
                match unaffected {
                    Some(unaffected) => unaffected.changes,
                    // As for a live view's schema commit, changes that do
                    // not fit leave a full rebuild to the checkpoint.
                    None => return unavailable(commit_tables, None, None),
                }
            }
            Some(RelationalRowChangeCapture::Invalidated { reason }) => {
                return unavailable(
                    commit_tables,
                    Some(RelationalRowPageLiveError::Invalidated(reason)),
                    None,
                );
            }
        };
        // A row change to a pending table ends the wait: the overlay cannot
        // hold it, so it would be checkpoint debt no limit bounds. The
        // table joins the commit's tables, which publication requires to be
        // empty for the checkpoint to keep waiting.
        let (pending, changes): (Vec<_>, Vec<_>) = changes
            .into_iter()
            .partition(|change| tables.contains(&change.table));
        for change in pending {
            if !commit_tables.contains(&change.table) {
                commit_tables.push(change.table);
            }
        }
        let Some(encoded_bytes) = changes.iter().try_fold(0usize, |total, change| {
            total.checked_add(crate::relational::estimated_row_change_encoding_bytes(
                change,
            )?)
        }) else {
            return unavailable(
                commit_tables,
                Some(RelationalRowPageLiveError::Admission(
                    "schema rewrite row change byte accounting overflow".to_string(),
                )),
                None,
            );
        };
        let schema_change = !commit_tables.is_empty();
        match rewrite.view.advance(
            next_epoch,
            Some(RelationalRowChangeCapture::Captured {
                changes,
                encoded_bytes,
            }),
            self.live_limits,
        ) {
            Ok(view) => unavailable(
                commit_tables,
                None,
                Some(RelationalRowSchemaRewrite {
                    view: Arc::new(view),
                    tables,
                }),
            ),
            Err(_) if schema_change => unavailable(commit_tables, None, None),
            Err(error) => unavailable(commit_tables, Some(error), None),
        }
    }
}

#[doc(hidden)]
pub struct RelationalRowLiveUnavailable {
    pub identity: RelationalRowPageReadViewIdentity,
    pub failed_commit_epoch: u64,
    pub error: RelationalRowPageLiveError,
    /// For a schema change, or a commit after one that can wait: the pending
    /// rewrite advanced by the commit's changes to the tables it does not
    /// rewrite, when they fit the live limits.
    pub schema_rewrite: Option<RelationalRowSchemaRewrite>,
}

/// A pending schema checkpoint whose unaffected tables can be planned
/// incrementally (#1593).
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct RelationalRowSchemaRewrite {
    /// The base view advanced by every commit up to the schema change,
    /// excluding the rewritten tables' changes in that commit.
    pub view: Arc<RelationalRowPageReadView>,
    /// Tables whose schema or row encoding changed; they are rewritten whole.
    pub tables: std::collections::BTreeSet<String>,
}
