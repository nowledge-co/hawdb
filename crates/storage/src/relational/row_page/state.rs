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
    RelationalOverflowRootReader, RelationalRowChangeCapture, RelationalRowChangeCaptureLimits,
    RelationalRowPageLiveError, RelationalRowPageReadView, RelationalRowPageReadViewIdentity,
    RelationalRowStorageResidencyReport, RelationalState, StoreId,
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
        report.live_page_bytes = base.root_page_count * base.page_bytes;
        report.allocated_page_bytes = report.allocated_page_count * base.page_bytes;
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

    pub fn stage_live_publication(
        &self,
        current_epoch: u64,
        next_epoch: u64,
        capture: Option<RelationalRowChangeCapture>,
    ) -> Option<Result<Arc<RelationalRowPageReadView>, RelationalRowLiveUnavailable>> {
        let view = self.current_read_view(current_epoch)?;
        if let Some(RelationalRowChangeCapture::RequiresCheckpoint { tables }) = capture.as_ref() {
            return Some(Err(RelationalRowLiveUnavailable {
                identity: view.identity(),
                failed_commit_epoch: next_epoch,
                error: RelationalRowPageLiveError::RequiresCheckpoint {
                    tables: tables.clone(),
                },
            }));
        }
        Some(
            view.advance(next_epoch, capture, self.live_limits)
                .map(Arc::new)
                .map_err(|error| RelationalRowLiveUnavailable {
                    identity: view.identity(),
                    failed_commit_epoch: next_epoch,
                    error,
                }),
        )
    }
}

#[doc(hidden)]
pub struct RelationalRowLiveUnavailable {
    pub identity: RelationalRowPageReadViewIdentity,
    pub failed_commit_epoch: u64,
    pub error: RelationalRowPageLiveError,
}
