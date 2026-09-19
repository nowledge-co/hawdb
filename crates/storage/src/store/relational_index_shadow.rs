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

//! Relational index-page publication and generation-pinned read views.
//!
//! `Shadow` and `DemandPaged` keep the files derived, so canonical checkpoint
//! success does not depend on them. The explicit `Authoritative` mode promotes
//! the complete generation binding and its recovery/live view into a required
//! open, constraint, and SQL dependency. Every mode uses the same immutable,
//! generation/epoch-fenced reader.

#[path = "relational_index_shadow/authoritative.rs"]
mod authoritative;
#[path = "relational_index_shadow/constraint_qualification.rs"]
mod constraint_qualification;
#[path = "relational_index_shadow/transaction.rs"]
mod transaction;

pub use constraint_qualification::{
    RelationalConstraintQualificationProbeReport, RelationalConstraintQualificationReport,
    RelationalConstraintQualificationUse, RELATIONAL_CONSTRAINT_QUALIFICATION_PROTOCOL,
};
pub use hawdb_storage::relational::{
    RelationalIndexShadowCheckpointReport, RelationalIndexShadowCheckpointStatus,
    RelationalIndexShadowRecoveryStatus,
};
use hawdb_storage::relational_index_view::{
    map_index_row_snapshot_error, CanonicalRelationalIndexRowSource,
};
#[doc(hidden)]
pub use hawdb_storage::relational_index_view::{
    RelationalIndexProbeStatistics, RelationalIndexReadView, RelationalTransactionIndexView,
};
pub use hawdb_storage::relational_index_view::{
    RelationalIndexReadViewBackendReport, RelationalIndexReadViewReport,
};

use super::{GraphStore, HawDBError};
use hawdb_integrity::IntegrityHasher;
#[cfg(test)]
use hawdb_storage::{relational_index_shadow_artifact_file, RelationalIndexMode};
use hawdb_storage::{
    relational_index_shadow_manifest_generation_file, RelationalCheckpointIndexLoad,
    RelationalIndexChangeCapture, RelationalIndexChangeCaptureLimits, RelationalIndexRangeScan,
    RelationalIndexReadLimits, RelationalIndexRecoveryBuilder, RelationalIndexRecoveryConfig,
    RelationalIndexRecoveryReader, RelationalIndexRecoveryReport, RelationalIndexRole,
    RelationalIndexShadowBuildReport, RelationalIndexShadowConfig, RelationalIndexShadowError,
    RelationalIndexShadowReader, RelationalIndexShadowWriter, RelationalKey,
    RelationalOverflowPublicationConfig, RelationalOverflowRootReader, RelationalRecoveryFence,
    RelationalRecoverySourceIdentity, RelationalReplayAccessSet,
    RelationalRowPagePublicationConfig, RelationalRowPageReadView, RelationalRowPageRootReader,
    RelationalRowPageSnapshotReader, RelationalScalarType, RelationalSparseRecoveryStage,
    RelationalTableSchema, RelationalTransaction, RelationalValue, StorageResidencyMode,
    RELATIONAL_PRIMARY_INDEX_NAME,
};
use std::{collections::BTreeSet, sync::Arc};

pub use hawdb_storage::relational_index_view::{
    RelationalIndexQualificationProbe, RelationalIndexQualificationProbeKind,
    RelationalIndexQualificationProbeReport, RelationalIndexViewQualificationOptions,
    RelationalIndexViewQualificationReport, RELATIONAL_INDEX_VIEW_QUALIFICATION_PROTOCOL,
};

fn qualification_probe_error(
    ordinal: usize,
    table: &str,
    index: &str,
    error: RelationalIndexShadowError,
) -> HawDBError {
    let context = format!(
        "relational index qualification probe {ordinal} on {table}.{index} failed: {error}"
    );
    match error {
        RelationalIndexShadowError::Corrupt(_) => HawDBError::StorageIntegrity(context),
        RelationalIndexShadowError::Admission(_)
        | RelationalIndexShadowError::Durability(_)
        | RelationalIndexShadowError::MissingIndex { .. }
        | RelationalIndexShadowError::StaleGeneration { .. } => HawDBError::Storage(context),
    }
}

fn hash_bounded_bytes(hasher: &mut IntegrityHasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn relational_index_key(
    schema: &RelationalTableSchema,
    values: &[RelationalValue],
    columns: &[String],
) -> RelationalKey {
    RelationalKey(
        columns
            .iter()
            .map(|column| {
                let position = schema
                    .column_position(column)
                    .expect("validated relational index column");
                values[position].clone()
            })
            .collect(),
    )
}

fn push_qualification_probe(
    probes: &mut Vec<RelationalIndexQualificationProbe>,
    unique: &mut BTreeSet<RelationalIndexQualificationProbe>,
    probe: RelationalIndexQualificationProbe,
    max_probes: usize,
) -> bool {
    if unique.contains(&probe) {
        return true;
    }
    if probes.len() >= max_probes {
        return false;
    }
    unique.insert(probe.clone());
    probes.push(probe);
    true
}

fn relational_keys_digest(keys: &[RelationalKey]) -> String {
    let mut hasher = IntegrityHasher::new();
    hasher.update(b"hawdb-relational-index-qualification-keys-v1\0");
    hasher.update(&(keys.len() as u64).to_le_bytes());
    for key in keys {
        hasher.update(&(key.0.len() as u64).to_le_bytes());
        for value in &key.0 {
            hash_relational_value(&mut hasher, value);
        }
    }
    hasher.finish().sha256.to_string()
}

fn hash_relational_value(hasher: &mut IntegrityHasher, value: &RelationalValue) {
    match value {
        RelationalValue::Null => hasher.update(&[0]),
        RelationalValue::Boolean(value) => hasher.update(&[1, u8::from(*value)]),
        RelationalValue::BigInt(value) => {
            hasher.update(&[2]);
            hasher.update(&value.to_le_bytes());
        }
        RelationalValue::DoublePrecision(value) => {
            hasher.update(&[3]);
            hasher.update(&value.to_bits().to_le_bytes());
        }
        RelationalValue::Text(value) => {
            hasher.update(&[4]);
            hash_bounded_bytes(hasher, value.as_bytes());
        }
        RelationalValue::Bytea(value) => {
            hasher.update(&[5]);
            hash_bounded_bytes(hasher, value);
        }
        RelationalValue::Uuid(value) => {
            hasher.update(&[7]);
            hasher.update(value.as_bytes());
        }
        RelationalValue::Overflow(reference) => {
            hasher.update(&[6, relational_scalar_type_tag(reference.scalar_type)]);
            hash_bounded_bytes(hasher, reference.digest.as_bytes());
            hasher.update(&reference.compressed_bytes.to_le_bytes());
            hasher.update(&reference.uncompressed_bytes.to_le_bytes());
        }
    }
}

const fn relational_scalar_type_tag(scalar_type: RelationalScalarType) -> u8 {
    match scalar_type {
        RelationalScalarType::Boolean => 0,
        RelationalScalarType::BigInt => 1,
        RelationalScalarType::DoublePrecision => 2,
        RelationalScalarType::Text => 3,
        RelationalScalarType::Bytea => 4,
        RelationalScalarType::Uuid => 5,
    }
}

#[derive(Debug)]
pub(super) struct PreparedRelationalIndexCandidate {
    pub(super) candidate: Option<RelationalIndexShadowBuildReport>,
    pub(super) report: RelationalIndexShadowCheckpointReport,
}

pub(super) use hawdb_storage::relational::{
    RelationalIndexLiveUnavailable, RelationalIndexShadowState,
};
impl GraphStore {
    pub(super) fn relational_checkpoint_index_load(&self) -> RelationalCheckpointIndexLoad {
        if self
            .relational_index_shadow
            .mode
            .requires_authoritative_indexes()
        {
            RelationalCheckpointIndexLoad::OmitMaterializedPostings
        } else {
            RelationalCheckpointIndexLoad::MaterializedPostings
        }
    }

    pub(super) fn prepare_relational_index_candidate(
        &self,
        generation: u64,
        source_commit_epoch: u64,
    ) -> Option<PreparedRelationalIndexCandidate> {
        if !self
            .relational_index_shadow
            .mode
            .publishes_persistent_indexes()
        {
            return None;
        }
        let result = self.durable.as_ref().map_or_else(
            || {
                Err(RelationalIndexShadowError::Durability(
                    "relational index checkpoint requires a durable store".to_string(),
                ))
            },
            |durable| {
                let writer =
                    RelationalIndexShadowWriter::new(RelationalIndexShadowConfig::default());
                if self.relational_state.canonical_row_metadata_only() {
                    let overflow = Arc::new(
                        RelationalOverflowRootReader::open_generation(
                            durable.root_path(),
                            generation,
                            RelationalOverflowPublicationConfig::default(),
                        )
                        .map_err(|error| RelationalIndexShadowError::Corrupt(error.to_string()))?,
                    );
                    let rows = Arc::new(
                        RelationalRowPageRootReader::open_generation(
                            durable.root_path(),
                            generation,
                            RelationalRowPagePublicationConfig::default(),
                        )
                        .map_err(|error| RelationalIndexShadowError::Corrupt(error.to_string()))?,
                    );
                    let reader = RelationalRowPageSnapshotReader::new(
                        Arc::new(RelationalRowPageReadView::from_base(rows)),
                        overflow,
                        None,
                        Arc::clone(&durable.segment_cache),
                        durable.store_id(),
                    )
                    .map_err(map_index_row_snapshot_error)?;
                    let source =
                        CanonicalRelationalIndexRowSource::new(reader, &self.relational_state);
                    writer.publish_generation_from_source(
                        durable.root_path(),
                        &self.relational_state,
                        &source,
                        generation,
                        source_commit_epoch,
                    )
                } else {
                    writer.publish_generation(
                        durable.root_path(),
                        &self.relational_state,
                        generation,
                        source_commit_epoch,
                    )
                }
            },
        );
        Some(match result {
            Ok(candidate) => PreparedRelationalIndexCandidate {
                report: RelationalIndexShadowCheckpointReport::published(candidate.clone()),
                candidate: Some(candidate),
            },
            Err(error) => PreparedRelationalIndexCandidate {
                candidate: None,
                report: RelationalIndexShadowCheckpointReport::failed(
                    generation,
                    source_commit_epoch,
                    error.to_string(),
                ),
            },
        })
    }

    pub(super) fn install_prepared_relational_index_candidate(
        &mut self,
        prepared: Option<PreparedRelationalIndexCandidate>,
    ) {
        let Some(prepared) = prepared else {
            return;
        };
        let Some(report) = prepared.candidate else {
            self.relational_index_shadow.generation_artifacts = None;
            self.relational_index_shadow.recovery_builder = None;
            self.relational_index_shadow.recovery_report = None;
            self.relational_index_shadow.read_view = None;
            self.relational_index_shadow.recovery_status =
                RelationalIndexShadowRecoveryStatus::CandidateUnavailable {
                    generation: prepared.report.generation,
                    source_commit_epoch: prepared.report.source_commit_epoch,
                    reason: prepared.report.error.clone().unwrap_or_else(|| {
                        "relational index candidate was not published".to_string()
                    }),
                };
            self.relational_index_shadow.checkpoint_report = Some(prepared.report);
            return;
        };
        self.relational_index_shadow.generation_artifacts = Some(report.generation_artifacts);
        self.relational_index_shadow.expected_previous_generation = Some(report.generation);
        self.relational_index_shadow.recovery_builder = None;
        self.relational_index_shadow.recovery_report = None;
        match self.open_base_relational_index_read_view() {
            Ok(view) => {
                self.relational_index_shadow.read_view = Some(view);
                self.relational_index_shadow.recovery_status =
                    RelationalIndexShadowRecoveryStatus::CheckpointReady {
                        generation: report.generation,
                        source_commit_epoch: report.source_commit_epoch,
                        index_roots: report.index_roots,
                        page_count: report.pages_written,
                    };
                self.relational_index_shadow.checkpoint_report =
                    Some(RelationalIndexShadowCheckpointReport::published(report));
            }
            Err(error) => {
                self.relational_index_shadow.read_view = None;
                self.relational_index_shadow.recovery_status =
                    RelationalIndexShadowRecoveryStatus::InvalidWritable {
                        error: format!(
                            "published relational index candidate could not be pinned: {error}"
                        ),
                    };
                self.relational_index_shadow.checkpoint_report =
                    Some(RelationalIndexShadowCheckpointReport::failed(
                        report.generation,
                        report.source_commit_epoch,
                        error.to_string(),
                    ));
            }
        }
    }

    fn open_base_relational_index_read_view(
        &self,
    ) -> Result<Arc<RelationalIndexReadView>, hawdb_storage::RelationalIndexShadowError> {
        let durable = self.durable.as_ref().ok_or_else(|| {
            hawdb_storage::RelationalIndexShadowError::Admission(
                "relational index read view requires a durable store".to_string(),
            )
        })?;
        let generation_artifacts =
            durable
                .relational_index_generation_artifacts
                .ok_or_else(|| {
                    RelationalIndexShadowError::Admission(
                        "canonical manifest does not bind a relational index generation"
                            .to_string(),
                    )
                })?;
        if Some(generation_artifacts) != self.relational_index_shadow.generation_artifacts {
            return Err(RelationalIndexShadowError::Corrupt(
                "relational index read selection does not match the canonical manifest binding"
                    .to_string(),
            ));
        }
        let reader = RelationalIndexShadowReader::open_bound_generation_with_cache(
            durable.root_path(),
            generation_artifacts,
            RelationalIndexShadowConfig::default(),
            Arc::clone(&durable.segment_cache),
            durable.store_id(),
        )?;
        reader.validate_required_roots(&self.relational_state)?;
        Ok(Arc::new(RelationalIndexReadView::from_base(reader)))
    }

    fn open_recovered_relational_index_read_view(
        &self,
        recovered_commit_epoch: u64,
        expected_recovery_source: RelationalRecoverySourceIdentity,
    ) -> Result<Arc<RelationalIndexReadView>, hawdb_storage::RelationalIndexShadowError> {
        let durable = self.durable.as_ref().ok_or_else(|| {
            hawdb_storage::RelationalIndexShadowError::Admission(
                "relational index read view requires a durable store".to_string(),
            )
        })?;
        let generation_artifacts = self
            .relational_index_shadow
            .generation_artifacts
            .ok_or_else(|| {
                RelationalIndexShadowError::Admission(
                    "canonical manifest does not bind a relational index generation".to_string(),
                )
            })?;
        let reader = RelationalIndexRecoveryReader::open_bound_generation_with_cache(
            durable.root_path(),
            generation_artifacts,
            RelationalRecoveryFence::new(recovered_commit_epoch, expected_recovery_source),
            RelationalIndexShadowConfig::default(),
            RelationalIndexRecoveryConfig::default(),
            Arc::clone(&durable.segment_cache),
            durable.store_id(),
        )?;
        reader.validate_required_roots(&self.relational_state)?;
        Ok(Arc::new(RelationalIndexReadView::from_recovered(reader)))
    }

    pub(super) fn relational_index_live_capture_limits(
        &self,
    ) -> Option<RelationalIndexChangeCaptureLimits> {
        self.relational_index_shadow
            .current_read_view(self.commit_epoch)
            .map(|_| self.relational_index_shadow.live_limits)
    }

    pub(super) fn stage_relational_index_live_publication(
        &self,
        next_epoch: u64,
        capture: Option<RelationalIndexChangeCapture>,
    ) -> Option<Result<Arc<RelationalIndexReadView>, RelationalIndexLiveUnavailable>> {
        self.relational_index_shadow
            .stage_live_publication(self.commit_epoch, next_epoch, capture)
    }

    pub(super) fn publish_relational_index_live_view(
        &mut self,
        publication: Option<Result<Arc<RelationalIndexReadView>, RelationalIndexLiveUnavailable>>,
    ) {
        match publication {
            None => {}
            Some(Ok(view)) => {
                let identity = view.identity();
                self.relational_index_shadow.recovery_status =
                    RelationalIndexShadowRecoveryStatus::LiveCurrent {
                        base_generation: identity.base_generation,
                        delta_generation: identity.delta_generation,
                        base_commit_epoch: identity.base_commit_epoch,
                        visible_commit_epoch: identity.visible_commit_epoch,
                        live_batches: view.live_batch_count(),
                        live_entries: view.live_entry_count(),
                        live_bytes: view.live_encoded_bytes(),
                    };
                self.relational_index_shadow.read_view = Some(view);
            }
            Some(Err(unavailable)) => {
                self.relational_index_shadow.read_view = None;
                self.relational_index_shadow.recovery_status =
                    RelationalIndexShadowRecoveryStatus::LiveUnavailable {
                        base_generation: unavailable.identity.base_generation,
                        base_commit_epoch: unavailable.identity.base_commit_epoch,
                        last_visible_commit_epoch: unavailable.identity.visible_commit_epoch,
                        failed_commit_epoch: unavailable.failed_commit_epoch,
                        reason: unavailable.reason,
                    };
            }
        }
    }

    pub(super) fn uses_sparse_read_only_relational_recovery(&self) -> bool {
        self.durable
            .as_ref()
            .is_some_and(|durable| durable.read_only)
            && self.residency_mode == StorageResidencyMode::OutOfCore
            && self
                .relational_index_shadow
                .mode
                .requires_authoritative_indexes()
            && self.relational_state.canonical_row_metadata_only()
    }

    /// Completes one already-durable non-relational commit.
    ///
    /// Callers invoke this only after applying the canonical graph or catalog
    /// mutation. Relational index contents do not change, but their immutable
    /// read view must advance to the same global commit epoch so a later
    /// snapshot cannot combine graph state with a stale relational identity.
    pub(super) fn finish_non_relational_commit(&mut self) {
        let next_commit_epoch = self.commit_epoch + 1;
        let index_publication =
            self.stage_relational_index_live_publication(next_commit_epoch, None);
        let row_publication = self.stage_relational_row_live_publication(next_commit_epoch, None);
        self.commit_epoch = next_commit_epoch;
        self.publish_relational_index_live_view(index_publication);
        self.publish_relational_row_live_view(row_publication);
    }

    pub(super) fn mount_relational_index_shadow_for_recovery(&mut self) {
        if !self
            .relational_index_shadow
            .mode
            .publishes_persistent_indexes()
        {
            return;
        }
        let Some(durable) = self.durable.as_ref() else {
            return;
        };
        let root = durable.root_path().to_path_buf();
        let checkpoint_generation = durable.checkpoint_epoch;
        let checkpoint_commit_epoch = durable.checkpoint_commit_epoch;
        let read_only = durable.read_only;
        let Some(binding) = durable.relational_index_generation_artifacts else {
            self.relational_index_shadow.generation_artifacts = None;
            self.relational_index_shadow.recovery_status =
                RelationalIndexShadowRecoveryStatus::Missing;
            return;
        };
        self.relational_index_shadow.generation_artifacts = Some(binding);
        let manifest_path = root.join(relational_index_shadow_manifest_generation_file(
            binding.generation,
        ));
        if !manifest_path.exists() {
            self.reject_invalid_generation_aligned_index(
                read_only,
                "canonical manifest selects a missing relational index generation manifest"
                    .to_string(),
            );
            return;
        }
        match self.open_base_relational_index_read_view() {
            Ok(view) => {
                let manifest = view.base_manifest();
                self.relational_index_shadow.expected_previous_generation =
                    Some(manifest.generation);
                self.relational_index_shadow.read_view = Some(Arc::clone(&view));
                if manifest.generation == checkpoint_generation
                    && manifest.source_commit_epoch == checkpoint_commit_epoch
                {
                    let recovery_builder = (!read_only).then(|| {
                        RelationalIndexRecoveryBuilder::new(
                            &root,
                            manifest.generation,
                            manifest.source_commit_epoch,
                            RelationalIndexRecoveryConfig::default(),
                        )
                    });
                    match recovery_builder {
                        Some(Ok(builder)) => {
                            self.relational_index_shadow.recovery_builder = Some(builder);
                        }
                        Some(Err(error)) => {
                            self.relational_index_shadow.recovery_status =
                                RelationalIndexShadowRecoveryStatus::RecoveryUnavailable {
                                    base_generation: manifest.generation,
                                    base_commit_epoch: manifest.source_commit_epoch,
                                    recovered_commit_epoch: self.commit_epoch,
                                    reason: error.to_string(),
                                };
                            return;
                        }
                        None => {}
                    }
                    self.relational_index_shadow.recovery_status =
                        RelationalIndexShadowRecoveryStatus::CheckpointReady {
                            generation: manifest.generation,
                            source_commit_epoch: manifest.source_commit_epoch,
                            index_roots: manifest.roots.len(),
                            page_count: manifest.page_count,
                        };
                } else if manifest.generation <= checkpoint_generation
                    && manifest.source_commit_epoch <= checkpoint_commit_epoch
                {
                    self.relational_index_shadow.read_view = None;
                    self.relational_index_shadow.recovery_status =
                        RelationalIndexShadowRecoveryStatus::Stale {
                            generation: manifest.generation,
                            source_commit_epoch: manifest.source_commit_epoch,
                            checkpoint_generation,
                            checkpoint_commit_epoch,
                        };
                } else {
                    self.relational_index_shadow.read_view = None;
                    let error = format!(
                        "index generation/epoch {}/{} is ahead of checkpoint {checkpoint_generation}/{checkpoint_commit_epoch}",
                        manifest.generation, manifest.source_commit_epoch
                    );
                    self.reject_invalid_generation_aligned_index(read_only, error);
                }
            }
            Err(error) => {
                self.reject_invalid_generation_aligned_index(read_only, error.to_string());
            }
        }
    }

    fn reject_invalid_generation_aligned_index(&mut self, read_only: bool, error: String) {
        self.relational_index_shadow.expected_previous_generation = None;
        self.relational_index_shadow.recovery_builder = None;
        self.relational_index_shadow.read_view = None;
        self.relational_index_shadow.recovery_status = if read_only {
            RelationalIndexShadowRecoveryStatus::InvalidReadOnly { error }
        } else {
            RelationalIndexShadowRecoveryStatus::InvalidWritable { error }
        };
    }

    pub fn relational_index_shadow_checkpoint_report(
        &self,
    ) -> Option<&RelationalIndexShadowCheckpointReport> {
        self.relational_index_shadow.checkpoint_report.as_ref()
    }

    pub fn relational_index_shadow_recovery_status(&self) -> &RelationalIndexShadowRecoveryStatus {
        &self.relational_index_shadow.recovery_status
    }

    pub fn relational_index_recovery_report(&self) -> Option<&RelationalIndexRecoveryReport> {
        self.relational_index_shadow.recovery_report.as_ref()
    }

    #[doc(hidden)]
    pub fn relational_index_probe_statistics(
        &self,
        table: &str,
        index: &str,
        prefix_len: usize,
    ) -> Option<RelationalIndexProbeStatistics> {
        self.relational_index_shadow
            .current_read_view(self.commit_epoch)?
            .fresh_probe_statistics(table, index, prefix_len)
    }

    #[cfg(test)]
    pub(crate) fn visit_relational_index_read_view_prefix(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        match self
            .relational_index_shadow
            .current_read_view(self.commit_epoch)
        {
            Some(view) => Some(view.visit_prefix_postings(table, index, prefix, limits, visit)),
            None => self
                .relational_index_shadow
                .selected_read_failure()
                .map(Err),
        }
    }

    #[doc(hidden)]
    pub fn visit_relational_index_read_view_prefix_entries(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        match self
            .relational_index_shadow
            .current_read_view(self.commit_epoch)
        {
            Some(view) => Some(view.visit_prefix_entries(table, index, prefix, limits, visit)),
            None => self
                .relational_index_shadow
                .selected_read_failure()
                .map(Err),
        }
    }

    #[doc(hidden)]
    pub fn visit_relational_index_read_view_prefix_entries_many(
        &self,
        table: &str,
        index: &str,
        prefixes: &[RelationalKey],
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        match self
            .relational_index_shadow
            .current_read_view(self.commit_epoch)
        {
            Some(view) => {
                Some(view.visit_prefix_entries_many(table, index, prefixes, limits, visit))
            }
            None => self
                .relational_index_shadow
                .selected_read_failure()
                .map(Err),
        }
    }

    #[doc(hidden)]
    pub fn visit_relational_index_read_view_range_entries(
        &self,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        match self
            .relational_index_shadow
            .current_read_view(self.commit_epoch)
        {
            Some(view) => Some(view.visit_range_entries(table, index, scan, limits, visit)),
            None => self
                .relational_index_shadow
                .selected_read_failure()
                .map(Err),
        }
    }

    /// Differentially checks the pinned demand-paged relational index view
    /// against the current materialized oracle without changing SQL routing.
    pub fn qualify_relational_index_read_view(
        &self,
        options: RelationalIndexViewQualificationOptions,
    ) -> crate::Result<RelationalIndexViewQualificationReport> {
        self.relational_state
            .require_materialized_rows("relational index differential qualification")
            .map_err(|error| HawDBError::Storage(error.to_string()))?;
        let view = self
            .relational_index_shadow
            .current_read_view(self.commit_epoch)
            .ok_or_else(|| {
                HawDBError::Storage(format!(
                    "relational index read view is unavailable at commit epoch {}",
                    self.commit_epoch
                ))
            })?;
        let tables_discovered = self.relational_state.table_schemas().count();
        let indexes_discovered = self
            .relational_state
            .table_schemas()
            .map(|schema| schema.required_index_definitions().len())
            .sum();
        let mut probes = Vec::new();
        let mut unique_probes = BTreeSet::new();
        let mut truncated = tables_discovered > options.max_tables.get();
        let tables_sampled = tables_discovered.min(options.max_tables.get());
        let mut rows_sampled = 0usize;
        'tables: for schema in self
            .relational_state
            .table_schemas()
            .take(options.max_tables.get())
        {
            let definitions = schema.required_index_definitions();
            for (primary_key, row) in self
                .relational_state
                .rows(&schema.name)
                .take(options.max_rows_per_table.get())
            {
                rows_sampled = rows_sampled.checked_add(1).ok_or_else(|| {
                    HawDBError::Storage(
                        "relational index qualification row sample counter overflow".to_string(),
                    )
                })?;
                for definition in &definitions {
                    let key = if definition.role == RelationalIndexRole::Primary {
                        primary_key.clone()
                    } else {
                        relational_index_key(schema, row.values(), &definition.columns)
                    };
                    if !push_qualification_probe(
                        &mut probes,
                        &mut unique_probes,
                        RelationalIndexQualificationProbe {
                            table: schema.name.clone(),
                            index: definition.name.clone(),
                            kind: RelationalIndexQualificationProbeKind::Exact,
                            key: key.clone(),
                        },
                        options.max_probes.get(),
                    ) {
                        truncated = true;
                        break 'tables;
                    }
                    if definition.role != RelationalIndexRole::Primary
                        && definition.columns.len() > 1
                    {
                        for prefix_len in 1..definition.columns.len() {
                            if !push_qualification_probe(
                                &mut probes,
                                &mut unique_probes,
                                RelationalIndexQualificationProbe {
                                    table: schema.name.clone(),
                                    index: definition.name.clone(),
                                    kind: RelationalIndexQualificationProbeKind::LeadingPrefix,
                                    key: RelationalKey(key.0[..prefix_len].to_vec()),
                                },
                                options.max_probes.get(),
                            ) {
                                truncated = true;
                                break 'tables;
                            }
                        }
                    }
                }
            }
        }
        let indexes_probed = probes
            .iter()
            .map(|probe| (probe.table.as_str(), probe.index.as_str()))
            .collect::<BTreeSet<_>>()
            .len();
        let mut probe_reports = Vec::with_capacity(probes.len());
        let mut mismatches = 0usize;
        for (ordinal, probe) in probes.into_iter().enumerate() {
            let mut candidate = Vec::new();
            let read = match probe.kind {
                RelationalIndexQualificationProbeKind::Exact => view.visit_exact_postings(
                    &probe.table,
                    &probe.index,
                    &probe.key,
                    options.read_limits,
                    |primary_key| {
                        candidate.push(primary_key.clone());
                        true
                    },
                ),
                RelationalIndexQualificationProbeKind::LeadingPrefix => view.visit_prefix_postings(
                    &probe.table,
                    &probe.index,
                    &probe.key,
                    options.read_limits,
                    |primary_key| {
                        candidate.push(primary_key.clone());
                        true
                    },
                ),
            }
            .map_err(|error| {
                qualification_probe_error(ordinal, &probe.table, &probe.index, error)
            })?;
            candidate.sort();
            candidate.dedup();
            let oracle = self.relational_index_oracle_rows(&probe, options.read_limits)?;
            let matched = candidate == oracle;
            mismatches += usize::from(!matched);
            probe_reports.push(RelationalIndexQualificationProbeReport {
                ordinal,
                table: probe.table,
                index: probe.index,
                kind: probe.kind,
                candidate_rows: candidate.len(),
                oracle_rows: oracle.len(),
                candidate_digest: relational_keys_digest(&candidate),
                oracle_digest: relational_keys_digest(&oracle),
                matched,
                read,
            });
        }
        let identity = view.identity();
        let ready = mismatches == 0 && !truncated && indexes_probed == indexes_discovered;
        Ok(RelationalIndexViewQualificationReport {
            protocol: RELATIONAL_INDEX_VIEW_QUALIFICATION_PROTOCOL,
            base_generation: identity.base_generation,
            delta_generation: identity.delta_generation,
            base_commit_epoch: identity.base_commit_epoch,
            visible_commit_epoch: identity.visible_commit_epoch,
            root_set_digest: identity.root_set_digest.to_string(),
            tables_discovered,
            tables_sampled,
            rows_sampled,
            indexes_discovered,
            indexes_probed,
            probes: probe_reports,
            mismatches,
            truncated,
            ready,
        })
    }

    fn relational_index_oracle_rows(
        &self,
        probe: &RelationalIndexQualificationProbe,
        limits: RelationalIndexReadLimits,
    ) -> crate::Result<Vec<RelationalKey>> {
        let mut rows = if probe.index == RELATIONAL_PRIMARY_INDEX_NAME {
            self.relational_state
                .row(&probe.table, &probe.key)
                .map(|_| vec![probe.key.clone()])
                .unwrap_or_default()
        } else {
            match probe.kind {
                RelationalIndexQualificationProbeKind::Exact => self
                    .relational_state
                    .index_prefix_lookup(
                        &probe.table,
                        &probe.index,
                        &probe.key,
                        limits.max_rows.get().saturating_add(1),
                    )
                    .ok_or_else(|| {
                        HawDBError::Storage(format!(
                            "relational index qualification oracle is missing {}.{}",
                            probe.table, probe.index
                        ))
                    })?
                    .into_iter()
                    .cloned()
                    .collect(),
                RelationalIndexQualificationProbeKind::LeadingPrefix => self
                    .relational_state
                    .index_prefix_lookup(
                        &probe.table,
                        &probe.index,
                        &probe.key,
                        limits.max_rows.get().saturating_add(1),
                    )
                    .ok_or_else(|| {
                        HawDBError::Storage(format!(
                            "relational index qualification oracle is missing {}.{}",
                            probe.table, probe.index
                        ))
                    })?
                    .into_iter()
                    .cloned()
                    .collect(),
            }
        };
        if rows.len() > limits.max_rows.get() {
            return Err(HawDBError::Storage(format!(
                "relational index qualification oracle exceeds row limit {}",
                limits.max_rows
            )));
        }
        rows.sort();
        rows.dedup();
        Ok(rows)
    }

    pub(super) fn stage_recovered_relational_transaction(
        &mut self,
        transaction: RelationalTransaction,
        replay_access: Option<RelationalReplayAccessSet>,
        expected_epoch: u64,
    ) -> Result<(), hawdb_storage::RelationalError> {
        let authoritative = self
            .relational_index_shadow
            .mode
            .requires_authoritative_indexes();
        let index_limits = self
            .relational_index_shadow
            .recovery_builder
            .as_ref()
            .map(RelationalIndexRecoveryBuilder::capture_limits);
        if authoritative && index_limits.is_none() {
            return Err(hawdb_storage::RelationalError::Corruption(
                "authoritative relational index recovery requires a bound base generation and writable recovery-delta builder"
                    .to_string(),
            ));
        }
        let row_limits = self.relational_row_recovery_capture_limits();
        let (next, index_capture, row_capture) = match (index_limits, row_limits) {
            (Some(index_limits), Some(row_limits)) if authoritative => {
                let replay_access = replay_access.as_ref().ok_or_else(|| {
                    hawdb_storage::RelationalError::Corruption(
                        "authoritative relational WAL is missing its exact replay access set"
                            .to_string(),
                    )
                })?;
                let (next, index_capture, row_capture) =
                    if self.relational_state.canonical_row_metadata_only() {
                        let hydrated_access = self
                            .hydrate_sparse_relational_recovery_access(replay_access, row_limits)?;
                        self.relational_state
                            .stage_sparse_transaction_for_authoritative_recovery_with_replay_access(
                                RelationalSparseRecoveryStage {
                                    transaction,
                                    hydrated_access,
                                    mutation_limits: self.relational_mutation_limits,
                                    overflow_config: self.relational_overflow_config,
                                    index_capture_limits: index_limits,
                                    row_capture_limits: row_limits,
                                    expected_replay_access: replay_access,
                                },
                            )?
                    } else {
                        self.relational_state
                            .stage_transaction_for_authoritative_recovery_with_replay_access(
                                transaction,
                                self.relational_mutation_limits,
                                self.relational_overflow_config,
                                index_limits,
                                row_limits,
                                replay_access,
                            )?
                    };
                (next, Some(index_capture), Some(row_capture))
            }
            (Some(index_limits), Some(row_limits)) => {
                let (next, index_capture, row_capture) = self
                    .relational_state
                    .stage_transaction_with_index_and_row_changes(
                        transaction,
                        self.relational_mutation_limits,
                        self.relational_overflow_config,
                        index_limits,
                        row_limits,
                    )?;
                (next, Some(index_capture), Some(row_capture))
            }
            (Some(index_limits), None) if authoritative => {
                let (next, capture) = self
                    .relational_state
                    .stage_transaction_for_authoritative_recovery(
                        transaction,
                        self.relational_mutation_limits,
                        self.relational_overflow_config,
                        index_limits,
                    )?;
                (next, Some(capture), None)
            }
            (Some(index_limits), None) => {
                let (next, capture) = self.relational_state.stage_transaction_with_index_changes(
                    transaction,
                    self.relational_mutation_limits,
                    self.relational_overflow_config,
                    index_limits,
                )?;
                (next, Some(capture), None)
            }
            (None, Some(row_limits)) => {
                let (next, capture) = self.relational_state.stage_transaction_with_row_changes(
                    transaction,
                    self.relational_mutation_limits,
                    self.relational_overflow_config,
                    row_limits,
                )?;
                (next, None, Some(capture))
            }
            (None, None) => (
                self.relational_state.stage_transaction(
                    transaction,
                    self.relational_mutation_limits,
                    self.relational_overflow_config,
                )?,
                None,
                None,
            ),
        };
        self.relational_state = next;
        if let Some(capture) = index_capture
            && let Some(mut builder) = self.relational_index_shadow.recovery_builder.take()
        {
            if let Err(error) = builder.record(expected_epoch, capture) {
                self.mark_relational_index_recovery_unavailable(expected_epoch, error.to_string());
            } else {
                self.relational_index_shadow.recovery_builder = Some(builder);
            }
        }
        self.record_relational_row_recovery_capture(expected_epoch, row_capture);
        Ok(())
    }

    pub(super) fn invalidate_relational_index_recovery(
        &mut self,
        recovered_commit_epoch: u64,
        reason: impl Into<String>,
    ) {
        if self.relational_index_shadow.recovery_builder.is_some() {
            self.mark_relational_index_recovery_unavailable(recovered_commit_epoch, reason.into());
        }
    }

    pub(super) fn finish_relational_index_recovery(
        &mut self,
        recovery_source: Option<RelationalRecoverySourceIdentity>,
    ) {
        let Some(builder) = self.relational_index_shadow.recovery_builder.take() else {
            if let RelationalIndexShadowRecoveryStatus::CheckpointReady {
                generation,
                source_commit_epoch,
                ..
            } = self.relational_index_shadow.recovery_status
                && self.commit_epoch > source_commit_epoch
            {
                let Some(recovery_source) = recovery_source else {
                    self.mark_relational_index_recovery_unavailable(
                        self.commit_epoch,
                        "WAL recovery did not produce a relational recovery source identity"
                            .to_string(),
                    );
                    return;
                };
                match self
                    .open_recovered_relational_index_read_view(self.commit_epoch, recovery_source)
                {
                    Ok(view) => {
                        let residency = view.residency_report();
                        self.relational_index_shadow.read_view = Some(view);
                        self.relational_index_shadow.recovery_status =
                            RelationalIndexShadowRecoveryStatus::WalRecovered {
                                base_generation: generation,
                                base_commit_epoch: source_commit_epoch,
                                recovered_commit_epoch: self.commit_epoch,
                                delta_pages: residency.recovery_delta_pages,
                                delta_entries: residency.recovery_delta_entries,
                                peak_dirty_bytes: 0,
                            };
                    }
                    Err(error) => self.mark_relational_index_recovery_unavailable(
                        self.commit_epoch,
                        format!(
                            "read-only recovery requires an exact published index delta: {error}"
                        ),
                    ),
                }
            }
            return;
        };
        if self.commit_epoch == builder.base_commit_epoch() {
            return;
        }
        let Some(recovery_source) = recovery_source else {
            self.mark_relational_index_recovery_unavailable(
                self.commit_epoch,
                "WAL recovery did not produce a relational recovery source identity".to_string(),
            );
            return;
        };
        match builder.finish_with_recovery_source(self.commit_epoch, recovery_source) {
            Ok(report) => {
                match self.open_recovered_relational_index_read_view(
                    report.recovered_commit_epoch,
                    recovery_source,
                ) {
                    Ok(view) => {
                        self.relational_index_shadow.read_view = Some(view);
                        self.relational_index_shadow.recovery_status =
                            RelationalIndexShadowRecoveryStatus::WalRecovered {
                                base_generation: report.base_generation,
                                base_commit_epoch: report.base_commit_epoch,
                                recovered_commit_epoch: report.recovered_commit_epoch,
                                delta_pages: report.delta_pages,
                                delta_entries: report.delta_entries,
                                peak_dirty_bytes: report.peak_dirty_bytes,
                            };
                        self.relational_index_shadow.recovery_report = Some(report);
                    }
                    Err(error) => self.mark_relational_index_recovery_unavailable(
                        self.commit_epoch,
                        format!("recovered relational index view could not be pinned: {error}"),
                    ),
                }
            }
            Err(error) => {
                self.mark_relational_index_recovery_unavailable(
                    self.commit_epoch,
                    error.to_string(),
                );
            }
        }
    }

    fn mark_relational_index_recovery_unavailable(
        &mut self,
        recovered_commit_epoch: u64,
        reason: String,
    ) {
        let (base_generation, base_commit_epoch) =
            match &self.relational_index_shadow.recovery_status {
                RelationalIndexShadowRecoveryStatus::CheckpointReady {
                    generation,
                    source_commit_epoch,
                    ..
                } => (*generation, *source_commit_epoch),
                RelationalIndexShadowRecoveryStatus::WalRecovered {
                    base_generation,
                    base_commit_epoch,
                    ..
                }
                | RelationalIndexShadowRecoveryStatus::RecoveryUnavailable {
                    base_generation,
                    base_commit_epoch,
                    ..
                } => (*base_generation, *base_commit_epoch),
                _ => return,
            };
        self.relational_index_shadow.recovery_builder = None;
        self.relational_index_shadow.recovery_report = None;
        self.relational_index_shadow.read_view = None;
        self.relational_index_shadow.recovery_status =
            RelationalIndexShadowRecoveryStatus::RecoveryUnavailable {
                base_generation,
                base_commit_epoch,
                recovered_commit_epoch,
                reason,
            };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Catalog;
    use hawdb_storage::relational_index_view::RelationalIndexReadViewKind;
    use hawdb_storage::{
        DurabilityPolicy, RelationalColumnSchema, RelationalConflictAction,
        RelationalForeignKeySchema, RelationalIndexSchema, RelationalInsertMode, RelationalKey,
        RelationalReferentialAction, RelationalRow, RelationalScalarType, RelationalTableSchema,
        RelationalTransaction, RelationalUpsertAssignment, RelationalUpsertValue, RelationalValue,
        RelationalWrite, WalReplayConfig,
    };
    use std::num::NonZeroUsize;

    #[test]
    fn relational_index_facade_reports_keep_storage_type_identity() {
        use hawdb_storage::relational_index_view as owner;
        use std::any::TypeId;

        assert_eq!(
            TypeId::of::<crate::RelationalIndexReadViewReport>(),
            TypeId::of::<owner::RelationalIndexReadViewReport>()
        );
        assert_eq!(
            TypeId::of::<crate::RelationalIndexReadViewBackendReport>(),
            TypeId::of::<owner::RelationalIndexReadViewBackendReport>()
        );
        assert_eq!(
            TypeId::of::<crate::RelationalIndexStorageResidencyReport>(),
            TypeId::of::<owner::RelationalIndexStorageResidencyReport>()
        );
    }

    #[test]
    fn checkpoint_aligns_relational_index_candidate_without_making_it_canonical() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-store-relational-index-shadow-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_mode: hawdb_storage::RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let published_identity;
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open shadow-enabled store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![
                            RelationalWrite::CreateTable(RelationalTableSchema {
                                name: "documents".to_string(),
                                columns: vec![
                                    RelationalColumnSchema {
                                        name: "id".to_string(),
                                        scalar_type: RelationalScalarType::Text,
                                        nullable: false,
                                        default: None,
                                    },
                                    RelationalColumnSchema {
                                        name: "owner".to_string(),
                                        scalar_type: RelationalScalarType::Text,
                                        nullable: false,
                                        default: None,
                                    },
                                ],
                                primary_key: vec!["id".to_string()],
                                unique_constraints: Vec::new(),
                                foreign_keys: Vec::new(),
                                indexes: vec![RelationalIndexSchema {
                                    name: "documents_owner_idx".to_string(),
                                    columns: vec!["owner".to_string()],
                                    unique: false,
                                }],
                            }),
                            RelationalWrite::Insert {
                                table: "documents".to_string(),
                                rows: vec![hawdb_storage::RelationalRow::new(vec![
                                    RelationalValue::Text("doc-1".to_string()),
                                    RelationalValue::Text("owner-1".to_string()),
                                ])],
                                mode: hawdb_storage::RelationalInsertMode::Error,
                            },
                        ],
                    },
                )
                .expect("commit relational source");
            store
                .checkpoint(&catalog)
                .expect("publish canonical checkpoint");
            let report = store
                .relational_index_shadow_checkpoint_report()
                .expect("shadow report");
            assert_eq!(
                report.status,
                RelationalIndexShadowCheckpointStatus::Published
            );
            assert_eq!(report.index_roots, 2);
            let generation_artifacts = report
                .generation_artifacts
                .expect("published generation artifact identity");
            assert_eq!(generation_artifacts.generation, report.generation);
            assert_eq!(
                generation_artifacts.source_commit_epoch,
                report.source_commit_epoch
            );
            assert_eq!(
                generation_artifacts.page_artifact.encoded_len,
                report.artifact_bytes
            );
            assert_eq!(
                generation_artifacts.manifest_artifact.encoded_len,
                report.manifest_bytes
            );
            assert_eq!(
                store
                    .durable
                    .as_ref()
                    .expect("durable store")
                    .relational_index_generation_artifacts,
                Some(generation_artifacts)
            );
            let durable_manifest = crate::store::durable::DurableManifest::load(
                &path.join(crate::store::MANIFEST_FILE),
            )
            .expect("load canonical manifest binding");
            assert_eq!(
                durable_manifest.relational_index_generation_artifacts,
                Some(generation_artifacts)
            );
            assert!(path
                .join(relational_index_shadow_manifest_generation_file(
                    report.generation
                ))
                .exists());
            let checkpoint = store
                .durable
                .as_ref()
                .expect("durable store")
                .read_checkpoint_text(replay)
                .expect("read generation checkpoint");
            assert!(!checkpoint.contains("relational_index_manifest_"));
            let view = current_index_view(&store);
            assert_eq!(view.kind(), RelationalIndexReadViewKind::Base);
            assert_eq!(view.identity().base_generation, report.generation);
            assert_eq!(view.identity().delta_generation, None);
            assert_eq!(view.identity().base_commit_epoch, 1);
            assert_eq!(view.identity().visible_commit_epoch, 1);
            published_identity = view.identity();
            let snapshot = store.snapshot();
            assert!(Arc::ptr_eq(view, current_index_view(&snapshot)));
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("reopen with bounded shadow validation");
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::CheckpointReady { index_roots: 2, .. }
            ));
            assert_eq!(
                current_index_view(&store).kind(),
                RelationalIndexReadViewKind::Base
            );
            assert_eq!(current_index_view(&store).identity(), published_identity);
            assert_eq!(store.relational_state().row_count("documents"), 1);
        }

        std::fs::write(
            path.join(relational_index_shadow_manifest_generation_file(
                published_identity.base_generation,
            )),
            b"corrupt",
        )
        .expect("corrupt generation-aligned manifest");
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("shadow corruption must not reject canonical open");
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::InvalidWritable { .. }
            ));
            assert_eq!(store.relational_state().row_count("documents"), 1);
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                WalReplayConfig {
                    relational_index_mode: hawdb_storage::RelationalIndexMode::DemandPaged,
                    ..WalReplayConfig::default()
                },
            )
            .expect("canonical open remains available for a lazily selected reader");
            let error = store
                .visit_relational_index_read_view_prefix(
                    "documents",
                    "documents_owner_idx",
                    &RelationalKey(vec![RelationalValue::Text("owner-1".to_string())]),
                    RelationalIndexReadLimits::default(),
                    |_| true,
                )
                .expect("demand mode must retain a fail-closed selected result")
                .expect_err("selected corrupt generation must not fall back");
            assert!(matches!(error, RelationalIndexShadowError::Corrupt(_)));
        }
        std::fs::remove_dir_all(path).expect("remove shadow checkpoint fixture");
    }

    #[test]
    fn candidate_admission_failure_never_fails_the_canonical_checkpoint() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-store-relational-index-candidate-admission-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_mode: hawdb_storage::RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let oversized_id = "x".repeat(
            RelationalIndexShadowConfig::default()
                .page_limits
                .max_key_bytes
                .get()
                + 1,
        );
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open shadow-enabled store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_recovery_documents_table(&oversized_id),
                )
                .expect("commit oversized canonical row");
            store
                .checkpoint(&catalog)
                .expect("candidate admission must not fail canonical checkpoint");
            let report = store
                .relational_index_shadow_checkpoint_report()
                .expect("candidate failure report");
            assert_eq!(report.status, RelationalIndexShadowCheckpointStatus::Failed);
            assert_eq!(report.generation_artifacts, None);
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::CandidateUnavailable { .. }
            ));
            let generation = store
                .durable
                .as_ref()
                .expect("durable store")
                .checkpoint_epoch;
            assert_eq!(
                store
                    .durable
                    .as_ref()
                    .expect("durable store")
                    .relational_index_generation_artifacts,
                None
            );
            assert!(!path
                .join(relational_index_shadow_manifest_generation_file(generation))
                .exists());
            assert!(!path
                .join(hawdb_storage::relational_index_shadow_artifact_file(
                    generation
                ))
                .exists());
            assert_eq!(store.relational_state().row_count("documents"), 1);
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("canonical checkpoint must reopen without its shadow candidate");
            assert_eq!(
                store.relational_index_shadow_recovery_status(),
                &RelationalIndexShadowRecoveryStatus::Missing
            );
            assert_eq!(store.relational_state().row_count("documents"), 1);
        }
        std::fs::remove_dir_all(path).expect("remove candidate admission fixture");
    }

    #[test]
    fn unbound_legacy_candidate_is_never_selected_for_recovery() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-store-relational-index-unbound-legacy-{}-{nonce}",
            std::process::id()
        ));
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                WalReplayConfig::default(),
            )
            .expect("open materialized source");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_recovery_documents_table("doc-1"),
                )
                .expect("commit canonical relational source");
            store
                .checkpoint(&catalog)
                .expect("publish checkpoint without relational index binding");
            let durable = store.durable.as_ref().expect("durable store");
            assert_eq!(durable.relational_index_generation_artifacts, None);
            RelationalIndexShadowWriter::new(RelationalIndexShadowConfig::default())
                .publish(
                    &path,
                    store.relational_state(),
                    durable.checkpoint_epoch,
                    durable.checkpoint_commit_epoch,
                    None,
                )
                .expect("publish legacy unbound candidate");
        }
        assert!(path
            .join(hawdb_storage::RELATIONAL_INDEX_SHADOW_MANIFEST_FILE)
            .exists());

        let mut catalog = Catalog::default();
        let store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                relational_index_mode: RelationalIndexMode::Shadow,
                ..WalReplayConfig::default()
            },
        )
        .expect("unbound candidate must not block canonical open");
        assert_eq!(
            store.relational_index_shadow_recovery_status(),
            &RelationalIndexShadowRecoveryStatus::Missing
        );
        assert_eq!(store.relational_index_shadow.generation_artifacts, None);
        assert!(store.relational_index_shadow.read_view.is_none());
        assert_eq!(store.relational_state().row_count("documents"), 1);

        drop(store);
        std::fs::remove_dir_all(path).expect("remove unbound legacy fixture");
    }

    #[test]
    fn bound_relational_index_generation_is_backed_up_restored_and_scrubbed() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-store-relational-index-bound-backup-{}-{nonce}",
            std::process::id()
        ));
        let backup = path.with_extension("backup");
        let restored = path.with_extension("restored");
        let corrupt_restored = path.with_extension("corrupt-restored");
        let replay = WalReplayConfig {
            relational_index_mode: hawdb_storage::RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let binding;
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open shadow-enabled backup source");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_recovery_documents_table("doc-1"),
                )
                .expect("commit relational backup source");
            store
                .checkpoint(&catalog)
                .expect("publish bound generation");
            store
                .scrub_storage()
                .expect("scrub bound relational index generation");
            store
                .backup_to(&catalog, &backup)
                .expect("back up bound relational index generation");
            binding = store
                .durable
                .as_ref()
                .expect("durable store")
                .relational_index_generation_artifacts
                .expect("canonical relational index binding");
        }
        let page_name = relational_index_shadow_artifact_file(binding.generation);
        let generation_manifest_name =
            relational_index_shadow_manifest_generation_file(binding.generation);
        assert!(backup.join(&page_name).exists());
        assert!(backup.join(&generation_manifest_name).exists());

        crate::store::restore_storage_backup(&backup, &restored)
            .expect("restore bound relational index generation");
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &restored,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open restored bound generation");
            assert_eq!(
                store
                    .durable
                    .as_ref()
                    .expect("durable restored store")
                    .relational_index_generation_artifacts,
                Some(binding)
            );
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::CheckpointReady { .. }
            ));
        }

        let backup_page_path = backup.join(&page_name);
        let mut backup_page_bytes =
            std::fs::read(&backup_page_path).expect("read backed-up page artifact");
        backup_page_bytes[0] ^= 0xff;
        std::fs::write(&backup_page_path, backup_page_bytes)
            .expect("corrupt backed-up page artifact");
        let restore_error = crate::store::restore_storage_backup(&backup, &corrupt_restored)
            .expect_err("restore must reject a corrupt bound page artifact");
        assert!(restore_error.to_string().contains("verification failed"));
        assert!(!corrupt_restored.exists());

        let page_path = path.join(page_name);
        let mut page_bytes = std::fs::read(&page_path).expect("read bound page artifact");
        page_bytes[0] ^= 0xff;
        std::fs::write(&page_path, page_bytes).expect("corrupt bound page artifact");
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("shadow corruption remains isolated from canonical open");
            let error = store
                .scrub_storage()
                .expect_err("scrub must verify the full bound page artifact");
            assert!(error.to_string().contains("relational index page artifact"));
        }

        std::fs::remove_dir_all(path).expect("remove bound backup source");
        std::fs::remove_dir_all(backup).expect("remove bound backup");
        std::fs::remove_dir_all(restored).expect("remove restored bound backup");
    }

    #[test]
    fn bound_relational_index_reclamation_closes_readers_before_unlink() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-store-relational-index-bound-reclaim-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_mode: hawdb_storage::RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .expect("open generation reclamation store");
        store
            .commit_relational_transaction(&mut catalog, create_recovery_documents_table("doc-1"))
            .expect("commit first relational generation");
        store.checkpoint(&catalog).expect("publish generation one");
        let snapshot = store.snapshot();
        snapshot
            .visit_relational_index_read_view_prefix(
                "documents",
                "documents_owner_idx",
                &RelationalKey(vec![RelationalValue::Text("owner-1".to_string())]),
                RelationalIndexReadLimits::default(),
                |_| true,
            )
            .expect("snapshot has selected index view")
            .expect("open generation-one page reader");

        let insert = |id: &str| RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "documents".to_string(),
                rows: vec![RelationalRow::new(vec![
                    RelationalValue::Text(id.to_string()),
                    RelationalValue::Text("owner-1".to_string()),
                ])],
                mode: RelationalInsertMode::Error,
            }],
        };
        store
            .commit_relational_transaction(&mut catalog, insert("doc-2"))
            .expect("commit second relational generation");
        store
            .checkpoint_with_reader_epoch(&catalog, Some(snapshot.commit_epoch()))
            .expect("retain generation while reader is pinned");
        assert!(path.join(relational_index_shadow_artifact_file(1)).exists());
        assert!(path
            .join(relational_index_shadow_manifest_generation_file(1))
            .exists());
        drop(snapshot);

        store
            .commit_relational_transaction(&mut catalog, insert("doc-3"))
            .expect("commit third relational generation");
        store
            .checkpoint(&catalog)
            .expect("publish after generation-one reader closes");

        assert!(!path.join(relational_index_shadow_artifact_file(1)).exists());
        assert!(!path
            .join(relational_index_shadow_manifest_generation_file(1))
            .exists());
        for generation in [2, 3] {
            assert!(path
                .join(relational_index_shadow_artifact_file(generation))
                .exists());
            assert!(path
                .join(relational_index_shadow_manifest_generation_file(generation))
                .exists());
        }
        drop(store);
        std::fs::remove_dir_all(path).expect("remove generation reclamation fixture");
    }

    #[test]
    fn abandoned_future_candidate_never_replaces_the_selected_generation() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-store-relational-index-abandoned-candidate-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_mode: hawdb_storage::RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let selected_generation;
        let abandoned_generation;
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open shadow-enabled store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_recovery_documents_table("doc-1"),
                )
                .expect("commit first row");
            store.checkpoint(&catalog).expect("publish generation one");
            selected_generation = current_index_view(&store).identity().base_generation;
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![RelationalWrite::Insert {
                            table: "documents".to_string(),
                            rows: vec![hawdb_storage::RelationalRow::new(vec![
                                RelationalValue::Text("doc-2".to_string()),
                                RelationalValue::Text("owner-2".to_string()),
                            ])],
                            mode: hawdb_storage::RelationalInsertMode::Error,
                        }],
                    },
                )
                .expect("commit WAL-only second row");
            let prepared = store
                .prepare_checkpoint(&catalog)
                .expect("prepare future checkpoint")
                .expect("durable checkpoint preparation");
            abandoned_generation = prepared.generation;
            assert!(abandoned_generation > selected_generation);
            assert!(path
                .join(relational_index_shadow_manifest_generation_file(
                    abandoned_generation,
                ))
                .exists());
            drop(prepared);
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("reopen selected checkpoint plus WAL");
            assert_eq!(store.relational_state().row_count("documents"), 2);
            assert_eq!(
                current_index_view(&store).identity().base_generation,
                selected_generation
            );
            assert_eq!(
                current_index_view(&store).identity().visible_commit_epoch,
                2
            );
            assert!(!path
                .join(relational_index_shadow_manifest_generation_file(
                    abandoned_generation,
                ))
                .exists());
            assert!(!path
                .join(hawdb_storage::relational_index_shadow_artifact_file(
                    abandoned_generation,
                ))
                .exists());
        }
        std::fs::remove_dir_all(path).expect("remove abandoned candidate fixture");
    }

    #[test]
    fn reopen_publishes_bounded_relational_index_wal_deltas_after_replay() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-store-relational-index-recovery-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_mode: hawdb_storage::RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open recovery-enabled store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_recovery_documents_table("doc-1"),
                )
                .expect("commit recovery base");
            store
                .checkpoint(&catalog)
                .expect("checkpoint recovery base");
            let base_qualification = store
                .qualify_relational_index_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify base relational index view");
            assert_qualification_ready(&base_qualification, 1, 4);
            assert!(base_qualification.probes.iter().any(|probe| {
                probe.kind == RelationalIndexQualificationProbeKind::LeadingPrefix
                    && matches!(
                        probe.read.backend,
                        RelationalIndexReadViewBackendReport::Base(_)
                    )
            }));
            let truncated = store
                .qualify_relational_index_read_view(RelationalIndexViewQualificationOptions {
                    max_probes: NonZeroUsize::new(1).unwrap(),
                    ..RelationalIndexViewQualificationOptions::default()
                })
                .expect("bound qualification probe count");
            assert!(truncated.truncated);
            assert!(!truncated.ready);
            assert_eq!(truncated.probes.len(), 1);
            let pinned = store.snapshot();
            let pinned_view = current_index_view(&pinned);
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![RelationalWrite::Insert {
                            table: "documents".to_string(),
                            rows: vec![hawdb_storage::RelationalRow::new(vec![
                                RelationalValue::Text("doc-2".to_string()),
                                RelationalValue::Text("owner-1".to_string()),
                            ])],
                            mode: hawdb_storage::RelationalInsertMode::Error,
                        }],
                    },
                )
                .expect("append relational WAL after checkpoint");
            let live_view = current_index_view(&store);
            assert_eq!(live_view.kind(), RelationalIndexReadViewKind::Base);
            assert_eq!(live_view.identity().visible_commit_epoch, 2);
            assert_eq!(live_view.live_batch_count(), 1);
            assert_eq!(live_view.live_entry_count(), 4);
            assert!(live_view.live_encoded_bytes() > 0);
            assert_eq!(pinned_view.identity().visible_commit_epoch, 1);
            assert_eq!(pinned_view.live_batch_count(), 0);
            assert!(!Arc::ptr_eq(pinned_view, live_view));
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::LiveCurrent {
                    visible_commit_epoch: 2,
                    live_batches: 1,
                    live_entries: 4,
                    ..
                }
            ));
            let inserted_qualification = store
                .qualify_relational_index_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify relational index view with live insert");
            assert_qualification_ready(&inserted_qualification, 2, 4);
            assert!(inserted_qualification.probes.iter().all(|probe| {
                probe.read.live_entries_visited == probe.read.live_entries_matched
                    && probe.read.live_entries_visited < 4
            }));

            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![RelationalWrite::DeleteByPrimaryKey {
                            table: "documents".to_string(),
                            keys: vec![RelationalKey(vec![RelationalValue::Text(
                                "doc-1".to_string(),
                            )])],
                        }],
                    },
                )
                .expect("append relational delete after checkpoint");
            let deleted_qualification = store
                .qualify_relational_index_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify relational index view with live delete");
            assert_qualification_ready(&deleted_qualification, 3, 4);
            assert!(deleted_qualification.probes.iter().all(|probe| {
                probe.read.live_entries_visited == probe.read.live_entries_matched
                    && probe.read.live_entries_visited < 8
            }));
            assert!(deleted_qualification.probes.iter().any(|probe| {
                probe.kind == RelationalIndexQualificationProbeKind::LeadingPrefix
                    && probe.candidate_rows == 1
                    && probe.oracle_rows == 1
            }));

            store
                .create_node(&mut catalog, "Document", Default::default())
                .expect("commit graph-only WAL after checkpoint");
            let graph_advanced_view = current_index_view(&store);
            assert_eq!(graph_advanced_view.identity().visible_commit_epoch, 4);
            assert_eq!(graph_advanced_view.live_batch_count(), 2);
            assert_eq!(graph_advanced_view.live_entry_count(), 8);
            let graph_qualification = store
                .qualify_relational_index_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify graph-advanced relational index view");
            assert_qualification_ready(&graph_qualification, 4, 4);
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("replay relational WAL and publish index deltas");
            assert_eq!(store.relational_state().row_count("documents"), 1);
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::WalRecovered {
                    base_commit_epoch: 1,
                    recovered_commit_epoch: 4,
                    delta_pages: 1,
                    delta_entries: 8,
                    ..
                }
            ));
            let report = store
                .relational_index_recovery_report()
                .expect("recovery evidence report");
            assert_eq!(report.delta_entries, 8);
            assert!(report.peak_dirty_bytes > 0);
            let view = current_index_view(&store);
            assert_eq!(view.kind(), RelationalIndexReadViewKind::Recovered);
            assert!(view.identity().delta_generation.is_some());
            assert_eq!(view.identity().base_commit_epoch, 1);
            assert_eq!(view.identity().visible_commit_epoch, 4);
            let recovered_qualification = store
                .qualify_relational_index_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify recovered relational index view");
            assert_qualification_ready(&recovered_qualification, 4, 4);
            assert!(recovered_qualification.probes.iter().all(|probe| {
                matches!(
                    probe.read.backend,
                    RelationalIndexReadViewBackendReport::Recovered(_)
                )
            }));
            assert!(path
                .join(hawdb_storage::RELATIONAL_INDEX_RECOVERY_MANIFEST_FILE)
                .exists());
        }
        std::fs::remove_dir_all(path).expect("remove recovery replay fixture");
    }

    #[test]
    fn constraint_qualification_covers_base_live_recovery_and_pinned_views() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-relational-constraint-qualification-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_mode: RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open constraint qualification store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_constraint_qualification_state(),
                )
                .expect("commit constraint qualification base");
            store
                .checkpoint(&catalog)
                .expect("checkpoint constraint qualification base");

            let base = store
                .qualify_relational_constraint_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify base constraint view");
            assert_constraint_qualification_ready(&base, 1);
            assert!(base.probes.iter().all(|probe| {
                matches!(
                    probe.read.backend,
                    RelationalIndexReadViewBackendReport::Base(_)
                )
            }));

            let truncated = store
                .qualify_relational_constraint_read_view(RelationalIndexViewQualificationOptions {
                    max_probes: NonZeroUsize::new(1).unwrap(),
                    ..RelationalIndexViewQualificationOptions::default()
                })
                .expect("report an exhausted constraint probe budget");
            assert!(truncated.truncated);
            assert!(!truncated.ready);

            let budget_error = store
                .qualify_relational_constraint_read_view(RelationalIndexViewQualificationOptions {
                    read_limits: RelationalIndexReadLimits {
                        max_rows: NonZeroUsize::new(1).unwrap(),
                        ..RelationalIndexReadLimits::default()
                    },
                    ..RelationalIndexViewQualificationOptions::default()
                })
                .expect_err("two foreign-key referrers must exhaust a one-row budget");
            assert!(
                matches!(budget_error, HawDBError::Storage(message) if message.contains("qualification probe"))
            );

            let pinned = Arc::clone(current_index_view(&store));
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![
                            RelationalWrite::Upsert {
                                table: "accounts".to_string(),
                                rows: vec![
                                    constraint_account_row(
                                        "account-c",
                                        "tenant-2",
                                        RelationalValue::Text("c@example.test".to_string()),
                                        "carol",
                                    ),
                                    constraint_account_row(
                                        "ignored-primary-key",
                                        "tenant-2",
                                        RelationalValue::Text("b2@example.test".to_string()),
                                        "bob",
                                    ),
                                ],
                                conflict_columns: vec!["handle".to_string()],
                                action: RelationalConflictAction::Update(vec![
                                    RelationalUpsertAssignment {
                                        column: "tenant".to_string(),
                                        value: RelationalUpsertValue::ExcludedColumn(
                                            "tenant".to_string(),
                                        ),
                                    },
                                    RelationalUpsertAssignment {
                                        column: "email".to_string(),
                                        value: RelationalUpsertValue::ExcludedColumn(
                                            "email".to_string(),
                                        ),
                                    },
                                ]),
                            },
                            RelationalWrite::Insert {
                                table: "sessions".to_string(),
                                rows: vec![constraint_session_row(
                                    "session-3",
                                    "account-c",
                                    RelationalValue::Text("account-b".to_string()),
                                )],
                                mode: RelationalInsertMode::Error,
                            },
                        ],
                    },
                )
                .expect("commit live UPSERT and foreign-key changes");

            let live = store
                .qualify_relational_constraint_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify live constraint view");
            assert_constraint_qualification_ready(&live, 2);
            assert!(live.probes.iter().all(|probe| {
                probe.read.live_entries_visited == probe.read.live_entries_matched
            }));
            assert!(live
                .probes
                .iter()
                .any(|probe| probe.read.live_entries_visited > 0));
            assert!(live
                .probes
                .iter()
                .any(|probe| probe.read.live_entries_visited == 0));

            let carol = RelationalKey(vec![RelationalValue::Text("carol".to_string())]);
            let mut pinned_rows = Vec::new();
            pinned
                .visit_exact_postings(
                    "accounts",
                    "accounts_handle_idx",
                    &carol,
                    RelationalIndexReadLimits::default(),
                    |key| {
                        pinned_rows.push(key.clone());
                        true
                    },
                )
                .expect("read the pre-commit pinned constraint view");
            assert!(pinned_rows.is_empty());
            let mut current_rows = Vec::new();
            current_index_view(&store)
                .visit_exact_postings(
                    "accounts",
                    "accounts_handle_idx",
                    &carol,
                    RelationalIndexReadLimits::default(),
                    |key| {
                        current_rows.push(key.clone());
                        true
                    },
                )
                .expect("read the current constraint view");
            assert_eq!(
                current_rows,
                vec![RelationalKey(vec![RelationalValue::Text(
                    "account-c".to_string()
                )])]
            );
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("reopen constraint qualification store");
            let recovered = store
                .qualify_relational_constraint_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect("qualify recovered constraint view");
            assert_constraint_qualification_ready(&recovered, 2);
            assert!(recovered.probes.iter().all(|probe| {
                matches!(
                    probe.read.backend,
                    RelationalIndexReadViewBackendReport::Recovered(_)
                )
            }));
        }
        std::fs::remove_dir_all(path).expect("remove constraint qualification fixture");
    }

    #[test]
    fn constraint_qualification_fails_closed_on_a_corrupt_selected_page() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-relational-constraint-corruption-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_mode: RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let generation;
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open constraint corruption fixture");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_constraint_qualification_state(),
                )
                .expect("commit constraint corruption source");
            store
                .checkpoint(&catalog)
                .expect("checkpoint constraint corruption source");
            generation = current_index_view(&store).identity().base_generation;
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open cold constraint corruption reader");
            let artifact = path.join(hawdb_storage::relational_index_shadow_artifact_file(
                generation,
            ));
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&artifact)
                .expect("open constraint page artifact");
            let mut first = [0_u8; 1];
            file.read_exact(&mut first).expect("read page prefix");
            first[0] ^= 0xff;
            file.seek(SeekFrom::Start(0)).expect("rewind page artifact");
            file.write_all(&first).expect("corrupt page prefix");
            file.sync_all().expect("sync corrupt page prefix");

            let error = store
                .qualify_relational_constraint_read_view(
                    RelationalIndexViewQualificationOptions::default(),
                )
                .expect_err("constraint qualification must reject a corrupt selected page");
            assert!(
                matches!(error, HawDBError::StorageIntegrity(message) if message.contains("qualification probe"))
            );
            assert_eq!(store.relational_state().row_count("accounts"), 2);
        }
        std::fs::remove_dir_all(path).expect("remove constraint corruption fixture");
    }

    #[test]
    fn schema_wal_invalidates_shadow_recovery_without_blocking_canonical_open() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-store-relational-index-schema-recovery-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_mode: hawdb_storage::RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open schema recovery store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_recovery_documents_table("doc-1"),
                )
                .expect("commit schema recovery base");
            store.checkpoint(&catalog).expect("checkpoint schema base");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![RelationalWrite::CreateIndex {
                            table: "documents".to_string(),
                            index: RelationalIndexSchema {
                                name: "documents_id_idx".to_string(),
                                columns: vec!["id".to_string()],
                                unique: false,
                            },
                        }],
                    },
                )
                .expect("append schema-changing relational WAL");
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::LiveUnavailable {
                    last_visible_commit_epoch: 1,
                    failed_commit_epoch: 2,
                    reason,
                    ..
                } if reason.contains("schema-changing WAL")
            ));
            assert!(store
                .relational_index_shadow
                .current_read_view(store.commit_epoch)
                .is_none());
        }
        {
            let mut catalog = Catalog::default();
            let store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("canonical recovery survives derived schema invalidation");
            assert!(store
                .relational_state()
                .table_schema("documents")
                .expect("recovered documents schema")
                .indexes
                .iter()
                .any(|index| index.name == "documents_id_idx"));
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::RecoveryUnavailable { reason, .. }
                    if reason.contains("schema-changing WAL")
            ));
            assert!(store.relational_index_recovery_report().is_none());
            assert!(store
                .relational_index_shadow
                .current_read_view(store.commit_epoch)
                .is_none());
        }
        std::fs::remove_dir_all(path).expect("remove schema recovery fixture");
    }

    #[test]
    fn authoritative_relational_indexes_gate_constraints_and_recover_live_commits() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-authoritative-relational-index-{}-{nonce}",
            std::process::id()
        ));
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                WalReplayConfig {
                    relational_index_mode: RelationalIndexMode::Shadow,
                    ..WalReplayConfig::default()
                },
            )
            .expect("open authoritative bootstrap store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_constraint_qualification_state(),
                )
                .expect("commit authoritative bootstrap state");
            store
                .checkpoint(&catalog)
                .expect("publish authoritative bootstrap generation");
            assert!(store
                .relational_state()
                .materialized_index_postings_resident());
        }

        let replay = WalReplayConfig {
            relational_index_mode: RelationalIndexMode::Authoritative,
            ..WalReplayConfig::default()
        };
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("open authoritative relational index store");
            store
                .validate_authoritative_relational_index_open()
                .expect("authoritative view is current");
            assert!(!store
                .relational_state()
                .materialized_index_postings_resident());
            assert!(store
                .relational_state()
                .index_lookup(
                    "accounts",
                    "accounts_handle_idx",
                    &RelationalKey(vec![RelationalValue::Text("bob".to_string())]),
                )
                .is_none());
            let pinned_snapshot = store.snapshot();
            pinned_snapshot
                .ensure_usable()
                .expect("authoritative snapshot retains its complete binding");
            let base_epoch = store.commit_epoch;
            let base_lsn = store.durable.as_ref().expect("durable store").next_lsn;

            let duplicate = store.commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "accounts".to_string(),
                        rows: vec![constraint_account_row(
                            "account-c",
                            "tenant-3",
                            RelationalValue::Text("c@example.test".to_string()),
                            "alice",
                        )],
                        mode: RelationalInsertMode::Error,
                    }],
                },
            );
            assert!(
                matches!(duplicate, Err(HawDBError::Storage(message)) if message.contains("duplicate key"))
            );
            assert_eq!(store.commit_epoch, base_epoch);
            assert_eq!(
                store.durable.as_ref().expect("durable store").next_lsn,
                base_lsn
            );

            let missing_foreign_key = store.commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "sessions".to_string(),
                        rows: vec![constraint_session_row(
                            "session-invalid",
                            "account-missing",
                            RelationalValue::Null,
                        )],
                        mode: RelationalInsertMode::Error,
                    }],
                },
            );
            assert!(
                matches!(missing_foreign_key, Err(HawDBError::Storage(message)) if message.contains("no visible target"))
            );
            assert_eq!(store.commit_epoch, base_epoch);
            assert_eq!(
                store.durable.as_ref().expect("durable store").next_lsn,
                base_lsn
            );

            let default_live_limits = store.relational_index_shadow.live_limits;
            store.relational_index_shadow.live_limits = RelationalIndexChangeCaptureLimits {
                max_entries: NonZeroUsize::new(1).expect("test limit is non-zero"),
                max_bytes: default_live_limits.max_bytes,
            };
            let live_budget_failure = store.commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "accounts".to_string(),
                        rows: vec![constraint_account_row(
                            "account-c",
                            "tenant-3",
                            RelationalValue::Text("c@example.test".to_string()),
                            "carol",
                        )],
                        mode: RelationalInsertMode::Error,
                    }],
                },
            );
            assert!(
                matches!(live_budget_failure, Err(HawDBError::Storage(message)) if message.contains("capture"))
            );
            assert_eq!(store.commit_epoch, base_epoch);
            assert_eq!(
                store.durable.as_ref().expect("durable store").next_lsn,
                base_lsn
            );
            store.relational_index_shadow.live_limits = default_live_limits;

            store
                .commit_relational_transaction(
                    &mut catalog,
                    RelationalTransaction {
                        writes: vec![RelationalWrite::Upsert {
                            table: "accounts".to_string(),
                            rows: vec![constraint_account_row(
                                "ignored-primary-key",
                                "tenant-2",
                                RelationalValue::Text("updated@example.test".to_string()),
                                "bob",
                            )],
                            conflict_columns: vec!["handle".to_string()],
                            action: RelationalConflictAction::Update(vec![
                                RelationalUpsertAssignment {
                                    column: "tenant".to_string(),
                                    value: RelationalUpsertValue::ExcludedColumn(
                                        "tenant".to_string(),
                                    ),
                                },
                                RelationalUpsertAssignment {
                                    column: "email".to_string(),
                                    value: RelationalUpsertValue::ExcludedColumn(
                                        "email".to_string(),
                                    ),
                                },
                            ]),
                        }],
                    },
                )
                .expect("commit upsert through authoritative unique index");
            assert_eq!(store.commit_epoch, base_epoch + 1);
            assert_eq!(pinned_snapshot.commit_epoch, base_epoch);
            pinned_snapshot
                .ensure_usable()
                .expect("pinned authoritative snapshot remains usable after publication");
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::LiveCurrent {
                    visible_commit_epoch,
                    ..
                } if *visible_commit_epoch == base_epoch + 1
            ));
            let account_b = store
                .relational_state()
                .row(
                    "accounts",
                    &RelationalKey(vec![RelationalValue::Text("account-b".to_string())]),
                )
                .expect("upserted account");
            assert_eq!(
                account_b.values()[2],
                RelationalValue::Text("updated@example.test".to_string())
            );
            assert!(!store
                .relational_state()
                .materialized_index_postings_resident());
        }
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                replay,
            )
            .expect("recover authoritative live commit");
            store
                .validate_authoritative_relational_index_open()
                .expect("recovered authoritative view is current");
            assert!(!store
                .relational_state()
                .materialized_index_postings_resident());
            assert!(matches!(
                store.relational_index_shadow_recovery_status(),
                RelationalIndexShadowRecoveryStatus::WalRecovered { .. }
            ));
            store
                .checkpoint(&catalog)
                .expect("checkpoint recovered authoritative state");
            assert!(!store
                .relational_state()
                .materialized_index_postings_resident());
        }
        std::fs::remove_dir_all(path).expect("remove authoritative fixture");
    }

    #[test]
    fn authoritative_open_rejects_missing_and_corrupt_bound_generations() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let missing_path = std::env::temp_dir().join(format!(
            "hawdb-authoritative-missing-index-{}-{nonce}",
            std::process::id()
        ));
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &missing_path,
                &mut catalog,
                DurabilityPolicy::default(),
                WalReplayConfig::default(),
            )
            .expect("open unbound materialized store");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_recovery_documents_table("doc-1"),
                )
                .expect("commit unbound source");
            store
                .checkpoint(&catalog)
                .expect("publish checkpoint without index binding");
        }
        let mut catalog = Catalog::default();
        let missing = GraphStore::open_with_durability_and_replay_config(
            &missing_path,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                relational_index_mode: RelationalIndexMode::Authoritative,
                ..WalReplayConfig::default()
            },
        )
        .expect_err("authoritative open must reject a missing binding");
        assert!(missing
            .to_string()
            .contains("authoritative relational index"));

        let corrupt_path = std::env::temp_dir().join(format!(
            "hawdb-authoritative-corrupt-index-{}-{nonce}",
            std::process::id()
        ));
        let generation;
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &corrupt_path,
                &mut catalog,
                DurabilityPolicy::default(),
                WalReplayConfig {
                    relational_index_mode: RelationalIndexMode::Shadow,
                    ..WalReplayConfig::default()
                },
            )
            .expect("open corrupt authoritative bootstrap");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_recovery_documents_table("doc-1"),
                )
                .expect("commit corrupt authoritative source");
            store
                .checkpoint(&catalog)
                .expect("publish bound generation for corruption test");
            generation = current_index_view(&store).identity().base_generation;
        }
        std::fs::write(
            corrupt_path.join(relational_index_shadow_manifest_generation_file(generation)),
            b"corrupt",
        )
        .expect("corrupt bound generation manifest");
        let mut catalog = Catalog::default();
        let corrupt = GraphStore::open_with_durability_and_replay_config(
            &corrupt_path,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                relational_index_mode: RelationalIndexMode::Authoritative,
                ..WalReplayConfig::default()
            },
        )
        .expect_err("authoritative open must reject a corrupt bound generation");
        assert!(corrupt
            .to_string()
            .contains("authoritative relational index"));

        std::fs::remove_dir_all(missing_path).expect("remove missing binding fixture");
        std::fs::remove_dir_all(corrupt_path).expect("remove corrupt binding fixture");
    }

    #[test]
    fn authoritative_constraint_corruption_rejects_before_wal_and_poisons_service() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-authoritative-constraint-corruption-{}-{nonce}",
            std::process::id()
        ));
        let (generation, pages);
        {
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                WalReplayConfig {
                    relational_index_mode: RelationalIndexMode::Shadow,
                    ..WalReplayConfig::default()
                },
            )
            .expect("open authoritative corruption bootstrap");
            store
                .commit_relational_transaction(
                    &mut catalog,
                    create_constraint_qualification_state(),
                )
                .expect("commit authoritative corruption source");
            store
                .checkpoint(&catalog)
                .expect("publish authoritative corruption generation");
            let report = store
                .relational_index_shadow_checkpoint_report()
                .expect("published authoritative corruption report");
            generation = report.generation;
            pages = report.pages_written;
        }

        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            WalReplayConfig {
                relational_index_mode: RelationalIndexMode::Authoritative,
                ..WalReplayConfig::default()
            },
        )
        .expect("open cold authoritative corruption reader");
        let page_bytes = RelationalIndexShadowConfig::default()
            .page_limits
            .max_page_bytes
            .get() as u64;
        let artifact = path.join(relational_index_shadow_artifact_file(generation));
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&artifact)
            .expect("open authoritative page artifact");
        for page in 0..pages {
            file.seek(SeekFrom::Start(page * page_bytes))
                .expect("seek authoritative page");
            let mut byte = [0_u8; 1];
            file.read_exact(&mut byte)
                .expect("read authoritative page byte");
            byte[0] ^= 0xff;
            file.seek(SeekFrom::Start(page * page_bytes))
                .expect("rewind authoritative page");
            file.write_all(&byte)
                .expect("corrupt authoritative page byte");
        }
        file.sync_all().expect("sync authoritative corruption");

        let epoch = store.commit_epoch;
        let next_lsn = store.durable.as_ref().expect("durable store").next_lsn;
        let error = store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "accounts".to_string(),
                        rows: vec![constraint_account_row(
                            "account-c",
                            "tenant-3",
                            RelationalValue::Text("c@example.test".to_string()),
                            "carol",
                        )],
                        mode: RelationalInsertMode::Error,
                    }],
                },
            )
            .expect_err("corrupt authoritative constraint page must reject the mutation");
        assert!(matches!(error, HawDBError::StorageIntegrity(_)));
        assert_eq!(store.commit_epoch, epoch);
        assert_eq!(
            store.durable.as_ref().expect("durable store").next_lsn,
            next_lsn
        );
        assert!(matches!(
            store.ensure_usable(),
            Err(HawDBError::StorageIntegrity(_))
        ));

        drop(file);
        drop(store);
        std::fs::remove_dir_all(path).expect("remove authoritative corruption fixture");
    }

    #[test]
    fn probe_statistics_remain_available_until_the_target_index_changes() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "hawdb-live-index-statistics-{}-{nonce}",
            std::process::id()
        ));
        let replay = WalReplayConfig {
            relational_index_mode: hawdb_storage::RelationalIndexMode::Shadow,
            ..WalReplayConfig::default()
        };
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open_with_durability_and_replay_config(
            &path,
            &mut catalog,
            DurabilityPolicy::default(),
            replay,
        )
        .expect("open live-statistics store");
        store
            .commit_relational_transaction(&mut catalog, create_recovery_documents_table("doc-1"))
            .expect("commit statistics source");
        store
            .checkpoint(&catalog)
            .expect("checkpoint statistics source");

        let initial = store
            .relational_index_probe_statistics("documents", "documents_owner_idx", 1)
            .expect("checkpoint statistics");
        store
            .create_node(&mut catalog, "Document", Default::default())
            .expect("commit unrelated graph mutation");
        assert_eq!(
            store
                .relational_index_probe_statistics("documents", "documents_owner_idx", 1)
                .expect("statistics survive an unrelated graph mutation"),
            initial
        );

        store
            .commit_relational_transaction(
                &mut catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "documents".to_string(),
                        rows: vec![RelationalRow::new(vec![
                            RelationalValue::Text("doc-2".to_string()),
                            RelationalValue::Text("owner-2".to_string()),
                        ])],
                        mode: RelationalInsertMode::Error,
                    }],
                },
            )
            .expect("commit target index mutation");
        assert!(store
            .relational_index_probe_statistics("documents", "documents_owner_idx", 1)
            .is_none());

        drop(store);
        std::fs::remove_dir_all(path).expect("remove live-statistics fixture");
    }

    fn create_recovery_documents_table(first_id: &str) -> RelationalTransaction {
        RelationalTransaction {
            writes: vec![
                RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "documents".to_string(),
                    columns: vec![
                        RelationalColumnSchema {
                            name: "id".to_string(),
                            scalar_type: RelationalScalarType::Text,
                            nullable: false,
                            default: None,
                        },
                        RelationalColumnSchema {
                            name: "owner".to_string(),
                            scalar_type: RelationalScalarType::Text,
                            nullable: false,
                            default: None,
                        },
                    ],
                    primary_key: vec!["id".to_string()],
                    unique_constraints: vec![vec!["id".to_string()]],
                    foreign_keys: Vec::new(),
                    indexes: vec![
                        RelationalIndexSchema {
                            name: "documents_owner_idx".to_string(),
                            columns: vec!["owner".to_string()],
                            unique: false,
                        },
                        RelationalIndexSchema {
                            name: "documents_owner_id_idx".to_string(),
                            columns: vec!["owner".to_string(), "id".to_string()],
                            unique: false,
                        },
                    ],
                }),
                RelationalWrite::Insert {
                    table: "documents".to_string(),
                    rows: vec![hawdb_storage::RelationalRow::new(vec![
                        RelationalValue::Text(first_id.to_string()),
                        RelationalValue::Text("owner-1".to_string()),
                    ])],
                    mode: hawdb_storage::RelationalInsertMode::Error,
                },
            ],
        }
    }

    fn create_constraint_qualification_state() -> RelationalTransaction {
        RelationalTransaction {
            writes: vec![
                RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "accounts".to_string(),
                    columns: vec![
                        constraint_text_column("id", false),
                        constraint_text_column("tenant", false),
                        constraint_text_column("email", true),
                        constraint_text_column("handle", false),
                    ],
                    primary_key: vec!["id".to_string()],
                    unique_constraints: vec![vec!["tenant".to_string(), "email".to_string()]],
                    foreign_keys: Vec::new(),
                    indexes: vec![RelationalIndexSchema {
                        name: "accounts_handle_idx".to_string(),
                        columns: vec!["handle".to_string()],
                        unique: true,
                    }],
                }),
                RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "sessions".to_string(),
                    columns: vec![
                        constraint_text_column("id", false),
                        constraint_text_column("account_id", false),
                        constraint_text_column("inviter_id", true),
                    ],
                    primary_key: vec!["id".to_string()],
                    unique_constraints: Vec::new(),
                    foreign_keys: vec![
                        RelationalForeignKeySchema {
                            columns: vec!["account_id".to_string()],
                            referenced_table: "accounts".to_string(),
                            referenced_columns: vec!["id".to_string()],
                            on_delete: RelationalReferentialAction::Restrict,
                            on_update: RelationalReferentialAction::Restrict,
                        },
                        RelationalForeignKeySchema {
                            columns: vec!["inviter_id".to_string()],
                            referenced_table: "accounts".to_string(),
                            referenced_columns: vec!["id".to_string()],
                            on_delete: RelationalReferentialAction::Restrict,
                            on_update: RelationalReferentialAction::Restrict,
                        },
                    ],
                    indexes: Vec::new(),
                }),
                RelationalWrite::Insert {
                    table: "accounts".to_string(),
                    rows: vec![
                        constraint_account_row(
                            "account-a",
                            "tenant-1",
                            RelationalValue::Null,
                            "alice",
                        ),
                        constraint_account_row(
                            "account-b",
                            "tenant-1",
                            RelationalValue::Text("b@example.test".to_string()),
                            "bob",
                        ),
                    ],
                    mode: RelationalInsertMode::Error,
                },
                RelationalWrite::Insert {
                    table: "sessions".to_string(),
                    rows: vec![
                        constraint_session_row("session-1", "account-a", RelationalValue::Null),
                        constraint_session_row(
                            "session-2",
                            "account-a",
                            RelationalValue::Text("account-a".to_string()),
                        ),
                    ],
                    mode: RelationalInsertMode::Error,
                },
            ],
        }
    }

    fn constraint_text_column(name: &str, nullable: bool) -> RelationalColumnSchema {
        RelationalColumnSchema {
            name: name.to_string(),
            scalar_type: RelationalScalarType::Text,
            nullable,
            default: None,
        }
    }

    fn constraint_account_row(
        id: &str,
        tenant: &str,
        email: RelationalValue,
        handle: &str,
    ) -> RelationalRow {
        RelationalRow::new(vec![
            RelationalValue::Text(id.to_string()),
            RelationalValue::Text(tenant.to_string()),
            email,
            RelationalValue::Text(handle.to_string()),
        ])
    }

    fn constraint_session_row(
        id: &str,
        account_id: &str,
        inviter_id: RelationalValue,
    ) -> RelationalRow {
        RelationalRow::new(vec![
            RelationalValue::Text(id.to_string()),
            RelationalValue::Text(account_id.to_string()),
            inviter_id,
        ])
    }

    fn assert_constraint_qualification_ready(
        report: &RelationalConstraintQualificationReport,
        visible_commit_epoch: u64,
    ) {
        assert_eq!(
            report.protocol,
            RELATIONAL_CONSTRAINT_QUALIFICATION_PROTOCOL
        );
        assert_eq!(report.visible_commit_epoch, visible_commit_epoch);
        assert_eq!(report.tables_discovered, 2);
        assert_eq!(report.tables_sampled, 2);
        assert_eq!(report.unique_targets_discovered, 4);
        assert_eq!(report.nullable_unique_targets_discovered, 1);
        assert_eq!(report.foreign_keys_discovered, 2);
        assert_eq!(report.mismatches, 0);
        assert!(!report.truncated);
        assert!(report.ready);
        assert!(!report.probes.is_empty());
        assert!(report.probes.iter().all(|probe| {
            probe.matched
                && probe.semantic_valid
                && probe.candidate_rows == probe.oracle_rows
                && probe.candidate_digest == probe.oracle_digest
        }));
        let uses = report
            .probes
            .iter()
            .flat_map(|probe| probe.uses.iter().copied())
            .collect::<BTreeSet<_>>();
        let uses_covered = report
            .probes
            .iter()
            .map(|probe| probe.uses.len())
            .sum::<usize>();
        assert_eq!(
            uses,
            BTreeSet::from([
                RelationalConstraintQualificationUse::PrimaryKeyIdentity,
                RelationalConstraintQualificationUse::UniqueEnforcement,
                RelationalConstraintQualificationUse::UpsertConflict,
                RelationalConstraintQualificationUse::ForeignKeyTarget,
                RelationalConstraintQualificationUse::ForeignKeyReferrers,
                RelationalConstraintQualificationUse::NullableUniqueNoConflict,
                RelationalConstraintQualificationUse::AbsentKeyNoConflict,
            ])
        );
        assert_eq!(report.uses_covered, uses_covered);
    }

    fn current_index_view(store: &GraphStore) -> &Arc<RelationalIndexReadView> {
        store
            .relational_index_shadow
            .current_read_view(store.commit_epoch)
            .unwrap_or_else(|| {
                panic!(
                    "store at epoch {} has no generation-pinned relational index view: {:?}",
                    store.commit_epoch,
                    store.relational_index_shadow_recovery_status()
                )
            })
    }

    fn assert_qualification_ready(
        report: &RelationalIndexViewQualificationReport,
        visible_commit_epoch: u64,
        indexes: usize,
    ) {
        assert_eq!(
            report.protocol,
            RELATIONAL_INDEX_VIEW_QUALIFICATION_PROTOCOL
        );
        assert_eq!(report.visible_commit_epoch, visible_commit_epoch);
        assert_eq!(report.tables_discovered, 1);
        assert_eq!(report.tables_sampled, 1);
        assert!(report.rows_sampled > 0);
        assert_eq!(report.indexes_discovered, indexes);
        assert_eq!(report.indexes_probed, indexes);
        assert_eq!(report.mismatches, 0);
        assert!(!report.truncated);
        assert!(report.ready);
        assert!(!report.probes.is_empty());
        assert!(report.probes.iter().all(|probe| {
            probe.matched
                && probe.candidate_rows == probe.oracle_rows
                && probe.candidate_digest == probe.oracle_digest
        }));
    }
}
