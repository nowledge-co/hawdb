// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only advance of a published projection's source epoch (#1756).
//!
//! A host that commits graph changes the projection does not index (for
//! example import bookkeeping) still needs the projection's source epoch to
//! reach the graph epoch. An empty apply plus checkpoint used to rewrite every
//! payload, lexical, vector and layout file to change two integers. The
//! advance here publishes one new selector over the active immutable closure
//! instead: the same segments, mutation runs and resident snapshot, a
//! generation one higher, `source_graph_commit_epoch` set to the target and
//! `content_source_graph_commit_epoch` naming the epoch the content files keep.
//!
//! The selector rename is the only commit point, exactly as for a content
//! publication: before it every reader and reopen sees the old epoch, after it
//! the new one. No other file is created, linked or rewritten. Whether the
//! range may be skipped is not decided here; the embedded store proves it
//! against its own search change log before calling these functions.

use super::*;
use hawdb_core::SearchProjectionEpochAdvanceRefusal;

/// Outcome of [`SearchIndex::advance_source_graph_commit_epoch`] and
/// [`SearchOutOfCoreReader::advance_source_graph_commit_epoch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionEpochAdvanceReport {
    pub source_graph_commit_epoch_before: u64,
    pub source_graph_commit_epoch_after: u64,
    /// `false` when the projection was already at the target; nothing was written.
    pub advanced: bool,
    /// The selector generation that is active after the call.
    pub projection_generation: u64,
    /// The epoch stamped in the snapshot, lexical and vector files, which an
    /// advance leaves unchanged.
    pub content_source_graph_commit_epoch: u64,
    /// Bytes written: only the new selector manifest, or zero for a no-op.
    pub metadata_bytes_written: u64,
    /// Search change-log entries in the advanced range that the caller
    /// inspected. Zero when the advance was called below the embedded store.
    pub checked_change_log_entries: usize,
    /// Relational primary-key changes in the range allowed only because their
    /// tables were declared non-projection tables at open. Zero below the
    /// embedded store.
    pub allowed_relational_change_count: usize,
}

pub(crate) fn epoch_advance_refused(
    reason: SearchProjectionEpochAdvanceRefusal,
    source_graph_commit_epoch: Option<u64>,
    target_graph_commit_epoch: u64,
    message: impl Into<String>,
) -> HawDBError {
    HawDBError::SearchProjectionEpochAdvanceRefused {
        reason,
        source_graph_commit_epoch,
        target_graph_commit_epoch,
        message: message.into(),
    }
}

/// Checks the requested target against the current epoch. Returns `None` for
/// a no-op (already at the target).
fn admit_target(source: Option<u64>, target: u64) -> Result<Option<u64>> {
    let Some(source) = source else {
        return Err(epoch_advance_refused(
            SearchProjectionEpochAdvanceRefusal::RangeUnavailable,
            None,
            target,
            "the search projection has no source graph commit epoch to advance from",
        ));
    };
    if target < source {
        return Err(HawDBError::Semantic(format!(
            "search projection epoch advance target {target} is below the projection's source epoch {source}"
        )));
    }
    Ok((target > source).then_some(source))
}

fn no_op_report(
    manifest: &SearchOutOfCoreManifestBody,
    epoch: u64,
) -> SearchProjectionEpochAdvanceReport {
    SearchProjectionEpochAdvanceReport {
        source_graph_commit_epoch_before: epoch,
        source_graph_commit_epoch_after: epoch,
        advanced: false,
        projection_generation: manifest.generation,
        content_source_graph_commit_epoch: manifest
            .content_source_graph_commit_epoch
            .unwrap_or(epoch),
        metadata_bytes_written: 0,
        checked_change_log_entries: 0,
        allowed_relational_change_count: 0,
    }
}

/// Publishes the advanced selector. The caller holds `root`'s publication
/// lease and has already decoded `active` from its selector under that lease.
fn publish(
    root: &Path,
    active: &SearchOutOfCoreManifestBody,
    source: u64,
    target: u64,
) -> Result<SearchProjectionEpochAdvanceReport> {
    let content = active.content_source_graph_commit_epoch.unwrap_or(source);
    let mut next = active.clone();
    next.generation = active
        .generation
        .checked_add(1)
        .ok_or_else(|| HawDBError::Storage("search out-of-core generation overflow".into()))?;
    next.source_graph_commit_epoch = Some(target);
    next.content_source_graph_commit_epoch = Some(content);
    let bytes = next.encode()?;
    let manifest_path = root.join(OUT_OF_CORE_MANIFEST_FILE);
    let tmp_path = manifest_path.with_extension(format!("tmp.{}", next.generation));
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    #[cfg(test)]
    resident_checkpoint_fault(ResidentCheckpointStep::EpochAdvanceBeforeSelector)?;
    durable_replace_file(&tmp_path, &manifest_path)?;
    #[cfg(test)]
    resident_checkpoint_fault(ResidentCheckpointStep::EpochAdvanceAfterSelector)?;
    Ok(SearchProjectionEpochAdvanceReport {
        source_graph_commit_epoch_before: source,
        source_graph_commit_epoch_after: target,
        advanced: true,
        projection_generation: next.generation,
        content_source_graph_commit_epoch: content,
        metadata_bytes_written: bytes.len() as u64,
        checked_change_log_entries: 0,
        allowed_relational_change_count: 0,
    })
}

fn read_active(root: &Path) -> Result<Option<(Vec<u8>, SearchOutOfCoreManifestBody)>> {
    let path = root.join(OUT_OF_CORE_MANIFEST_FILE);
    if !fs::try_exists(&path)? {
        return Ok(None);
    }
    let bytes = read_bounded_file(&path, MAX_OUT_OF_CORE_MANIFEST_BYTES)?;
    let manifest = SearchOutOfCoreManifestBody::decode(&bytes)?;
    Ok(Some((bytes, manifest)))
}

impl SearchIndex {
    /// Advances a persistent, unregistered resident projection's source epoch
    /// to `target` by publishing only a new selector manifest; the snapshot,
    /// segment, lexical, vector and layout files are reused as they are.
    ///
    /// The caller must already have proved that no graph commit in
    /// `(source, target]` changed anything the projection indexes; this
    /// method cannot see the graph. [`crate`] users reach it through
    /// `EmbeddedStoreHandle::advance_search_projection_source_epoch`, which
    /// performs that check against the database's own search change log.
    ///
    /// Refuses with [`HawDBError::SearchProjectionEpochAdvanceRefused`] when
    /// the in-memory projection differs from its last checkpoint
    /// (`UncheckpointedChanges`) or has no source epoch (`RangeUnavailable`).
    pub fn advance_source_graph_commit_epoch(
        &mut self,
        target: u64,
    ) -> Result<SearchProjectionEpochAdvanceReport> {
        self.require_unregistered_publication()?;
        let path = self.path.clone().ok_or_else(|| {
            HawDBError::Semantic(
                "an in-memory search projection has no published epoch to advance".into(),
            )
        })?;
        let source = self.source_graph_commit_epoch;
        let durable = *self
            .durable_source_graph_commit_epoch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let uncheckpointed = |message: &str| {
            epoch_advance_refused(
                SearchProjectionEpochAdvanceRefusal::UncheckpointedChanges,
                durable,
                target,
                message,
            )
        };
        if source != durable {
            return Err(uncheckpointed(
                "the resident projection's source epoch is not checkpointed; checkpoint it, or apply and checkpoint the range",
            ));
        }
        let advance_from = admit_target(source, target)?;
        let _lease = SearchProjectionPublishLease::acquire(&path)?;
        let Some((bytes, active)) = read_active(&path)? else {
            return Err(uncheckpointed(
                "the resident projection has never been checkpointed",
            ));
        };
        // The selector must describe exactly the in-memory documents, epochs
        // and vector identity, so that skipping the rewrite loses nothing.
        match validate_resident_snapshot_manifest(self, &bytes) {
            Ok(_) => {}
            Err(HawDBError::SearchProjectionMismatch { .. }) => {
                return Err(uncheckpointed(
                    "the resident projection differs from its last checkpoint; apply and checkpoint the range",
                ));
            }
            Err(error) => return Err(error),
        }
        if active.source_graph_commit_epoch != source {
            return Err(uncheckpointed(
                "the published selector is at a different source epoch than the resident projection",
            ));
        }
        let Some(from) = advance_from else {
            return Ok(no_op_report(&active, target));
        };
        let report = publish(&path, &active, from, target)?;
        *self
            .artifact_source_graph_commit_epoch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(report.content_source_graph_commit_epoch);
        self.source_graph_commit_epoch = Some(target);
        *self
            .durable_source_graph_commit_epoch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(target);
        Ok(report)
    }

    /// Reopen half: the selector records an advance past the loaded stamp.
    pub(crate) fn apply_published_epoch_advance(&mut self, epoch: u64) {
        *self
            .artifact_source_graph_commit_epoch
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = self.source_graph_commit_epoch;
        self.source_graph_commit_epoch = Some(epoch);
        *self
            .durable_source_graph_commit_epoch
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(epoch);
    }

    /// The epoch the loaded snapshot, lexical and vector artifacts carry.
    pub(crate) fn artifact_source_graph_commit_epoch(&self) -> Option<u64> {
        self.artifact_source_graph_commit_epoch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .or(self.source_graph_commit_epoch)
    }
}

impl SearchOutOfCoreReader {
    /// Advances the durable head of an unregistered out-of-core projection to
    /// `target` by publishing only a new selector manifest over this reader's
    /// immutable closure. Refresh the reader afterwards to observe it.
    ///
    /// As for [`SearchIndex::advance_source_graph_commit_epoch`], the caller
    /// must already have proved that the range changed nothing the projection
    /// indexes. A selector that moved since this reader was opened fails with
    /// the same generation conflict as a content publication.
    pub fn advance_source_graph_commit_epoch(
        &self,
        target: u64,
    ) -> Result<SearchProjectionEpochAdvanceReport> {
        let advance_from = admit_target(self.manifest.source_graph_commit_epoch, target)?;
        let _lease = SearchProjectionPublishLease::acquire(&self.root)?;
        let Some((_, active)) = read_active(&self.root)? else {
            return Err(HawDBError::StorageIntegrity(
                "search out-of-core selector disappeared under its reader".into(),
            ));
        };
        if active != self.manifest {
            return Err(generation_writer::generation_changed(
                self.manifest.generation,
                active.generation,
            ));
        }
        let Some(from) = advance_from else {
            return Ok(no_op_report(&active, target));
        };
        publish(&self.root, &active, from, target)
    }
}

#[cfg(test)]
mod tests;
