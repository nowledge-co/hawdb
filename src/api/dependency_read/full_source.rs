// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Full baseline input and tracked reads share one immutable source epoch.
use super::projection_feed::with_projection_read;
use super::*;
use hawdb_storage::store::ProjectionFullSourceCursor;
pub use hawdb_storage::store::{ProjectionFullSourceItem, ProjectionFullSourceLimits};

/// Exhaustive graph candidates and canonical relational keys for baseline work.
/// Completion is input enumeration, not a verified baseline or stage receipt.
/// Owns source/history pins; source changes conservatively invalidate its reads.
#[derive(Debug)]
pub struct PreparedProjectionFullSource {
    prepared: PreparedRead,
    cursor: ProjectionFullSourceCursor,
    failed: bool,
}
impl Database {
    pub fn begin_prepared_full_projection_source(
        &self,
        reads: PreparedReadLimits,
        total: ProjectionFeedLimits,
        source: ProjectionFullSourceLimits,
        task: hawdb_core::RuntimeTaskContext,
    ) -> std::result::Result<PreparedProjectionFullSource, ProjectionFeedError> {
        task.checkpoint()
            .map_err(|e| ProjectionFeedError::Source(HawDBError::Execution(e.to_string())))?;
        let mut prepared = self
            .begin_prepared_read(reads)
            .map_err(ProjectionFeedError::Source)?;
        prepared
            .dependencies
            .graph
            .record(VersionKey::Database)
            .map_err(|e| ProjectionFeedError::Source(HawDBError::Execution(e.to_string())))?;
        prepared.read.task_context = Some(task.clone());
        let cursor = prepared
            .read
            .store
            .capture_full_projection_source(total, source, task)?;
        Ok(PreparedProjectionFullSource {
            prepared,
            cursor,
            failed: false,
        })
    }
}
impl PreparedProjectionFullSource {
    pub fn source_epoch(&self) -> u64 {
        self.cursor.source_epoch()
    }
    pub fn usage(&self) -> ProjectionFeedUsage {
        self.cursor.usage()
    }
    pub fn is_complete(&self) -> bool {
        !self.failed && self.cursor.is_complete()
    }

    /// A step admits candidate/key/metadata work and input bytes. Missing graph
    /// IDs count too. Row-page reads have separate per-seek limits; logical step
    /// counters are not a physical-I/O, RSS or elapsed-time bound. The cursor's
    /// two bounded key slots may retain a measured key across an input-byte yield.
    pub fn visit_next(
        &mut self,
        step: ProjectionFeedLimits,
        mut visitor: impl FnMut(&ProjectionFeedRead<'_>, ProjectionFullSourceItem<'_>) -> Result<()>,
    ) -> std::result::Result<ProjectionFeedStep, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        self.failed = true;
        self.prepared
            .read
            .store
            .ensure_usable()
            .map_err(ProjectionFeedError::Source)?;
        let prepared = &mut self.prepared;
        let result = self.cursor.visit_next(step, |item| {
            with_projection_read(prepared, |context| visitor(context, item))
        });
        if result.is_ok() {
            self.failed = false;
        }
        result
    }

    /// No output coverage is minted: a mapper/stage must still be verified and
    /// bound to the complete baseline by the artifact-owning session protocol.
    pub fn finish(self) -> std::result::Result<PreparedReadDependencies, ProjectionFeedError> {
        if self.failed {
            return Err(ProjectionFeedError::Failed);
        }
        self.cursor.finish()?;
        self.prepared
            .read
            .store
            .ensure_usable()
            .map_err(ProjectionFeedError::Source)?;
        self.prepared.finish().map_err(ProjectionFeedError::Source)
    }
}
