// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Verified, metadata-only advance of the search projection's source graph
//! commit epoch (#1756). See `docs/SEARCH_PROJECTION_EPOCH_ADVANCE.md`.

use super::*;
use crate::search::SearchProjectionEpochAdvanceReport;
use hawdb_core::SearchProjectionEpochAdvanceRefusal;

impl EmbeddedStoreHandle {
    /// Advances the search projection's source graph commit epoch to
    /// `target_graph_commit_epoch` without rewriting any projection data file,
    /// after proving that the graph commits in between changed nothing the
    /// projection indexes. See
    /// [`EmbeddedStore::advance_search_projection_source_epoch`].
    pub fn advance_search_projection_source_epoch(
        &self,
        target_graph_commit_epoch: u64,
    ) -> Result<SearchProjectionEpochAdvanceReport> {
        let _permit = self.admit_typed_maintenance(RuntimeWorkPriority::Background, 0, 1)?;
        self.write_store()?
            .advance_search_projection_source_epoch(target_graph_commit_epoch)
    }
}

impl EmbeddedStore {
    /// Moves the search projection's source graph commit epoch from its
    /// current value `S` to `target_graph_commit_epoch` (`T`) by publishing
    /// only a new selector manifest. Payload, lexical, vector, layout and
    /// snapshot files are reused as they are, so the cost is one manifest
    /// write (kilobytes) and one directory sync, independent of corpus size.
    /// An empty apply followed by a checkpoint rewrites the whole projection
    /// instead.
    ///
    /// **Safety contract.** HawDB does not trust the caller. `T` must be the
    /// current graph commit epoch. Holding the store write lock (this
    /// `&mut self`) and, for the whole check, the database runtime's read
    /// guard, which every graph commit needs for writing, HawDB checks its own
    /// search change log for `(S, T]`. The log must cover the whole range, and
    /// every commit in it must have an empty projection capture:
    /// - no node that exists at `T` with a label that maps to a search
    ///   projection kind;
    /// - no search document deletion;
    /// - no relational primary-key change, except to tables declared with
    ///   [`EmbeddedStoreOpenOptions::with_non_projection_relational_tables`]
    ///   (counted in `allowed_relational_change_count`);
    /// - no relational change that cannot be attributed to tables.
    ///
    /// Graph commits that only touch non-projection labels, such as import
    /// bookkeeping, and additive schema statements qualify. Otherwise it
    /// publishes nothing and returns
    /// [`HawDBError::SearchProjectionEpochAdvanceRefused`], which is not a
    /// storage failure: apply or catch up the range normally instead.
    /// Refusal reasons:
    /// - `ProjectionChanged`;
    /// - `ChangeLogGap`: a commit in the range could not be captured, or the
    ///   log was trimmed or begins after a reopen;
    /// - `RangeUnavailable`: no source epoch, or `T` is not the graph's
    ///   current commit epoch;
    /// - `UncheckpointedChanges`: a resident projection whose in-memory state
    ///   differs from its last checkpoint.
    ///
    /// **Crash safety.** The selector rename is the only commit point, as in
    /// every projection publication: after a crash or reopen the projection
    /// is entirely at `S` or entirely at `T`.
    ///
    /// Covers the resident projection (as used by
    /// [`Self::apply_initial_import_projection_delta_and_checkpoint`] and
    /// [`Self::apply_search_projection_delta_and_checkpoint`]) and incremental
    /// out-of-core maintenance. `T == S` (the projection is already at the
    /// current epoch) returns a report with `advanced == false` and writes
    /// nothing; `T < S` is a semantic error.
    pub fn advance_search_projection_source_epoch(
        &mut self,
        target_graph_commit_epoch: u64,
    ) -> Result<SearchProjectionEpochAdvanceReport> {
        let target = target_graph_commit_epoch;
        if self.incremental_search_enabled() {
            // A previous response may have been lost after publication.
            self.refresh_incremental_search_reader()?;
            let projection = self.out_of_core_search_projection.as_ref().ok_or_else(|| {
                HawDBError::Semantic("incremental search maintenance is not configured".into())
            })?;
            let source = projection.reader.source_graph_commit_epoch();
            let checked = self.verify_search_projection_epoch_range(source, target)?;
            let result = projection.reader.advance_source_graph_commit_epoch(target);
            // Refresh even on error: never infer rollback from an error alone.
            self.refresh_incremental_search_reader()?;
            let mut report = result?;
            report.checked_change_log_entries = checked.checked_change_log_entries;
            report.allowed_relational_change_count = checked.allowed_relational_change_count;
            return Ok(report);
        }
        let projection = require_search_projection_mut(&mut self.search_projection)?;
        let durable = projection
            .index()
            .projection_freshness()
            .durable_source_graph_commit_epoch;
        if projection.uncheckpointed_initial_import_deltas != 0 {
            return Err(HawDBError::SearchProjectionEpochAdvanceRefused {
                reason: SearchProjectionEpochAdvanceRefusal::UncheckpointedChanges,
                source_graph_commit_epoch: durable,
                target_graph_commit_epoch: target,
                message:
                    "initial-import pages are applied but not checkpointed; checkpoint them first"
                        .into(),
            });
        }
        let source = projection
            .index()
            .projection_freshness()
            .source_graph_commit_epoch;
        if source != durable {
            return Err(HawDBError::SearchProjectionEpochAdvanceRefused {
                reason: SearchProjectionEpochAdvanceRefusal::UncheckpointedChanges,
                source_graph_commit_epoch: durable,
                target_graph_commit_epoch: target,
                message: "the resident projection's source epoch is not checkpointed; checkpoint it, or apply and checkpoint the range".into(),
            });
        }
        let checked = self.verify_search_projection_epoch_range(source, target)?;
        let projection = require_search_projection_mut(&mut self.search_projection)?;
        let mut report = projection
            .index_mut()
            .advance_source_graph_commit_epoch(target)?;
        report.checked_change_log_entries = checked.checked_change_log_entries;
        report.allowed_relational_change_count = checked.allowed_relational_change_count;
        Ok(report)
    }

    fn verify_search_projection_epoch_range(
        &self,
        source: Option<u64>,
        target: u64,
    ) -> Result<crate::SearchProjectionEpochAdvanceVerification> {
        let Some(source) = source else {
            return Err(HawDBError::SearchProjectionEpochAdvanceRefused {
                reason: SearchProjectionEpochAdvanceRefusal::RangeUnavailable,
                source_graph_commit_epoch: None,
                target_graph_commit_epoch: target,
                message: "the search projection has no source graph commit epoch to advance from"
                    .into(),
            });
        };
        if target < source {
            return Err(HawDBError::Semantic(format!(
                "search projection epoch advance target {target} is below the projection's source epoch {source}"
            )));
        }
        self.graph
            .database()
            .verify_search_projection_epoch_advance(
                source,
                target,
                &self.non_projection_relational_tables,
            )
    }
}
