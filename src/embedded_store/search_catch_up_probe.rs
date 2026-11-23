// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Read-only probe of the pending search projection change batch, the
//! recommended pre-check for bounded catch-up (#1924).

use super::*;

/// The next change batch bounded search projection catch-up would select,
/// measured without hydrating or publishing anything.
///
/// `covers_source` is the condition under which a catch-up with the same
/// per-batch change budget can publish: when it is `false`, the batch is a
/// prefix ending before the source head and catch-up refuses it with
/// [`HawDBError::SearchProjectionPrefixBehindSource`]. Probe again with a
/// larger budget (at least `operation_count` of a covering probe), or
/// re-prepare through the staged registered-consumer path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchProjectionChangeBatchProbe {
    /// The projection watermark the batch starts after.
    pub start_commit_epoch: u64,
    /// Changefeed operations in the selected batch.
    pub operation_count: usize,
    /// The last source commit the batch covers completely.
    pub complete_through_commit_epoch: Option<u64>,
    /// The graph commit epoch observed after selecting the batch.
    pub source_commit_epoch: u64,
    /// Whether the batch covers every commit through `source_commit_epoch`.
    pub covers_source: bool,
}

/// The projection bounded catch-up maintains, for read-only changefeed checks.
pub(super) enum MaintainedSearchProjection<'a> {
    Resident(&'a SearchIndex),
    Incremental(SearchProjectionFreshness),
}

impl EmbeddedStore {
    /// Resolves the projection that catch-up maintains. A missing, detached,
    /// unmaintained or ambiguous projection is a state or configuration
    /// condition, not storage damage, so it is a non-poisoning `Semantic`
    /// error rather than `Storage` (#1924). An incremental reader that failed
    /// to attach its durable head is detached and reported as not attached;
    /// the attach failure itself was returned by the call that hit it.
    pub(super) fn maintained_search_projection(&self) -> Result<MaintainedSearchProjection<'_>> {
        match (
            self.search_projection.as_ref(),
            self.out_of_core_search_projection.as_ref(),
        ) {
            (Some(projection), None) => Ok(MaintainedSearchProjection::Resident(projection.index())),
            (None, Some(projection)) if self.incremental_search_enabled() => Ok(
                MaintainedSearchProjection::Incremental(projection.freshness()),
            ),
            (None, Some(_)) => Err(HawDBError::Semantic(
                "search projection has no catch-up maintenance: this out-of-core projection is qualified read-only; open it with incremental search maintenance"
                    .into(),
            )),
            (None, None) => Err(HawDBError::Semantic(
                "search projection is not attached: open the store with a search projection, or reopen it after its incremental reader failed to attach the durable head"
                    .into(),
            )),
            (Some(_), Some(_)) => Err(HawDBError::Semantic(
                "search projection ownership is ambiguous: both a resident and an out-of-core projection are attached"
                    .into(),
            )),
        }
    }

    /// Selects the change batch that bounded catch-up with a per-batch change
    /// budget of `max_operations` would apply next, without hydration,
    /// application, checkpoint or watermark changes. Returns `None` when the
    /// projection is caught up. Both full-residency and incremental
    /// maintenance select from the same watermark their catch-up uses.
    ///
    /// Use it before `catch_up_search_projection*`: catch up only when
    /// `covers_source` is true. A single source commit larger than
    /// `max_operations` is refused with the typed
    /// [`HawDBError::SearchProjectionBatchBudgetExceeded`], exactly as
    /// catch-up would refuse it. Neither refusal is a storage failure. A
    /// store without a maintained projection (none attached, a detached
    /// incremental reader, or a qualified read-only out-of-core projection)
    /// is a non-poisoning `Semantic` error.
    pub fn search_projection_change_batch_probe(
        &self,
        max_operations: usize,
    ) -> Result<Option<SearchProjectionChangeBatchProbe>> {
        if max_operations == 0 {
            return Err(HawDBError::Semantic(
                "search projection change batch probe max_operations must be greater than zero"
                    .to_string(),
            ));
        }
        let database = self.graph.database();
        let (start_commit_epoch, batch) = match self.maintained_search_projection()? {
            MaintainedSearchProjection::Incremental(freshness) => {
                let start = freshness.durable_source_graph_commit_epoch.unwrap_or(0);
                (
                    start,
                    database
                        .build_search_projection_change_batch_after(start, Some(max_operations))?,
                )
            }
            MaintainedSearchProjection::Resident(index) => {
                let start = index
                    .projection_freshness()
                    .source_graph_commit_epoch
                    .unwrap_or(0);
                (
                    start,
                    database.build_search_projection_change_batch_from_freshness(
                        index,
                        Some(max_operations),
                    )?,
                )
            }
        };
        let Some(batch) = batch else {
            return Ok(None);
        };
        // Read the head after selection so a concurrent commit can only make
        // `covers_source` conservative, never optimistic.
        let source_commit_epoch = database.commit_epoch()?;
        let complete_through_commit_epoch = batch.complete_through_commit_epoch();
        Ok(Some(SearchProjectionChangeBatchProbe {
            start_commit_epoch,
            operation_count: batch.operation_count(),
            complete_through_commit_epoch,
            source_commit_epoch,
            covers_source: complete_through_commit_epoch == Some(source_commit_epoch),
        }))
    }
}

impl EmbeddedStoreHandle {
    /// Read-only pre-check for bounded search projection catch-up. See
    /// [`EmbeddedStore::search_projection_change_batch_probe`].
    pub fn search_projection_change_batch_probe(
        &self,
        max_operations: usize,
    ) -> Result<Option<SearchProjectionChangeBatchProbe>> {
        self.read_store()?
            .search_projection_change_batch_probe(max_operations)
    }
}
