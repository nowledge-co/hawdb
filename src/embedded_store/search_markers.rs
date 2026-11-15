// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Host-requested staleness markers on the search projection (#1809).

use super::*;

/// Longest reason a host may record; reasons are stored one per line.
pub const MAX_SEARCH_PROJECTION_MARKER_REASON_BYTES: usize = 1024;

/// Which staleness marker a host records on the search projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchProjectionMarker {
    FullReindexNeeded,
    MetadataRepairNeeded,
}

impl EmbeddedStoreHandle {
    /// Marks the search projection as needing a full reindex, for example
    /// after an external vector store rebuilt every embedding.
    ///
    /// This writes the projection's existing `.reindex_needed` marker, which
    /// [`SearchProjectionFreshness::full_reindex_needed`] reports in
    /// [`Self::runtime_status`], search candidate reports and readiness.
    /// HawDB never clears it here: only a full reindex (a rebuild of the
    /// projection) or the host's own preparation flow clears it. Recording
    /// the same reason again is a no-op; a different reason is added.
    ///
    /// Requires write admission and a writable graph. A read-only handle, a
    /// handle without a search projection, and an empty, multi-line or longer
    /// than [`MAX_SEARCH_PROJECTION_MARKER_REASON_BYTES`] reason are refused
    /// before anything is written, with `Execution` (read-only) or `Semantic`
    /// errors. Returns the projection freshness after the mark.
    pub fn mark_search_projection_full_reindex_needed(
        &self,
        reason: &str,
    ) -> Result<SearchProjectionFreshness> {
        self.mark_search_projection(SearchProjectionMarker::FullReindexNeeded, reason)
    }

    /// Marks the search projection's metadata as needing repair, under the
    /// existing `.projection_metadata_repair_needed` marker
    /// ([`SearchProjectionFreshness::metadata_repair_needed`]). The marker
    /// keeps the latest reason. Otherwise behaves like
    /// [`Self::mark_search_projection_full_reindex_needed`].
    pub fn mark_search_projection_metadata_repair_needed(
        &self,
        reason: &str,
    ) -> Result<SearchProjectionFreshness> {
        self.mark_search_projection(SearchProjectionMarker::MetadataRepairNeeded, reason)
    }

    fn mark_search_projection(
        &self,
        marker: SearchProjectionMarker,
        reason: &str,
    ) -> Result<SearchProjectionFreshness> {
        validate_marker_reason(reason)?;
        let _permit = self.admit_typed_maintenance(RuntimeWorkPriority::Background, 0, 1)?;
        self.write_store()?.mark_search_projection(marker, reason)
    }
}

impl EmbeddedStore {
    fn mark_search_projection(
        &mut self,
        marker: SearchProjectionMarker,
        reason: &str,
    ) -> Result<SearchProjectionFreshness> {
        if self.graph.database().config().read_only {
            return Err(HawDBError::Execution(
                "database is opened in read-only mode; search projection markers need a writable handle"
                    .to_string(),
            ));
        }
        match (
            self.search_projection.as_ref(),
            self.out_of_core_search_projection.as_ref(),
        ) {
            (Some(projection), None) => {
                let index = projection.index();
                match marker {
                    SearchProjectionMarker::FullReindexNeeded => {
                        index.mark_full_reindex_needed(reason)?
                    }
                    SearchProjectionMarker::MetadataRepairNeeded => {
                        index.mark_metadata_repair_needed(reason)?
                    }
                }
                Ok(projection.freshness())
            }
            (None, Some(projection)) => {
                let reader = projection.reader();
                match marker {
                    SearchProjectionMarker::FullReindexNeeded => {
                        reader.mark_full_reindex_needed(reason)?
                    }
                    SearchProjectionMarker::MetadataRepairNeeded => {
                        reader.mark_metadata_repair_needed(reason)?
                    }
                }
                Ok(projection.freshness())
            }
            _ => Err(HawDBError::Semantic(
                "no search projection is configured on this handle".to_string(),
            )),
        }
    }
}

fn validate_marker_reason(reason: &str) -> Result<()> {
    if reason.trim().is_empty() {
        return Err(HawDBError::Semantic(
            "search projection marker reason must not be empty".to_string(),
        ));
    }
    if reason.contains(['\n', '\r']) {
        return Err(HawDBError::Semantic(
            "search projection marker reason must be a single line".to_string(),
        ));
    }
    if reason.trim() != reason {
        return Err(HawDBError::Semantic(
            "search projection marker reason must not start or end with whitespace".to_string(),
        ));
    }
    if reason.len() > MAX_SEARCH_PROJECTION_MARKER_REASON_BYTES {
        return Err(HawDBError::Semantic(format!(
            "search projection marker reason exceeds {MAX_SEARCH_PROJECTION_MARKER_REASON_BYTES} bytes"
        )));
    }
    Ok(())
}
