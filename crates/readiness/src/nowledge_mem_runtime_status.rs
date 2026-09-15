//! Host-neutral runtime and production status for embedded Nowledge Mem.
//!
//! The root Skein facade samples graph, storage, and search state. This module
//! owns the typed reduction into observable readiness contracts.

use crate::bounded_read_evidence::NowledgeMemGraphMode;
use skein_route_ownership::graph::NowledgeMemRouteOwnershipReadinessReport;
use skein_search::SearchProjectionFreshness;
use skein_storage::{SearchProjectionChangefeedStatus, SearchProjectionMutationId};

pub const NOWLEDGE_MEM_RUNTIME_STATUS_PROTOCOL: &str = "skein-nowledge-mem-runtime-status-v1";
pub const NOWLEDGE_MEM_PRODUCTION_STATUS_PROTOCOL: &str = "skein-nowledge-mem-production-status-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemRuntimeStatus {
    pub protocol: String,
    pub graph_commit_epoch: u64,
    pub changefeed: SearchProjectionChangefeedStatus,
    pub projection_freshness: Option<SearchProjectionFreshness>,
}

impl NowledgeMemRuntimeStatus {
    pub fn projection_commit_lag(&self) -> u64 {
        self.changefeed.projection_commit_lag_after(
            self.projection_freshness
                .as_ref()
                .and_then(|freshness| freshness.durable_source_graph_commit_epoch)
                .unwrap_or(0),
        )
    }

    pub fn projection_stale(&self) -> bool {
        self.projection_commit_lag() > 0
    }

    pub fn json(&self) -> serde_json::Value {
        let freshness = self.projection_freshness.as_ref();
        serde_json::json!({
            "protocol": self.protocol,
            "graph_commit_epoch": self.graph_commit_epoch,
            "changefeed": {
                "graph_commit_epoch": self.changefeed.graph_commit_epoch,
                "resume_floor_commit_epoch": self.changefeed.resume_floor_commit_epoch,
                "oldest_retained_mutation_id": self.changefeed.oldest_retained_mutation_id.map(SearchProjectionMutationId::commit_epoch),
                "newest_retained_mutation_id": self.changefeed.newest_retained_mutation_id.map(SearchProjectionMutationId::commit_epoch),
                "retained_mutation_count": self.changefeed.retained_mutation_count,
                "restart_recoverable": self.changefeed.restart_recoverable,
            },
            "projection": {
                "opened": freshness.is_some(),
                "document_count": freshness.map(|freshness| freshness.document_count),
                "source_graph_commit_epoch": freshness.and_then(|freshness| freshness.source_graph_commit_epoch),
                "durable_source_graph_commit_epoch": freshness.and_then(|freshness| freshness.durable_source_graph_commit_epoch),
                "has_uncheckpointed_changes": freshness.is_some_and(|freshness| freshness.has_uncheckpointed_changes),
                "full_reindex_needed": freshness.is_some_and(|freshness| freshness.full_reindex_needed),
                "full_reindex_reasons": freshness.map(|freshness| freshness.full_reindex_reasons.as_slice()).unwrap_or_default(),
                "metadata_repair_needed": freshness.is_some_and(|freshness| freshness.metadata_repair_needed),
                "metadata_repair_reasons": freshness.map(|freshness| freshness.metadata_repair_reasons.as_slice()).unwrap_or_default(),
                "embedding_model": freshness.and_then(|freshness| freshness.embedding_model.as_deref()),
                "embedding_version": freshness.and_then(|freshness| freshness.embedding_version.as_deref()),
                "embedding_dimension": freshness.and_then(|freshness| freshness.embedding_dimension),
                "commit_lag": self.projection_commit_lag(),
                "stale": self.projection_stale(),
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemProductionStatus {
    pub protocol: String,
    pub mode: NowledgeMemGraphMode,
    pub graph_open: bool,
    pub graph_read_only: bool,
    pub graph_skein_cutover_effective: bool,
    pub graph_route_ownership_present: bool,
    pub graph_route_ownership_ready: bool,
    pub graph_skein_route_count: usize,
    pub graph_legacy_route_count: usize,
    pub search_projection_open: bool,
    pub search_skein_cutover_effective: bool,
    pub graph_commit_epoch: u64,
    pub search_projection_source_graph_commit_epoch: Option<u64>,
    pub search_projection_durable_source_graph_commit_epoch: Option<u64>,
    pub search_projection_commit_lag: u64,
    pub search_projection_stale: bool,
    pub search_projection_full_reindex_needed: bool,
    pub search_projection_metadata_repair_needed: bool,
    pub search_projection_changefeed_restart_recoverable: bool,
    pub blocker_codes: Vec<String>,
    pub runtime_status: NowledgeMemRuntimeStatus,
    pub route_ownership: Option<NowledgeMemRouteOwnershipReadinessReport>,
}

impl NowledgeMemProductionStatus {
    #[doc(hidden)]
    pub fn from_runtime(
        mode: NowledgeMemGraphMode,
        graph_read_only: bool,
        runtime_status: NowledgeMemRuntimeStatus,
        route_ownership: Option<NowledgeMemRouteOwnershipReadinessReport>,
    ) -> Self {
        let freshness = runtime_status.projection_freshness.as_ref();
        let search_projection_open = freshness.is_some();
        let search_projection_commit_lag = runtime_status.projection_commit_lag();
        let search_projection_stale = search_projection_open && runtime_status.projection_stale();
        let search_projection_full_reindex_needed =
            freshness.is_some_and(|freshness| freshness.full_reindex_needed);
        let search_projection_metadata_repair_needed =
            freshness.is_some_and(|freshness| freshness.metadata_repair_needed);
        let graph_route_ownership_present = route_ownership.is_some();
        let graph_route_ownership_ready =
            route_ownership.as_ref().is_some_and(|report| report.ready);
        let graph_skein_route_count = route_ownership
            .as_ref()
            .map(|report| report.skein_route_count)
            .unwrap_or(0);
        let graph_legacy_route_count = route_ownership
            .as_ref()
            .map(|report| report.legacy_route_count)
            .unwrap_or(0);
        let mut blocker_codes = Vec::new();
        if graph_read_only {
            blocker_codes.push("graph_opened_read_only".to_string());
        }
        if !graph_route_ownership_present {
            blocker_codes.push("graph_route_ownership_missing".to_string());
        } else if !graph_route_ownership_ready {
            blocker_codes.push("graph_route_ownership_not_ready".to_string());
        }
        if graph_legacy_route_count > 0 {
            blocker_codes.push("graph_legacy_routes_remaining".to_string());
        }
        if !search_projection_open {
            blocker_codes.push("search_projection_not_open".to_string());
        }
        if search_projection_stale {
            blocker_codes.push("search_projection_stale".to_string());
        }
        if search_projection_full_reindex_needed {
            blocker_codes.push("search_projection_full_reindex_needed".to_string());
        }
        if search_projection_metadata_repair_needed {
            blocker_codes.push("search_projection_metadata_repair_needed".to_string());
        }
        if !runtime_status.changefeed.restart_recoverable {
            blocker_codes.push("search_projection_changefeed_not_restart_recoverable".to_string());
        }

        let graph_skein_cutover_effective = !graph_read_only
            && route_ownership
                .as_ref()
                .is_some_and(|report| report.production_cutover_ready);
        let search_skein_cutover_effective = search_projection_open
            && !search_projection_stale
            && !search_projection_full_reindex_needed
            && !search_projection_metadata_repair_needed
            && runtime_status.changefeed.restart_recoverable;

        Self {
            protocol: NOWLEDGE_MEM_PRODUCTION_STATUS_PROTOCOL.to_string(),
            mode,
            graph_open: true,
            graph_read_only,
            graph_skein_cutover_effective,
            graph_route_ownership_present,
            graph_route_ownership_ready,
            graph_skein_route_count,
            graph_legacy_route_count,
            search_projection_open,
            search_skein_cutover_effective,
            graph_commit_epoch: runtime_status.graph_commit_epoch,
            search_projection_source_graph_commit_epoch: freshness
                .and_then(|freshness| freshness.source_graph_commit_epoch),
            search_projection_durable_source_graph_commit_epoch: freshness
                .and_then(|freshness| freshness.durable_source_graph_commit_epoch),
            search_projection_commit_lag,
            search_projection_stale,
            search_projection_full_reindex_needed,
            search_projection_metadata_repair_needed,
            search_projection_changefeed_restart_recoverable: runtime_status
                .changefeed
                .restart_recoverable,
            blocker_codes,
            runtime_status,
            route_ownership,
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode.as_str(),
            "graph": {
                "open": self.graph_open,
                "read_only": self.graph_read_only,
                "skein_cutover_effective": self.graph_skein_cutover_effective,
                "route_ownership_present": self.graph_route_ownership_present,
                "route_ownership_ready": self.graph_route_ownership_ready,
                "skein_route_count": self.graph_skein_route_count,
                "legacy_route_count": self.graph_legacy_route_count,
                "commit_epoch": self.graph_commit_epoch,
            },
            "search": {
                "projection_open": self.search_projection_open,
                "skein_cutover_effective": self.search_skein_cutover_effective,
                "source_graph_commit_epoch": self.search_projection_source_graph_commit_epoch,
                "durable_source_graph_commit_epoch": self.search_projection_durable_source_graph_commit_epoch,
                "commit_lag": self.search_projection_commit_lag,
                "stale": self.search_projection_stale,
                "full_reindex_needed": self.search_projection_full_reindex_needed,
                "metadata_repair_needed": self.search_projection_metadata_repair_needed,
                "changefeed_restart_recoverable": self.search_projection_changefeed_restart_recoverable,
            },
            "blocker_codes": self.blocker_codes,
            "runtime_status": self.runtime_status.json(),
            "route_ownership": self.route_ownership.as_ref().map(NowledgeMemRouteOwnershipReadinessReport::json),
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{NowledgeMemProductionStatus, NowledgeMemRuntimeStatus};
    use crate::bounded_read_evidence::NowledgeMemGraphMode;
    use skein_storage::{SearchProjectionChangefeedStatus, SearchProjectionMutationId};

    fn changefeed() -> SearchProjectionChangefeedStatus {
        SearchProjectionChangefeedStatus {
            graph_commit_epoch: 8,
            resume_floor_commit_epoch: 3,
            oldest_retained_mutation_id: Some(SearchProjectionMutationId(4)),
            newest_retained_mutation_id: Some(SearchProjectionMutationId(8)),
            first_rebuild_required_mutation_id: None,
            retained_mutation_count: 5,
            retained_bytes: 0,
            max_retained_bytes: None,
            restart_recoverable: true,
        }
    }

    #[test]
    fn runtime_status_reports_changefeed_lag_without_host_state() {
        let status = NowledgeMemRuntimeStatus {
            protocol: "test".to_string(),
            graph_commit_epoch: 8,
            changefeed: changefeed(),
            projection_freshness: None,
        };

        assert_eq!(status.projection_commit_lag(), 8);
        assert!(status.projection_stale());
        assert_eq!(
            status.json()["changefeed"]["newest_retained_mutation_id"],
            8
        );
    }

    #[test]
    fn production_status_fails_closed_without_route_or_projection_evidence() {
        let status = NowledgeMemProductionStatus::from_runtime(
            NowledgeMemGraphMode::WritableCutover,
            false,
            NowledgeMemRuntimeStatus {
                protocol: "test".to_string(),
                graph_commit_epoch: 8,
                changefeed: changefeed(),
                projection_freshness: None,
            },
            None,
        );

        assert!(!status.graph_skein_cutover_effective);
        assert!(!status.search_skein_cutover_effective);
        assert!(status
            .blocker_codes
            .contains(&"graph_route_ownership_missing".to_string()));
        assert!(status
            .blocker_codes
            .contains(&"search_projection_not_open".to_string()));
    }
}
