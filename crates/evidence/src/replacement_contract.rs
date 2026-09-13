//! Shared replacement-evidence identifiers and projection field inventory.
//!
//! Producers and readiness reducers use one contract without pulling search
//! execution or database-running workload fixtures into the evidence layer.

pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL: &str =
    "skein-nowledge-mem-search-candidate-report-v1";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL: &str =
    "skein-nowledge-mem-search-candidate-readiness-v1";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-candidate-shadow-evidence";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE: &str = "nmem-rust-bridge";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE: &str =
    "search_candidate_shadow_trace";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE: &str =
    "/search-index/skein-shadow/candidate-evidence";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE: &str = "skein";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE: &str = "skein-shadow";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE: &str = "lancedb";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE: &str = "skein";

pub const NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS: &[&str] = &[
    "kind",
    "external_id",
    "source_id",
    "space_id",
    "labels",
    "unit_type",
    "lifecycle_state",
    "temporal_context",
    "importance",
    "confidence",
    "created_at",
    "updated_at",
    "event_start",
    "event_end",
    "is_latest",
];

pub const NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL: &str =
    "skein-nowledge-graph-route-workload-fixture-v1";
