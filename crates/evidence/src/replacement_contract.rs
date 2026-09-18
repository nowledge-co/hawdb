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

//! Shared replacement-evidence identifiers and projection field inventory.
//!
//! Producers and readiness reducers use one contract without pulling search
//! execution or database-running workload fixtures into the evidence layer.

pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_REPORT_PROTOCOL: &str =
    "hawdb-nowledge-mem-search-candidate-report-v1";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_READINESS_PROTOCOL: &str =
    "hawdb-nowledge-mem-search-candidate-readiness-v1";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL: &str =
    "hawdb-nowledge-search-candidate-shadow-evidence";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE: &str = "nmem-rust-bridge";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE: &str =
    "search_candidate_shadow_trace";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE: &str =
    "/search-index/hawdb-shadow/candidate-evidence";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE: &str = "hawdb";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE: &str = "hawdb-shadow";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE: &str = "lancedb";
pub const NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE: &str = "hawdb";

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
    "hawdb-nowledge-graph-route-workload-fixture-v1";
