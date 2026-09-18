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

//! Pure Nowledge Mem library-readiness evidence reduction.
//!
//! The embedded facade owns database opening and probe execution. This module
//! evaluates the resulting redacted evidence without a dependency on that host.

use crate::bounded_read_evidence::{
    NowledgeMemGraphMode, NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL,
};
use crate::{NowledgeMemReadinessAreaMap, NowledgeMemReadinessAreaSummary};
use hawdb_evidence::inventory::background_maintenance_evidence_health;
use hawdb_evidence::json_access::{
    evidence_bool, evidence_string, evidence_u64, nested_bool, nested_u64, nested_value,
    string_array_at,
};
use hawdb_evidence::replacement_contract::{
    NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE, NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
};
use hawdb_evidence::resource_profile::{
    production_resource_profile_blocker_codes, production_resource_profile_ready,
};
use hawdb_route_ownership::graph::{
    nowledge_mem_graph_read_route_catalog_digest, NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
};
use hawdb_route_ownership::{
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL,
};
use std::collections::BTreeSet;

const NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL: &str =
    "hawdb-nowledge-search-projection-evidence";
const NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL: &str =
    "hawdb-nowledge-search-projection-shadow-evidence";
const NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE: &str = "hawdb-rust-cli";
pub const SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY: &str =
    "search_projection_shadow_pushdown_evidence_not_ready";
const HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING: &str =
    "hawdb_search_projection_segment_descriptor_missing";
const HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING: &str =
    "hawdb_search_projection_segment_descriptor_fields_missing";

fn search_route_ownership_ready(evidence: &serde_json::Value) -> bool {
    evidence.get("protocol").and_then(serde_json::Value::as_str)
        == Some(NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL)
        && evidence.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
        && evidence
            .get("production_cutover_ready")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        && evidence
            .get("lancedb_route_count")
            .and_then(serde_json::Value::as_u64)
            == Some(0)
        && evidence
            .get("blocker_codes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty)
}

fn active_search_route_ownership_ready(evidence: &serde_json::Value) -> bool {
    evidence.get("protocol").and_then(serde_json::Value::as_str)
        == Some(NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL)
        && evidence.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
        && evidence
            .get("production_cutover_ready")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        && evidence
            .get("lancedb_route_count")
            .and_then(serde_json::Value::as_u64)
            == Some(0)
        && evidence
            .get("blocker_codes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty)
}

fn active_search_route_readiness_ready(evidence: &serde_json::Value) -> bool {
    evidence.get("protocol").and_then(serde_json::Value::as_str)
        == Some(NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL)
        && evidence.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
        && evidence
            .get("production_cutover_ready")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        && evidence
            .get("lancedb_handle_required_route_count")
            .and_then(serde_json::Value::as_u64)
            == Some(0)
        && evidence
            .get("blocker_codes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(Vec::is_empty)
}

pub struct NowledgeMemLibraryReadinessEvidence<'a> {
    pub bounded_read_evidence: &'a serde_json::Value,
    pub storage_recovery: &'a serde_json::Value,
    pub background_maintenance: &'a serde_json::Value,
    pub query_family_evidence: &'a serde_json::Value,
    pub graph_route_readiness: &'a serde_json::Value,
    pub search_route_ownership: &'a serde_json::Value,
    pub active_search_route_ownership: &'a serde_json::Value,
    pub active_search_route_readiness: &'a serde_json::Value,
    pub search_projection_evidence: &'a serde_json::Value,
    pub search_projection_shadow_evidence: &'a serde_json::Value,
    pub search_candidate_shadow_evidence: &'a serde_json::Value,
    pub workload_fixture_evidence: &'a serde_json::Value,
    pub production_resource_profile: &'a serde_json::Value,
}

fn library_readiness_blocker_codes(
    evidence: &NowledgeMemLibraryReadinessEvidence<'_>,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if !bounded_read_evidence_ready(evidence.bounded_read_evidence) {
        blockers.push("bounded_read_evidence_not_ready");
    }
    if evidence
        .storage_recovery
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("storage_recovery_not_ready");
    }
    if !library_background_maintenance_ready(evidence.background_maintenance) {
        blockers.push("background_maintenance_not_ready");
    }
    if evidence
        .query_family_evidence
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("query_family_evidence_not_ready");
    }
    if evidence
        .graph_route_readiness
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blockers.push("graph_route_readiness_not_ready");
    }
    if !search_route_ownership_ready(evidence.search_route_ownership) {
        blockers.push("search_route_ownership_not_ready");
    }
    if !active_search_route_ownership_ready(evidence.active_search_route_ownership) {
        blockers.push("active_search_route_ownership_not_ready");
    }
    if !active_search_route_readiness_ready(evidence.active_search_route_readiness) {
        blockers.push("active_search_route_readiness_not_ready");
    }
    if !search_projection_evidence_ready(evidence.search_projection_evidence) {
        blockers.push("search_projection_evidence_not_ready");
    }
    if !search_projection_shadow_evidence_ready(evidence.search_projection_shadow_evidence) {
        blockers.push("search_projection_shadow_evidence_not_ready");
    }
    if !search_candidate_shadow_evidence_ready(evidence.search_candidate_shadow_evidence) {
        blockers.push("search_candidate_shadow_evidence_not_ready");
    }
    if !workload_fixture_evidence_ready(evidence.workload_fixture_evidence) {
        blockers.push("workload_fixture_evidence_not_ready");
    }
    if !production_resource_profile_ready(evidence.production_resource_profile) {
        blockers.push("production_resource_profile_not_ready");
    }
    blockers
}

fn library_readiness_by_area(
    evidence: &NowledgeMemLibraryReadinessEvidence<'_>,
) -> NowledgeMemReadinessAreaMap {
    NowledgeMemReadinessAreaMap {
        graph: NowledgeMemReadinessAreaSummary::new("graph", true, Vec::new()),
        query: bounded_read_readiness_area(evidence.bounded_read_evidence),
        query_family: readiness_area(
            "query_family",
            evidence.query_family_evidence,
            "query_family_evidence_not_ready",
        ),
        graph_route: readiness_area(
            "graph_route",
            evidence.graph_route_readiness,
            "graph_route_readiness_not_ready",
        ),
        search_route_ownership: search_route_ownership_readiness_area(
            evidence.search_route_ownership,
            evidence.active_search_route_ownership,
            evidence.active_search_route_readiness,
        ),
        storage: storage_readiness_area(
            evidence.storage_recovery,
            evidence.production_resource_profile,
        ),
        search_projection: search_projection_readiness_area(evidence.search_projection_evidence),
        search_projection_shadow: search_projection_shadow_readiness_area(
            evidence.search_projection_shadow_evidence,
        ),
        search_candidate_shadow: search_candidate_shadow_readiness_area(
            evidence.search_candidate_shadow_evidence,
        ),
        workload_fixture: workload_fixture_readiness_area(evidence.workload_fixture_evidence),
        background: background_maintenance_readiness_area(evidence.background_maintenance),
    }
}

fn storage_readiness_area(
    storage_recovery: &serde_json::Value,
    production_resource_profile: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let mut blocker_codes = Vec::new();
    if storage_recovery
        .get("ready")
        .and_then(serde_json::Value::as_bool)
        != Some(true)
    {
        blocker_codes.push("storage_recovery_not_ready".to_string());
    }
    blocker_codes.extend(production_resource_profile_blocker_codes(
        production_resource_profile,
    ));
    NowledgeMemReadinessAreaSummary::new("storage", blocker_codes.is_empty(), blocker_codes)
}

fn bounded_read_readiness_area(evidence: &serde_json::Value) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = bounded_read_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new("query", blocker_codes.is_empty(), blocker_codes)
}

fn search_route_ownership_readiness_area(
    evidence: &serde_json::Value,
    active_route_evidence: &serde_json::Value,
    active_read_evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let ready = search_route_ownership_ready(evidence)
        && active_search_route_ownership_ready(active_route_evidence)
        && active_search_route_readiness_ready(active_read_evidence);
    let blocker_codes = if ready {
        Vec::new()
    } else {
        let mut codes = evidence
            .get("blocker_codes")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        codes.extend(
            active_route_evidence
                .get("blocker_codes")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string),
        );
        codes.extend(
            active_read_evidence
                .get("blocker_codes")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string),
        );
        if codes.is_empty() {
            vec!["search_route_ownership_not_ready".to_string()]
        } else {
            codes
        }
    };
    NowledgeMemReadinessAreaSummary::new("search_route_ownership", ready, blocker_codes)
}

fn bounded_read_evidence_ready(evidence: &serde_json::Value) -> bool {
    bounded_read_readiness_blocker_codes(evidence).is_empty()
}

fn bounded_read_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("bounded_read_evidence_missing".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol") != Some(NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL) {
        blockers.insert("bounded_read_protocol_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("bounded_read_not_ready".to_string());
    }
    if evidence_string(evidence, "mode") != Some(NowledgeMemGraphMode::ShadowReadOnly.as_str()) {
        blockers.insert("bounded_read_not_shadow_read_only".to_string());
    }
    let max_rows = evidence_u64(evidence, "max_rows");
    if !max_rows.is_some_and(|value| value > 0) {
        blockers.insert("bounded_read_missing_max_rows".to_string());
    }
    let expected_execution_row_cap = max_rows.and_then(|value| value.checked_add(1));
    if expected_execution_row_cap.is_none()
        || evidence_u64(evidence, "execution_row_cap") != expected_execution_row_cap
    {
        blockers.insert("bounded_read_execution_row_cap_mismatch".to_string());
    }
    if evidence_u64(evidence, "estimated_payload_bytes").is_none() {
        blockers.insert("bounded_read_estimated_payload_bytes_missing".to_string());
    }
    if !evidence_u64(evidence, "max_estimated_payload_bytes").is_some_and(|value| value > 0) {
        blockers.insert("bounded_read_max_estimated_payload_bytes_missing".to_string());
    }
    if evidence_bool(evidence, "payload_budget_exceeded") != Some(false) {
        blockers.insert("bounded_read_payload_budget_exceeded".to_string());
    }
    if evidence_bool(evidence, "row_limit_enforced_before_output") != Some(true) {
        blockers.insert("bounded_read_row_limit_not_enforced_before_output".to_string());
    }
    if evidence_bool(evidence, "operator_row_cap_enabled") != Some(true) {
        blockers.insert("bounded_read_operator_row_cap_disabled".to_string());
    }
    if evidence_bool(evidence, "row_budget_exceeded") == Some(true) {
        blockers.insert("bounded_read_row_budget_exceeded".to_string());
    }
    if evidence_bool(evidence, "streaming").is_none() {
        blockers.insert("bounded_read_streaming_evidence_missing".to_string());
    }
    if evidence_bool(evidence, "blocking_operator_memory_reports_complete") != Some(true) {
        blockers.insert("bounded_read_blocking_operator_memory_report_incomplete".to_string());
    }
    if evidence_bool(evidence, "blocking_operator_memory_within_budget") != Some(true) {
        blockers.insert("bounded_read_blocking_operator_memory_budget_exceeded".to_string());
    }
    if evidence_bool(evidence, "spill_within_budget") != Some(true) {
        blockers.insert("bounded_read_blocking_operator_spill_budget_exceeded".to_string());
    }
    if !string_array_at(evidence, &["missing_covered_routes"])
        .is_some_and(|routes| routes.is_empty())
    {
        blockers.insert("bounded_read_missing_covered_routes".to_string());
    }
    if evidence_string(evidence, "route_catalog_version")
        != Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION)
        || evidence_string(evidence, "route_catalog_digest")
            != Some(nowledge_mem_graph_read_route_catalog_digest().as_str())
    {
        blockers.insert("bounded_read_route_catalog_stale".to_string());
    }
    if evidence_bool(evidence, "route_primary_ready") != Some(true)
        || evidence_bool(evidence, "route_query_plan_evidence_ready") != Some(true)
        || evidence_bool(evidence, "route_query_profile_evidence_ready") != Some(true)
        || evidence_bool(evidence, "route_query_api_behavior_evidence_ready") != Some(true)
        || evidence_bool(
            evidence,
            "route_relationship_property_pruning_evidence_ready",
        ) != Some(true)
    {
        blockers.insert("bounded_read_graph_route_readiness_not_ready".to_string());
    }
    let required_pruning_count =
        evidence_u64(evidence, "relationship_property_pruning_required_count");
    if required_pruning_count.is_none()
        || required_pruning_count
            != evidence_u64(evidence, "relationship_property_pruning_report_count")
    {
        blockers.insert("bounded_read_relationship_property_pruning_missing".to_string());
    }
    blockers.into_iter().collect()
}

fn search_projection_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = search_projection_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "search_projection",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn search_projection_shadow_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = search_projection_shadow_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "search_projection_shadow",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn search_projection_evidence_ready(evidence: &serde_json::Value) -> bool {
    search_projection_readiness_blocker_codes(evidence).is_empty()
}

fn search_projection_shadow_evidence_ready(evidence: &serde_json::Value) -> bool {
    search_projection_shadow_readiness_blocker_codes(evidence).is_empty()
}

fn search_projection_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("search_projection_not_configured".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol") != Some(NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL) {
        blockers.insert("search_projection_protocol_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("search_projection_not_ready".to_string());
    }
    if evidence_bool(evidence, "derived_projection") != Some(true) {
        blockers.insert("search_projection_not_derived".to_string());
    }
    if evidence_bool(evidence, "all_tables_covered") != Some(true)
        || !evidence_u64(evidence, "covered_table_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "covered_table_count")
            != evidence_u64(evidence, "required_table_count")
    {
        blockers.insert("search_projection_tables_not_ready".to_string());
    }
    if evidence_bool(evidence, "fts_ready") != Some(true) {
        blockers.insert("search_projection_fts_not_ready".to_string());
    }
    if evidence_bool(evidence, "vector_ready") != Some(true) {
        blockers.insert("search_projection_vector_not_ready".to_string());
    }
    if evidence_bool(evidence, "document_identity_ready") != Some(true) {
        blockers.insert("search_projection_document_identity_not_ready".to_string());
    }
    if evidence_bool(evidence, "embedding_identity_ready") != Some(true) {
        blockers.insert("search_projection_embedding_identity_not_ready".to_string());
    }
    if evidence_bool(evidence, "fail_soft_ready") != Some(true) {
        blockers.insert("search_projection_fail_soft_not_ready".to_string());
    }
    if evidence_bool(evidence, "rebuild_marker_ready") != Some(true) {
        blockers.insert("search_projection_rebuild_marker_not_ready".to_string());
    }
    if evidence_bool(evidence, "metadata_repair_marker_ready") != Some(true) {
        blockers.insert("search_projection_metadata_repair_marker_not_ready".to_string());
    }
    if evidence_bool(evidence, "incremental_update_ready") != Some(true) {
        blockers.insert("search_projection_incremental_update_not_ready".to_string());
    }
    if evidence_bool(evidence, "source_chunk_ready") != Some(true) {
        blockers.insert("search_projection_source_chunk_not_ready".to_string());
    }
    if evidence_bool(evidence, "predicate_pushdown_ready") != Some(true) {
        blockers.insert("search_projection_predicate_pushdown_not_ready".to_string());
    }
    if evidence_bool(evidence, "production_filter_pruning_ready") != Some(true) {
        blockers.insert("search_projection_production_filter_pruning_not_ready".to_string());
    }
    if evidence_bool(evidence, "compressed_vector_projection_ready") == Some(false) {
        blockers.insert("search_projection_compressed_vector_not_ready".to_string());
    }
    blockers.into_iter().collect()
}

fn search_projection_shadow_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("search_projection_not_configured".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol")
        != Some(NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL)
    {
        blockers.insert("search_projection_shadow_protocol_mismatch".to_string());
    }
    if evidence_string(evidence, "evidence_source")
        != Some(NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE)
    {
        blockers.insert("search_projection_shadow_evidence_source_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("search_projection_shadow_not_ready".to_string());
    }
    if evidence_bool(evidence, "primary_ready") != Some(true) {
        blockers.insert("search_projection_shadow_primary_not_ready".to_string());
    }
    if evidence_bool(evidence, "shadow_ready") != Some(true) {
        blockers.insert("search_projection_shadow_shadow_not_ready".to_string());
    }
    if evidence_bool(evidence, "document_count_parity") != Some(true)
        || evidence_bool(evidence, "document_identity_parity") != Some(true)
    {
        blockers.insert("search_projection_shadow_document_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["table_parity", "ready"]) != Some(true)
        && evidence_bool(evidence, "table_parity_ready") != Some(true)
    {
        blockers.insert("search_projection_shadow_table_parity_not_ready".to_string());
    }
    if evidence_bool(evidence, "embedding_identity_parity") != Some(true) {
        blockers.insert("search_projection_shadow_embedding_identity_not_ready".to_string());
    }
    if evidence_bool(evidence, "lifecycle_parity") != Some(true) {
        blockers.insert("search_projection_shadow_lifecycle_not_ready".to_string());
    }
    if evidence_bool(evidence, "incremental_watermark_parity") != Some(true) {
        blockers.insert("search_projection_shadow_incremental_watermark_not_ready".to_string());
    }
    if evidence_bool(evidence, "predicate_pushdown_parity") != Some(true) {
        blockers.insert("search_projection_shadow_predicate_pushdown_not_ready".to_string());
    }
    if nested_bool(evidence, &["pushdown_evidence", "ready"]) != Some(true) {
        blockers.insert(SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY.to_string());
    }
    if nested_bool(
        evidence,
        &[
            "pushdown_evidence",
            "shadow_persisted_segment_descriptor_ready",
        ],
    ) != Some(true)
    {
        blockers.insert(HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING.to_string());
    }
    if nested_bool(
        evidence,
        &[
            "pushdown_evidence",
            "shadow_segment_descriptor_scan_filter_fields_ready",
        ],
    ) != Some(true)
    {
        blockers.insert(HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING.to_string());
    }
    blockers.into_iter().collect()
}

fn search_candidate_shadow_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = search_candidate_shadow_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "search_candidate_shadow",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn search_candidate_shadow_evidence_ready(evidence: &serde_json::Value) -> bool {
    search_candidate_shadow_readiness_blocker_codes(evidence).is_empty()
}

fn workload_fixture_readiness_area(
    evidence: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let blocker_codes = workload_fixture_readiness_blocker_codes(evidence);
    NowledgeMemReadinessAreaSummary::new(
        "workload_fixture",
        blocker_codes.is_empty(),
        blocker_codes,
    )
}

fn workload_fixture_evidence_ready(evidence: &serde_json::Value) -> bool {
    workload_fixture_readiness_blocker_codes(evidence).is_empty()
}

pub fn workload_fixture_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("workload_fixture_evidence_missing".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol") != Some(NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL)
    {
        blockers.insert("workload_fixture_protocol_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("workload_fixture_not_ready".to_string());
    }
    if !evidence_u64(evidence, "route_count").is_some_and(|count| count > 0)
        || !evidence_u64(evidence, "query_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_query_count") != Some(0)
    {
        blockers.insert("workload_fixture_route_queries_not_ready".to_string());
    }
    if !evidence_u64(evidence, "bounded_expansion_probe_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_bounded_expansion_probe_count") != Some(0)
    {
        blockers.insert("workload_fixture_bounded_expansion_not_ready".to_string());
    }
    if !evidence_u64(evidence, "search_metadata_probe_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_search_metadata_probe_count") != Some(0)
    {
        blockers.insert("workload_fixture_search_metadata_not_ready".to_string());
    }
    if !evidence_u64(evidence, "graph_rag_probe_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_graph_rag_probe_count") != Some(0)
    {
        blockers.insert("workload_fixture_graph_rag_not_ready".to_string());
    }
    if !array_at(evidence, &["graph_rag_reports"]).is_some_and(|reports| {
        reports.iter().any(|report| {
            report.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
                && report
                    .get("label_count")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|count| count > 0)
                && report
                    .get("relationship_type_count")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|count| count > 0)
                && report
                    .get("route_count")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|count| count > 0)
                && report
                    .get("parameter_requirement_count")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|count| count > 0)
                && report
                    .get("row_count")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|count| count > 0)
                && report
                    .get("row_budget_exceeded")
                    .and_then(serde_json::Value::as_bool)
                    == Some(false)
                && report
                    .get("payload_budget_exceeded")
                    .and_then(serde_json::Value::as_bool)
                    == Some(false)
                && report
                    .get("blocking_operator_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(0)
                && report.get("streaming").and_then(serde_json::Value::as_bool) == Some(false)
                && report
                    .get("error_class")
                    .and_then(serde_json::Value::as_str)
                    .is_none()
        })
    }) {
        blockers.insert("workload_fixture_graph_rag_probe_missing".to_string());
    }
    if !evidence_u64(evidence, "source_projection_probe_count").is_some_and(|count| count > 0)
        || evidence_u64(evidence, "failed_source_projection_probe_count") != Some(0)
    {
        blockers.insert("workload_fixture_source_projection_not_ready".to_string());
    }
    if !array_at(evidence, &["source_projection_reports"]).is_some_and(|reports| {
        reports.iter().any(|report| {
            report.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
                && report
                    .get("too_small_batch_failed_closed")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
                && report
                    .get("operation_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(2)
                && report
                    .get("upserted_documents")
                    .and_then(serde_json::Value::as_u64)
                    == Some(2)
                && report
                    .get("deleted_documents")
                    .and_then(serde_json::Value::as_u64)
                    == Some(0)
                && report
                    .get("source_document_count")
                    .and_then(serde_json::Value::as_u64)
                    == Some(2)
                && report
                    .get("indexed_source_document_ready")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
                && report
                    .get("source_graph_commit_epoch")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                && report
                    .get("complete_through_graph_commit_epoch")
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
                && report
                    .get("error_class")
                    .and_then(serde_json::Value::as_str)
                    .is_none()
        })
    }) {
        blockers.insert("workload_fixture_source_projection_probe_missing".to_string());
    }
    blockers.into_iter().collect()
}

fn search_candidate_shadow_readiness_blocker_codes(evidence: &serde_json::Value) -> Vec<String> {
    let mut blockers = evidence_blocker_codes(evidence);
    if evidence.get("present").and_then(serde_json::Value::as_bool) == Some(false) {
        if blockers.is_empty() {
            blockers.insert("search_candidate_shadow_evidence_missing".to_string());
        }
        return blockers.into_iter().collect();
    }
    if evidence_string(evidence, "protocol")
        != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL)
    {
        blockers.insert("search_candidate_shadow_protocol_mismatch".to_string());
    }
    if evidence_string(evidence, "route") != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE) {
        blockers.insert("search_candidate_shadow_route_mismatch".to_string());
    }
    if evidence_string(evidence, "evidence_source")
        != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE)
    {
        blockers.insert("search_candidate_shadow_evidence_source_mismatch".to_string());
    }
    if evidence_bool(evidence, "ready") != Some(true) {
        blockers.insert("search_candidate_shadow_not_ready".to_string());
    }
    if evidence_string(evidence, "candidate_primary_engine")
        != Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE)
    {
        blockers.insert("search_candidate_primary_engine_not_hawdb".to_string());
    }
    if !search_candidate_shadow_counts_ready(evidence) {
        blockers.insert("search_candidate_counts_not_ready".to_string());
    }
    if evidence_bool(evidence, "text_retriever_ready") != Some(true) {
        blockers.insert("search_candidate_text_retriever_not_ready".to_string());
    }
    if evidence_bool(evidence, "vector_retriever_ready") != Some(true) {
        blockers.insert("search_candidate_vector_retriever_not_ready".to_string());
    }
    if evidence_bool(evidence, "fts_top_k_overlap_ready") != Some(true) {
        blockers.insert("search_candidate_fts_top_k_overlap_not_ready".to_string());
    }
    if evidence_bool(evidence, "vector_top_k_overlap_ready") != Some(true) {
        blockers.insert("search_candidate_vector_top_k_overlap_not_ready".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "source_chunk_identity_ready"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_source_chunk_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["candidate_readiness", "fail_soft_observed"]) != Some(true) {
        blockers.insert("search_candidate_fail_soft_not_observed".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "projection_marker_status_visible"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_projection_marker_status_missing".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "projection_watermark_ready"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_projection_watermark_missing".to_string());
    }
    if nested_bool(
        evidence,
        &["candidate_readiness", "embedding_identity_ready"],
    ) != Some(true)
    {
        blockers.insert("search_candidate_embedding_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["candidate_identity", "ready"]) != Some(true)
        || nested_bool(evidence, &["candidate_identity", "parity"]) != Some(true)
    {
        blockers.insert("search_candidate_identity_not_ready".to_string());
    }
    if nested_bool(evidence, &["filter_pushdown", "ready"]) != Some(true)
        || evidence_bool(evidence, "filter_pushdown_ready") != Some(true)
    {
        blockers.insert("search_candidate_filter_pushdown_not_ready".to_string());
    }
    if !nested_u64(evidence, &["filter_pushdown", "field_summary_count"])
        .is_some_and(|count| count > 0)
        || !string_array_at(evidence, &["filter_pushdown", "missing_required_fields"])
            .is_some_and(|fields| fields.is_empty())
    {
        blockers.insert("search_candidate_field_pruning_missing".to_string());
    }
    blockers.into_iter().collect()
}

fn array_at<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a Vec<serde_json::Value>> {
    nested_value(value, path)?.as_array()
}

fn evidence_blocker_codes(evidence: &serde_json::Value) -> BTreeSet<String> {
    evidence
        .get("blocker_codes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn search_candidate_shadow_counts_ready(evidence: &serde_json::Value) -> bool {
    let request_count = evidence_u64(evidence, "request_count");
    let primary_candidate_count = evidence_u64(evidence, "primary_candidate_count");
    let shadow_candidate_count = evidence_u64(evidence, "shadow_candidate_count");
    let matched_candidate_count = evidence_u64(evidence, "matched_candidate_count");
    let primary_only_candidate_count = evidence_u64(evidence, "primary_only_candidate_count");
    request_count.is_some_and(|count| count > 0)
        && primary_candidate_count.is_some()
        && primary_candidate_count == shadow_candidate_count
        && matched_candidate_count == shadow_candidate_count
        && primary_only_candidate_count == Some(0)
}

fn readiness_area(
    name: &'static str,
    evidence: &serde_json::Value,
    fallback_blocker_code: &'static str,
) -> NowledgeMemReadinessAreaSummary {
    let ready = evidence.get("ready").and_then(serde_json::Value::as_bool) == Some(true);
    NowledgeMemReadinessAreaSummary::new(
        name,
        ready,
        readiness_blocker_codes(evidence, fallback_blocker_code, ready),
    )
}

fn background_maintenance_readiness_area(
    background_maintenance: &serde_json::Value,
) -> NowledgeMemReadinessAreaSummary {
    let health = background_maintenance_evidence_health(Some(background_maintenance), true);
    NowledgeMemReadinessAreaSummary::new("background", health.ready, health.blocker_codes)
}

fn library_background_maintenance_ready(background_maintenance: &serde_json::Value) -> bool {
    background_maintenance_evidence_health(Some(background_maintenance), true).ready
}

fn readiness_blocker_codes(
    evidence: &serde_json::Value,
    fallback_blocker_code: &'static str,
    ready: bool,
) -> Vec<String> {
    if ready {
        return Vec::new();
    }
    let codes = evidence
        .get("blocker_codes")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if codes.is_empty() {
        vec![fallback_blocker_code.to_string()]
    } else {
        codes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemLibraryReadinessAssessment {
    pub blocker_codes: Vec<String>,
    pub readiness_by_area: NowledgeMemReadinessAreaMap,
}

pub fn assess_nowledge_mem_library_readiness(
    evidence: &NowledgeMemLibraryReadinessEvidence<'_>,
) -> NowledgeMemLibraryReadinessAssessment {
    let blocker_codes = library_readiness_blocker_codes(evidence)
        .into_iter()
        .map(str::to_string)
        .collect();
    let readiness_by_area = library_readiness_by_area(evidence);
    NowledgeMemLibraryReadinessAssessment {
        blocker_codes,
        readiness_by_area,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_evidence_fails_closed_in_every_area() {
        let missing = serde_json::json!({ "present": false });
        let assessment =
            assess_nowledge_mem_library_readiness(&NowledgeMemLibraryReadinessEvidence {
                bounded_read_evidence: &missing,
                storage_recovery: &missing,
                background_maintenance: &missing,
                query_family_evidence: &missing,
                graph_route_readiness: &missing,
                search_route_ownership: &missing,
                active_search_route_ownership: &missing,
                active_search_route_readiness: &missing,
                search_projection_evidence: &missing,
                search_projection_shadow_evidence: &missing,
                search_candidate_shadow_evidence: &missing,
                workload_fixture_evidence: &missing,
                production_resource_profile: &missing,
            });

        assert_eq!(assessment.blocker_codes.len(), 13);
        assert!(!assessment.readiness_by_area.query.ready);
        assert!(!assessment.readiness_by_area.storage.ready);
        assert!(!assessment.readiness_by_area.search_projection.ready);
        assert!(!assessment.readiness_by_area.workload_fixture.ready);
        assert!(!assessment.readiness_by_area.background.ready);
    }
}
