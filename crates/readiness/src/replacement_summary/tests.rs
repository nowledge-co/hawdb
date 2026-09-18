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

use super::{
    nowledge_graph_route_readiness_summary_from_bundle, nowledge_replacement_summary_json,
    nowledge_replacement_summary_json_with_options, NowledgeReplacementSummaryOptions,
    GRAPH_LAYER_REPLACEMENT_SCOPE, HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING,
    HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING,
    HAWDB_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE, LARGE_BLOB_VALUE_STORE_SCOPE,
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL, NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
    REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
    REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
    SEARCH_PROJECTION_REPLACEMENT_SCOPE, SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY,
    SQLITE_CONTENT_STORE_SCOPE,
};
use crate::graph_route::{NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL, NMEM_GRAPH_ROUTE_READINESS_PROTOCOL};
use crate::graph_summary::nowledge_graph_route_readiness_summary;
use crate::source_mutation::{
    NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
    REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES,
};
use hawdb_core::GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL;
use hawdb_evidence::replacement_contract::{
    NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE,
};
use hawdb_route_ownership::graph::{
    nowledge_mem_graph_read_route_spec, nowledge_mem_graph_read_route_specs_json,
};

mod differential;

#[test]
fn replacement_summary_keeps_production_replacement_zero_without_cutover_evidence() {
    let bundle = serde_json::json!({
        "coverage": {
            "coverage_per_million": 1_000_000
        },
        "inventory_gate": {
            "coverage_per_million": 1_000_000,
            "blockers": []
        },
        "cutover": {
            "decision": "ready",
            "matched_per_million": 1_000_000,
            "blockers": []
        },
        "migration_gate": {
            "decision": "ready",
            "blockers": []
        },
        "replacement_readiness_per_million": 1_000_000,
        "replacement_readiness_by_query_family": []
    });

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["business_surface"]["ready"], true);
    assert_eq!(summary["shadow_parity"]["ready"], true);
    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(
        summary["blocking_categories"],
        serde_json::json!([
            "active_search_route_ownership",
            "active_search_route_readiness",
            "bounded_read_evidence",
            "cutover_evidence",
            "dual_engine_evidence",
            "graph_route_readiness",
            "previous_wrapper_contract",
            "query_family_readiness",
            "query_runtime_preflight",
            "search_candidate_shadow_evidence",
            "search_projection_evidence",
            "search_projection_shadow_evidence",
            "search_route_ownership",
            "shadow_parity",
            "source_mutation_dual_write_readiness",
            "workload_fixture_evidence"
        ])
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "cutover_evidence"));
}

#[test]
fn replacement_summary_reports_production_ready_when_all_evidence_is_ready() {
    let bundle = production_ready_bundle();

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], true);
    assert_eq!(summary["production_replacement_per_million"], 1_000_000);
    assert_eq!(
        summary["replacement_boundaries"]["graph_layer"],
        serde_json::json!({
            "scope": GRAPH_LAYER_REPLACEMENT_SCOPE,
            "replacement_role": "primary_replacement",
            "storage_owner": "hawdb",
        })
    );
    assert_eq!(
        summary["replacement_boundaries"]["search_projection"],
        serde_json::json!({
            "scope": SEARCH_PROJECTION_REPLACEMENT_SCOPE,
            "replacement_role": "rebuildable_projection",
            "storage_owner": "hawdb",
        })
    );
    assert_eq!(
        summary["replacement_boundaries"]["content_store"],
        serde_json::json!({
            "scope": SQLITE_CONTENT_STORE_SCOPE,
            "replacement_role": "external_out_of_scope",
            "storage_owner": "nowledge_mem",
        })
    );
    assert_eq!(
        summary["replacement_boundaries"]["large_blob_store"],
        serde_json::json!({
            "scope": LARGE_BLOB_VALUE_STORE_SCOPE,
            "replacement_role": "external_out_of_scope",
            "storage_owner": "nowledge_mem",
        })
    );
    assert_eq!(summary["blocking_categories"], serde_json::json!([]));
    assert_eq!(summary["missing_evidence"], serde_json::json!([]));
    assert_eq!(summary["next_actions"], serde_json::json!([]));
    assert_eq!(summary["bounded_read_evidence"]["present"], true);
    assert_eq!(summary["bounded_read_evidence"]["ready"], true);
    assert_eq!(summary["bounded_read_evidence"]["mode"], "shadow_read_only");
    assert_eq!(summary["bounded_read_evidence"]["execution_row_cap"], 513);
    assert_eq!(
        summary["bounded_read_evidence"]["route_primary_ready"],
        true
    );
    assert_eq!(
        summary["bounded_read_evidence"]["route_query_plan_evidence_ready"],
        true
    );
    assert_eq!(
        summary["bounded_read_evidence"]["route_query_profile_evidence_ready"],
        true
    );
    assert_eq!(
        summary["bounded_read_evidence"]["route_relationship_property_pruning_evidence_ready"],
        true
    );
    assert_eq!(summary["graph_route_readiness"]["present"], true);
    assert_eq!(summary["graph_route_readiness"]["ready"], true);
    assert_eq!(
        summary["graph_route_readiness"]["evidence_route_coverage_matches"],
        true
    );
    assert_eq!(
        summary["graph_route_readiness"]["route_primary_ready"],
        true
    );
    assert_eq!(summary["query_runtime_preflight"]["present"], true);
    assert_eq!(summary["query_runtime_preflight"]["ready"], true);
    assert_eq!(
        summary["query_runtime_preflight"]["required_routes_covered"],
        true
    );
    assert_eq!(
        summary["query_runtime_preflight"]["route_coverage_ready"],
        true
    );
    assert_eq!(
        summary["query_runtime_preflight"]["probe_details_ready"],
        true
    );
    assert_eq!(summary["workload_fixture_evidence"]["present"], true);
    assert_eq!(summary["workload_fixture_evidence"]["ready"], true);
    assert_eq!(
        summary["workload_fixture_evidence"]["failed_query_count"],
        0
    );
    assert_eq!(
        summary["workload_fixture_evidence"]["failed_bounded_expansion_probe_count"],
        0
    );
    assert_eq!(
        summary["workload_fixture_evidence"]["failed_search_metadata_probe_count"],
        0
    );
    assert_eq!(
        summary["workload_fixture_evidence"]["failed_graph_rag_probe_count"],
        0
    );
    assert_eq!(
        summary["workload_fixture_evidence"]["graph_rag_reports"][0]["ready"],
        true
    );
    assert_eq!(
        summary["workload_fixture_evidence"]["graph_rag_reports"][0]["parameter_requirement_count"],
        1
    );
    assert_eq!(
        summary["workload_fixture_evidence"]["failed_source_projection_probe_count"],
        0
    );
    assert_eq!(
        summary["workload_fixture_evidence"]["source_projection_reports"][0]["ready"],
        true
    );
    assert_eq!(
        summary["workload_fixture_evidence"]["source_projection_reports"][0]
            ["too_small_batch_failed_closed"],
        true
    );
    assert_eq!(
        summary["cutover_evidence"]["storage_recovery_protocol_matches"],
        true
    );
    assert_eq!(
        summary["cutover_evidence"]["storage_recovery_durable"],
        true
    );
    assert_eq!(
        summary["cutover_evidence"]["storage_recovery_checkpoint_boundary_present"],
        true
    );
    assert_eq!(
        summary["cutover_evidence"]["storage_recovery_wal_replay_bounded"],
        true
    );
    assert_eq!(
        summary["cutover_evidence"]["storage_recovery_replay_boundary_consistent"],
        true
    );
    assert_eq!(
        summary["cutover_evidence"]["storage_recovery_torn_tail_clean"],
        true
    );
    assert_eq!(
        summary["cutover_evidence"]["background_maintenance_protocol_matches"],
        true
    );
    assert_eq!(
        summary["cutover_evidence"]
            ["background_maintenance_executable_search_projection_graph_delta_count"],
        2
    );
    assert_eq!(
        summary["cutover_evidence"]
            ["background_maintenance_admitted_search_projection_graph_delta_count"],
        1
    );
    assert_eq!(
        summary["cutover_evidence"]
            ["background_maintenance_deferred_search_projection_graph_delta_count"],
        1
    );
    assert_eq!(
        summary["cutover_evidence"]
            ["background_maintenance_executable_search_projection_graph_delta_operations"],
        8
    );
    assert_eq!(
        summary["cutover_evidence"]
            ["background_maintenance_admitted_search_projection_graph_delta_operations"],
        3
    );
    assert_eq!(
        summary["cutover_evidence"]["background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"],
        42
    );
    assert_eq!(summary["shadow_evidence"]["ready"], true);
    assert_eq!(
        summary["shadow_evidence"]["ready_wrapper_identity"],
        "nowledge-previous-wrapper:test"
    );
    assert_eq!(summary["previous_wrapper_contract_evidence"]["ready"], true);
    assert_eq!(summary["full_contract_evidence"]["ready"], true);
    assert_eq!(
        summary["full_contract_evidence"]["full_contract_checked"],
        true
    );
    assert_eq!(
        summary["full_contract_evidence"]["full_contract_ready"],
        true
    );
    assert_eq!(summary["full_contract_evidence"]["selected_checks"], 2);
    assert_eq!(summary["full_contract_evidence"]["check_count"], 2);
    assert_eq!(
        summary["previous_wrapper_contract_evidence"]["wrapper_identity"],
        "nowledge-previous-wrapper:test"
    );
    assert_eq!(summary["dual_engine_evidence"]["present"], true);
    assert_eq!(summary["dual_engine_evidence"]["ready"], true);
    assert_eq!(summary["dual_engine_evidence"]["consistent"], true);
    assert_eq!(summary["dual_engine_evidence"]["primary_engine"], "hawdb");
    assert_eq!(
        summary["dual_engine_evidence"]["shadow_engine"],
        "previous-wrapper"
    );
    assert_eq!(summary["search_projection_evidence"]["present"], true);
    assert_eq!(summary["search_projection_evidence"]["ready"], true);
    assert_eq!(
        summary["search_projection_evidence"]["covered_table_count"],
        6
    );
    assert_eq!(
        summary["search_projection_evidence"]["required_table_count"],
        6
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["present"],
        true
    );
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], true);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["evidence_source"],
        HAWDB_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["primary_engine"],
        "lancedb"
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["shadow_engine"],
        "hawdb"
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["table_parity_ready"],
        true
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["document_identity_parity"],
        true
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["predicate_pushdown_parity"],
        true
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"],
        true
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_persisted_segment_descriptor_ready"],
        true
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_scan_filter_fields_ready"],
        true
    );
    assert_eq!(summary["search_candidate_shadow_evidence"]["present"], true);
    assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], true);
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["candidate_counts_ready"],
        true
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["candidate_identity_ready"],
        true
    );
    assert_eq!(summary["search_route_ownership"]["present"], true);
    assert_eq!(summary["search_route_ownership"]["ready"], true);
    assert_eq!(
        summary["search_route_ownership"]["required_route_count"],
        REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES.len()
    );
    assert_eq!(summary["search_route_ownership"]["lancedb_route_count"], 0);
    assert_eq!(summary["active_search_route_ownership"]["present"], true);
    assert_eq!(summary["active_search_route_ownership"]["ready"], true);
    assert_eq!(
        summary["active_search_route_ownership"]["required_route_count"],
        REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len()
    );
    assert_eq!(
        summary["active_search_route_ownership"]["lancedb_route_count"],
        0
    );
    assert_eq!(summary["active_search_route_readiness"]["present"], true);
    assert_eq!(summary["active_search_route_readiness"]["ready"], true);
    assert_eq!(
        summary["active_search_route_readiness"]["ready_route_count"],
        REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len()
    );
    assert_eq!(
        summary["active_search_route_readiness"]["lancedb_handle_required_route_count"],
        0
    );
    assert_eq!(
        summary["replacement_readiness_by_query_family"][0]["query_family"],
        "memory_lookup"
    );
}

#[test]
fn graph_route_readiness_summary_parses_direct_library_value() {
    let bundle = production_ready_bundle();
    let direct =
        nowledge_graph_route_readiness_summary(bundle.get("graph_route_readiness").unwrap());
    let from_bundle = nowledge_graph_route_readiness_summary_from_bundle(&bundle);

    assert!(direct.ready);
    assert_eq!(direct, from_bundle);
    assert_eq!(
        direct.covered_route_count,
        Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
    );
    assert_eq!(direct.missing_required_routes, Vec::<&'static str>::new());
    assert!(direct.route_catalog_metadata_ready);
    assert_eq!(
        direct.missing_route_catalog_metadata_routes,
        Vec::<&'static str>::new()
    );
    assert_eq!(
        direct.route_catalog_metadata_mismatch_routes,
        Vec::<String>::new()
    );
}

#[test]
fn graph_route_readiness_summary_fails_closed_for_missing_library_value() {
    let direct = nowledge_graph_route_readiness_summary(&serde_json::Value::Null);

    assert!(!direct.present);
    assert!(!direct.ready);
    assert_eq!(direct.covered_routes, Vec::<String>::new());
}

#[test]
fn graph_route_readiness_summary_fails_closed_for_stale_route_catalog_metadata() {
    let mut bundle = production_ready_bundle();
    bundle["graph_route_readiness"]
        .as_object_mut()
        .unwrap()
        .remove("route_catalog");
    bundle["graph_route_readiness"]["routes"][0]
        .as_object_mut()
        .unwrap()
        .remove("owner");

    let direct =
        nowledge_graph_route_readiness_summary(bundle.get("graph_route_readiness").unwrap());

    assert!(!direct.ready);
    assert!(!direct.route_catalog_metadata_ready);
    assert_eq!(
        direct.missing_route_catalog_metadata_routes,
        vec![REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0]]
    );
    assert_eq!(
        direct.route_catalog_metadata_mismatch_routes,
        vec!["__route_catalog__".to_string()]
    );
}

#[test]
fn graph_route_readiness_summary_requires_api_behavior_evidence() {
    let mut bundle = production_ready_bundle();
    bundle["graph_route_readiness"]
        .as_object_mut()
        .unwrap()
        .remove("route_query_api_behavior_evidence_ready");

    let direct =
        nowledge_graph_route_readiness_summary(bundle.get("graph_route_readiness").unwrap());
    let summary = nowledge_replacement_summary_json(&bundle);

    assert!(!direct.ready);
    assert_eq!(direct.route_query_api_behavior_evidence_ready, None);
    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["graph_route_readiness"]["ready"], false);
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "graph_route_readiness_ready"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "attach_graph_route_readiness_evidence"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| {
                        field == "graph_route_readiness.route_query_api_behavior_evidence_ready"
                    })
        }));
}

#[test]
fn replacement_summary_reports_trace_candidate_evidence_but_blocks_production() {
    let mut bundle = production_ready_bundle();
    bundle["search_candidate_shadow_evidence"] = ready_search_candidate_trace_evidence();

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["evidence_source"],
        NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["primary_engine"],
        NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["shadow_engine"],
        NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["row_count_parity"],
        true
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"],
        true
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence_ready"));
}

#[test]
fn replacement_summary_rejects_weak_search_candidate_trace_evidence() {
    let mut bundle = production_ready_bundle();
    bundle["search_candidate_shadow_evidence"] = ready_search_candidate_trace_evidence();
    bundle["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"] =
        serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["shadow_scan_field_summary_count"] =
        serde_json::json!(0);
    bundle["search_candidate_shadow_evidence"]["blocker_codes"] =
        serde_json::json!(["search_candidate_field_pruning_missing"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["shadow_scan_field_summary_count"],
        0
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence_ready"));
}

#[test]
fn replacement_summary_requires_search_candidate_bridge_scan_fields() {
    let mut bundle = production_ready_bundle();
    bundle["search_candidate_shadow_evidence"]["row_count_parity"] = serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["shadow_scan_filter_pushdown_ready"] =
        serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"] =
        serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["shadow_scan_field_summary_count"] =
        serde_json::json!(0);
    bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["row_count_parity"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["shadow_scan_filter_pushdown_ready"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"],
        false
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence_ready"));
}

#[test]
fn replacement_summary_requires_search_candidate_retriever_leg_evidence() {
    let mut bundle = production_ready_bundle();
    bundle["search_candidate_shadow_evidence"]["text_retriever_ready"] = serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["vector_retriever_ready"] = serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["text_retriever_ready"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["vector_retriever_ready"],
        false
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence_ready"));
}

#[test]
fn replacement_summary_requires_search_candidate_top_k_overlap_evidence() {
    let mut bundle = production_ready_bundle();
    bundle["search_candidate_shadow_evidence"]["fts_top_k_overlap_ready"] =
        serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["vector_top_k_overlap_ready"] =
        serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["fts_top_k_overlap_ready"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["vector_top_k_overlap_ready"],
        false
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence_ready"));
}

#[test]
fn replacement_summary_requires_search_candidate_readiness_evidence() {
    let mut bundle = production_ready_bundle();
    bundle["search_candidate_shadow_evidence"]["candidate_readiness"] = serde_json::json!({
        "source_chunk_identity_ready": false,
        "fail_soft_observed": false,
        "projection_marker_status_visible": false,
        "projection_watermark_ready": false,
        "embedding_identity_ready": false,
    });
    bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["source_chunk_identity_ready"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["fail_soft_observed"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["projection_marker_status_visible"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["projection_watermark_ready"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["embedding_identity_ready"],
        false
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence_ready"));
}

#[test]
fn replacement_summary_blocks_production_without_required_query_families() {
    let mut bundle = production_ready_bundle();
    bundle["replacement_readiness_by_query_family"] = serde_json::json!([
        {
            "query_family": "read",
            "replacement_readiness_per_million": 1_000_000
        },
        {
            "query_family": "mutation",
            "replacement_readiness_per_million": 1_000_000
        }
    ]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "query_family_readiness"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "replacement_readiness_by_query_family_ready"));
    assert_eq!(
        summary["replacement_readiness_family_summary"]["missing_required_query_families"],
        serde_json::json!([
            "memory_lookup",
            "graph_traversal",
            "projected_graph",
            "label_stats_read",
            "search_projection"
        ])
    );
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "close_blocked_query_families"));
}

#[test]
fn replacement_summary_blocks_production_without_search_projection_evidence() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("search_projection_evidence");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_evidence"]["present"], false);
    assert_eq!(summary["search_projection_evidence"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_evidence"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "attach_search_projection_replacement_evidence"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "search_projection_evidence.fts_ready")
        }));
}

#[test]
fn replacement_summary_recomputes_search_projection_evidence_readiness() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_projection_evidence"]["covered_table_count"] = serde_json::json!(5);
    bundle["search_projection_evidence"]["blocker_codes"] =
        serde_json::json!(["missing_source_chunks_index"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_evidence"]["covered_table_count"],
        5
    );
    assert_eq!(
        summary["search_projection_evidence"]["blocker_codes"],
        serde_json::json!(["missing_source_chunks_index"])
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_evidence_ready"));
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_evidence"));
}

#[test]
fn replacement_summary_requires_search_projection_document_identity() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_projection_evidence"]["document_identity_ready"] = serde_json::json!(false);
    bundle["search_projection_evidence"]["blocker_codes"] =
        serde_json::json!(["document_identity_not_ready"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_evidence"]["document_identity_ready"],
        false
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_evidence_ready"));
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_evidence"));
}

#[test]
fn replacement_summary_requires_search_projection_production_filter_pruning() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_projection_evidence"]["production_filter_pruning_ready"] =
        serde_json::json!(false);
    bundle["search_projection_evidence"]["blocker_codes"] =
        serde_json::json!(["hawdb_production_filter_pruning_not_ready"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_evidence"]["production_filter_pruning_ready"],
        false
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_evidence_ready"));
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_evidence"));
}

#[test]
fn replacement_summary_requires_search_projection_evidence_protocol() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_evidence"]["protocol"] = serde_json::json!("handwritten");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["search_projection_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_evidence"]["protocol"],
        "handwritten"
    );
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "attach_search_projection_replacement_evidence"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "search_projection_evidence.protocol")
        }));
}

#[test]
fn replacement_summary_blocks_production_without_search_projection_shadow_evidence() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("search_projection_shadow_evidence");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["present"],
        false
    );
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_shadow_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_shadow_evidence"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "run_search_projection_shadow_evidence"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "search_projection_shadow_evidence.table_parity.ready")
        }));
}

#[test]
fn replacement_summary_recomputes_search_projection_shadow_evidence_readiness() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["table_parity"]["ready"] = serde_json::json!(false);
    bundle["search_projection_shadow_evidence"]["blocker_codes"] =
        serde_json::json!(["table_parity_mismatch"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["table_parity_ready"],
        false
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["blocker_codes"],
        serde_json::json!(["table_parity_mismatch"])
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_shadow_evidence_ready"));
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_shadow_evidence"));
}

#[test]
fn replacement_summary_requires_search_projection_shadow_document_identity() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["document_count_parity"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["document_identity_parity"] =
        serde_json::json!(false);
    bundle["search_projection_shadow_evidence"]["blocker_codes"] =
        serde_json::json!(["document_identity_mismatch"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["document_count_parity"],
        true
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["document_identity_parity"],
        false
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["blocker_codes"],
        serde_json::json!(["document_identity_mismatch"])
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_shadow_evidence_ready"));
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_shadow_evidence"));
}

#[test]
fn replacement_summary_requires_search_projection_shadow_pushdown_evidence() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"] =
        serde_json::json!(false);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_persisted_segment_descriptor_ready"] = serde_json::json!(false);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"],
        false
    );
    assert!(
        summary["search_projection_shadow_evidence"]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY)
    );
    assert!(
        summary["search_projection_shadow_evidence"]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING)
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_shadow_evidence_ready"));
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_shadow_evidence"));
}

#[test]
fn replacement_summary_requires_search_projection_shadow_descriptor_fields() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"] =
        serde_json::json!(false);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_descriptor_scan_filter_fields_ready"] = serde_json::json!(false);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_descriptor_field_summaries"] = serde_json::json!([]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_scan_filter_fields_ready"],
        false
    );
    assert!(
        summary["search_projection_shadow_evidence"]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING)
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_projection_shadow_evidence_ready"));
}

#[test]
fn replacement_summary_recomputes_search_projection_shadow_descriptor_field_coverage() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"] =
        serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_descriptor_scan_filter_fields_ready"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["primary_scan_filter_fields"] = serde_json::json!(["unit_type", "importance"]);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]["shadow_scan_filter_fields"] =
        serde_json::json!(["unit_type", "importance"]);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_descriptor_field_summaries"] =
        serde_json::json!([{ "field": "unit_type" }, { "field": "importance" }]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"],
        false
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_scan_filter_fields_ready"],
        false
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["scan_filter_fields_ready"],
        false
    );
    assert!(
        summary["search_projection_shadow_evidence"]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING)
    );
}

#[test]
fn replacement_summary_recomputes_search_projection_shadow_descriptor_capabilities() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"] =
        serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_descriptor_scan_filter_fields_ready"] = serde_json::json!(true);
    let summaries = bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_descriptor_field_summaries"]
        .as_array_mut()
        .unwrap();
    let confidence = summaries
        .iter_mut()
        .find(|summary| summary["field"] == "confidence")
        .unwrap();
    confidence["numeric_range_summary_used"] = serde_json::json!(false);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"],
        false
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_capabilities_ready"],
        false
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["missing_numeric_range_fields"],
        serde_json::json!(["confidence"])
    );
    assert!(
        summary["search_projection_shadow_evidence"]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING)
    );
}

#[test]
fn replacement_summary_recomputes_search_projection_shadow_descriptor_summary_counts() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"] =
        serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_descriptor_scan_filter_fields_ready"] = serde_json::json!(true);
    let summaries = bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_descriptor_field_summaries"]
        .as_array_mut()
        .unwrap();
    let unit_type = summaries
        .iter_mut()
        .find(|summary| summary["field"] == "unit_type")
        .unwrap();
    unit_type["value_summary_used"] = serde_json::json!(true);
    unit_type["value_summary_segment_count"] = serde_json::json!(0);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"],
        false
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_capabilities_ready"],
        false
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["missing_value_summary_fields"],
        serde_json::json!(["unit_type"])
    );
    assert!(
        summary["search_projection_shadow_evidence"]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == HAWDB_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING)
    );
}

#[test]
fn replacement_summary_recomputes_search_projection_shadow_document_pruning() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"] =
        serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_document_pruning_ready"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_pruned_document_count"] = serde_json::json!(0);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]["ready"],
        false
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_document_pruning_ready"],
        false
    );
    assert_eq!(
        summary["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_pruned_document_count"],
        0
    );
    assert!(
        summary["search_projection_shadow_evidence"]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY)
    );
}

#[test]
fn replacement_summary_requires_search_projection_shadow_evidence_protocol() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_shadow_evidence"]["protocol"] = serde_json::json!("handwritten");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["protocol"],
        "handwritten"
    );
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "run_search_projection_shadow_evidence"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "search_projection_shadow_evidence.protocol")
        }));
}

#[test]
fn replacement_summary_requires_search_projection_shadow_evidence_source() {
    let mut bundle = production_ready_bundle();
    bundle["search_projection_shadow_evidence"]["evidence_source"] =
        serde_json::json!("manual-json");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_projection_shadow_evidence"]["evidence_source"],
        "manual-json"
    );
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "run_search_projection_shadow_evidence"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "search_projection_shadow_evidence.evidence_source")
        }));
}

#[test]
fn replacement_summary_blocks_production_without_search_candidate_shadow_evidence() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("search_candidate_shadow_evidence");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["present"],
        false
    );
    assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "run_search_candidate_shadow_evidence"));
}

#[test]
fn replacement_summary_blocks_production_without_source_mutation_readiness() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("source_mutation_dual_write_readiness");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(
        summary["source_mutation_dual_write_readiness"]["present"],
        false
    );
    assert_eq!(
        summary["source_mutation_dual_write_readiness"]["ready"],
        false
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "source_mutation_dual_write_readiness"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "source_mutation_dual_write_readiness"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "attach_source_mutation_dual_write_readiness"));
}

#[test]
fn replacement_summary_blocks_production_when_source_mutation_readiness_is_incomplete() {
    let mut bundle = production_ready_bundle();
    bundle["source_mutation_dual_write_readiness"]["ready"] = serde_json::json!(false);
    bundle["source_mutation_dual_write_readiness"]["ready_family_count"] =
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len() - 1);
    bundle["source_mutation_dual_write_readiness"]["missing_required_families"] =
        serde_json::json!(["source_ingest_create"]);
    bundle["source_mutation_dual_write_readiness"]["blocker_codes"] =
        serde_json::json!(["source_mutation_dual_write_missing_required_families"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(
        summary["source_mutation_dual_write_readiness"]["ready"],
        false
    );
    assert_eq!(
        summary["source_mutation_dual_write_readiness"]["missing_required_families"],
        serde_json::json!(["source_ingest_create"])
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "source_mutation_dual_write_readiness_ready"));
    assert!(summary["blockers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| { item == "source_mutation_dual_write_missing_required_families" }));
}

#[test]
fn replacement_summary_blocks_production_without_search_route_ownership() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("search_route_ownership");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["search_route_ownership"]["present"], false);
    assert_eq!(summary["search_route_ownership"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_route_ownership"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_route_ownership"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "attach_search_route_ownership"));
}

#[test]
fn replacement_summary_blocks_production_without_active_search_route_ownership() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("active_search_route_ownership");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["active_search_route_ownership"]["present"], false);
    assert_eq!(summary["active_search_route_ownership"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "active_search_route_ownership"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "active_search_route_ownership"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "attach_active_search_route_ownership"));
}

#[test]
fn replacement_summary_blocks_production_when_active_search_routes_still_use_lancedb() {
    let mut bundle = production_ready_bundle();
    bundle["active_search_route_ownership"]["ready"] = serde_json::json!(false);
    bundle["active_search_route_ownership"]["production_cutover_ready"] = serde_json::json!(false);
    bundle["active_search_route_ownership"]["hawdb_route_count"] =
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len() - 1);
    bundle["active_search_route_ownership"]["lancedb_route_count"] = serde_json::json!(1);
    bundle["active_search_route_ownership"]["lancedb_routes"] = serde_json::json!(["mcp_search"]);
    bundle["active_search_route_ownership"]["blocker_codes"] =
        serde_json::json!(["active_search_route_ownership_lancedb_routes_remaining"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["active_search_route_ownership"]["ready"], false);
    assert_eq!(
        summary["active_search_route_ownership"]["lancedb_route_count"],
        1
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "active_search_route_ownership"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "active_search_route_ownership_ready"));
    assert!(summary["blockers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "active_search_route_ownership_lancedb_routes_remaining"));
}

#[test]
fn replacement_summary_blocks_production_without_active_search_route_readiness() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("active_search_route_readiness");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["active_search_route_readiness"]["present"], false);
    assert_eq!(summary["active_search_route_readiness"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "active_search_route_readiness"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "active_search_route_readiness"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "attach_active_search_route_readiness"));
}

#[test]
fn replacement_summary_blocks_production_when_active_search_reads_still_need_lancedb() {
    let mut bundle = production_ready_bundle();
    bundle["active_search_route_readiness"]["ready"] = serde_json::json!(false);
    bundle["active_search_route_readiness"]["production_cutover_ready"] = serde_json::json!(false);
    bundle["active_search_route_readiness"]["ready_route_count"] =
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len() - 1);
    bundle["active_search_route_readiness"]["lancedb_handle_required_route_count"] =
        serde_json::json!(1);
    bundle["active_search_route_readiness"]["lancedb_handle_required_routes"] =
        serde_json::json!(["source_chunk_recall"]);
    bundle["active_search_route_readiness"]["blocker_codes"] =
        serde_json::json!(["active_search_route_readiness_lancedb_handle_required"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["active_search_route_readiness"]["ready"], false);
    assert_eq!(
        summary["active_search_route_readiness"]["lancedb_handle_required_route_count"],
        1
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "active_search_route_readiness"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "active_search_route_readiness_ready"));
    assert!(summary["blockers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "active_search_route_readiness_lancedb_handle_required"));
}

#[test]
fn replacement_summary_requires_search_candidate_identity_parity() {
    let mut bundle = production_ready_bundle();
    bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["candidate_identity"]["ready"] =
        serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["candidate_identity"]["parity"] =
        serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["blocker_codes"] =
        serde_json::json!(["search_candidate_identity_mismatch"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["candidate_identity_ready"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["candidate_identity_parity"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["blocker_codes"],
        serde_json::json!(["search_candidate_identity_mismatch"])
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence_ready"));
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence"));
}

#[test]
fn replacement_summary_requires_search_candidate_filter_pushdown() {
    let mut bundle = production_ready_bundle();
    bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["ready"] =
        serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["field_summary_count"] =
        serde_json::json!(0);
    bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["missing_required_fields"] =
        serde_json::json!(["unit_type"]);
    bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["field_capabilities_ready"] =
        serde_json::json!(false);
    bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["missing_value_summary_fields"] =
        serde_json::json!(["unit_type"]);
    bundle["search_candidate_shadow_evidence"]["blocker_codes"] = serde_json::json!([
        "search_candidate_field_pruning_missing",
        "search_candidate_field_pruning_capability_missing"
    ]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["filter_pushdown_ready"],
        false
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["filter_pushdown_field_summary_count"],
        0
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence_ready"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "run_search_candidate_shadow_evidence"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "search_candidate_shadow_evidence.filter_pushdown.ready")
        }));
}

#[test]
fn replacement_summary_recomputes_search_candidate_filter_pushdown_capabilities() {
    let mut bundle = production_ready_bundle();
    bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["ready"] =
        serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["field_capabilities_ready"] =
        serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["missing_numeric_range_fields"] =
        serde_json::json!(["importance"]);
    bundle["search_candidate_shadow_evidence"]["blocker_codes"] = serde_json::json!([]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["filter_pushdown_field_capabilities_ready"],
        true
    );
    assert_eq!(
        summary["search_candidate_shadow_evidence"]["filter_pushdown_missing_numeric_range_fields"],
        serde_json::json!(["importance"])
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "search_candidate_shadow_evidence_ready"));
}

#[test]
fn replacement_summary_blocks_production_without_bounded_read_evidence() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("bounded_read_evidence");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["bounded_read_evidence"]["present"], false);
    assert_eq!(summary["bounded_read_evidence"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "bounded_read_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "bounded_read_evidence"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "attach_bounded_read_profile"));
}

#[test]
fn replacement_summary_requires_shadow_read_only_bounded_read_evidence() {
    let mut bundle = production_ready_bundle();
    bundle["bounded_read_evidence"]["mode"] = serde_json::json!("writable_cutover");
    bundle["bounded_read_evidence"]["blocker_codes"] = serde_json::json!(["not_shadow_read_only"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["bounded_read_evidence"]["ready"], false);
    assert_eq!(summary["bounded_read_evidence"]["mode"], "writable_cutover");
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "bounded_read_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "bounded_read_evidence_ready"));
}

#[test]
fn replacement_summary_blocks_payload_budget_exceeded_bounded_read_evidence() {
    let mut bundle = production_ready_bundle();
    bundle["bounded_read_evidence"]["ready"] = serde_json::json!(false);
    bundle["bounded_read_evidence"]["estimated_payload_bytes"] = serde_json::json!(8192);
    bundle["bounded_read_evidence"]["max_estimated_payload_bytes"] = serde_json::json!(4096);
    bundle["bounded_read_evidence"]["payload_budget_exceeded"] = serde_json::json!(true);
    bundle["bounded_read_evidence"]["blocker_codes"] =
        serde_json::json!(["payload_budget_exceeded"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["bounded_read_evidence"]["ready"], false);
    assert_eq!(
        summary["bounded_read_evidence"]["estimated_payload_bytes"],
        8192
    );
    assert_eq!(
        summary["bounded_read_evidence"]["max_estimated_payload_bytes"],
        4096
    );
    assert_eq!(
        summary["bounded_read_evidence"]["payload_budget_exceeded"],
        true
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "bounded_read_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "bounded_read_evidence_ready"));
}

#[test]
fn replacement_summary_requires_bounded_read_route_coverage() {
    let mut bundle = production_ready_bundle();
    let covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| *route != "/graph/explore")
        .collect::<Vec<_>>();
    bundle["bounded_read_evidence"]["covered_routes"] = serde_json::json!(covered_routes);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["bounded_read_evidence"]["ready"], false);
    assert_eq!(
        summary["bounded_read_evidence"]["missing_covered_routes"],
        serde_json::json!(["/graph/explore"])
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "bounded_read_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "bounded_read_evidence_ready"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "attach_bounded_read_profile"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "bounded_read_evidence.covered_routes")
        }));
}

#[test]
fn replacement_summary_requires_bounded_read_graph_route_summary() {
    let mut bundle = production_ready_bundle();
    bundle["bounded_read_evidence"]
        .as_object_mut()
        .unwrap()
        .remove("route_relationship_property_pruning_evidence_ready");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["bounded_read_evidence"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "bounded_read_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "bounded_read_evidence_ready"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "attach_bounded_read_profile"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| {
                        field == "bounded_read_evidence.route_relationship_property_pruning_evidence_ready"
                    })
        }));
}

#[test]
fn replacement_summary_requires_bounded_read_api_behavior_evidence() {
    let mut bundle = production_ready_bundle();
    bundle["bounded_read_evidence"]
        .as_object_mut()
        .unwrap()
        .remove("route_query_api_behavior_evidence_ready");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["bounded_read_evidence"]["ready"], false);
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "bounded_read_evidence_ready"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "attach_bounded_read_profile"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| {
                        field == "bounded_read_evidence.route_query_api_behavior_evidence_ready"
                    })
        }));
}

#[test]
fn replacement_summary_requires_bounded_read_evidence_protocol() {
    let mut bundle = production_ready_bundle();
    bundle["bounded_read_evidence"]["protocol"] = serde_json::json!("handwritten");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["bounded_read_evidence"]["ready"], false);
    assert_eq!(summary["bounded_read_evidence"]["protocol"], "handwritten");
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "attach_bounded_read_profile"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "bounded_read_evidence.protocol")
        }));
}

#[test]
fn replacement_summary_blocks_production_without_graph_route_readiness() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("graph_route_readiness");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["graph_route_readiness"]["present"], false);
    assert_eq!(summary["graph_route_readiness"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "graph_route_readiness"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "graph_route_readiness"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "attach_graph_route_readiness_evidence"));
}

#[test]
fn replacement_summary_rejects_stale_graph_route_coverage() {
    let mut bundle = production_ready_bundle();
    let covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| *route != "/graph/explore")
        .collect::<Vec<_>>();
    bundle["graph_route_readiness"]["covered_routes"] = serde_json::json!(covered_routes);
    bundle["graph_route_readiness"]["covered_route_count"] =
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() - 1);
    bundle["graph_route_readiness"]["evidence_route_coverage_matches"] = serde_json::json!(false);
    bundle["graph_route_readiness"]["route_primary_blocker_codes"] =
        serde_json::json!(["route_coverage_evidence_mismatch"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["graph_route_readiness"]["ready"], false);
    assert_eq!(
        summary["graph_route_readiness"]["missing_required_routes"],
        serde_json::json!(["/graph/explore"])
    );
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "graph_route_readiness_ready"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "attach_graph_route_readiness_evidence"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "graph_route_readiness.evidence_route_coverage_matches")
        }));
}

#[test]
fn replacement_summary_blocks_production_without_query_runtime_preflight() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("query_runtime_preflight");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["query_runtime_preflight"]["present"], false);
    assert_eq!(summary["query_runtime_preflight"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "query_runtime_preflight"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "query_runtime_preflight"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "attach_query_runtime_preflight_evidence"));
}

#[test]
fn replacement_summary_requires_query_runtime_preflight_route_coverage() {
    let mut bundle = production_ready_bundle();
    bundle["query_runtime_preflight"]["covered_route_count"] = serde_json::json!(14);
    bundle["query_runtime_preflight"]["required_routes_covered"] = serde_json::json!(false);
    bundle["query_runtime_preflight"]["missing_required_routes"] =
        serde_json::json!(["/graph/explore"]);
    bundle["query_runtime_preflight"]["covered_routes"] = serde_json::json!([
        "/graph/overview",
        "/graph/expand/{node_id}",
        "/graph/live-preview",
        "/graph/live-preview/{node_id}",
        "/graph/community-members/{community_id}",
        "/library/community/{community_id}/subgraph",
        "/library/community/{community_id}/recent-memories",
        "/library/community/{community_id}/related",
        "/graph/analysis",
        "/graph/augmentation/state",
        "/graph/augmentation/pagerank/plan",
        "/graph/node-details/{node_id}",
        "/graph/orphans",
        "/graph/shortest-path"
    ]);
    bundle["query_runtime_preflight"]["probes"]
        .as_array_mut()
        .unwrap()
        .retain(|probe| {
            probe.get("route").and_then(serde_json::Value::as_str) != Some("/graph/explore")
        });

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["query_runtime_preflight"]["ready"], false);
    assert_eq!(
        summary["query_runtime_preflight"]["route_coverage_ready"],
        false
    );
    assert_eq!(
        summary["query_runtime_preflight"]["missing_required_routes"],
        serde_json::json!(["/graph/explore"])
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "query_runtime_preflight"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "query_runtime_preflight_ready"));
}

#[test]
fn replacement_summary_recomputes_query_runtime_route_coverage_from_probes() {
    let mut bundle = production_ready_bundle();
    bundle["query_runtime_preflight"]["covered_route_count"] = serde_json::json!(15);
    bundle["query_runtime_preflight"]["required_routes_covered"] = serde_json::json!(true);
    bundle["query_runtime_preflight"]["missing_required_routes"] = serde_json::json!([]);
    bundle["query_runtime_preflight"]["covered_routes"] =
        serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES);
    bundle["query_runtime_preflight"]["probes"]
        .as_array_mut()
        .unwrap()
        .retain(|probe| {
            probe.get("route").and_then(serde_json::Value::as_str) != Some("/graph/explore")
        });

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["query_runtime_preflight"]["ready"], false);
    assert_eq!(
        summary["query_runtime_preflight"]["route_coverage_ready"],
        false
    );
    assert_eq!(
        summary["query_runtime_preflight"]["missing_required_routes"],
        serde_json::json!(["/graph/explore"])
    );
    assert!(!summary["query_runtime_preflight"]["covered_routes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "/graph/explore"));
}

#[test]
fn replacement_summary_rejects_query_runtime_unknown_route_probe() {
    let mut bundle = production_ready_bundle();
    let mut probe = bundle["query_runtime_preflight"]["probes"][0].clone();
    probe["route"] = serde_json::json!("/graph/stale-route");
    bundle["query_runtime_preflight"]["probes"]
        .as_array_mut()
        .unwrap()
        .push(probe);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["query_runtime_preflight"]["ready"], false);
    assert_eq!(
        summary["query_runtime_preflight"]["route_coverage_ready"],
        false
    );
    assert_eq!(
        summary["query_runtime_preflight"]["unknown_routes"],
        serde_json::json!(["/graph/stale-route"])
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "query_runtime_preflight"));
}

#[test]
fn replacement_summary_rejects_query_runtime_duplicate_route_probe() {
    let mut bundle = production_ready_bundle();
    let probe = bundle["query_runtime_preflight"]["probes"][0].clone();
    bundle["query_runtime_preflight"]["probes"]
        .as_array_mut()
        .unwrap()
        .push(probe);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["query_runtime_preflight"]["ready"], false);
    assert_eq!(
        summary["query_runtime_preflight"]["route_coverage_ready"],
        false
    );
    assert_eq!(
        summary["query_runtime_preflight"]["duplicate_routes"],
        serde_json::json!([REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0]])
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "query_runtime_preflight"));
}

#[test]
fn replacement_summary_requires_workload_fixture_evidence() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("workload_fixture_evidence");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["workload_fixture_evidence"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "workload_fixture_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "workload_fixture_evidence"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "run_workload_fixture_evidence"));
}

#[test]
fn replacement_summary_rejects_failed_workload_fixture_probe_counts() {
    let mut bundle = production_ready_bundle();
    bundle["workload_fixture_evidence"]["ready"] = serde_json::json!(false);
    bundle["workload_fixture_evidence"]["failed_search_metadata_probe_count"] =
        serde_json::json!(1);
    bundle["workload_fixture_evidence"]["blocker_codes"] =
        serde_json::json!(["workload_fixture_search_metadata_not_ready"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(
        summary["workload_fixture_evidence"]["failed_search_metadata_probe_count"],
        1
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "workload_fixture_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "workload_fixture_evidence_ready"));
    assert!(summary["blockers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "workload_fixture_search_metadata_not_ready"));
}

#[test]
fn replacement_summary_rejects_failed_graph_rag_workload_fixture() {
    let mut bundle = production_ready_bundle();
    bundle["workload_fixture_evidence"]["ready"] = serde_json::json!(false);
    bundle["workload_fixture_evidence"]["failed_graph_rag_probe_count"] = serde_json::json!(1);
    bundle["workload_fixture_evidence"]["graph_rag_reports"] = serde_json::json!([
        {
            "name": "memory-to-entity",
            "ready": false,
            "schema_protocol": GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL,
            "label_count": 2,
            "relationship_type_count": 1,
            "route_count": 1,
            "parameter_requirement_count": 1,
            "row_count": 1,
            "row_budget_exceeded": false,
            "payload_budget_exceeded": false,
            "blocking_operator_count": 0,
            "streaming": false,
            "error_class": "execution"
        }
    ]);
    bundle["workload_fixture_evidence"]["blocker_codes"] =
        serde_json::json!(["workload_fixture_graph_rag_not_ready"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(
        summary["workload_fixture_evidence"]["failed_graph_rag_probe_count"],
        1
    );
    assert_eq!(
        summary["workload_fixture_evidence"]["graph_rag_reports"][0]["ready"],
        false
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "workload_fixture_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "workload_fixture_evidence_ready"));
    assert!(summary["blockers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "workload_fixture_graph_rag_not_ready"));
}

#[test]
fn replacement_summary_rejects_failed_source_projection_workload_fixture() {
    let mut bundle = production_ready_bundle();
    bundle["workload_fixture_evidence"]["ready"] = serde_json::json!(false);
    bundle["workload_fixture_evidence"]["failed_source_projection_probe_count"] =
        serde_json::json!(1);
    bundle["workload_fixture_evidence"]["source_projection_reports"] = serde_json::json!([
        {
            "name": "source-ingest-composite-changefeed",
            "ready": false,
            "source_graph_commit_epoch": 43,
            "complete_through_graph_commit_epoch": 43,
            "too_small_batch_failed_closed": false,
            "operation_count": 1,
            "upserted_documents": 1,
            "deleted_documents": 0,
            "source_document_count": 1,
            "indexed_source_document_ready": false,
            "error_class": "batch_split"
        }
    ]);
    bundle["workload_fixture_evidence"]["blocker_codes"] =
        serde_json::json!(["workload_fixture_source_projection_not_ready"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(
        summary["workload_fixture_evidence"]["failed_source_projection_probe_count"],
        1
    );
    assert_eq!(
        summary["workload_fixture_evidence"]["source_projection_reports"][0]
            ["too_small_batch_failed_closed"],
        false
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "workload_fixture_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "workload_fixture_evidence_ready"));
    assert!(summary["blockers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "workload_fixture_source_projection_not_ready"));
}

#[test]
fn replacement_summary_blocks_production_without_graph_delta_aggregate_evidence() {
    let mut bundle = production_ready_bundle();
    bundle["cutover_evidence"]
        .as_object_mut()
        .unwrap()
        .remove("background_maintenance_admitted_search_projection_graph_delta_count");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "background_maintenance"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "background_maintenance_search_projection_graph_delta"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| {
            item["action"] == "attach_background_maintenance_report"
                && item["evidence_fields"].as_array().unwrap().iter().any(|field| {
                    field
                        == "cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count"
                })
        }));
}

#[test]
fn replacement_summary_blocks_production_when_dual_engine_evidence_is_not_ready() {
    let mut bundle = production_ready_bundle();
    bundle["dual_engine_evidence"]["ready"] = serde_json::json!(false);
    bundle["dual_engine_evidence"]["primary_only_check_count"] = serde_json::json!(1);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["dual_engine_evidence"]["present"], true);
    assert_eq!(summary["dual_engine_evidence"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "dual_engine_evidence"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "rerun_dual_engine_shadow_gate"));
}

#[test]
fn replacement_summary_blocks_production_when_dual_engine_counts_are_inconsistent() {
    let mut bundle = production_ready_bundle();
    bundle["dual_engine_evidence"]["ready"] = serde_json::json!(true);
    bundle["dual_engine_evidence"]["matched_check_count"] = serde_json::json!(0);
    bundle["dual_engine_evidence"]["primary_only_check_count"] = serde_json::json!(1);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["dual_engine_evidence"]["ready"], true);
    assert_eq!(summary["dual_engine_evidence"]["consistent"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "dual_engine_evidence"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "rerun_dual_engine_shadow_gate"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "dual_engine_evidence.consistent")
        }));
}

#[test]
fn replacement_summary_blocks_production_without_dual_engine_evidence() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("dual_engine_evidence");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["dual_engine_evidence"]["present"], false);
    assert_eq!(
        summary["dual_engine_evidence"]["ready"],
        serde_json::Value::Null
    );
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "dual_engine_evidence"));
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "dual_engine_evidence"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "rerun_dual_engine_shadow_gate"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "dual_engine_evidence.present")
        }));
}

#[test]
fn replacement_summary_blocks_production_without_previous_wrapper_contract_evidence() {
    let mut bundle = production_ready_bundle();
    bundle
        .as_object_mut()
        .unwrap()
        .remove("previous_wrapper_contract_evidence");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(
        summary["previous_wrapper_contract_evidence"]["ready"],
        false
    );
    assert_eq!(
        summary["missing_evidence"],
        serde_json::json!(["previous_wrapper_contract_evidence"])
    );
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "run_full_previous_wrapper_contract_check"));
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "previous_wrapper_contract"));
}

#[test]
fn replacement_summary_blocks_production_without_shadow_wrapper_identity() {
    let mut bundle = production_ready_bundle();
    bundle["shadow_ready"]
        .as_object_mut()
        .unwrap()
        .remove("wrapper_identity");

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["shadow_evidence"]["ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "shadow_parity"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "run_previous_wrapper_shadow_gate"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "shadow_ready.wrapper_identity")
        }));
}

#[test]
fn replacement_summary_blocks_production_without_full_contract_evidence() {
    let mut bundle = production_ready_bundle();
    bundle["full_contract_checked"] = serde_json::json!(false);
    bundle["full_contract_ready"] = serde_json::json!(false);
    bundle["selected_checks"] = serde_json::json!(1);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert_eq!(summary["production_replacement_per_million"], 0);
    assert_eq!(summary["previous_wrapper_contract_evidence"]["ready"], true);
    assert_eq!(summary["full_contract_evidence"]["ready"], false);
    assert!(summary["missing_evidence"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "full_contract_evidence"));
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "previous_wrapper_contract"));
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "run_full_previous_wrapper_contract_check"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "full_contract_ready")
        }));
}

#[test]
fn replacement_summary_can_omit_family_details_for_compact_output() {
    let bundle = production_ready_bundle();

    let summary = nowledge_replacement_summary_json_with_options(
        &bundle,
        NowledgeReplacementSummaryOptions {
            include_family_details: false,
            max_family_items: None,
            include_blocker_details: true,
            max_blockers: None,
        },
    );

    assert_eq!(
        summary["replacement_readiness_by_query_family"],
        serde_json::json!([])
    );
    assert_eq!(
        summary["replacement_readiness_family_summary"]["total_count"],
        REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.len()
    );
    assert_eq!(
        summary["replacement_readiness_family_summary"]["ready_count"],
        REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.len()
    );
    assert_eq!(
        summary["replacement_readiness_family_summary"]["omitted_count"],
        REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.len()
    );
    assert_eq!(summary["production_cutover_ready"], true);
}

#[test]
fn replacement_summary_can_limit_family_details() {
    let bundle = serde_json::json!({
        "inventory_gate": {
            "coverage_per_million": 1_000_000,
            "blockers": []
        },
        "cutover": {
            "decision": "ready",
            "matched_per_million": 1_000_000,
            "blockers": []
        },
        "migration_gate": {
            "decision": "ready",
            "blockers": []
        },
        "cutover_evidence": {
            "eligible": true,
            "replacement_readiness_invalid_family_count": 0,
            "blockers": []
        },
        "shadow_run": {},
        "shadow_ready": {},
        "replacement_readiness_per_million": 1_000_000,
        "replacement_readiness_by_query_family": [
            {
                "query_family": "memory_lookup",
                "replacement_readiness_per_million": 1_000_000
            },
            {
                "query_family": "search_projection",
                "replacement_readiness_per_million": 0
            }
        ]
    });

    let summary = nowledge_replacement_summary_json_with_options(
        &bundle,
        NowledgeReplacementSummaryOptions {
            include_family_details: true,
            max_family_items: Some(1),
            include_blocker_details: true,
            max_blockers: None,
        },
    );

    assert_eq!(
        summary["replacement_readiness_by_query_family"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        summary["replacement_readiness_family_summary"]["total_count"],
        2
    );
    assert_eq!(
        summary["replacement_readiness_family_summary"]["ready_count"],
        1
    );
    assert_eq!(
        summary["replacement_readiness_family_summary"]["blocked_count"],
        1
    );
    assert_eq!(
        summary["replacement_readiness_family_summary"]["omitted_count"],
        1
    );
    assert_eq!(
        summary["replacement_readiness_family_summary"]["blocked_query_families"],
        serde_json::json!(["search_projection"])
    );
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| action["action"] == "close_blocked_query_families"));
}

#[test]
fn replacement_summary_can_omit_blocker_details_for_compact_output() {
    let bundle = blocked_bundle();

    let summary = nowledge_replacement_summary_json_with_options(
        &bundle,
        NowledgeReplacementSummaryOptions {
            include_family_details: false,
            max_family_items: None,
            include_blocker_details: false,
            max_blockers: None,
        },
    );

    assert_eq!(summary["blockers"], serde_json::json!([]));
    assert_eq!(summary["blocker_summary"]["total_count"], 5);
    assert_eq!(summary["blocker_summary"]["omitted_count"], 5);
    assert_eq!(
        summary["replacement_readiness_by_query_family"],
        serde_json::json!([])
    );
}

#[test]
fn replacement_summary_can_limit_blocker_details() {
    let bundle = blocked_bundle();

    let summary = nowledge_replacement_summary_json_with_options(
        &bundle,
        NowledgeReplacementSummaryOptions {
            include_family_details: true,
            max_family_items: None,
            include_blocker_details: true,
            max_blockers: Some(2),
        },
    );

    assert_eq!(summary["blockers"].as_array().unwrap().len(), 2);
    assert_eq!(summary["blocker_summary"]["total_count"], 5);
    assert_eq!(summary["blocker_summary"]["omitted_count"], 3);
}

#[test]
fn replacement_summary_groups_storage_and_background_blockers() {
    let bundle = serde_json::json!({
        "inventory_gate": {
            "coverage_per_million": 1_000_000,
            "blockers": []
        },
        "cutover": {
            "decision": "blocked",
            "matched_per_million": 500_000,
            "blockers": ["primary-only projected graph"]
        },
        "migration_gate": {
            "decision": "blocked",
            "blockers": ["shadow parity blocked"]
        },
        "cutover_evidence": {
            "eligible": false,
            "storage_recovery_required": true,
            "storage_recovery_present": false,
            "storage_recovery_ready": false,
            "storage_recovery_blocker_codes": ["wal_replay_unbounded"],
            "storage_recovery_blockers": [
                "WAL replay was not opened with a configured entry bound"
            ],
            "background_maintenance_required": true,
            "background_maintenance_present": false,
            "background_maintenance_ready": false,
            "background_maintenance_blocker_codes": ["missing_evidence"],
            "background_maintenance_blockers": [
                "background maintenance summary is missing"
            ],
            "replacement_readiness_min_per_million": 500_000,
            "replacement_readiness_invalid_family_count": 0,
            "replacement_readiness_blockers": [
                "query family search is below full replacement readiness"
            ],
            "blockers": ["cutover evidence blocked"]
        },
        "replacement_readiness_per_million": 500_000,
        "replacement_readiness_by_query_family": []
    });

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(
        summary["blocking_categories"],
        serde_json::json!([
            "active_search_route_ownership",
            "active_search_route_readiness",
            "background_maintenance",
            "bounded_read_evidence",
            "cutover_evidence",
            "dual_engine_evidence",
            "graph_route_readiness",
            "migration_gate",
            "previous_wrapper_contract",
            "query_family_readiness",
            "query_runtime_preflight",
            "search_candidate_shadow_evidence",
            "search_projection_evidence",
            "search_projection_shadow_evidence",
            "search_route_ownership",
            "shadow_parity",
            "source_mutation_dual_write_readiness",
            "storage_recovery",
            "workload_fixture_evidence"
        ])
    );
    assert_eq!(
        summary["missing_evidence"],
        serde_json::json!([
            "storage_recovery",
            "background_maintenance",
            "replacement_readiness_by_query_family_ready",
            "previous_wrapper_contract_evidence",
            "full_contract_evidence",
            "dual_engine_evidence",
            "source_mutation_dual_write_readiness",
            "search_projection_evidence",
            "search_projection_shadow_evidence",
            "search_candidate_shadow_evidence",
            "search_route_ownership",
            "active_search_route_ownership",
            "active_search_route_readiness",
            "bounded_read_evidence",
            "graph_route_readiness",
            "query_runtime_preflight",
            "workload_fixture_evidence",
            "shadow_run",
            "shadow_ready"
        ])
    );
    assert!(summary["blockers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item == "background maintenance summary is missing"));
    assert_eq!(
        summary["cutover_evidence"]["storage_recovery_blocker_codes"],
        serde_json::json!(["wal_replay_unbounded"])
    );
    assert_eq!(
        summary["cutover_evidence"]["background_maintenance_blocker_codes"],
        serde_json::json!(["missing_evidence"])
    );
    assert_eq!(
        summary["next_actions"],
        serde_json::json!([
            {
                "action": "close_blocked_query_families",
                "reason": "one or more required query families are missing or below full replacement readiness",
                "evidence_fields": [
                    "replacement_readiness_per_million",
                    "replacement_readiness_by_query_family"
                ]
            },
            {
                "action": "run_previous_wrapper_shadow_gate",
                "reason": "shadow parity or migration gate decision is not ready",
                "evidence_fields": [
                    "cutover.matched_per_million",
                    "cutover.decision",
                    "migration_gate.decision",
                    "shadow_run.evidence_kind",
                    "shadow_ready.engine_kind",
                    "shadow_ready.wrapper_identity",
                    "cutover_evidence.ready_wrapper_identity",
                    "previous_wrapper_contract_evidence.wrapper_identity"
                ]
            },
            {
                "action": "provide_eligible_cutover_evidence",
                "reason": "cutover evidence is missing or not eligible for production replacement",
                "evidence_fields": [
                    "cutover_evidence.eligible",
                    "cutover_evidence.evidence_kind",
                    "cutover_evidence.ready_engine_kind",
                    "cutover_evidence.ready_wrapper_identity"
                ]
            },
            {
                "action": "run_full_previous_wrapper_contract_check",
                "reason": "previous-wrapper contract evidence is missing or not ready",
                "evidence_fields": [
                    "required_contract_ready",
                    "full_contract_checked",
                    "full_contract_ready",
                    "selected_checks",
                    "check_count",
                    "previous_wrapper_contract_evidence.ready",
                    "previous_wrapper_contract_evidence.wrapper_identity",
                    "previous_wrapper_contract_evidence.blocker_codes"
                ]
            },
            {
                "action": "rerun_dual_engine_shadow_gate",
                "reason": "side-by-side dual-engine evidence is missing or not ready",
                "evidence_fields": [
                    "dual_engine_evidence.present",
                    "dual_engine_evidence.ready",
                    "dual_engine_evidence.consistent",
                    "dual_engine_evidence.primary_check_count",
                    "dual_engine_evidence.shadow_check_count",
                    "dual_engine_evidence.matched_check_count",
                    "dual_engine_evidence.primary_only_check_count",
                    "dual_engine_evidence.matched_per_million"
                ]
            },
            {
                "action": "attach_source_mutation_dual_write_readiness",
                "reason": "Source ingest/create, refresh/reparse, indexed transition, revision edges, and search-projection effects must be covered by durable dual-write readiness",
                "evidence_fields": [
                    "source_mutation_dual_write_readiness.protocol",
                    "source_mutation_dual_write_readiness.ready",
                    "source_mutation_dual_write_readiness.required_family_count",
                    "source_mutation_dual_write_readiness.evidence_family_count",
                    "source_mutation_dual_write_readiness.ready_family_count",
                    "source_mutation_dual_write_readiness.missing_required_families",
                    "source_mutation_dual_write_readiness.blocker_codes"
                ]
            },
            {
                "action": "attach_search_projection_replacement_evidence",
                "reason": "LanceDB replacement evidence is missing or not ready",
                "evidence_fields": [
                    "search_projection_evidence.protocol",
                    "search_projection_evidence.present",
                    "search_projection_evidence.ready",
                    "search_projection_evidence.derived_projection",
                    "search_projection_evidence.all_tables_covered",
                    "search_projection_evidence.covered_table_count",
                    "search_projection_evidence.required_table_count",
                    "search_projection_evidence.fts_ready",
                    "search_projection_evidence.vector_ready",
                    "search_projection_evidence.document_identity_ready",
                    "search_projection_evidence.embedding_identity_ready",
                    "search_projection_evidence.fail_soft_ready",
                    "search_projection_evidence.rebuild_marker_ready",
                    "search_projection_evidence.metadata_repair_marker_ready",
                    "search_projection_evidence.incremental_update_ready",
                    "search_projection_evidence.source_chunk_ready",
                    "search_projection_evidence.predicate_pushdown_ready",
                    "search_projection_evidence.production_filter_pruning_ready",
                    "search_projection_evidence.compressed_vector_projection_required",
                    "search_projection_evidence.compressed_vector_projection_ready",
                    "search_projection_evidence.blocker_codes"
                ]
            },
            {
                "action": "run_search_projection_shadow_evidence",
                "reason": "LanceDB/HawDB search projection side-by-side evidence is missing or not ready",
                "evidence_fields": [
                    "search_projection_shadow_evidence.protocol",
                    "search_projection_shadow_evidence.evidence_source",
                    "search_projection_shadow_evidence.present",
                    "search_projection_shadow_evidence.ready",
                    "search_projection_shadow_evidence.primary_ready",
                    "search_projection_shadow_evidence.shadow_ready",
                    "search_projection_shadow_evidence.document_count_parity",
                    "search_projection_shadow_evidence.document_identity_parity",
                    "search_projection_shadow_evidence.table_parity.ready",
                    "search_projection_shadow_evidence.embedding_identity_parity",
                    "search_projection_shadow_evidence.lifecycle_parity",
                    "search_projection_shadow_evidence.incremental_watermark_parity",
                    "search_projection_shadow_evidence.blocker_codes"
                ]
            },
            {
                "action": "run_search_candidate_shadow_evidence",
                "reason": "LanceDB/HawDB search candidate side-by-side evidence is missing or not ready",
                "evidence_fields": [
                    "search_candidate_shadow_evidence.protocol",
                    "search_candidate_shadow_evidence.evidence_source",
                    "search_candidate_shadow_evidence.route",
                    "search_candidate_shadow_evidence.present",
                    "search_candidate_shadow_evidence.ready",
                    "search_candidate_shadow_evidence.candidate_primary_engine",
                    "search_candidate_shadow_evidence.request_count",
                    "search_candidate_shadow_evidence.primary_candidate_count",
                    "search_candidate_shadow_evidence.shadow_candidate_count",
                    "search_candidate_shadow_evidence.matched_candidate_count",
                    "search_candidate_shadow_evidence.primary_only_candidate_count",
                    "search_candidate_shadow_evidence.row_count_parity",
                    "search_candidate_shadow_evidence.text_retriever_ready",
                    "search_candidate_shadow_evidence.vector_retriever_ready",
                    "search_candidate_shadow_evidence.fts_top_k_overlap_ready",
                    "search_candidate_shadow_evidence.vector_top_k_overlap_ready",
                    "search_candidate_shadow_evidence.candidate_readiness.source_chunk_identity_ready",
                    "search_candidate_shadow_evidence.candidate_readiness.fail_soft_observed",
                    "search_candidate_shadow_evidence.candidate_readiness.projection_marker_status_visible",
                    "search_candidate_shadow_evidence.candidate_readiness.projection_watermark_ready",
                    "search_candidate_shadow_evidence.candidate_readiness.embedding_identity_ready",
                    "search_candidate_shadow_evidence.candidate_identity.ready",
                    "search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready",
                    "search_candidate_shadow_evidence.shadow_scan_field_pruning_ready",
                    "search_candidate_shadow_evidence.shadow_scan_field_summary_count",
                    "search_candidate_shadow_evidence.filter_pushdown.ready",
                    "search_candidate_shadow_evidence.filter_pushdown.field_summary_count",
                    "search_candidate_shadow_evidence.filter_pushdown.missing_required_fields",
                    "search_candidate_shadow_evidence.filter_pushdown.field_capabilities_ready",
                    "search_candidate_shadow_evidence.filter_pushdown.missing_value_summary_fields",
                    "search_candidate_shadow_evidence.filter_pushdown.missing_numeric_range_fields",
                    "search_candidate_shadow_evidence.filter_pushdown.missing_timestamp_range_fields",
                    "search_candidate_shadow_evidence.blocker_codes"
                ]
            },
            {
                "action": "attach_search_route_ownership",
                "reason": "search projection route ownership is missing or still routes a projection family to LanceDB",
                "evidence_fields": [
                    "search_route_ownership.protocol",
                    "search_route_ownership.ready",
                    "search_route_ownership.production_cutover_ready",
                    "search_route_ownership.required_route_count",
                    "search_route_ownership.explicit_route_count",
                    "search_route_ownership.hawdb_route_count",
                    "search_route_ownership.lancedb_route_count",
                    "search_route_ownership.missing_required_routes",
                    "search_route_ownership.lancedb_routes",
                    "search_route_ownership.blocker_codes"
                ]
            },
            {
                "action": "attach_active_search_route_ownership",
                "reason": "active search route ownership is missing or still routes a business search path to LanceDB",
                "evidence_fields": [
                    "active_search_route_ownership.protocol",
                    "active_search_route_ownership.ready",
                    "active_search_route_ownership.production_cutover_ready",
                    "active_search_route_ownership.required_route_count",
                    "active_search_route_ownership.explicit_route_count",
                    "active_search_route_ownership.hawdb_route_count",
                    "active_search_route_ownership.lancedb_route_count",
                    "active_search_route_ownership.missing_required_routes",
                    "active_search_route_ownership.lancedb_routes",
                    "active_search_route_ownership.blocker_codes"
                ]
            },
            {
                "action": "attach_active_search_route_readiness",
                "reason": "active search route read evidence is missing or still requires LanceDB for a business search path",
                "evidence_fields": [
                    "active_search_route_readiness.protocol",
                    "active_search_route_readiness.ready",
                    "active_search_route_readiness.production_cutover_ready",
                    "active_search_route_readiness.required_route_count",
                    "active_search_route_readiness.evidence_route_count",
                    "active_search_route_readiness.ready_route_count",
                    "active_search_route_readiness.hawdb_route_count",
                    "active_search_route_readiness.lancedb_handle_required_route_count",
                    "active_search_route_readiness.missing_required_routes",
                    "active_search_route_readiness.non_hawdb_routes",
                    "active_search_route_readiness.lancedb_handle_required_routes",
                    "active_search_route_readiness.candidate_not_ready_routes",
                    "active_search_route_readiness.candidate_identity_not_ready_routes",
                    "active_search_route_readiness.embedding_identity_not_ready_routes",
                    "active_search_route_readiness.zero_vector_semantics_not_ready_routes",
                    "active_search_route_readiness.cjk_tokenization_not_ready_routes",
                    "active_search_route_readiness.metadata_pushdown_not_ready_routes",
                    "active_search_route_readiness.ranking_window_not_ready_routes",
                    "active_search_route_readiness.ranking_not_ready_routes",
                    "active_search_route_readiness.fail_soft_not_ready_routes",
                    "active_search_route_readiness.fail_soft_reason_codes_not_ready_routes",
                    "active_search_route_readiness.repair_rebuild_markers_not_ready_routes",
                    "active_search_route_readiness.blocker_codes"
                ]
            },
            {
                "action": "attach_bounded_read_profile",
                "reason": "bounded read execution profile is missing or not ready",
                "evidence_fields": [
                    "bounded_read_evidence.protocol",
                    "bounded_read_evidence.present",
                    "bounded_read_evidence.ready",
                    "bounded_read_evidence.mode",
                    "bounded_read_evidence.max_rows",
                    "bounded_read_evidence.execution_row_cap",
                    "bounded_read_evidence.estimated_payload_bytes",
                    "bounded_read_evidence.max_estimated_payload_bytes",
                    "bounded_read_evidence.payload_budget_exceeded",
                    "bounded_read_evidence.row_limit_enforced_before_output",
                    "bounded_read_evidence.operator_row_cap_enabled",
                    "bounded_read_evidence.blocking_operator_count",
                    "bounded_read_evidence.blocking_operator_memory_reports_complete",
                    "bounded_read_evidence.blocking_operator_memory_within_budget",
                    "bounded_read_evidence.spill_within_budget",
                    "bounded_read_evidence.covered_routes",
                    "bounded_read_evidence.route_primary_ready",
                    "bounded_read_evidence.primary_ready_routes",
                    "bounded_read_evidence.route_query_plan_evidence_ready",
                    "bounded_read_evidence.route_query_profile_evidence_ready",
                    "bounded_read_evidence.route_query_api_behavior_evidence_ready",
                    "bounded_read_evidence.relationship_property_pruning_required_count",
                    "bounded_read_evidence.relationship_property_pruning_report_count",
                    "bounded_read_evidence.route_relationship_property_pruning_evidence_ready",
                    "bounded_read_evidence.blocker_codes"
                ]
            },
            {
                "action": "attach_graph_route_readiness_evidence",
                "reason": "graph route readiness evidence is missing, stale, or does not cover all required graph read routes",
                "evidence_fields": [
                    "graph_route_readiness.protocol",
                    "graph_route_readiness.present",
                    "graph_route_readiness.ready",
                    "graph_route_readiness.evidence_protocol",
                    "graph_route_readiness.evidence_ready",
                    "graph_route_readiness.required_route_count",
                    "graph_route_readiness.covered_route_count",
                    "graph_route_readiness.covered_routes",
                    "graph_route_readiness.route_coverage_ready",
                    "graph_route_readiness.evidence_route_coverage_present",
                    "graph_route_readiness.evidence_route_coverage_matches",
                    "graph_route_readiness.route_query_runtime_ready",
                    "graph_route_readiness.route_query_plan_evidence_ready",
                    "graph_route_readiness.route_query_profile_evidence_ready",
                    "graph_route_readiness.route_query_api_behavior_evidence_ready",
                    "graph_route_readiness.relationship_property_pruning_required_count",
                    "graph_route_readiness.relationship_property_pruning_report_count",
                    "graph_route_readiness.route_relationship_property_pruning_evidence_ready",
                    "graph_route_readiness.route_primary_ready",
                    "graph_route_readiness.primary_ready_route_count",
                    "graph_route_readiness.route_primary_blocker_codes"
                ]
            },
            {
                "action": "attach_query_runtime_preflight_evidence",
                "reason": "query runtime preflight evidence is missing or does not cover all required graph read routes",
                "evidence_fields": [
                    "query_runtime_preflight.protocol",
                    "query_runtime_preflight.present",
                    "query_runtime_preflight.ready",
                    "query_runtime_preflight.database_opened",
                    "query_runtime_preflight.probe_count",
                    "query_runtime_preflight.passed_probe_count",
                    "query_runtime_preflight.failed_probe_count",
                    "query_runtime_preflight.required_routes_covered",
                    "query_runtime_preflight.route_coverage_ready",
                    "query_runtime_preflight.probe_details_ready",
                    "query_runtime_preflight.blocker_codes",
                    "query_runtime_preflight.probes"
                ]
            },
            {
                "action": "run_workload_fixture_evidence",
                "reason": "graph route, bounded expansion, metadata-filtered search, or source projection workload fixture evidence is missing or not ready",
                "evidence_fields": [
                    "workload_fixture_evidence.protocol",
                    "workload_fixture_evidence.present",
                    "workload_fixture_evidence.ready",
                    "workload_fixture_evidence.route_count",
                    "workload_fixture_evidence.query_count",
                    "workload_fixture_evidence.failed_query_count",
                    "workload_fixture_evidence.bounded_expansion_probe_count",
                    "workload_fixture_evidence.failed_bounded_expansion_probe_count",
                    "workload_fixture_evidence.search_metadata_probe_count",
                    "workload_fixture_evidence.failed_search_metadata_probe_count",
                    "workload_fixture_evidence.graph_rag_probe_count",
                    "workload_fixture_evidence.failed_graph_rag_probe_count",
                    "workload_fixture_evidence.graph_rag_reports",
                    "workload_fixture_evidence.source_projection_probe_count",
                    "workload_fixture_evidence.failed_source_projection_probe_count",
                    "workload_fixture_evidence.source_projection_reports",
                    "workload_fixture_evidence.blocker_codes"
                ]
            },
            {
                "action": "attach_storage_recovery_report",
                "reason": "required storage recovery evidence is missing or blocked",
                "evidence_fields": [
                    "cutover_evidence.storage_recovery_present",
                    "cutover_evidence.storage_recovery_ready",
                    "cutover_evidence.storage_recovery_protocol_matches",
                    "cutover_evidence.storage_recovery_durable",
                    "cutover_evidence.storage_recovery_checkpoint_boundary_present",
                    "cutover_evidence.storage_recovery_wal_replay_bounded",
                    "cutover_evidence.storage_recovery_replay_boundary_consistent",
                    "cutover_evidence.storage_recovery_torn_tail_clean",
                    "cutover_evidence.storage_recovery_blocker_codes"
                ]
            },
            {
                "action": "attach_background_maintenance_report",
                "reason": "required background maintenance QoS or graph-delta evidence is missing or blocked",
                "evidence_fields": [
                    "cutover_evidence.background_maintenance_present",
                    "cutover_evidence.background_maintenance_ready",
                    "cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
                    "cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
                    "cutover_evidence.background_maintenance_blocker_codes"
                ]
            }
        ])
    );
    assert_eq!(summary["production_replacement_per_million"], 0);
}

#[test]
fn replacement_summary_recomputes_storage_recovery_raw_fields() {
    let mut bundle = production_ready_bundle();
    bundle["cutover_evidence"]["storage_recovery_ready"] = serde_json::json!(true);
    bundle["cutover_evidence"]["storage_recovery_replay_boundary_consistent"] =
        serde_json::json!(false);
    bundle["cutover_evidence"]["storage_recovery_blocker_codes"] =
        serde_json::json!(["replay_boundary_inconsistent"]);

    let summary = nowledge_replacement_summary_json(&bundle);

    assert_eq!(summary["production_cutover_ready"], false);
    assert!(summary["blocking_categories"]
        .as_array()
        .unwrap()
        .iter()
        .any(|category| category == "storage_recovery"));
    assert_eq!(
        summary["cutover_evidence"]["storage_recovery_replay_boundary_consistent"],
        serde_json::json!(false)
    );
    assert!(summary["next_actions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|action| {
            action["action"] == "attach_storage_recovery_report"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| {
                        field == "cutover_evidence.storage_recovery_replay_boundary_consistent"
                    })
        }));
}

fn production_ready_bundle() -> serde_json::Value {
    let query_runtime_preflight = ready_query_runtime_preflight();
    let mut bundle = serde_json::json!({
        "required_contract_ready": true,
        "full_contract_checked": true,
        "full_contract_ready": true,
        "selected_checks": 2,
        "check_count": 2,
        "inventory_gate": {
            "coverage_per_million": 1_000_000,
            "blockers": []
        },
        "cutover": {
            "decision": "ready",
            "matched_per_million": 1_000_000,
            "blockers": []
        },
        "migration_gate": {
            "decision": "ready",
            "blockers": []
        },
        "cutover_evidence": {
            "eligible": true,
            "evidence_kind": "previous_wrapper",
            "ready_engine_kind": "previous_wrapper",
            "ready_wrapper_identity": "nowledge-previous-wrapper:test",
            "storage_recovery_required": true,
            "storage_recovery_present": true,
            "storage_recovery_ready": true,
            "storage_recovery_protocol_matches": true,
            "storage_recovery_durable": true,
            "storage_recovery_checkpoint_boundary_present": true,
            "storage_recovery_wal_replay_bounded": true,
            "storage_recovery_replay_boundary_consistent": true,
            "storage_recovery_torn_tail_clean": true,
            "storage_recovery_blocker_codes": [],
            "storage_recovery_blockers": [],
            "background_maintenance_required": true,
            "background_maintenance_present": true,
            "background_maintenance_ready": true,
            "background_maintenance_protocol_matches": true,
            "background_maintenance_executable_search_projection_graph_delta_count": 2,
            "background_maintenance_admitted_search_projection_graph_delta_count": 1,
            "background_maintenance_deferred_search_projection_graph_delta_count": 1,
            "background_maintenance_rejected_search_projection_graph_delta_count": 0,
            "background_maintenance_executable_search_projection_graph_delta_operations": 8,
            "background_maintenance_admitted_search_projection_graph_delta_operations": 3,
            "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": 42,
            "background_maintenance_blocker_codes": [],
            "background_maintenance_blockers": [],
            "replacement_readiness_min_per_million": 1_000_000,
            "replacement_readiness_invalid_family_count": 0,
            "replacement_readiness_blockers": [],
            "blockers": []
        },
        "shadow_run": {
            "evidence_kind": "previous_wrapper"
        },
        "shadow_ready": {
            "engine_kind": "previous_wrapper",
            "wrapper_identity": "nowledge-previous-wrapper:test"
        },
        "dual_engine_evidence": {
            "ready": true,
            "primary_engine": "hawdb",
            "shadow_engine": "previous-wrapper",
            "primary_check_count": 1,
            "shadow_check_count": 1,
            "matched_check_count": 1,
            "primary_only_check_count": 0,
            "matched_per_million": 1_000_000
        },
        "search_projection_evidence": {
            "protocol": "hawdb-nowledge-search-projection-evidence",
            "ready": true,
            "derived_projection": true,
            "all_tables_covered": true,
            "covered_table_count": 6,
            "required_table_count": 6,
            "fts_ready": true,
            "vector_ready": true,
            "document_identity_ready": true,
            "embedding_identity_ready": true,
            "fail_soft_ready": true,
            "rebuild_marker_ready": true,
            "metadata_repair_marker_ready": true,
            "incremental_update_ready": true,
            "source_chunk_ready": true,
            "predicate_pushdown_ready": true,
            "production_filter_pruning_ready": true,
            "compressed_vector_projection_required": true,
            "compressed_vector_projection_ready": true,
            "blocker_codes": []
        },
        "search_projection_shadow_evidence": {
            "protocol": "hawdb-nowledge-search-projection-shadow-evidence",
            "evidence_source": HAWDB_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
            "ready": true,
            "primary_engine": "lancedb",
            "shadow_engine": "hawdb",
            "primary_ready": true,
            "shadow_ready": true,
            "document_count_parity": true,
            "document_identity_parity": true,
            "table_parity": {
                "ready": true
            },
            "embedding_identity_parity": true,
            "lifecycle_parity": true,
            "incremental_watermark_parity": true,
            "predicate_pushdown_parity": true,
            "pushdown_evidence": {
                "ready": true,
                "predicate_pushdown_parity": true,
                "primary_predicate_pushdown_ready": true,
                "shadow_predicate_pushdown_ready": true,
                "shadow_persisted_segment_descriptor_ready": true,
                "shadow_segment_descriptor_scan_filter_fields_ready": true,
                "primary_scan_filter_fields": scan_filter_fields_json(),
                "shadow_scan_filter_fields": scan_filter_fields_json(),
                "shadow_segment_descriptor_field_summaries": scan_filter_field_summaries_json()
            },
            "blocker_codes": []
        },
        "search_candidate_shadow_evidence": {
            "protocol": "hawdb-nowledge-search-candidate-shadow-evidence",
            "route": "/search-index/hawdb-shadow/candidate-evidence",
            "evidence_source": "nmem-rust-bridge",
            "ready": true,
            "candidate_primary_engine": "hawdb",
            "request_count": 2,
            "primary_candidate_count": 3,
            "shadow_candidate_count": 3,
            "matched_candidate_count": 3,
            "primary_only_candidate_count": 0,
            "candidate_identity": {
                "ready": true,
                "id_space": "search_candidate_id",
                "representation": "per_request_sorted_candidate_ids",
                "primary_checksum": 123,
                "shadow_checksum": 123,
                "matched_checksum": 123,
                "parity": true
            },
            "filter_pushdown_ready": true,
            "filter_pushdown": {
                "ready": true,
                "pushed_predicate_count": 1,
                "shadow_scan_present": true,
                "required_fields": scan_filter_fields_json(),
                "missing_required_fields": [],
                "missing_value_summary_fields": [],
                "missing_numeric_range_fields": [],
                "missing_timestamp_range_fields": [],
                "field_capabilities_ready": true,
                "field_summary_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
                "field_summaries": scan_filter_field_summaries_json(),
                "blocker_codes": []
            },
            "blocker_codes": []
        },
        "search_route_ownership": ready_route_ownership_json(
            REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES
        ),
        "active_search_route_ownership": ready_route_ownership_json(
            REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES
        ),
        "active_search_route_readiness": ready_active_search_route_readiness_json(),
        "bounded_read_evidence": {
            "protocol": "hawdb-nowledge-mem-bounded-read-evidence-v2",
            "mode": "shadow_read_only",
            "max_rows": 512,
            "execution_row_cap": 513,
            "estimated_payload_bytes": 128,
            "max_estimated_payload_bytes": 4194304,
            "payload_budget_exceeded": false,
            "row_limit_enforced_before_output": true,
            "operator_row_cap_enabled": true,
            "streaming": false,
            "blocking_operator_count": 0,
            "route_primary_ready": true,
            "primary_ready_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "route_query_plan_evidence_ready": true,
            "route_query_profile_evidence_ready": true,
            "route_query_api_behavior_evidence_ready": true,
            "relationship_property_pruning_required_count": 0,
            "relationship_property_pruning_report_count": 0,
            "route_relationship_property_pruning_evidence_ready": true,
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "blocker_codes": []
        },
        "source_mutation_dual_write_readiness": ready_source_mutation_dual_write_readiness(),
        "workload_fixture_evidence": ready_workload_fixture_evidence(),
        "graph_route_readiness": ready_graph_route_readiness(),
        "query_runtime_preflight": query_runtime_preflight,
        "previous_wrapper_contract_evidence": {
            "ready": true,
            "evidence_kind": "previous_wrapper_contract",
            "wrapper_identity": "nowledge-previous-wrapper:test",
            "requires_full_contract_ready": true,
            "requires_wrapper_identity": true,
            "blocker_codes": [],
            "blockers": []
        },
        "replacement_readiness_per_million": 1_000_000,
        "replacement_readiness_by_query_family": [
            {
                "query_family": "memory_lookup",
                "replacement_readiness_per_million": 1_000_000
            },
            {
                "query_family": "graph_traversal",
                "replacement_readiness_per_million": 1_000_000
            },
            {
                "query_family": "projected_graph",
                "replacement_readiness_per_million": 1_000_000
            },
            {
                "query_family": "label_stats_read",
                "replacement_readiness_per_million": 1_000_000
            },
            {
                "query_family": "search_projection",
                "replacement_readiness_per_million": 1_000_000
            }
        ]
    });
    bundle["bounded_read_evidence"]["blocking_operator_memory_reports_complete"] =
        serde_json::json!(true);
    bundle["bounded_read_evidence"]["blocking_operator_memory_within_budget"] =
        serde_json::json!(true);
    bundle["bounded_read_evidence"]["spill_within_budget"] = serde_json::json!(true);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_document_pruning_ready"] = serde_json::json!(true);
    bundle["cutover_evidence"]["background_maintenance_foreground_admission_probe_ready"] =
        serde_json::json!(true);
    bundle["cutover_evidence"]["background_maintenance_foreground_admission_probe_admission"] =
        serde_json::json!("admit");
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_pruning_candidate_document_count"] = serde_json::json!(4);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_pruned_document_count"] = serde_json::json!(2);
    bundle["search_projection_shadow_evidence"]["pushdown_evidence"]
        ["shadow_segment_scanned_document_count"] = serde_json::json!(2);
    bundle["search_candidate_shadow_evidence"]["row_count_parity"] = serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["text_retriever_ready"] = serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["vector_retriever_ready"] = serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["retriever_leg_candidate_counts"] = serde_json::json!({
        "text": 3,
        "vector": 3,
    });
    bundle["search_candidate_shadow_evidence"]["fts_top_k_overlap_ready"] = serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["vector_top_k_overlap_ready"] =
        serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["top_k_overlap_observed"] = serde_json::json!({
        "fts": true,
        "vector": true,
    });
    bundle["search_candidate_shadow_evidence"]["candidate_readiness"] = serde_json::json!({
        "source_chunk_identity_ready": true,
        "fail_soft_observed": true,
        "projection_marker_status_visible": true,
        "projection_watermark_ready": true,
        "embedding_identity_ready": true,
    });
    bundle["search_candidate_shadow_evidence"]["shadow_scan_present"] = serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["shadow_scan_filter_pushdown_ready"] =
        serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"] =
        serde_json::json!(true);
    bundle["search_candidate_shadow_evidence"]["shadow_scan_field_summary_count"] =
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len());
    bundle
}

fn scan_filter_fields_json() -> serde_json::Value {
    serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS)
}

fn ready_route_ownership_json(routes: &[&str]) -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL,
        "ready": true,
        "production_cutover_ready": true,
        "require_all_hawdb": true,
        "required_route_count": routes.len(),
        "explicit_route_count": routes.len(),
        "hawdb_route_count": routes.len(),
        "lancedb_route_count": 0,
        "missing_required_routes": [],
        "lancedb_routes": [],
        "blocker_codes": []
    })
}

fn ready_active_search_route_readiness_json() -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL,
        "ready": true,
        "production_cutover_ready": true,
        "require_all_hawdb": true,
        "required_route_count": REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len(),
        "evidence_route_count": REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len(),
        "ready_route_count": REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len(),
        "hawdb_route_count": REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len(),
        "lancedb_handle_required_route_count": 0,
        "missing_required_routes": [],
        "non_hawdb_routes": [],
        "lancedb_handle_required_routes": [],
        "candidate_not_ready_routes": [],
        "metadata_pushdown_not_ready_routes": [],
        "ranking_not_ready_routes": [],
        "fail_soft_not_ready_routes": [],
        "blocker_codes": []
    })
}

fn ready_source_mutation_dual_write_readiness() -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
        "ready": true,
        "required_family_count": REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len(),
        "evidence_family_count": REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len(),
        "ready_family_count": REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len(),
        "missing_required_families": [],
        "unknown_families": [],
        "duplicate_families": [],
        "blocker_codes": []
    })
}

fn ready_search_candidate_trace_evidence() -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        "route": NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        "evidence_source": NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE,
        "present": true,
        "ready": true,
        "reported_ready": true,
        "engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
        "primary_engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE,
        "shadow_engine": NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE,
        "row_count_parity": true,
        "vector_top_k_overlap_ready": true,
        "fts_top_k_overlap_ready": true,
        "shadow_scan_present": true,
        "shadow_scan_filter_pushdown_ready": true,
        "shadow_scan_field_pruning_ready": true,
        "shadow_scan_field_summary_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
        "shadow_scan_input_predicate_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
        "shadow_scan_pushed_predicate_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
        "shadow_scan_residual_predicate_count": 0,
        "shadow_scan_filtered_out_count": 4,
        "shadow_scan_pruned_document_count": 2,
        "shadow_scan_scanned_document_count": 2,
        "shadow_scan_parse_error": null,
        "shadow_scan_unsatisfiable": false,
        "blocker_codes": []
    })
}

fn scan_filter_field_summaries_json() -> serde_json::Value {
    serde_json::Value::Array(
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .map(|field| {
                descriptor_field(
                    field,
                    true,
                    matches!(*field, "importance" | "confidence"),
                    matches!(
                        *field,
                        "created_at" | "updated_at" | "event_start" | "event_end"
                    ),
                )
            })
            .collect(),
    )
}

fn descriptor_field(
    field: &str,
    value_summary_used: bool,
    numeric_range_summary_used: bool,
    timestamp_range_summary_used: bool,
) -> serde_json::Value {
    serde_json::json!({
        "field": field,
        "segment_count": 1,
        "present_document_count": 1,
        "value_summary_used": value_summary_used,
        "value_summary_segment_count": usize::from(value_summary_used),
        "numeric_range_summary_used": numeric_range_summary_used,
        "numeric_range_segment_count": usize::from(numeric_range_summary_used),
        "timestamp_range_summary_used": timestamp_range_summary_used,
        "timestamp_range_segment_count": usize::from(timestamp_range_summary_used),
    })
}

fn ready_query_runtime_preflight() -> serde_json::Value {
    let probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .map(|route| ready_query_runtime_preflight_probe(route))
        .collect::<Vec<_>>();
    serde_json::json!({
        "protocol": "hawdb-nowledge-query-runtime-preflight-v1",
        "ready": true,
        "database_opened": true,
        "redaction": {
            "ready": true,
            "rows_copied": false,
            "parameters_copied": false,
            "local_paths_copied": false,
            "raw_errors_copied": false
        },
        "probe_count": probes.len(),
        "passed_probe_count": probes.len(),
        "failed_probe_count": 0,
        "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
        "missing_required_routes": [],
        "required_routes_covered": true,
        "unknown_routes": [],
        "duplicate_routes": [],
        "route_coverage_ready": true,
        "route_coverage_blocker_codes": [],
        "blocker_codes": [],
        "probes": probes,
    })
}

fn ready_workload_fixture_evidence() -> serde_json::Value {
    serde_json::json!({
        "protocol": NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL,
        "present": true,
        "ready": true,
        "route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "query_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "failed_query_count": 0,
        "bounded_expansion_probe_count": 2,
        "failed_bounded_expansion_probe_count": 0,
        "search_metadata_probe_count": 3,
        "failed_search_metadata_probe_count": 0,
        "graph_rag_probe_count": 1,
        "failed_graph_rag_probe_count": 0,
        "graph_rag_reports": [
            {
                "name": "memory-to-entity",
                "ready": true,
                "schema_protocol": GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL,
                "context_epoch": 42,
                "schema_fingerprint": 1001,
                "label_count": 2,
                "relationship_type_count": 1,
                "property_count": 3,
                "route_count": 1,
                "common_path_count": 1,
                "parameter_requirement_count": 1,
                "row_count": 1,
                "max_rows": 4,
                "execution_row_cap": 5,
                "estimated_payload_bytes": 128,
                "row_budget_exceeded": false,
                "payload_budget_exceeded": false,
                "blocking_operator_count": 0,
                "streaming": false,
                "error_class": null
            }
        ],
        "source_projection_probe_count": 1,
        "failed_source_projection_probe_count": 0,
        "source_projection_reports": [
            {
                "name": "source-ingest-composite-changefeed",
                "ready": true,
                "source_graph_commit_epoch": 43,
                "complete_through_graph_commit_epoch": 43,
                "too_small_batch_failed_closed": true,
                "operation_count": 2,
                "upserted_documents": 2,
                "deleted_documents": 0,
                "source_document_count": 2,
                "indexed_source_document_ready": true,
                "error_class": null
            }
        ],
        "blocker_codes": []
    })
}

fn ready_graph_route_readiness() -> serde_json::Value {
    let routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .map(|route| {
            let spec = nowledge_mem_graph_read_route_spec(route).unwrap();
            serde_json::json!({
                "route": route,
                "owner": spec.owner.as_str(),
                "required_evidence_kind": spec.required_evidence_kind.as_str(),
                "stale_on_catalog_change": spec.stale_on_catalog_change,
                "primary_ready": true
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "protocol": NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
        "evidence_protocol": NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL,
        "evidence_ready": true,
        "route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
        "missing_required_routes": [],
        "required_routes_covered": true,
        "unknown_routes": [],
        "duplicate_routes": [],
        "route_coverage_ready": true,
        "route_coverage_blocker_codes": [],
        "evidence_route_coverage_present": true,
        "evidence_route_coverage_matches": true,
        "evidence_route_coverage_blocker_codes": [],
        "shadow_compare_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "primary_ready_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "query_runtime_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "query_runtime_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "query_runtime_plan_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "query_runtime_profile_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "query_runtime_failed_query_count": 0,
        "query_runtime_missing_plan_evidence_count": 0,
        "query_runtime_missing_profile_evidence_count": 0,
        "relationship_property_pruning_required_count": 0,
        "relationship_property_pruning_report_count": 0,
        "missing_query_runtime_routes": [],
        "route_query_runtime_ready": true,
        "route_query_plan_evidence_ready": true,
        "route_query_profile_evidence_ready": true,
        "route_query_api_behavior_evidence_ready": true,
        "route_relationship_property_pruning_evidence_ready": true,
        "route_primary_ready": true,
        "route_primary_blocker_codes": [],
        "route_catalog": nowledge_mem_graph_read_route_specs_json(),
        "routes": routes
    })
}

fn ready_query_runtime_preflight_probe(route: &str) -> serde_json::Value {
    serde_json::json!({
        "route": route,
        "ready": true,
        "success": true,
        "selected_plan_fingerprint": format!("fixture:{route}"),
        "physical_operator_count": 2,
        "physical_operator_class_count": 2,
        "optimizer_decision_count": 1,
        "optimizer_rule_event_count": 1,
        "plan_cache_bypassed": false,
        "scan_pruning": {
            "ready": true,
            "report_count": 1
        }
    })
}

fn blocked_bundle() -> serde_json::Value {
    serde_json::json!({
        "inventory_gate": {
            "coverage_per_million": 1_000_000,
            "blockers": ["inventory blocked"]
        },
        "cutover": {
            "decision": "blocked",
            "matched_per_million": 500_000,
            "blockers": ["primary-only projected graph"]
        },
        "migration_gate": {
            "decision": "blocked",
            "blockers": ["shadow parity blocked"]
        },
        "cutover_evidence": {
            "eligible": false,
            "storage_recovery_required": true,
            "storage_recovery_ready": false,
            "storage_recovery_blocker_codes": ["wal_replay_unbounded"],
            "storage_recovery_blockers": [
                "WAL replay was not opened with a configured entry bound"
            ],
            "background_maintenance_required": true,
            "background_maintenance_ready": false,
            "background_maintenance_blocker_codes": ["missing_evidence"],
            "background_maintenance_blockers": [
                "background maintenance summary is missing"
            ],
            "replacement_readiness_min_per_million": 500_000,
            "replacement_readiness_invalid_family_count": 0,
            "replacement_readiness_blockers": [],
            "blockers": []
        },
        "replacement_readiness_per_million": 500_000,
        "replacement_readiness_by_query_family": []
    })
}
