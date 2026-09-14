//! Pure replacement-readiness reduction over externally supplied evidence.
//!
//! Database probes, file adapters, and activation remain at the embedded facade.

use crate::evidence_json::{
    json_get_array_path, json_get_array_path_from_dynamic, json_get_bool_path,
    json_get_bool_path_from_dynamic, json_get_path, json_get_path_from_dynamic, json_get_str_path,
    json_get_str_path_from_dynamic, json_get_string_array_path,
    json_get_string_array_path_from_dynamic, json_get_u64_path, json_get_u64_path_from_dynamic,
};
use crate::graph_summary::nowledge_graph_route_readiness_summary_from_bundle;
use crate::source_mutation::{
    NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
    REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES,
};
use skein_core::GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL;
use skein_evidence::inventory::{
    replacement_readiness_family_evidence_health_from_bundle,
    REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};
use skein_evidence::replacement_contract::{
    NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL, NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE, NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
    NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
};
use skein_route_ownership::graph::REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES;
use skein_route_ownership::{
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL, REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES,
    REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES,
};
use std::collections::{BTreeMap, BTreeSet};

const SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-evidence";
const SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-shadow-evidence";
const SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE: &str = "skein-rust-library";
const SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-mem-bounded-read-evidence-v2";
const SKEIN_NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL: &str =
    "skein-nowledge-query-runtime-preflight-v1";
const SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY: &str =
    "search_projection_shadow_pushdown_evidence_not_ready";
const SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING: &str =
    "skein_search_projection_segment_descriptor_missing";
const SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING: &str =
    "skein_search_projection_segment_descriptor_fields_missing";
const REQUIRED_SEARCH_VALUE_SUMMARY_FIELDS: &[&str] = &[
    "kind",
    "external_id",
    "source_id",
    "space_id",
    "unit_type",
    "lifecycle_state",
    "is_latest",
];
const REQUIRED_SEARCH_NUMERIC_RANGE_FIELDS: &[&str] = &["importance", "confidence"];
const REQUIRED_SEARCH_TIMESTAMP_RANGE_FIELDS: &[&str] =
    &["created_at", "updated_at", "event_start", "event_end"];
const GRAPH_LAYER_REPLACEMENT_SCOPE: &str = "kuzu_ladybug_graph_layer";
const SEARCH_PROJECTION_REPLACEMENT_SCOPE: &str = "lancedb_search_projection";
const SQLITE_CONTENT_STORE_SCOPE: &str = "sqlite_content_store";
const LARGE_BLOB_VALUE_STORE_SCOPE: &str = "large_blob_value_store";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeReplacementSummaryOptions {
    pub include_family_details: bool,
    pub max_family_items: Option<usize>,
    pub include_blocker_details: bool,
    pub max_blockers: Option<usize>,
}

impl Default for NowledgeReplacementSummaryOptions {
    fn default() -> Self {
        Self {
            include_family_details: true,
            max_family_items: None,
            include_blocker_details: true,
            max_blockers: None,
        }
    }
}

pub fn nowledge_replacement_summary_json(bundle: &serde_json::Value) -> serde_json::Value {
    nowledge_replacement_summary_json_with_options(
        bundle,
        NowledgeReplacementSummaryOptions::default(),
    )
}

pub fn nowledge_replacement_summary_json_with_options(
    bundle: &serde_json::Value,
    options: NowledgeReplacementSummaryOptions,
) -> serde_json::Value {
    let covered_business_surface_per_million =
        json_get_u64_path(bundle, &["inventory_gate", "coverage_per_million"])
            .or_else(|| json_get_u64_path(bundle, &["coverage", "coverage_per_million"]));
    let shadow_parity_per_million = json_get_u64_path(bundle, &["cutover", "matched_per_million"])
        .or_else(|| json_get_u64_path(bundle, &["migration_gate", "shadow_matched_per_million"]));
    let replacement_readiness_per_million =
        json_get_u64_path(bundle, &["replacement_readiness_per_million"]);
    let migration_gate_decision = json_get_str_path(bundle, &["migration_gate", "decision"]);
    let cutover_decision = json_get_str_path(bundle, &["cutover", "decision"]);
    let cutover_evidence_eligible =
        json_get_bool_path(bundle, &["cutover_evidence", "eligible"]).unwrap_or(false);
    let shadow_evidence = shadow_evidence_summary(bundle);
    let shadow_evidence_ready = shadow_evidence.ready;
    let previous_wrapper_contract_ready =
        json_get_bool_path(bundle, &["previous_wrapper_contract_evidence", "ready"])
            .unwrap_or(false);
    let full_contract_evidence = full_contract_evidence_summary(bundle);
    let full_contract_evidence_ready = full_contract_evidence.ready;
    let dual_engine_evidence = dual_engine_evidence_summary(bundle);
    let dual_engine_evidence_present = dual_engine_evidence.present;
    let dual_engine_evidence_ready = dual_engine_evidence.ready;
    let dual_engine_evidence_consistent = dual_engine_evidence.consistent;
    let search_projection_evidence = search_projection_evidence_summary(bundle);
    let search_projection_evidence_ready = search_projection_evidence.ready;
    let search_projection_shadow_evidence = search_projection_shadow_evidence_summary(bundle);
    let search_projection_shadow_evidence_ready = search_projection_shadow_evidence.ready;
    let search_candidate_shadow_evidence = search_candidate_shadow_evidence_summary(bundle);
    let search_candidate_shadow_evidence_ready = search_candidate_shadow_evidence.ready;
    let search_candidate_shadow_evidence_json =
        search_candidate_shadow_evidence_summary_json(&search_candidate_shadow_evidence);
    let search_route_ownership = search_route_ownership_summary(bundle);
    let search_route_ownership_ready = search_route_ownership.ready;
    let active_search_route_ownership = active_search_route_ownership_summary(bundle);
    let active_search_route_ownership_ready = active_search_route_ownership.ready;
    let active_search_route_readiness = active_search_route_readiness_summary(bundle);
    let active_search_route_readiness_ready = active_search_route_readiness.ready;
    let bounded_read_evidence = bounded_read_evidence_summary(bundle);
    let bounded_read_evidence_ready = bounded_read_evidence.ready;
    let graph_route_readiness = nowledge_graph_route_readiness_summary_from_bundle(bundle);
    let graph_route_readiness_ready = graph_route_readiness.ready;
    let query_runtime_preflight = query_runtime_preflight_summary(bundle);
    let query_runtime_preflight_ready = query_runtime_preflight.ready;
    let workload_fixture_evidence = workload_fixture_evidence_summary(bundle);
    let workload_fixture_evidence_ready = workload_fixture_evidence.ready;
    let source_mutation_readiness = source_mutation_dual_write_readiness_summary(bundle);
    let source_mutation_readiness_ready = source_mutation_readiness.ready;
    let storage_recovery_evidence_ready =
        !storage_recovery_required(bundle) || storage_recovery_raw_evidence_ready(bundle);
    let background_graph_delta_evidence_missing =
        background_maintenance_graph_delta_evidence_missing(bundle);
    let family_health = replacement_readiness_family_evidence_health_from_bundle(bundle);
    let family_evidence_ready = family_health.present && family_health.ready;
    let production_cutover_ready = migration_gate_decision == Some("ready")
        && cutover_decision == Some("ready")
        && cutover_evidence_eligible
        && shadow_evidence_ready
        && previous_wrapper_contract_ready
        && full_contract_evidence_ready
        && dual_engine_evidence_present
        && dual_engine_evidence_ready == Some(true)
        && dual_engine_evidence_consistent
        && search_projection_evidence_ready
        && search_projection_shadow_evidence_ready
        && search_candidate_shadow_evidence_ready
        && search_route_ownership_ready
        && active_search_route_ownership_ready
        && active_search_route_readiness_ready
        && bounded_read_evidence_ready
        && graph_route_readiness_ready
        && query_runtime_preflight_ready
        && workload_fixture_evidence_ready
        && source_mutation_readiness_ready
        && storage_recovery_evidence_ready
        && !background_graph_delta_evidence_missing
        && family_evidence_ready
        && replacement_readiness_per_million == Some(1_000_000);
    let production_replacement_per_million = if production_cutover_ready {
        1_000_000
    } else {
        0
    };
    let blocking_categories = nowledge_replacement_blocking_categories(
        bundle,
        ReplacementReadinessInputs {
            covered_business_surface_per_million,
            shadow_parity_per_million,
            replacement_readiness_per_million,
            migration_gate_decision,
            cutover_decision,
            cutover_evidence_eligible,
            shadow_evidence_ready,
            previous_wrapper_contract_ready,
            full_contract_evidence_ready,
            dual_engine_evidence_present,
            dual_engine_evidence_ready,
            dual_engine_evidence_consistent,
            search_projection_evidence_ready,
            search_projection_shadow_evidence_ready,
            search_candidate_shadow_evidence_ready,
            search_route_ownership_ready,
            active_search_route_ownership_ready,
            active_search_route_readiness_ready,
            bounded_read_evidence_ready,
            graph_route_readiness_ready,
            query_runtime_preflight_ready,
            workload_fixture_evidence_ready,
            source_mutation_readiness_ready,
            background_graph_delta_evidence_missing,
            family_evidence_ready,
        },
    );
    let blockers = nowledge_replacement_blockers(bundle);
    let blocker_details = nowledge_replacement_blocker_details(&blockers, options);
    let missing_evidence = nowledge_replacement_missing_evidence(bundle);
    let family_details = replacement_readiness_family_details(bundle, options);
    let family_summary = replacement_readiness_family_summary(bundle, family_details.omitted_count);
    let next_actions = nowledge_replacement_next_actions(
        bundle,
        NextActionInputs {
            covered_business_surface_per_million,
            shadow_parity_per_million,
            replacement_readiness_per_million,
            migration_gate_decision,
            cutover_decision,
            cutover_evidence_eligible,
            shadow_evidence_ready,
            previous_wrapper_contract_ready,
            full_contract_evidence_ready,
            dual_engine_evidence_present,
            dual_engine_evidence_ready,
            dual_engine_evidence_consistent,
            search_projection_evidence_ready,
            search_projection_shadow_evidence_ready,
            search_candidate_shadow_evidence_ready,
            search_route_ownership_ready,
            active_search_route_ownership_ready,
            active_search_route_readiness_ready,
            bounded_read_evidence_ready,
            graph_route_readiness_ready,
            query_runtime_preflight_ready,
            workload_fixture_evidence_ready,
            source_mutation_readiness_ready,
            background_graph_delta_evidence_missing,
            family_evidence_ready,
            production_cutover_ready,
        },
    );

    let workload_fixture_evidence_json = serde_json::json!({
        "protocol": workload_fixture_evidence.protocol,
        "present": workload_fixture_evidence.present,
        "ready": workload_fixture_evidence.ready,
        "route_count": workload_fixture_evidence.route_count,
        "query_count": workload_fixture_evidence.query_count,
        "failed_query_count": workload_fixture_evidence.failed_query_count,
        "bounded_expansion_probe_count": workload_fixture_evidence.bounded_expansion_probe_count,
        "failed_bounded_expansion_probe_count": workload_fixture_evidence.failed_bounded_expansion_probe_count,
        "search_metadata_probe_count": workload_fixture_evidence.search_metadata_probe_count,
        "failed_search_metadata_probe_count": workload_fixture_evidence.failed_search_metadata_probe_count,
        "graph_rag_probe_count": workload_fixture_evidence.graph_rag_probe_count,
        "failed_graph_rag_probe_count": workload_fixture_evidence.failed_graph_rag_probe_count,
        "graph_rag_reports": workload_fixture_evidence.graph_rag_reports,
        "source_projection_probe_count": workload_fixture_evidence.source_projection_probe_count,
        "failed_source_projection_probe_count": workload_fixture_evidence.failed_source_projection_probe_count,
        "source_projection_reports": workload_fixture_evidence.source_projection_reports,
        "blocker_codes": workload_fixture_evidence.blocker_codes,
    });

    let mut summary = serde_json::json!({
        "protocol": "skein-nowledge-replacement-summary",
        "business_surface": {
            "covered_per_million": covered_business_surface_per_million,
            "ready": covered_business_surface_per_million == Some(1_000_000),
        },
        "shadow_parity": {
            "matched_per_million": shadow_parity_per_million,
            "decision": cutover_decision,
            "ready": cutover_decision == Some("ready") && shadow_parity_per_million == Some(1_000_000),
        },
        "replacement_readiness_per_million": replacement_readiness_per_million,
        "production_replacement_per_million": production_replacement_per_million,
        "production_cutover_ready": production_cutover_ready,
        "migration_gate_decision": migration_gate_decision,
        "dual_engine_evidence": {
            "present": dual_engine_evidence.present,
            "ready": dual_engine_evidence.ready,
            "consistent": dual_engine_evidence.consistent,
            "primary_engine": dual_engine_evidence.primary_engine,
            "shadow_engine": dual_engine_evidence.shadow_engine,
            "primary_check_count": dual_engine_evidence.primary_check_count,
            "shadow_check_count": dual_engine_evidence.shadow_check_count,
            "matched_check_count": dual_engine_evidence.matched_check_count,
            "primary_only_check_count": dual_engine_evidence.primary_only_check_count,
            "matched_per_million": dual_engine_evidence.matched_per_million,
        },
        "search_projection_evidence": {
            "protocol": search_projection_evidence.protocol,
            "present": search_projection_evidence.present,
            "ready": search_projection_evidence.ready,
            "derived_projection": search_projection_evidence.derived_projection,
            "all_tables_covered": search_projection_evidence.all_tables_covered,
            "covered_table_count": search_projection_evidence.covered_table_count,
            "required_table_count": search_projection_evidence.required_table_count,
            "fts_ready": search_projection_evidence.fts_ready,
            "vector_ready": search_projection_evidence.vector_ready,
            "document_identity_ready": search_projection_evidence.document_identity_ready,
            "embedding_identity_ready": search_projection_evidence.embedding_identity_ready,
            "fail_soft_ready": search_projection_evidence.fail_soft_ready,
            "rebuild_marker_ready": search_projection_evidence.rebuild_marker_ready,
            "metadata_repair_marker_ready": search_projection_evidence.metadata_repair_marker_ready,
            "incremental_update_ready": search_projection_evidence.incremental_update_ready,
            "source_chunk_ready": search_projection_evidence.source_chunk_ready,
            "predicate_pushdown_ready": search_projection_evidence.predicate_pushdown_ready,
            "production_filter_pruning_ready": search_projection_evidence.production_filter_pruning_ready,
            "compressed_vector_projection_required": search_projection_evidence.compressed_vector_projection_required,
            "compressed_vector_projection_ready": search_projection_evidence.compressed_vector_projection_ready,
            "blocker_codes": search_projection_evidence.blocker_codes,
        },
        "search_projection_shadow_evidence": {
            "protocol": search_projection_shadow_evidence.protocol,
            "evidence_source": search_projection_shadow_evidence.evidence_source,
            "present": search_projection_shadow_evidence.present,
            "ready": search_projection_shadow_evidence.ready,
            "primary_ready": search_projection_shadow_evidence.primary_ready,
            "shadow_ready": search_projection_shadow_evidence.shadow_ready,
            "document_count_parity": search_projection_shadow_evidence.document_count_parity,
            "document_identity_parity": search_projection_shadow_evidence.document_identity_parity,
            "table_parity_ready": search_projection_shadow_evidence.table_parity_ready,
            "embedding_identity_parity": search_projection_shadow_evidence.embedding_identity_parity,
            "lifecycle_parity": search_projection_shadow_evidence.lifecycle_parity,
            "incremental_watermark_parity": search_projection_shadow_evidence.incremental_watermark_parity,
            "predicate_pushdown_parity": search_projection_shadow_evidence.predicate_pushdown_parity,
            "pushdown_evidence": search_projection_shadow_evidence.pushdown_evidence,
            "primary_engine": search_projection_shadow_evidence.primary_engine,
            "shadow_engine": search_projection_shadow_evidence.shadow_engine,
            "blocker_codes": search_projection_shadow_evidence.blocker_codes,
        },
        "search_candidate_shadow_evidence": search_candidate_shadow_evidence_json,
        "bounded_read_evidence": {
            "protocol": bounded_read_evidence.protocol,
            "present": bounded_read_evidence.present,
            "ready": bounded_read_evidence.ready,
            "mode": bounded_read_evidence.mode,
            "max_rows": bounded_read_evidence.max_rows,
            "execution_row_cap": bounded_read_evidence.execution_row_cap,
            "estimated_payload_bytes": bounded_read_evidence.estimated_payload_bytes,
            "max_estimated_payload_bytes": bounded_read_evidence.max_estimated_payload_bytes,
            "payload_budget_exceeded": bounded_read_evidence.payload_budget_exceeded,
            "row_limit_enforced_before_output": bounded_read_evidence.row_limit_enforced_before_output,
            "operator_row_cap_enabled": bounded_read_evidence.operator_row_cap_enabled,
            "streaming": bounded_read_evidence.streaming,
            "blocking_operator_count": bounded_read_evidence.blocking_operator_count,
            "blocking_operator_memory_reports_complete": bounded_read_evidence.blocking_operator_memory_reports_complete,
            "blocking_operator_memory_within_budget": bounded_read_evidence.blocking_operator_memory_within_budget,
            "spill_within_budget": bounded_read_evidence.spill_within_budget,
            "covered_routes": bounded_read_evidence.covered_routes,
            "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_covered_routes": bounded_read_evidence.missing_covered_routes,
            "route_primary_ready": bounded_read_evidence.route_primary_ready,
            "primary_ready_routes": bounded_read_evidence.primary_ready_routes,
            "route_query_plan_evidence_ready": bounded_read_evidence.route_query_plan_evidence_ready,
            "route_query_profile_evidence_ready": bounded_read_evidence.route_query_profile_evidence_ready,
            "route_query_api_behavior_evidence_ready": bounded_read_evidence.route_query_api_behavior_evidence_ready,
            "relationship_property_pruning_required_count": bounded_read_evidence.relationship_property_pruning_required_count,
            "relationship_property_pruning_report_count": bounded_read_evidence.relationship_property_pruning_report_count,
            "route_relationship_property_pruning_evidence_ready": bounded_read_evidence.route_relationship_property_pruning_evidence_ready,
            "blocker_codes": bounded_read_evidence.blocker_codes,
        },
        "graph_route_readiness": graph_route_readiness.json(),
        "query_runtime_preflight": {
            "protocol": query_runtime_preflight.protocol,
            "present": query_runtime_preflight.present,
            "ready": query_runtime_preflight.ready,
            "database_opened": query_runtime_preflight.database_opened,
            "probe_count": query_runtime_preflight.probe_count,
            "passed_probe_count": query_runtime_preflight.passed_probe_count,
            "failed_probe_count": query_runtime_preflight.failed_probe_count,
            "required_route_count": query_runtime_preflight.required_route_count,
            "covered_route_count": query_runtime_preflight.covered_route_count,
            "covered_routes": query_runtime_preflight.covered_routes,
            "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": query_runtime_preflight.missing_required_routes,
            "unknown_routes": query_runtime_preflight.unknown_routes,
            "duplicate_routes": query_runtime_preflight.duplicate_routes,
            "required_routes_covered": query_runtime_preflight.required_routes_covered,
            "route_coverage_ready": query_runtime_preflight.route_coverage_ready,
            "probe_details_ready": query_runtime_preflight.probe_details_ready,
            "blocker_codes": query_runtime_preflight.blocker_codes,
        },
        "cutover_evidence": {
            "eligible": cutover_evidence_eligible,
            "evidence_kind": json_get_str_path(bundle, &["cutover_evidence", "evidence_kind"]),
            "ready_engine_kind": json_get_str_path(bundle, &["cutover_evidence", "ready_engine_kind"]),
            "ready_wrapper_identity": json_get_str_path(bundle, &["cutover_evidence", "ready_wrapper_identity"]),
            "storage_recovery_required": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]),
            "storage_recovery_ready": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]),
            "storage_recovery_protocol_matches": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_protocol_matches"]),
            "storage_recovery_durable": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_durable"]),
            "storage_recovery_checkpoint_boundary_present": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_checkpoint_boundary_present"]),
            "storage_recovery_wal_replay_bounded": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_wal_replay_bounded"]),
            "storage_recovery_torn_tail_clean": json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_torn_tail_clean"]),
            "storage_recovery_blocker_codes": json_get_array_path(bundle, &["cutover_evidence", "storage_recovery_blocker_codes"]),
            "background_maintenance_required": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_required"]),
            "background_maintenance_ready": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_ready"]),
            "background_maintenance_protocol_matches": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_protocol_matches"]),
            "background_maintenance_executable_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_executable_search_projection_graph_delta_count"]),
            "background_maintenance_admitted_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_admitted_search_projection_graph_delta_count"]),
            "background_maintenance_deferred_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_deferred_search_projection_graph_delta_count"]),
            "background_maintenance_rejected_search_projection_graph_delta_count": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_rejected_search_projection_graph_delta_count"]),
            "background_maintenance_executable_search_projection_graph_delta_operations": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_executable_search_projection_graph_delta_operations"]),
            "background_maintenance_admitted_search_projection_graph_delta_operations": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_admitted_search_projection_graph_delta_operations"]),
            "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": json_get_u64_path(bundle, &["cutover_evidence", "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"]),
            "background_maintenance_foreground_admission_probe_ready": json_get_bool_path(bundle, &["cutover_evidence", "background_maintenance_foreground_admission_probe_ready"]),
            "background_maintenance_foreground_admission_probe_admission": json_get_str_path(bundle, &["cutover_evidence", "background_maintenance_foreground_admission_probe_admission"]),
            "background_maintenance_blocker_codes": json_get_array_path(bundle, &["cutover_evidence", "background_maintenance_blocker_codes"]),
            "replacement_readiness_min_per_million": json_get_u64_path(bundle, &["cutover_evidence", "replacement_readiness_min_per_million"]),
        },
        "shadow_evidence": {
            "ready": shadow_evidence.ready,
            "run_evidence_kind": shadow_evidence.run_evidence_kind,
            "ready_engine_kind": shadow_evidence.ready_engine_kind,
            "ready_wrapper_identity": shadow_evidence.ready_wrapper_identity,
            "contract_wrapper_identity": shadow_evidence.contract_wrapper_identity,
            "cutover_ready_wrapper_identity": shadow_evidence.cutover_ready_wrapper_identity,
        },
        "previous_wrapper_contract_evidence": {
            "ready": previous_wrapper_contract_ready,
            "evidence_kind": json_get_str_path(bundle, &["previous_wrapper_contract_evidence", "evidence_kind"]),
            "wrapper_identity": json_get_str_path(bundle, &["previous_wrapper_contract_evidence", "wrapper_identity"]),
            "requires_full_contract_ready": json_get_bool_path(bundle, &["previous_wrapper_contract_evidence", "requires_full_contract_ready"]),
            "requires_wrapper_identity": json_get_bool_path(bundle, &["previous_wrapper_contract_evidence", "requires_wrapper_identity"]),
            "blocker_codes": json_get_array_path(bundle, &["previous_wrapper_contract_evidence", "blocker_codes"]),
        },
        "full_contract_evidence": {
            "ready": full_contract_evidence.ready,
            "required_contract_ready": full_contract_evidence.required_contract_ready,
            "full_contract_checked": full_contract_evidence.full_contract_checked,
            "full_contract_ready": full_contract_evidence.full_contract_ready,
            "selected_checks": full_contract_evidence.selected_checks,
            "check_count": full_contract_evidence.check_count,
        },
        "blocking_categories": blocking_categories,
        "blocker_summary": {
            "total_count": blockers.len(),
            "omitted_count": blocker_details.omitted_count,
        },
        "blockers": blocker_details.blockers,
        "missing_evidence": missing_evidence,
        "next_actions": next_actions,
        "replacement_readiness_family_summary": family_summary,
        "replacement_readiness_by_query_family": family_details.families,
    });
    summary["search_route_ownership"] = search_route_ownership.json();
    summary["active_search_route_ownership"] = active_search_route_ownership.json();
    summary["active_search_route_readiness"] = active_search_route_readiness.json();
    summary["source_mutation_dual_write_readiness"] = source_mutation_readiness.json();
    summary["cutover_evidence"]["storage_recovery_replay_boundary_consistent"] =
        json_get_bool_path(
            bundle,
            &[
                "cutover_evidence",
                "storage_recovery_replay_boundary_consistent",
            ],
        )
        .map_or(serde_json::Value::Null, serde_json::Value::Bool);
    summary["cutover_evidence"]["background_maintenance_memory_pressure_ready"] =
        json_get_bool_path(
            bundle,
            &[
                "cutover_evidence",
                "background_maintenance_memory_pressure_ready",
            ],
        )
        .map_or(serde_json::Value::Null, serde_json::Value::Bool);
    summary["cutover_evidence"]["background_maintenance_memory_budget_bytes"] = json_get_u64_path(
        bundle,
        &[
            "cutover_evidence",
            "background_maintenance_memory_budget_bytes",
        ],
    )
    .map_or(serde_json::Value::Null, serde_json::Value::from);
    summary["cutover_evidence"]["background_maintenance_estimated_memory_bytes"] =
        json_get_u64_path(
            bundle,
            &[
                "cutover_evidence",
                "background_maintenance_estimated_memory_bytes",
            ],
        )
        .map_or(serde_json::Value::Null, serde_json::Value::from);
    summary["cutover_evidence"]["background_maintenance_qos_snapshot_ready"] = json_get_bool_path(
        bundle,
        &[
            "cutover_evidence",
            "background_maintenance_qos_snapshot_ready",
        ],
    )
    .map_or(serde_json::Value::Null, serde_json::Value::Bool);
    summary["cutover_evidence"]["background_maintenance_qos_snapshot_foreground_admitted"] =
        json_get_u64_path(
            bundle,
            &[
                "cutover_evidence",
                "background_maintenance_qos_snapshot_foreground_admitted",
            ],
        )
        .map_or(serde_json::Value::Null, serde_json::Value::from);
    summary["cutover_evidence"]["background_maintenance_qos_snapshot_background_bounded"] =
        json_get_bool_path(
            bundle,
            &[
                "cutover_evidence",
                "background_maintenance_qos_snapshot_background_bounded",
            ],
        )
        .map_or(serde_json::Value::Null, serde_json::Value::Bool);
    summary["cutover_evidence"]
        ["background_maintenance_qos_snapshot_total_background_over_budget"] = json_get_u64_path(
        bundle,
        &[
            "cutover_evidence",
            "background_maintenance_qos_snapshot_total_background_over_budget",
        ],
    )
    .map_or(serde_json::Value::Null, serde_json::Value::from);
    summary["cutover_evidence"]["background_maintenance_qos_snapshot_blocker_codes"] =
        json_get_array_path(
            bundle,
            &[
                "cutover_evidence",
                "background_maintenance_qos_snapshot_blocker_codes",
            ],
        );
    if let Some(object) = summary.as_object_mut() {
        object.insert(
            "replacement_boundaries".to_string(),
            replacement_boundaries_json(),
        );
        object.insert(
            "workload_fixture_evidence".to_string(),
            workload_fixture_evidence_json,
        );
    }
    summary
}

fn replacement_boundaries_json() -> serde_json::Value {
    serde_json::json!({
        "graph_layer": {
            "scope": GRAPH_LAYER_REPLACEMENT_SCOPE,
            "replacement_role": "primary_replacement",
            "storage_owner": "skein",
        },
        "search_projection": {
            "scope": SEARCH_PROJECTION_REPLACEMENT_SCOPE,
            "replacement_role": "rebuildable_projection",
            "storage_owner": "skein",
        },
        "content_store": {
            "scope": SQLITE_CONTENT_STORE_SCOPE,
            "replacement_role": "external_out_of_scope",
            "storage_owner": "nowledge_mem",
        },
        "large_blob_store": {
            "scope": LARGE_BLOB_VALUE_STORE_SCOPE,
            "replacement_role": "external_out_of_scope",
            "storage_owner": "nowledge_mem",
        },
    })
}

struct NowledgeReplacementBlockerDetails {
    blockers: Vec<String>,
    omitted_count: usize,
}

fn nowledge_replacement_blocker_details(
    blockers: &[String],
    options: NowledgeReplacementSummaryOptions,
) -> NowledgeReplacementBlockerDetails {
    if !options.include_blocker_details {
        return NowledgeReplacementBlockerDetails {
            blockers: Vec::new(),
            omitted_count: blockers.len(),
        };
    }
    let limit = options.max_blockers.unwrap_or(blockers.len());
    NowledgeReplacementBlockerDetails {
        blockers: blockers.iter().take(limit).cloned().collect(),
        omitted_count: blockers.len().saturating_sub(limit),
    }
}

struct ReplacementReadinessFamilyDetails {
    families: serde_json::Value,
    omitted_count: usize,
}

fn replacement_readiness_family_details(
    bundle: &serde_json::Value,
    options: NowledgeReplacementSummaryOptions,
) -> ReplacementReadinessFamilyDetails {
    let families = bundle
        .get("replacement_readiness_by_query_family")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    if !options.include_family_details {
        return ReplacementReadinessFamilyDetails {
            families: serde_json::json!([]),
            omitted_count: families.len(),
        };
    }
    let limit = options.max_family_items.unwrap_or(families.len());
    let omitted_count = families.len().saturating_sub(limit);
    ReplacementReadinessFamilyDetails {
        families: serde_json::Value::Array(families.into_iter().take(limit).collect()),
        omitted_count,
    }
}

fn replacement_readiness_family_summary(
    bundle: &serde_json::Value,
    omitted_count: usize,
) -> serde_json::Value {
    let health = replacement_readiness_family_evidence_health_from_bundle(bundle);
    let families = bundle
        .get("replacement_readiness_by_query_family")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let total_count = families.len();
    let ready_count = families
        .iter()
        .filter(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
                == Some(1_000_000)
        })
        .count();
    let blocked_count = total_count.saturating_sub(ready_count);
    let min_replacement_readiness_per_million = families
        .iter()
        .filter_map(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
        })
        .min();
    let blocked_query_families = families
        .iter()
        .filter(|family| {
            family
                .get("replacement_readiness_per_million")
                .and_then(serde_json::Value::as_u64)
                != Some(1_000_000)
        })
        .filter_map(|family| {
            family
                .get("query_family")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::to_string)
        .collect::<Vec<_>>();
    serde_json::json!({
        "total_count": total_count,
        "ready_count": ready_count,
        "blocked_count": blocked_count,
        "omitted_count": omitted_count,
        "min_replacement_readiness_per_million": min_replacement_readiness_per_million,
        "blocked_query_families": blocked_query_families,
        "required_query_families": REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
        "missing_required_query_families": health.missing_required_query_families,
    })
}

struct ReplacementReadinessInputs<'a> {
    covered_business_surface_per_million: Option<u64>,
    shadow_parity_per_million: Option<u64>,
    replacement_readiness_per_million: Option<u64>,
    migration_gate_decision: Option<&'a str>,
    cutover_decision: Option<&'a str>,
    cutover_evidence_eligible: bool,
    shadow_evidence_ready: bool,
    previous_wrapper_contract_ready: bool,
    full_contract_evidence_ready: bool,
    dual_engine_evidence_present: bool,
    dual_engine_evidence_ready: Option<bool>,
    dual_engine_evidence_consistent: bool,
    search_projection_evidence_ready: bool,
    search_projection_shadow_evidence_ready: bool,
    search_candidate_shadow_evidence_ready: bool,
    search_route_ownership_ready: bool,
    active_search_route_ownership_ready: bool,
    active_search_route_readiness_ready: bool,
    bounded_read_evidence_ready: bool,
    graph_route_readiness_ready: bool,
    query_runtime_preflight_ready: bool,
    workload_fixture_evidence_ready: bool,
    source_mutation_readiness_ready: bool,
    background_graph_delta_evidence_missing: bool,
    family_evidence_ready: bool,
}

struct NextActionInputs<'a> {
    covered_business_surface_per_million: Option<u64>,
    shadow_parity_per_million: Option<u64>,
    replacement_readiness_per_million: Option<u64>,
    migration_gate_decision: Option<&'a str>,
    cutover_decision: Option<&'a str>,
    cutover_evidence_eligible: bool,
    shadow_evidence_ready: bool,
    previous_wrapper_contract_ready: bool,
    full_contract_evidence_ready: bool,
    dual_engine_evidence_present: bool,
    dual_engine_evidence_ready: Option<bool>,
    dual_engine_evidence_consistent: bool,
    search_projection_evidence_ready: bool,
    search_projection_shadow_evidence_ready: bool,
    search_candidate_shadow_evidence_ready: bool,
    search_route_ownership_ready: bool,
    active_search_route_ownership_ready: bool,
    active_search_route_readiness_ready: bool,
    bounded_read_evidence_ready: bool,
    graph_route_readiness_ready: bool,
    query_runtime_preflight_ready: bool,
    workload_fixture_evidence_ready: bool,
    source_mutation_readiness_ready: bool,
    background_graph_delta_evidence_missing: bool,
    family_evidence_ready: bool,
    production_cutover_ready: bool,
}

struct DualEngineEvidenceSummary<'a> {
    present: bool,
    ready: Option<bool>,
    consistent: bool,
    primary_engine: Option<&'a str>,
    shadow_engine: Option<&'a str>,
    primary_check_count: Option<u64>,
    shadow_check_count: Option<u64>,
    matched_check_count: Option<u64>,
    primary_only_check_count: Option<u64>,
    matched_per_million: Option<u64>,
}

struct ShadowEvidenceSummary<'a> {
    ready: bool,
    run_evidence_kind: Option<&'a str>,
    ready_engine_kind: Option<&'a str>,
    ready_wrapper_identity: Option<&'a str>,
    contract_wrapper_identity: Option<&'a str>,
    cutover_ready_wrapper_identity: Option<&'a str>,
}

struct SearchProjectionEvidenceSummary {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    derived_projection: Option<bool>,
    all_tables_covered: Option<bool>,
    covered_table_count: Option<u64>,
    required_table_count: Option<u64>,
    fts_ready: Option<bool>,
    vector_ready: Option<bool>,
    document_identity_ready: Option<bool>,
    embedding_identity_ready: Option<bool>,
    fail_soft_ready: Option<bool>,
    rebuild_marker_ready: Option<bool>,
    metadata_repair_marker_ready: Option<bool>,
    incremental_update_ready: Option<bool>,
    source_chunk_ready: Option<bool>,
    predicate_pushdown_ready: Option<bool>,
    production_filter_pruning_ready: Option<bool>,
    compressed_vector_projection_required: Option<bool>,
    compressed_vector_projection_ready: Option<bool>,
    blocker_codes: serde_json::Value,
}

struct SearchProjectionShadowEvidenceSummary<'a> {
    protocol: Option<String>,
    evidence_source: Option<String>,
    present: bool,
    ready: bool,
    primary_ready: Option<bool>,
    shadow_ready: Option<bool>,
    document_count_parity: Option<bool>,
    document_identity_parity: Option<bool>,
    table_parity_ready: Option<bool>,
    embedding_identity_parity: Option<bool>,
    lifecycle_parity: Option<bool>,
    incremental_watermark_parity: Option<bool>,
    predicate_pushdown_parity: Option<bool>,
    pushdown_evidence: serde_json::Value,
    primary_engine: Option<&'a str>,
    shadow_engine: Option<&'a str>,
    blocker_codes: serde_json::Value,
}

struct SearchCandidateShadowEvidenceSummary {
    protocol: Option<String>,
    evidence_source: Option<String>,
    route: Option<String>,
    present: bool,
    ready: bool,
    candidate_primary_engine: Option<String>,
    primary_engine: Option<String>,
    shadow_engine: Option<String>,
    request_count: Option<u64>,
    primary_candidate_count: Option<u64>,
    shadow_candidate_count: Option<u64>,
    matched_candidate_count: Option<u64>,
    primary_only_candidate_count: Option<u64>,
    candidate_counts_ready: bool,
    text_retriever_ready: Option<bool>,
    vector_retriever_ready: Option<bool>,
    candidate_identity_ready: Option<bool>,
    candidate_identity_parity: Option<bool>,
    row_count_parity: Option<bool>,
    vector_top_k_overlap_ready: Option<bool>,
    fts_top_k_overlap_ready: Option<bool>,
    source_chunk_identity_ready: Option<bool>,
    fail_soft_observed: Option<bool>,
    projection_marker_status_visible: Option<bool>,
    projection_watermark_ready: Option<bool>,
    embedding_identity_ready: Option<bool>,
    shadow_scan_filter_pushdown_ready: Option<bool>,
    shadow_scan_field_pruning_ready: Option<bool>,
    shadow_scan_field_summary_count: Option<u64>,
    filter_pushdown_ready: Option<bool>,
    filter_pushdown_field_summary_count: Option<u64>,
    filter_pushdown_missing_required_fields: Vec<String>,
    filter_pushdown_field_capabilities_ready: Option<bool>,
    filter_pushdown_missing_value_summary_fields: Vec<String>,
    filter_pushdown_missing_numeric_range_fields: Vec<String>,
    filter_pushdown_missing_timestamp_range_fields: Vec<String>,
    blocker_codes: serde_json::Value,
}

struct SearchRouteOwnershipSummary {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    production_cutover_ready: Option<bool>,
    require_all_skein: Option<bool>,
    required_route_count: Option<u64>,
    explicit_route_count: Option<u64>,
    skein_route_count: Option<u64>,
    lancedb_route_count: Option<u64>,
    missing_required_routes: Vec<String>,
    lancedb_routes: Vec<String>,
    blocker_codes: serde_json::Value,
}

struct ActiveSearchRouteReadinessSummary {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    production_cutover_ready: Option<bool>,
    require_all_skein: Option<bool>,
    required_route_count: Option<u64>,
    evidence_route_count: Option<u64>,
    ready_route_count: Option<u64>,
    skein_route_count: Option<u64>,
    lancedb_handle_required_route_count: Option<u64>,
    missing_required_routes: Vec<String>,
    non_skein_routes: Vec<String>,
    lancedb_handle_required_routes: Vec<String>,
    candidate_not_ready_routes: Vec<String>,
    candidate_identity_not_ready_routes: Vec<String>,
    embedding_identity_not_ready_routes: Vec<String>,
    zero_vector_semantics_not_ready_routes: Vec<String>,
    cjk_tokenization_not_ready_routes: Vec<String>,
    metadata_pushdown_not_ready_routes: Vec<String>,
    ranking_window_not_ready_routes: Vec<String>,
    ranking_not_ready_routes: Vec<String>,
    fail_soft_not_ready_routes: Vec<String>,
    fail_soft_reason_codes_not_ready_routes: Vec<String>,
    repair_rebuild_markers_not_ready_routes: Vec<String>,
    blocker_codes: serde_json::Value,
}

impl ActiveSearchRouteReadinessSummary {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "production_cutover_ready": self.production_cutover_ready,
            "require_all_skein": self.require_all_skein,
            "required_route_count": self.required_route_count,
            "evidence_route_count": self.evidence_route_count,
            "ready_route_count": self.ready_route_count,
            "skein_route_count": self.skein_route_count,
            "lancedb_handle_required_route_count": self.lancedb_handle_required_route_count,
            "missing_required_routes": self.missing_required_routes,
            "non_skein_routes": self.non_skein_routes,
            "lancedb_handle_required_routes": self.lancedb_handle_required_routes,
            "candidate_not_ready_routes": self.candidate_not_ready_routes,
            "candidate_identity_not_ready_routes": self.candidate_identity_not_ready_routes,
            "embedding_identity_not_ready_routes": self.embedding_identity_not_ready_routes,
            "zero_vector_semantics_not_ready_routes": self.zero_vector_semantics_not_ready_routes,
            "cjk_tokenization_not_ready_routes": self.cjk_tokenization_not_ready_routes,
            "metadata_pushdown_not_ready_routes": self.metadata_pushdown_not_ready_routes,
            "ranking_window_not_ready_routes": self.ranking_window_not_ready_routes,
            "ranking_not_ready_routes": self.ranking_not_ready_routes,
            "fail_soft_not_ready_routes": self.fail_soft_not_ready_routes,
            "fail_soft_reason_codes_not_ready_routes": self.fail_soft_reason_codes_not_ready_routes,
            "repair_rebuild_markers_not_ready_routes": self.repair_rebuild_markers_not_ready_routes,
            "blocker_codes": self.blocker_codes,
        })
    }
}

impl SearchRouteOwnershipSummary {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "production_cutover_ready": self.production_cutover_ready,
            "require_all_skein": self.require_all_skein,
            "required_route_count": self.required_route_count,
            "explicit_route_count": self.explicit_route_count,
            "skein_route_count": self.skein_route_count,
            "lancedb_route_count": self.lancedb_route_count,
            "missing_required_routes": self.missing_required_routes,
            "lancedb_routes": self.lancedb_routes,
            "blocker_codes": self.blocker_codes,
        })
    }
}

struct BoundedReadEvidenceSummary<'a> {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    mode: Option<&'a str>,
    max_rows: Option<u64>,
    execution_row_cap: Option<u64>,
    estimated_payload_bytes: Option<u64>,
    max_estimated_payload_bytes: Option<u64>,
    payload_budget_exceeded: Option<bool>,
    row_limit_enforced_before_output: Option<bool>,
    operator_row_cap_enabled: Option<bool>,
    streaming: Option<bool>,
    blocking_operator_count: Option<u64>,
    blocking_operator_memory_reports_complete: Option<bool>,
    blocking_operator_memory_within_budget: Option<bool>,
    spill_within_budget: Option<bool>,
    covered_routes: Vec<String>,
    missing_covered_routes: Vec<&'static str>,
    route_primary_ready: Option<bool>,
    primary_ready_routes: Vec<String>,
    route_query_plan_evidence_ready: Option<bool>,
    route_query_profile_evidence_ready: Option<bool>,
    route_query_api_behavior_evidence_ready: Option<bool>,
    relationship_property_pruning_required_count: Option<u64>,
    relationship_property_pruning_report_count: Option<u64>,
    route_relationship_property_pruning_evidence_ready: Option<bool>,
    blocker_codes: serde_json::Value,
}

struct QueryRuntimePreflightSummary {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    database_opened: Option<bool>,
    probe_count: Option<u64>,
    passed_probe_count: Option<u64>,
    failed_probe_count: Option<u64>,
    required_route_count: Option<u64>,
    covered_route_count: Option<u64>,
    covered_routes: Vec<String>,
    missing_required_routes: Vec<&'static str>,
    unknown_routes: Vec<String>,
    duplicate_routes: Vec<String>,
    required_routes_covered: Option<bool>,
    route_coverage_ready: bool,
    probe_details_ready: bool,
    blocker_codes: serde_json::Value,
}

struct WorkloadFixtureEvidenceSummary {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    route_count: Option<u64>,
    query_count: Option<u64>,
    failed_query_count: Option<u64>,
    bounded_expansion_probe_count: Option<u64>,
    failed_bounded_expansion_probe_count: Option<u64>,
    search_metadata_probe_count: Option<u64>,
    failed_search_metadata_probe_count: Option<u64>,
    graph_rag_probe_count: Option<u64>,
    failed_graph_rag_probe_count: Option<u64>,
    graph_rag_reports: serde_json::Value,
    source_projection_probe_count: Option<u64>,
    failed_source_projection_probe_count: Option<u64>,
    source_projection_reports: serde_json::Value,
    blocker_codes: serde_json::Value,
}

struct SourceMutationDualWriteReadinessSummary {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    required_family_count: Option<u64>,
    evidence_family_count: Option<u64>,
    ready_family_count: Option<u64>,
    missing_required_families: Vec<String>,
    unknown_families: Vec<String>,
    duplicate_families: Vec<String>,
    blocker_codes: serde_json::Value,
}

impl SourceMutationDualWriteReadinessSummary {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "required_family_count": self.required_family_count,
            "evidence_family_count": self.evidence_family_count,
            "ready_family_count": self.ready_family_count,
            "missing_required_families": self.missing_required_families,
            "unknown_families": self.unknown_families,
            "duplicate_families": self.duplicate_families,
            "blocker_codes": self.blocker_codes,
        })
    }
}

fn shadow_evidence_summary(bundle: &serde_json::Value) -> ShadowEvidenceSummary<'_> {
    let run_evidence_kind = json_get_str_path(bundle, &["shadow_run", "evidence_kind"]);
    let ready_engine_kind = json_get_str_path(bundle, &["shadow_ready", "engine_kind"]);
    let ready_wrapper_identity = json_get_str_path(bundle, &["shadow_ready", "wrapper_identity"]);
    let contract_wrapper_identity = json_get_str_path(
        bundle,
        &["previous_wrapper_contract_evidence", "wrapper_identity"],
    );
    let cutover_ready_wrapper_identity =
        json_get_str_path(bundle, &["cutover_evidence", "ready_wrapper_identity"]);
    ShadowEvidenceSummary {
        ready: run_evidence_kind == Some("previous_wrapper")
            && ready_engine_kind == Some("previous_wrapper")
            && ready_wrapper_identity.is_some()
            && ready_wrapper_identity == contract_wrapper_identity
            && ready_wrapper_identity == cutover_ready_wrapper_identity,
        run_evidence_kind,
        ready_engine_kind,
        ready_wrapper_identity,
        contract_wrapper_identity,
        cutover_ready_wrapper_identity,
    }
}

struct FullContractEvidenceSummary {
    ready: bool,
    required_contract_ready: Option<bool>,
    full_contract_checked: Option<bool>,
    full_contract_ready: Option<bool>,
    selected_checks: Option<u64>,
    check_count: Option<u64>,
}

fn full_contract_evidence_summary(bundle: &serde_json::Value) -> FullContractEvidenceSummary {
    let path = if json_get_path(bundle, &["contract_evidence"]).is_some() {
        &["contract_evidence"][..]
    } else {
        &[][..]
    };
    let required_contract_ready =
        json_get_bool_path_from_dynamic(bundle, path, "required_contract_ready");
    let full_contract_checked =
        json_get_bool_path_from_dynamic(bundle, path, "full_contract_checked");
    let full_contract_ready = json_get_bool_path_from_dynamic(bundle, path, "full_contract_ready");
    let selected_checks = json_get_u64_path_from_dynamic(bundle, path, "selected_checks");
    let check_count = json_get_u64_path_from_dynamic(bundle, path, "check_count");
    FullContractEvidenceSummary {
        ready: required_contract_ready == Some(true)
            && full_contract_checked == Some(true)
            && full_contract_ready == Some(true)
            && check_count.is_some_and(|count| count > 0)
            && selected_checks == check_count,
        required_contract_ready,
        full_contract_checked,
        full_contract_ready,
        selected_checks,
        check_count,
    }
}

fn dual_engine_evidence_summary(bundle: &serde_json::Value) -> DualEngineEvidenceSummary<'_> {
    let path = if json_get_path(bundle, &["dual_engine_evidence"]).is_some() {
        &["dual_engine_evidence"][..]
    } else {
        &["cutover", "dual_engine_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let primary_check_count = json_get_u64_path_from_dynamic(bundle, path, "primary_check_count");
    let shadow_check_count = json_get_u64_path_from_dynamic(bundle, path, "shadow_check_count");
    let matched_check_count = json_get_u64_path_from_dynamic(bundle, path, "matched_check_count");
    let primary_only_check_count =
        json_get_u64_path_from_dynamic(bundle, path, "primary_only_check_count");
    let matched_per_million = json_get_u64_path_from_dynamic(bundle, path, "matched_per_million");
    DualEngineEvidenceSummary {
        present,
        ready: json_get_bool_path_from_dynamic(bundle, path, "ready"),
        consistent: primary_check_count.is_some_and(|count| count > 0)
            && primary_check_count == shadow_check_count
            && matched_check_count == shadow_check_count
            && primary_only_check_count == Some(0)
            && matched_per_million == Some(1_000_000),
        primary_engine: json_get_str_path_from_dynamic(bundle, path, "primary_engine"),
        shadow_engine: json_get_str_path_from_dynamic(bundle, path, "shadow_engine"),
        primary_check_count,
        shadow_check_count,
        matched_check_count,
        primary_only_check_count,
        matched_per_million,
    }
}

fn search_projection_evidence_summary(
    bundle: &serde_json::Value,
) -> SearchProjectionEvidenceSummary {
    let path = if json_get_path(bundle, &["search_projection_evidence"]).is_some() {
        &["search_projection_evidence"][..]
    } else {
        &["cutover_evidence", "search_projection_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some_and(|value| !value.is_null());
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let covered_table_count = json_get_u64_path_from_dynamic(bundle, path, "covered_table_count");
    let required_table_count = json_get_u64_path_from_dynamic(bundle, path, "required_table_count");
    let derived_projection = json_get_bool_path_from_dynamic(bundle, path, "derived_projection");
    let all_tables_covered = json_get_bool_path_from_dynamic(bundle, path, "all_tables_covered");
    let fts_ready = json_get_bool_path_from_dynamic(bundle, path, "fts_ready");
    let vector_ready = json_get_bool_path_from_dynamic(bundle, path, "vector_ready");
    let document_identity_ready =
        json_get_bool_path_from_dynamic(bundle, path, "document_identity_ready");
    let embedding_identity_ready =
        json_get_bool_path_from_dynamic(bundle, path, "embedding_identity_ready");
    let fail_soft_ready = json_get_bool_path_from_dynamic(bundle, path, "fail_soft_ready");
    let rebuild_marker_ready =
        json_get_bool_path_from_dynamic(bundle, path, "rebuild_marker_ready");
    let metadata_repair_marker_ready =
        json_get_bool_path_from_dynamic(bundle, path, "metadata_repair_marker_ready");
    let incremental_update_ready =
        json_get_bool_path_from_dynamic(bundle, path, "incremental_update_ready");
    let source_chunk_ready = json_get_bool_path_from_dynamic(bundle, path, "source_chunk_ready");
    let predicate_pushdown_ready =
        json_get_bool_path_from_dynamic(bundle, path, "predicate_pushdown_ready");
    let production_filter_pruning_ready =
        json_get_bool_path_from_dynamic(bundle, path, "production_filter_pruning_ready");
    let compressed_vector_projection_required =
        json_get_bool_path_from_dynamic(bundle, path, "compressed_vector_projection_required");
    let compressed_vector_projection_ready =
        json_get_bool_path_from_dynamic(bundle, path, "compressed_vector_projection_ready");
    let ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL)
        && derived_projection == Some(true)
        && all_tables_covered == Some(true)
        && covered_table_count.is_some_and(|count| count > 0)
        && covered_table_count == required_table_count
        && fts_ready == Some(true)
        && vector_ready == Some(true)
        && document_identity_ready == Some(true)
        && embedding_identity_ready == Some(true)
        && fail_soft_ready == Some(true)
        && rebuild_marker_ready == Some(true)
        && metadata_repair_marker_ready == Some(true)
        && incremental_update_ready == Some(true)
        && source_chunk_ready == Some(true)
        && predicate_pushdown_ready == Some(true)
        && production_filter_pruning_ready == Some(true)
        && compressed_vector_projection_ready.unwrap_or(true);
    SearchProjectionEvidenceSummary {
        protocol,
        present,
        ready,
        derived_projection,
        all_tables_covered,
        covered_table_count,
        required_table_count,
        fts_ready,
        vector_ready,
        document_identity_ready,
        embedding_identity_ready,
        fail_soft_ready,
        rebuild_marker_ready,
        metadata_repair_marker_ready,
        incremental_update_ready,
        source_chunk_ready,
        predicate_pushdown_ready,
        production_filter_pruning_ready,
        compressed_vector_projection_required,
        compressed_vector_projection_ready,
        blocker_codes: json_get_array_path_from_dynamic(bundle, path, "blocker_codes"),
    }
}

fn search_projection_shadow_evidence_summary(
    bundle: &serde_json::Value,
) -> SearchProjectionShadowEvidenceSummary<'_> {
    let path = if json_get_path(bundle, &["search_projection_shadow_evidence"]).is_some() {
        &["search_projection_shadow_evidence"][..]
    } else {
        &["cutover_evidence", "search_projection_shadow_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let evidence_source =
        json_get_str_path_from_dynamic(bundle, path, "evidence_source").map(str::to_string);
    let primary_ready = json_get_bool_path_from_dynamic(bundle, path, "primary_ready");
    let shadow_ready = json_get_bool_path_from_dynamic(bundle, path, "shadow_ready");
    let document_count_parity =
        json_get_bool_path_from_dynamic(bundle, path, "document_count_parity");
    let document_identity_parity =
        json_get_bool_path_from_dynamic(bundle, path, "document_identity_parity");
    let table_parity_ready = {
        let mut table_parity_path = path.to_vec();
        table_parity_path.extend(["table_parity", "ready"]);
        json_get_bool_path(bundle, &table_parity_path)
            .or_else(|| json_get_bool_path_from_dynamic(bundle, path, "table_parity_ready"))
    };
    let embedding_identity_parity =
        json_get_bool_path_from_dynamic(bundle, path, "embedding_identity_parity");
    let lifecycle_parity = json_get_bool_path_from_dynamic(bundle, path, "lifecycle_parity");
    let incremental_watermark_parity =
        json_get_bool_path_from_dynamic(bundle, path, "incremental_watermark_parity");
    let predicate_pushdown_parity =
        json_get_bool_path_from_dynamic(bundle, path, "predicate_pushdown_parity");
    let pushdown_evidence = search_projection_shadow_pushdown_evidence_json(bundle, path);
    let pushdown_ready = json_get_bool_path(&pushdown_evidence, &["ready"]);
    let blocker_codes =
        search_projection_shadow_blocker_codes_with_pushdown(bundle, path, &pushdown_evidence);
    let ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL)
        && evidence_source.as_deref() == Some(SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE)
        && primary_ready == Some(true)
        && shadow_ready == Some(true)
        && document_count_parity == Some(true)
        && document_identity_parity == Some(true)
        && table_parity_ready == Some(true)
        && embedding_identity_parity == Some(true)
        && lifecycle_parity == Some(true)
        && incremental_watermark_parity == Some(true)
        && predicate_pushdown_parity == Some(true)
        && pushdown_ready == Some(true);
    SearchProjectionShadowEvidenceSummary {
        protocol,
        evidence_source,
        present,
        ready,
        primary_ready,
        shadow_ready,
        document_count_parity,
        document_identity_parity,
        table_parity_ready,
        embedding_identity_parity,
        lifecycle_parity,
        incremental_watermark_parity,
        predicate_pushdown_parity,
        pushdown_evidence,
        primary_engine: json_get_str_path_from_dynamic(bundle, path, "primary_engine"),
        shadow_engine: json_get_str_path_from_dynamic(bundle, path, "shadow_engine"),
        blocker_codes,
    }
}

fn search_candidate_shadow_evidence_summary(
    bundle: &serde_json::Value,
) -> SearchCandidateShadowEvidenceSummary {
    let path = if json_get_path(bundle, &["search_candidate_shadow_evidence"]).is_some() {
        &["search_candidate_shadow_evidence"][..]
    } else {
        &["cutover_evidence", "search_candidate_shadow_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let evidence_source =
        json_get_str_path_from_dynamic(bundle, path, "evidence_source").map(str::to_string);
    let route = json_get_str_path_from_dynamic(bundle, path, "route").map(str::to_string);
    let candidate_primary_engine =
        json_get_str_path_from_dynamic(bundle, path, "candidate_primary_engine")
            .map(str::to_string);
    let primary_engine =
        json_get_str_path_from_dynamic(bundle, path, "primary_engine").map(str::to_string);
    let shadow_engine =
        json_get_str_path_from_dynamic(bundle, path, "shadow_engine").map(str::to_string);
    let request_count = json_get_u64_path_from_dynamic(bundle, path, "request_count");
    let primary_candidate_count =
        json_get_u64_path_from_dynamic(bundle, path, "primary_candidate_count");
    let shadow_candidate_count =
        json_get_u64_path_from_dynamic(bundle, path, "shadow_candidate_count");
    let matched_candidate_count =
        json_get_u64_path_from_dynamic(bundle, path, "matched_candidate_count");
    let primary_only_candidate_count =
        json_get_u64_path_from_dynamic(bundle, path, "primary_only_candidate_count");
    let candidate_counts_ready = request_count.is_some_and(|count| count > 0)
        && primary_candidate_count == shadow_candidate_count
        && matched_candidate_count == shadow_candidate_count
        && primary_only_candidate_count == Some(0);
    let candidate_identity_ready = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["candidate_identity", "ready"])
            .collect::<Vec<_>>(),
    );
    let candidate_identity_parity = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["candidate_identity", "parity"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_ready = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "ready"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_field_summary_count = json_get_u64_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "field_summary_count"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_missing_required_fields = json_get_string_array_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "missing_required_fields"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_missing_required_fields_present = json_get_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "missing_required_fields"])
            .collect::<Vec<_>>(),
    )
    .is_some_and(serde_json::Value::is_array);
    let filter_pushdown_field_capabilities_ready = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "field_capabilities_ready"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_missing_value_summary_fields = json_get_string_array_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "missing_value_summary_fields"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_missing_numeric_range_fields = json_get_string_array_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "missing_numeric_range_fields"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_missing_timestamp_range_fields = json_get_string_array_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["filter_pushdown", "missing_timestamp_range_fields"])
            .collect::<Vec<_>>(),
    );
    let filter_pushdown_field_capability_fields_present = [
        "field_capabilities_ready",
        "missing_value_summary_fields",
        "missing_numeric_range_fields",
        "missing_timestamp_range_fields",
    ]
    .iter()
    .all(|field| {
        json_get_path(
            bundle,
            &path
                .iter()
                .copied()
                .chain(["filter_pushdown", field])
                .collect::<Vec<_>>(),
        )
        .is_some()
    });

    let row_count_parity = json_get_bool_path_from_dynamic(bundle, path, "row_count_parity");
    let vector_top_k_overlap_ready =
        json_get_bool_path_from_dynamic(bundle, path, "vector_top_k_overlap_ready");
    let fts_top_k_overlap_ready =
        json_get_bool_path_from_dynamic(bundle, path, "fts_top_k_overlap_ready");
    let source_chunk_identity_ready = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["candidate_readiness", "source_chunk_identity_ready"])
            .collect::<Vec<_>>(),
    );
    let fail_soft_observed = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["candidate_readiness", "fail_soft_observed"])
            .collect::<Vec<_>>(),
    );
    let projection_marker_status_visible = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["candidate_readiness", "projection_marker_status_visible"])
            .collect::<Vec<_>>(),
    );
    let projection_watermark_ready = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["candidate_readiness", "projection_watermark_ready"])
            .collect::<Vec<_>>(),
    );
    let embedding_identity_ready = json_get_bool_path(
        bundle,
        &path
            .iter()
            .copied()
            .chain(["candidate_readiness", "embedding_identity_ready"])
            .collect::<Vec<_>>(),
    );
    let shadow_scan_filter_pushdown_ready =
        json_get_bool_path_from_dynamic(bundle, path, "shadow_scan_filter_pushdown_ready");
    let shadow_scan_field_pruning_ready =
        json_get_bool_path_from_dynamic(bundle, path, "shadow_scan_field_pruning_ready");
    let shadow_scan_field_summary_count =
        json_get_u64_path_from_dynamic(bundle, path, "shadow_scan_field_summary_count");
    let text_retriever_ready =
        json_get_bool_path_from_dynamic(bundle, path, "text_retriever_ready");
    let vector_retriever_ready =
        json_get_bool_path_from_dynamic(bundle, path, "vector_retriever_ready");
    let blocker_codes = json_get_array_path_from_dynamic(bundle, path, "blocker_codes");

    let bridge_ready = evidence_source.as_deref()
        == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE)
        && candidate_primary_engine.as_deref()
            == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE)
        && candidate_counts_ready
        && candidate_identity_ready == Some(true)
        && candidate_identity_parity == Some(true)
        && filter_pushdown_ready == Some(true)
        && filter_pushdown_field_summary_count.is_some_and(|count| count > 0)
        && filter_pushdown_missing_required_fields_present
        && filter_pushdown_missing_required_fields.is_empty()
        && filter_pushdown_field_capability_fields_present
        && filter_pushdown_field_capabilities_ready == Some(true)
        && filter_pushdown_missing_value_summary_fields.is_empty()
        && filter_pushdown_missing_numeric_range_fields.is_empty()
        && filter_pushdown_missing_timestamp_range_fields.is_empty()
        && row_count_parity == Some(true)
        && text_retriever_ready == Some(true)
        && vector_retriever_ready == Some(true)
        && fts_top_k_overlap_ready == Some(true)
        && vector_top_k_overlap_ready == Some(true)
        && source_chunk_identity_ready == Some(true)
        && fail_soft_observed == Some(true)
        && projection_marker_status_visible == Some(true)
        && projection_watermark_ready == Some(true)
        && embedding_identity_ready == Some(true)
        && shadow_scan_filter_pushdown_ready == Some(true)
        && shadow_scan_field_pruning_ready == Some(true)
        && shadow_scan_field_summary_count.is_some_and(|count| count > 0);
    let ready = present
        && protocol.as_deref() == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL)
        && route.as_deref() == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE)
        && bridge_ready;
    SearchCandidateShadowEvidenceSummary {
        protocol,
        evidence_source,
        route,
        present,
        ready,
        candidate_primary_engine,
        primary_engine,
        shadow_engine,
        request_count,
        primary_candidate_count,
        shadow_candidate_count,
        matched_candidate_count,
        primary_only_candidate_count,
        candidate_counts_ready,
        text_retriever_ready,
        vector_retriever_ready,
        candidate_identity_ready,
        candidate_identity_parity,
        row_count_parity,
        vector_top_k_overlap_ready,
        fts_top_k_overlap_ready,
        source_chunk_identity_ready,
        fail_soft_observed,
        projection_marker_status_visible,
        projection_watermark_ready,
        embedding_identity_ready,
        shadow_scan_filter_pushdown_ready,
        shadow_scan_field_pruning_ready,
        shadow_scan_field_summary_count,
        filter_pushdown_ready,
        filter_pushdown_field_summary_count,
        filter_pushdown_missing_required_fields,
        filter_pushdown_field_capabilities_ready,
        filter_pushdown_missing_value_summary_fields,
        filter_pushdown_missing_numeric_range_fields,
        filter_pushdown_missing_timestamp_range_fields,
        blocker_codes,
    }
}

fn search_candidate_shadow_evidence_summary_json(
    evidence: &SearchCandidateShadowEvidenceSummary,
) -> serde_json::Value {
    serde_json::json!({
        "protocol": evidence.protocol,
        "evidence_source": evidence.evidence_source,
        "route": evidence.route,
        "present": evidence.present,
        "ready": evidence.ready,
        "candidate_primary_engine": evidence.candidate_primary_engine,
        "primary_engine": evidence.primary_engine,
        "shadow_engine": evidence.shadow_engine,
        "request_count": evidence.request_count,
        "primary_candidate_count": evidence.primary_candidate_count,
        "shadow_candidate_count": evidence.shadow_candidate_count,
        "matched_candidate_count": evidence.matched_candidate_count,
        "primary_only_candidate_count": evidence.primary_only_candidate_count,
        "candidate_counts_ready": evidence.candidate_counts_ready,
        "text_retriever_ready": evidence.text_retriever_ready,
        "vector_retriever_ready": evidence.vector_retriever_ready,
        "candidate_identity_ready": evidence.candidate_identity_ready,
        "candidate_identity_parity": evidence.candidate_identity_parity,
        "row_count_parity": evidence.row_count_parity,
        "vector_top_k_overlap_ready": evidence.vector_top_k_overlap_ready,
        "fts_top_k_overlap_ready": evidence.fts_top_k_overlap_ready,
        "source_chunk_identity_ready": evidence.source_chunk_identity_ready,
        "fail_soft_observed": evidence.fail_soft_observed,
        "projection_marker_status_visible": evidence.projection_marker_status_visible,
        "projection_watermark_ready": evidence.projection_watermark_ready,
        "embedding_identity_ready": evidence.embedding_identity_ready,
        "shadow_scan_filter_pushdown_ready": evidence.shadow_scan_filter_pushdown_ready,
        "shadow_scan_field_pruning_ready": evidence.shadow_scan_field_pruning_ready,
        "shadow_scan_field_summary_count": evidence.shadow_scan_field_summary_count,
        "filter_pushdown_ready": evidence.filter_pushdown_ready,
        "filter_pushdown_field_summary_count": evidence.filter_pushdown_field_summary_count,
        "filter_pushdown_missing_required_fields": evidence.filter_pushdown_missing_required_fields,
        "filter_pushdown_field_capabilities_ready": evidence.filter_pushdown_field_capabilities_ready,
        "filter_pushdown_missing_value_summary_fields": evidence.filter_pushdown_missing_value_summary_fields,
        "filter_pushdown_missing_numeric_range_fields": evidence.filter_pushdown_missing_numeric_range_fields,
        "filter_pushdown_missing_timestamp_range_fields": evidence.filter_pushdown_missing_timestamp_range_fields,
        "blocker_codes": evidence.blocker_codes,
    })
}

fn search_route_ownership_summary(bundle: &serde_json::Value) -> SearchRouteOwnershipSummary {
    route_ownership_summary_at(
        bundle,
        "search_route_ownership",
        REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES,
    )
}

fn active_search_route_ownership_summary(
    bundle: &serde_json::Value,
) -> SearchRouteOwnershipSummary {
    route_ownership_summary_at(
        bundle,
        "active_search_route_ownership",
        REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES,
    )
}

fn active_search_route_readiness_summary(
    bundle: &serde_json::Value,
) -> ActiveSearchRouteReadinessSummary {
    let path = if json_get_path(bundle, &["active_search_route_readiness"]).is_some() {
        vec!["active_search_route_readiness"]
    } else {
        vec!["cutover_evidence", "active_search_route_readiness"]
    };
    let path_refs = path.to_vec();
    let present = json_get_path(bundle, &path_refs).is_some_and(|value| !value.is_null());
    let protocol =
        json_get_str_path_from_dynamic(bundle, &path_refs, "protocol").map(str::to_string);
    let production_cutover_ready =
        json_get_bool_path_from_dynamic(bundle, &path_refs, "production_cutover_ready");
    let require_all_skein =
        json_get_bool_path_from_dynamic(bundle, &path_refs, "require_all_skein");
    let required_route_count =
        json_get_u64_path_from_dynamic(bundle, &path_refs, "required_route_count");
    let evidence_route_count =
        json_get_u64_path_from_dynamic(bundle, &path_refs, "evidence_route_count");
    let ready_route_count = json_get_u64_path_from_dynamic(bundle, &path_refs, "ready_route_count");
    let skein_route_count = json_get_u64_path_from_dynamic(bundle, &path_refs, "skein_route_count");
    let lancedb_handle_required_route_count =
        json_get_u64_path_from_dynamic(bundle, &path_refs, "lancedb_handle_required_route_count");
    let missing_required_routes =
        json_get_string_array_path_from_dynamic(bundle, &path_refs, "missing_required_routes");
    let non_skein_routes =
        json_get_string_array_path_from_dynamic(bundle, &path_refs, "non_skein_routes");
    let lancedb_handle_required_routes = json_get_string_array_path_from_dynamic(
        bundle,
        &path_refs,
        "lancedb_handle_required_routes",
    );
    let candidate_not_ready_routes =
        json_get_string_array_path_from_dynamic(bundle, &path_refs, "candidate_not_ready_routes");
    let candidate_identity_not_ready_routes = json_get_string_array_path_from_dynamic(
        bundle,
        &path_refs,
        "candidate_identity_not_ready_routes",
    );
    let embedding_identity_not_ready_routes = json_get_string_array_path_from_dynamic(
        bundle,
        &path_refs,
        "embedding_identity_not_ready_routes",
    );
    let zero_vector_semantics_not_ready_routes = json_get_string_array_path_from_dynamic(
        bundle,
        &path_refs,
        "zero_vector_semantics_not_ready_routes",
    );
    let cjk_tokenization_not_ready_routes = json_get_string_array_path_from_dynamic(
        bundle,
        &path_refs,
        "cjk_tokenization_not_ready_routes",
    );
    let metadata_pushdown_not_ready_routes = json_get_string_array_path_from_dynamic(
        bundle,
        &path_refs,
        "metadata_pushdown_not_ready_routes",
    );
    let ranking_window_not_ready_routes = json_get_string_array_path_from_dynamic(
        bundle,
        &path_refs,
        "ranking_window_not_ready_routes",
    );
    let ranking_not_ready_routes =
        json_get_string_array_path_from_dynamic(bundle, &path_refs, "ranking_not_ready_routes");
    let fail_soft_not_ready_routes =
        json_get_string_array_path_from_dynamic(bundle, &path_refs, "fail_soft_not_ready_routes");
    let fail_soft_reason_codes_not_ready_routes = json_get_string_array_path_from_dynamic(
        bundle,
        &path_refs,
        "fail_soft_reason_codes_not_ready_routes",
    );
    let repair_rebuild_markers_not_ready_routes = json_get_string_array_path_from_dynamic(
        bundle,
        &path_refs,
        "repair_rebuild_markers_not_ready_routes",
    );
    let blocker_codes = json_get_array_path_from_dynamic(bundle, &path_refs, "blocker_codes");
    let expected_route_count = REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len() as u64;
    let ready = present
        && protocol.as_deref() == Some(NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL)
        && production_cutover_ready == Some(true)
        && require_all_skein == Some(true)
        && required_route_count == Some(expected_route_count)
        && evidence_route_count == Some(expected_route_count)
        && ready_route_count == Some(expected_route_count)
        && skein_route_count == Some(expected_route_count)
        && lancedb_handle_required_route_count == Some(0)
        && missing_required_routes.is_empty()
        && non_skein_routes.is_empty()
        && lancedb_handle_required_routes.is_empty()
        && candidate_not_ready_routes.is_empty()
        && candidate_identity_not_ready_routes.is_empty()
        && embedding_identity_not_ready_routes.is_empty()
        && zero_vector_semantics_not_ready_routes.is_empty()
        && cjk_tokenization_not_ready_routes.is_empty()
        && metadata_pushdown_not_ready_routes.is_empty()
        && ranking_window_not_ready_routes.is_empty()
        && ranking_not_ready_routes.is_empty()
        && fail_soft_not_ready_routes.is_empty()
        && fail_soft_reason_codes_not_ready_routes.is_empty()
        && repair_rebuild_markers_not_ready_routes.is_empty()
        && blocker_codes.as_array().is_some_and(Vec::is_empty);

    ActiveSearchRouteReadinessSummary {
        protocol,
        present,
        ready,
        production_cutover_ready,
        require_all_skein,
        required_route_count,
        evidence_route_count,
        ready_route_count,
        skein_route_count,
        lancedb_handle_required_route_count,
        missing_required_routes,
        non_skein_routes,
        lancedb_handle_required_routes,
        candidate_not_ready_routes,
        candidate_identity_not_ready_routes,
        embedding_identity_not_ready_routes,
        zero_vector_semantics_not_ready_routes,
        cjk_tokenization_not_ready_routes,
        metadata_pushdown_not_ready_routes,
        ranking_window_not_ready_routes,
        ranking_not_ready_routes,
        fail_soft_not_ready_routes,
        fail_soft_reason_codes_not_ready_routes,
        repair_rebuild_markers_not_ready_routes,
        blocker_codes,
    }
}

fn route_ownership_summary_at(
    bundle: &serde_json::Value,
    field: &str,
    required_routes: &[&str],
) -> SearchRouteOwnershipSummary {
    let path = if json_get_path(bundle, &[field]).is_some() {
        vec![field]
    } else {
        vec!["cutover_evidence", field]
    };
    let path_refs = path.to_vec();
    let present = json_get_path(bundle, &path_refs).is_some_and(|value| !value.is_null());
    let protocol =
        json_get_str_path_from_dynamic(bundle, &path_refs, "protocol").map(str::to_string);
    let production_cutover_ready =
        json_get_bool_path_from_dynamic(bundle, &path_refs, "production_cutover_ready");
    let require_all_skein =
        json_get_bool_path_from_dynamic(bundle, &path_refs, "require_all_skein");
    let required_route_count =
        json_get_u64_path_from_dynamic(bundle, &path_refs, "required_route_count");
    let explicit_route_count =
        json_get_u64_path_from_dynamic(bundle, &path_refs, "explicit_route_count");
    let skein_route_count = json_get_u64_path_from_dynamic(bundle, &path_refs, "skein_route_count");
    let lancedb_route_count =
        json_get_u64_path_from_dynamic(bundle, &path_refs, "lancedb_route_count");
    let missing_required_routes =
        json_get_string_array_path_from_dynamic(bundle, &path_refs, "missing_required_routes");
    let lancedb_routes =
        json_get_string_array_path_from_dynamic(bundle, &path_refs, "lancedb_routes");
    let blocker_codes = json_get_array_path_from_dynamic(bundle, &path_refs, "blocker_codes");
    let expected_route_count = required_routes.len() as u64;
    let ready = present
        && protocol.as_deref() == Some(NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL)
        && production_cutover_ready == Some(true)
        && require_all_skein == Some(true)
        && required_route_count == Some(expected_route_count)
        && explicit_route_count == Some(expected_route_count)
        && skein_route_count == Some(expected_route_count)
        && lancedb_route_count == Some(0)
        && missing_required_routes.is_empty()
        && lancedb_routes.is_empty()
        && blocker_codes.as_array().is_some_and(Vec::is_empty);

    SearchRouteOwnershipSummary {
        protocol,
        present,
        ready,
        production_cutover_ready,
        require_all_skein,
        required_route_count,
        explicit_route_count,
        skein_route_count,
        lancedb_route_count,
        missing_required_routes,
        lancedb_routes,
        blocker_codes,
    }
}

fn search_projection_shadow_pushdown_evidence_json(
    bundle: &serde_json::Value,
    path: &[&str],
) -> serde_json::Value {
    let mut pushdown_path = path.to_vec();
    pushdown_path.push("pushdown_evidence");
    if let Some(pushdown) = json_get_path(bundle, &pushdown_path).filter(|value| value.is_object())
    {
        return search_projection_shadow_pushdown_evidence_with_recomputed_scan_fields(
            pushdown.clone(),
        );
    }

    let predicate_pushdown_parity =
        json_get_bool_path_from_dynamic(bundle, path, "predicate_pushdown_parity").unwrap_or(false);
    let primary_predicate_pushdown_ready = {
        let mut nested = path.to_vec();
        nested.extend(["primary_evidence", "predicate_pushdown_ready"]);
        json_get_bool_path(bundle, &nested)
    }
    .unwrap_or(false);
    let shadow_predicate_pushdown_ready = {
        let mut nested = path.to_vec();
        nested.extend(["shadow_evidence", "predicate_pushdown_ready"]);
        json_get_bool_path(bundle, &nested)
    }
    .unwrap_or(false);
    let shadow_persisted_segment_descriptor_ready = {
        let mut nested = path.to_vec();
        nested.extend([
            "shadow_evidence",
            "predicate_pushdown",
            "persisted_segment_descriptor_ready",
        ]);
        json_get_bool_path(bundle, &nested).unwrap_or(false)
    };
    let reported_shadow_segment_descriptor_scan_filter_fields_ready = {
        let mut nested = path.to_vec();
        nested.extend([
            "shadow_evidence",
            "predicate_pushdown",
            "segment_descriptor_scan_filter_fields_ready",
        ]);
        json_get_bool_path(bundle, &nested).unwrap_or(false)
    };
    let primary_scan_filter_fields = {
        let mut nested = path.to_vec();
        nested.extend([
            "primary_evidence",
            "predicate_pushdown",
            "scan_filter_fields",
        ]);
        json_get_string_array_path(bundle, &nested)
    };
    let shadow_scan_filter_fields = {
        let mut nested = path.to_vec();
        nested.extend([
            "shadow_evidence",
            "predicate_pushdown",
            "scan_filter_fields",
        ]);
        json_get_string_array_path(bundle, &nested)
    };
    let shadow_segment_descriptor_field_summaries = {
        let mut nested = path.to_vec();
        nested.extend([
            "shadow_evidence",
            "predicate_pushdown",
            "segment_descriptor_field_summaries",
        ]);
        json_get_path(bundle, &nested)
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]))
    };
    let shadow_segment_descriptor_scan_filter_fields_ready =
        reported_shadow_segment_descriptor_scan_filter_fields_ready
            && scan_filter_fields_cover_required(&primary_scan_filter_fields)
            && scan_filter_fields_cover_required(&shadow_scan_filter_fields)
            && segment_descriptor_summaries_cover_required(
                &shadow_segment_descriptor_field_summaries,
            );
    let shadow_segment_descriptor_capabilities =
        segment_descriptor_capability_report(&shadow_segment_descriptor_field_summaries);
    let shadow_segment_document_pruning = segment_document_pruning_report_from_path(
        bundle,
        path,
        &["shadow_evidence", "predicate_pushdown"],
    );
    let ready = predicate_pushdown_parity
        && primary_predicate_pushdown_ready
        && shadow_predicate_pushdown_ready
        && shadow_persisted_segment_descriptor_ready
        && shadow_segment_descriptor_scan_filter_fields_ready
        && shadow_segment_document_pruning.ready;
    serde_json::json!({
        "ready": ready,
        "predicate_pushdown_parity": predicate_pushdown_parity,
        "primary_predicate_pushdown_ready": primary_predicate_pushdown_ready,
        "shadow_predicate_pushdown_ready": shadow_predicate_pushdown_ready,
        "shadow_persisted_segment_descriptor_ready": shadow_persisted_segment_descriptor_ready,
        "shadow_segment_descriptor_scan_filter_fields_ready": shadow_segment_descriptor_scan_filter_fields_ready,
        "shadow_segment_descriptor_capabilities_ready": shadow_segment_descriptor_capabilities.ready,
        "shadow_segment_document_pruning_ready": shadow_segment_document_pruning.ready,
        "shadow_segment_pruning_candidate_document_count": shadow_segment_document_pruning.candidate_document_count,
        "shadow_segment_pruned_document_count": shadow_segment_document_pruning.pruned_document_count,
        "shadow_segment_scanned_document_count": shadow_segment_document_pruning.scanned_document_count,
        "missing_value_summary_fields": shadow_segment_descriptor_capabilities.missing_value_summary_fields,
        "missing_numeric_range_fields": shadow_segment_descriptor_capabilities.missing_numeric_range_fields,
        "missing_timestamp_range_fields": shadow_segment_descriptor_capabilities.missing_timestamp_range_fields,
        "primary_scan_filter_fields": primary_scan_filter_fields,
        "shadow_scan_filter_fields": shadow_scan_filter_fields,
        "shadow_segment_descriptor_field_summaries": shadow_segment_descriptor_field_summaries,
    })
}

fn search_projection_shadow_pushdown_evidence_with_recomputed_scan_fields(
    pushdown: serde_json::Value,
) -> serde_json::Value {
    let primary_scan_filter_fields =
        json_get_string_array_path(&pushdown, &["primary_scan_filter_fields"]);
    let shadow_scan_filter_fields =
        json_get_string_array_path(&pushdown, &["shadow_scan_filter_fields"]);
    let shadow_segment_descriptor_field_summaries =
        json_get_path(&pushdown, &["shadow_segment_descriptor_field_summaries"])
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));
    let scan_filter_fields_ready = scan_filter_fields_cover_required(&primary_scan_filter_fields)
        && scan_filter_fields_cover_required(&shadow_scan_filter_fields)
        && segment_descriptor_summaries_cover_required(&shadow_segment_descriptor_field_summaries);
    let reported_descriptor_fields_ready = json_get_bool_path(
        &pushdown,
        &["shadow_segment_descriptor_scan_filter_fields_ready"],
    ) == Some(true);
    let descriptor_fields_ready = reported_descriptor_fields_ready && scan_filter_fields_ready;
    let descriptor_capabilities =
        segment_descriptor_capability_report(&shadow_segment_descriptor_field_summaries);
    let segment_document_pruning = segment_document_pruning_report(&pushdown);
    let ready = json_get_bool_path(&pushdown, &["ready"]) == Some(true)
        && descriptor_fields_ready
        && segment_document_pruning.ready;

    let mut object = pushdown.as_object().cloned().unwrap_or_default();
    object.insert("ready".to_string(), serde_json::json!(ready));
    object.insert(
        "required_scan_filter_fields".to_string(),
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS),
    );
    object.insert(
        "shadow_segment_descriptor_scan_filter_fields_ready".to_string(),
        serde_json::json!(descriptor_fields_ready),
    );
    object.insert(
        "scan_filter_fields_ready".to_string(),
        serde_json::json!(scan_filter_fields_ready),
    );
    object.insert(
        "shadow_segment_descriptor_capabilities_ready".to_string(),
        serde_json::json!(descriptor_capabilities.ready),
    );
    object.insert(
        "shadow_segment_document_pruning_ready".to_string(),
        serde_json::json!(segment_document_pruning.ready),
    );
    object.insert(
        "shadow_segment_pruning_candidate_document_count".to_string(),
        serde_json::json!(segment_document_pruning.candidate_document_count),
    );
    object.insert(
        "shadow_segment_pruned_document_count".to_string(),
        serde_json::json!(segment_document_pruning.pruned_document_count),
    );
    object.insert(
        "shadow_segment_scanned_document_count".to_string(),
        serde_json::json!(segment_document_pruning.scanned_document_count),
    );
    object.insert(
        "missing_value_summary_fields".to_string(),
        serde_json::json!(descriptor_capabilities.missing_value_summary_fields),
    );
    object.insert(
        "missing_numeric_range_fields".to_string(),
        serde_json::json!(descriptor_capabilities.missing_numeric_range_fields),
    );
    object.insert(
        "missing_timestamp_range_fields".to_string(),
        serde_json::json!(descriptor_capabilities.missing_timestamp_range_fields),
    );
    serde_json::Value::Object(object)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SegmentDocumentPruningReport {
    ready: bool,
    candidate_document_count: Option<u64>,
    pruned_document_count: Option<u64>,
    scanned_document_count: Option<u64>,
}

fn segment_document_pruning_report(pushdown: &serde_json::Value) -> SegmentDocumentPruningReport {
    let candidate_document_count = json_get_u64_path(
        pushdown,
        &["shadow_segment_pruning_candidate_document_count"],
    );
    let pruned_document_count =
        json_get_u64_path(pushdown, &["shadow_segment_pruned_document_count"]);
    let scanned_document_count =
        json_get_u64_path(pushdown, &["shadow_segment_scanned_document_count"]);
    segment_document_pruning_report_from_counts(
        candidate_document_count,
        pruned_document_count,
        scanned_document_count,
    )
}

fn segment_document_pruning_report_from_path(
    bundle: &serde_json::Value,
    base_path: &[&str],
    relative_path: &[&str],
) -> SegmentDocumentPruningReport {
    let mut candidate_path = base_path.to_vec();
    candidate_path.extend(relative_path);
    candidate_path.push("segment_pruning_candidate_document_count");
    let mut pruned_path = base_path.to_vec();
    pruned_path.extend(relative_path);
    pruned_path.push("segment_pruned_document_count");
    let mut scanned_path = base_path.to_vec();
    scanned_path.extend(relative_path);
    scanned_path.push("segment_scanned_document_count");
    segment_document_pruning_report_from_counts(
        json_get_u64_path(bundle, &candidate_path),
        json_get_u64_path(bundle, &pruned_path),
        json_get_u64_path(bundle, &scanned_path),
    )
}

fn segment_document_pruning_report_from_counts(
    candidate_document_count: Option<u64>,
    pruned_document_count: Option<u64>,
    scanned_document_count: Option<u64>,
) -> SegmentDocumentPruningReport {
    let ready = matches!(
        (candidate_document_count, pruned_document_count, scanned_document_count),
        (Some(candidate), Some(pruned), Some(scanned))
            if candidate > 0
                && pruned > 0
                && scanned > 0
                && pruned.checked_add(scanned) == Some(candidate)
    );
    SegmentDocumentPruningReport {
        ready,
        candidate_document_count,
        pruned_document_count,
        scanned_document_count,
    }
}

fn scan_filter_fields_cover_required(scan_filter_fields: &[String]) -> bool {
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .all(|required| scan_filter_fields.iter().any(|field| field == required))
}

fn segment_descriptor_summaries_cover_required(
    segment_descriptor_field_summaries: &serde_json::Value,
) -> bool {
    let Some(summaries) = segment_descriptor_field_summaries.as_array() else {
        return false;
    };
    if summaries.is_empty() {
        return false;
    }
    let fields = summaries
        .iter()
        .filter_map(|summary| json_get_str_path(summary, &["field"]))
        .collect::<BTreeSet<_>>();
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .all(|required| fields.contains(required))
        && segment_descriptor_capability_report(segment_descriptor_field_summaries).ready
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SegmentDescriptorCapabilityReport {
    ready: bool,
    missing_value_summary_fields: Vec<&'static str>,
    missing_numeric_range_fields: Vec<&'static str>,
    missing_timestamp_range_fields: Vec<&'static str>,
}

fn segment_descriptor_capability_report(
    segment_descriptor_field_summaries: &serde_json::Value,
) -> SegmentDescriptorCapabilityReport {
    let summaries = segment_descriptor_field_summaries
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let missing_value_summary_fields = required_capability_missing_fields(
        summaries,
        REQUIRED_SEARCH_VALUE_SUMMARY_FIELDS,
        "value_summary_used",
        None,
    );
    let missing_numeric_range_fields = required_capability_missing_fields(
        summaries,
        REQUIRED_SEARCH_NUMERIC_RANGE_FIELDS,
        "numeric_range_summary_used",
        None,
    );
    let missing_timestamp_range_fields = required_capability_missing_fields(
        summaries,
        REQUIRED_SEARCH_TIMESTAMP_RANGE_FIELDS,
        "timestamp_range_summary_used",
        Some("numeric_range_summary_used"),
    );
    SegmentDescriptorCapabilityReport {
        ready: missing_value_summary_fields.is_empty()
            && missing_numeric_range_fields.is_empty()
            && missing_timestamp_range_fields.is_empty(),
        missing_value_summary_fields,
        missing_numeric_range_fields,
        missing_timestamp_range_fields,
    }
}

fn required_capability_missing_fields(
    summaries: &[serde_json::Value],
    required_fields: &'static [&'static str],
    capability: &str,
    alternative_capability: Option<&str>,
) -> Vec<&'static str> {
    required_fields
        .iter()
        .copied()
        .filter(|field| {
            !summaries.iter().any(|summary| {
                json_get_str_path(summary, &["field"]) == Some(*field)
                    && segment_descriptor_summary_base_ready(summary)
                    && (segment_descriptor_summary_capability_ready(summary, capability)
                        || alternative_capability.is_some_and(|capability| {
                            segment_descriptor_summary_capability_ready(summary, capability)
                        }))
            })
        })
        .collect()
}

fn segment_descriptor_summary_base_ready(summary: &serde_json::Value) -> bool {
    json_get_u64_path(summary, &["segment_count"]).is_some_and(|count| count > 0)
        && json_get_u64_path(summary, &["present_document_count"]).is_some()
}

fn segment_descriptor_summary_capability_ready(
    summary: &serde_json::Value,
    capability: &str,
) -> bool {
    json_get_bool_path(summary, &[capability]) == Some(true)
        && segment_descriptor_summary_capability_count(summary, capability)
            .is_some_and(|count| count > 0)
}

fn segment_descriptor_summary_capability_count(
    summary: &serde_json::Value,
    capability: &str,
) -> Option<u64> {
    match capability {
        "value_summary_used" => json_get_u64_path(summary, &["value_summary_segment_count"]),
        "numeric_range_summary_used" => {
            json_get_u64_path(summary, &["numeric_range_segment_count"])
        }
        "timestamp_range_summary_used" => {
            json_get_u64_path(summary, &["timestamp_range_segment_count"])
        }
        _ => None,
    }
}

fn search_projection_shadow_blocker_codes_with_pushdown(
    bundle: &serde_json::Value,
    path: &[&str],
    pushdown_evidence: &serde_json::Value,
) -> serde_json::Value {
    let mut blockers = json_get_string_array_path_from_dynamic(bundle, path, "blocker_codes")
        .into_iter()
        .collect::<BTreeSet<_>>();
    if json_get_bool_path(pushdown_evidence, &["ready"]) != Some(true) {
        blockers.insert(SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY.to_string());
    }
    if json_get_bool_path(
        pushdown_evidence,
        &["shadow_persisted_segment_descriptor_ready"],
    ) != Some(true)
    {
        blockers.insert(SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING.to_string());
    }
    if json_get_bool_path(
        pushdown_evidence,
        &["shadow_segment_descriptor_scan_filter_fields_ready"],
    ) != Some(true)
    {
        blockers.insert(SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_FIELDS_MISSING.to_string());
    }
    serde_json::json!(blockers.into_iter().collect::<Vec<_>>())
}

fn bounded_read_evidence_summary(bundle: &serde_json::Value) -> BoundedReadEvidenceSummary<'_> {
    let path = if json_get_path(bundle, &["bounded_read_evidence"]).is_some() {
        &["bounded_read_evidence"][..]
    } else {
        &["cutover_evidence", "bounded_read_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let mode = json_get_str_path_from_dynamic(bundle, path, "mode");
    let max_rows = json_get_u64_path_from_dynamic(bundle, path, "max_rows");
    let execution_row_cap = json_get_u64_path_from_dynamic(bundle, path, "execution_row_cap");
    let estimated_payload_bytes =
        json_get_u64_path_from_dynamic(bundle, path, "estimated_payload_bytes");
    let max_estimated_payload_bytes =
        json_get_u64_path_from_dynamic(bundle, path, "max_estimated_payload_bytes");
    let payload_budget_exceeded =
        json_get_bool_path_from_dynamic(bundle, path, "payload_budget_exceeded");
    let row_limit_enforced_before_output =
        json_get_bool_path_from_dynamic(bundle, path, "row_limit_enforced_before_output");
    let operator_row_cap_enabled =
        json_get_bool_path_from_dynamic(bundle, path, "operator_row_cap_enabled");
    let streaming = json_get_bool_path_from_dynamic(bundle, path, "streaming");
    let blocking_operator_count =
        json_get_u64_path_from_dynamic(bundle, path, "blocking_operator_count");
    let blocking_operator_memory_reports_complete =
        json_get_bool_path_from_dynamic(bundle, path, "blocking_operator_memory_reports_complete");
    let blocking_operator_memory_within_budget =
        json_get_bool_path_from_dynamic(bundle, path, "blocking_operator_memory_within_budget");
    let spill_within_budget = json_get_bool_path_from_dynamic(bundle, path, "spill_within_budget");
    let covered_routes = json_get_string_array_path_from_dynamic(bundle, path, "covered_routes");
    let primary_ready_routes =
        json_get_string_array_path_from_dynamic(bundle, path, "primary_ready_routes");
    let primary_ready_route_set = primary_ready_routes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let covered_route_set = covered_routes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let missing_covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !covered_route_set.contains(route))
        .collect::<Vec<_>>();
    let route_primary_ready = json_get_bool_path_from_dynamic(bundle, path, "route_primary_ready");
    let route_query_plan_evidence_ready =
        json_get_bool_path_from_dynamic(bundle, path, "route_query_plan_evidence_ready");
    let route_query_profile_evidence_ready =
        json_get_bool_path_from_dynamic(bundle, path, "route_query_profile_evidence_ready");
    let route_query_api_behavior_evidence_ready =
        json_get_bool_path_from_dynamic(bundle, path, "route_query_api_behavior_evidence_ready");
    let relationship_property_pruning_required_count = json_get_u64_path_from_dynamic(
        bundle,
        path,
        "relationship_property_pruning_required_count",
    );
    let relationship_property_pruning_report_count =
        json_get_u64_path_from_dynamic(bundle, path, "relationship_property_pruning_report_count");
    let route_relationship_property_pruning_evidence_ready = json_get_bool_path_from_dynamic(
        bundle,
        path,
        "route_relationship_property_pruning_evidence_ready",
    );
    let primary_ready_routes_cover_required = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .all(|route| primary_ready_route_set.contains(route));
    let relationship_property_pruning_counts_match =
        relationship_property_pruning_required_count == relationship_property_pruning_report_count;
    let ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL)
        && mode == Some("shadow_read_only")
        && max_rows.is_some_and(|value| value > 0)
        && execution_row_cap == max_rows.and_then(|value| value.checked_add(1))
        && estimated_payload_bytes.is_some()
        && max_estimated_payload_bytes.is_some_and(|value| value > 0)
        && payload_budget_exceeded == Some(false)
        && row_limit_enforced_before_output == Some(true)
        && operator_row_cap_enabled == Some(true)
        && streaming.is_some()
        && blocking_operator_memory_reports_complete == Some(true)
        && blocking_operator_memory_within_budget == Some(true)
        && spill_within_budget == Some(true)
        && missing_covered_routes.is_empty()
        && route_primary_ready == Some(true)
        && primary_ready_routes_cover_required
        && route_query_plan_evidence_ready == Some(true)
        && route_query_profile_evidence_ready == Some(true)
        && route_query_api_behavior_evidence_ready == Some(true)
        && route_relationship_property_pruning_evidence_ready == Some(true)
        && relationship_property_pruning_required_count.is_some()
        && relationship_property_pruning_counts_match;
    BoundedReadEvidenceSummary {
        protocol,
        present,
        ready,
        mode,
        max_rows,
        execution_row_cap,
        estimated_payload_bytes,
        max_estimated_payload_bytes,
        payload_budget_exceeded,
        row_limit_enforced_before_output,
        operator_row_cap_enabled,
        streaming,
        blocking_operator_count,
        blocking_operator_memory_reports_complete,
        blocking_operator_memory_within_budget,
        spill_within_budget,
        covered_routes,
        missing_covered_routes,
        route_primary_ready,
        primary_ready_routes,
        route_query_plan_evidence_ready,
        route_query_profile_evidence_ready,
        route_query_api_behavior_evidence_ready,
        relationship_property_pruning_required_count,
        relationship_property_pruning_report_count,
        route_relationship_property_pruning_evidence_ready,
        blocker_codes: json_get_array_path_from_dynamic(bundle, path, "blocker_codes"),
    }
}

fn query_runtime_preflight_summary(bundle: &serde_json::Value) -> QueryRuntimePreflightSummary {
    let path = if json_get_path(bundle, &["query_runtime_preflight"]).is_some() {
        &["query_runtime_preflight"][..]
    } else {
        &["cutover_evidence", "query_runtime_preflight"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let database_opened = json_get_bool_path_from_dynamic(bundle, path, "database_opened");
    let probe_count = json_get_u64_path_from_dynamic(bundle, path, "probe_count");
    let passed_probe_count = json_get_u64_path_from_dynamic(bundle, path, "passed_probe_count");
    let failed_probe_count = json_get_u64_path_from_dynamic(bundle, path, "failed_probe_count");
    let required_route_count = json_get_u64_path_from_dynamic(bundle, path, "required_route_count");
    let covered_route_count = json_get_u64_path_from_dynamic(bundle, path, "covered_route_count");
    let required_routes_covered =
        json_get_bool_path_from_dynamic(bundle, path, "required_routes_covered");
    let probe_routes = query_runtime_preflight_probe_routes(bundle, path);
    let probe_route_set = query_runtime_preflight_probe_route_set(bundle, path);
    let covered_routes = probe_route_set
        .iter()
        .map(String::to_string)
        .collect::<Vec<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !probe_route_set.contains(*route))
        .collect::<Vec<_>>();
    let unknown_routes = probe_route_set
        .iter()
        .filter(|route| !REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(&route.as_str()))
        .map(String::to_string)
        .collect::<Vec<_>>();
    let duplicate_routes = duplicate_probe_routes(&probe_routes);
    let required_route_len = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64;
    let route_coverage_ready = required_route_count == Some(required_route_len)
        && covered_route_count == Some(required_route_len)
        && required_routes_covered == Some(true)
        && missing_required_routes.is_empty()
        && unknown_routes.is_empty()
        && duplicate_routes.is_empty()
        && json_get_bool_path_from_dynamic(bundle, path, "route_coverage_ready")
            .is_none_or(|ready| ready);
    let probe_details_ready = query_runtime_preflight_probe_details_ready(bundle, path);
    let ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL)
        && database_opened == Some(true)
        && probe_count.is_some_and(|count| count > 0)
        && passed_probe_count == probe_count
        && failed_probe_count == Some(0)
        && route_coverage_ready
        && probe_details_ready
        && json_get_bool_path_from_dynamic(bundle, path, "ready") == Some(true);
    QueryRuntimePreflightSummary {
        protocol,
        present,
        ready,
        database_opened,
        probe_count,
        passed_probe_count,
        failed_probe_count,
        required_route_count,
        covered_route_count,
        covered_routes,
        missing_required_routes,
        unknown_routes,
        duplicate_routes,
        required_routes_covered,
        route_coverage_ready,
        probe_details_ready,
        blocker_codes: json_get_array_path_from_dynamic(bundle, path, "blocker_codes"),
    }
}

fn workload_fixture_evidence_summary(bundle: &serde_json::Value) -> WorkloadFixtureEvidenceSummary {
    let path = if json_get_path(bundle, &["workload_fixture_evidence"]).is_some() {
        &["workload_fixture_evidence"][..]
    } else {
        &["cutover_evidence", "workload_fixture_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let route_count = json_get_u64_path_from_dynamic(bundle, path, "route_count");
    let query_count = json_get_u64_path_from_dynamic(bundle, path, "query_count");
    let failed_query_count = json_get_u64_path_from_dynamic(bundle, path, "failed_query_count");
    let bounded_expansion_probe_count =
        json_get_u64_path_from_dynamic(bundle, path, "bounded_expansion_probe_count");
    let failed_bounded_expansion_probe_count =
        json_get_u64_path_from_dynamic(bundle, path, "failed_bounded_expansion_probe_count");
    let search_metadata_probe_count =
        json_get_u64_path_from_dynamic(bundle, path, "search_metadata_probe_count");
    let failed_search_metadata_probe_count =
        json_get_u64_path_from_dynamic(bundle, path, "failed_search_metadata_probe_count");
    let graph_rag_probe_count =
        json_get_u64_path_from_dynamic(bundle, path, "graph_rag_probe_count");
    let failed_graph_rag_probe_count =
        json_get_u64_path_from_dynamic(bundle, path, "failed_graph_rag_probe_count");
    let graph_rag_reports = json_get_array_path_from_dynamic(bundle, path, "graph_rag_reports");
    let graph_rag_ready = graph_rag_probe_count.is_some_and(|count| count > 0)
        && failed_graph_rag_probe_count == Some(0)
        && graph_rag_reports
            .as_array()
            .is_some_and(|reports| reports.iter().any(graph_rag_workload_report_ready));
    let source_projection_probe_count =
        json_get_u64_path_from_dynamic(bundle, path, "source_projection_probe_count");
    let failed_source_projection_probe_count =
        json_get_u64_path_from_dynamic(bundle, path, "failed_source_projection_probe_count");
    let source_projection_reports =
        json_get_array_path_from_dynamic(bundle, path, "source_projection_reports");
    let source_projection_ready = source_projection_probe_count.is_some_and(|count| count > 0)
        && failed_source_projection_probe_count == Some(0)
        && source_projection_reports
            .as_array()
            .is_some_and(|reports| reports.iter().any(source_projection_workload_report_ready));
    let ready = present
        && protocol.as_deref() == Some(NOWLEDGE_GRAPH_ROUTE_WORKLOAD_FIXTURE_PROTOCOL)
        && json_get_bool_path_from_dynamic(bundle, path, "ready") == Some(true)
        && route_count.is_some_and(|count| count > 0)
        && query_count.is_some_and(|count| count > 0)
        && failed_query_count == Some(0)
        && bounded_expansion_probe_count.is_some_and(|count| count > 0)
        && failed_bounded_expansion_probe_count == Some(0)
        && search_metadata_probe_count.is_some_and(|count| count > 0)
        && failed_search_metadata_probe_count == Some(0)
        && graph_rag_ready
        && source_projection_ready;
    WorkloadFixtureEvidenceSummary {
        protocol,
        present,
        ready,
        route_count,
        query_count,
        failed_query_count,
        bounded_expansion_probe_count,
        failed_bounded_expansion_probe_count,
        search_metadata_probe_count,
        failed_search_metadata_probe_count,
        graph_rag_probe_count,
        failed_graph_rag_probe_count,
        graph_rag_reports,
        source_projection_probe_count,
        failed_source_projection_probe_count,
        source_projection_reports,
        blocker_codes: json_get_array_path_from_dynamic(bundle, path, "blocker_codes"),
    }
}

fn source_mutation_dual_write_readiness_summary(
    bundle: &serde_json::Value,
) -> SourceMutationDualWriteReadinessSummary {
    let path = if json_get_path(bundle, &["source_mutation_dual_write_readiness"]).is_some() {
        &["source_mutation_dual_write_readiness"][..]
    } else {
        &["cutover_evidence", "source_mutation_dual_write_readiness"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let required_family_count =
        json_get_u64_path_from_dynamic(bundle, path, "required_family_count");
    let evidence_family_count =
        json_get_u64_path_from_dynamic(bundle, path, "evidence_family_count");
    let ready_family_count = json_get_u64_path_from_dynamic(bundle, path, "ready_family_count");
    let missing_required_families =
        json_get_string_array_path_from_dynamic(bundle, path, "missing_required_families");
    let unknown_families =
        json_get_string_array_path_from_dynamic(bundle, path, "unknown_families");
    let duplicate_families =
        json_get_string_array_path_from_dynamic(bundle, path, "duplicate_families");
    let blocker_codes = json_get_array_path_from_dynamic(bundle, path, "blocker_codes");
    let required_count = REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len() as u64;
    let ready = present
        && protocol.as_deref() == Some(NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL)
        && json_get_bool_path_from_dynamic(bundle, path, "ready") == Some(true)
        && required_family_count == Some(required_count)
        && evidence_family_count == Some(required_count)
        && ready_family_count == Some(required_count)
        && missing_required_families.is_empty()
        && unknown_families.is_empty()
        && duplicate_families.is_empty()
        && blocker_codes
            .as_array()
            .is_some_and(|blockers| blockers.is_empty());
    SourceMutationDualWriteReadinessSummary {
        protocol,
        present,
        ready,
        required_family_count,
        evidence_family_count,
        ready_family_count,
        missing_required_families,
        unknown_families,
        duplicate_families,
        blocker_codes,
    }
}

fn graph_rag_workload_report_ready(report: &serde_json::Value) -> bool {
    json_get_bool_path(report, &["ready"]) == Some(true)
        && json_get_str_path(report, &["schema_protocol"])
            == Some(GRAPH_RAG_SCHEMA_CONTEXT_PROTOCOL)
        && json_get_u64_path(report, &["label_count"]).is_some_and(|count| count > 0)
        && json_get_u64_path(report, &["relationship_type_count"]).is_some_and(|count| count > 0)
        && json_get_u64_path(report, &["route_count"]).is_some_and(|count| count > 0)
        && json_get_u64_path(report, &["parameter_requirement_count"])
            .is_some_and(|count| count > 0)
        && json_get_u64_path(report, &["row_count"]).is_some_and(|count| count > 0)
        && json_get_bool_path(report, &["row_budget_exceeded"]) == Some(false)
        && json_get_bool_path(report, &["payload_budget_exceeded"]) == Some(false)
        && json_get_u64_path(report, &["blocking_operator_count"]) == Some(0)
        && json_get_bool_path(report, &["streaming"]) == Some(false)
        && json_get_str_path(report, &["error_class"]).is_none()
}

fn source_projection_workload_report_ready(report: &serde_json::Value) -> bool {
    json_get_bool_path(report, &["ready"]) == Some(true)
        && json_get_bool_path(report, &["too_small_batch_failed_closed"]) == Some(true)
        && json_get_u64_path(report, &["operation_count"]) == Some(2)
        && json_get_u64_path(report, &["upserted_documents"]) == Some(2)
        && json_get_u64_path(report, &["deleted_documents"]) == Some(0)
        && json_get_u64_path(report, &["source_document_count"]) == Some(2)
        && json_get_bool_path(report, &["indexed_source_document_ready"]) == Some(true)
        && json_get_u64_path(report, &["source_graph_commit_epoch"]).is_some()
        && json_get_u64_path(report, &["complete_through_graph_commit_epoch"]).is_some()
        && json_get_str_path(report, &["error_class"]).is_none()
}

fn query_runtime_preflight_probe_route_set(
    bundle: &serde_json::Value,
    path: &[&str],
) -> BTreeSet<String> {
    query_runtime_preflight_probe_routes(bundle, path)
        .into_iter()
        .collect()
}

fn query_runtime_preflight_probe_routes(bundle: &serde_json::Value, path: &[&str]) -> Vec<String> {
    json_get_path_from_dynamic(bundle, path, "probes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|probe| {
            probe
                .get("route")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect()
}

fn duplicate_probe_routes(routes: &[String]) -> Vec<String> {
    let mut counts = BTreeMap::<&str, usize>::new();
    for route in routes {
        *counts.entry(route.as_str()).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(route, _)| route.to_string())
        .collect()
}

fn query_runtime_preflight_probe_details_ready(bundle: &serde_json::Value, path: &[&str]) -> bool {
    let Some(probes) =
        json_get_path_from_dynamic(bundle, path, "probes").and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    !probes.is_empty() && probes.iter().all(query_runtime_preflight_probe_ready)
}

fn query_runtime_preflight_probe_ready(probe: &serde_json::Value) -> bool {
    probe.get("ready").and_then(serde_json::Value::as_bool) == Some(true)
        && probe.get("success").and_then(serde_json::Value::as_bool) == Some(true)
        && probe
            .get("selected_plan_fingerprint")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|fingerprint| !fingerprint.is_empty())
        && probe
            .get("physical_operator_count")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|count| count > 0)
        && probe
            .get("physical_operator_class_count")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|count| count > 0)
        && probe
            .get("optimizer_decision_count")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|count| count > 0)
        && probe
            .get("optimizer_rule_event_count")
            .and_then(serde_json::Value::as_u64)
            .is_some()
        && probe
            .get("plan_cache_bypassed")
            .and_then(serde_json::Value::as_bool)
            == Some(false)
        && probe
            .get("scan_pruning")
            .is_some_and(serde_json::Value::is_object)
}

fn nowledge_replacement_blocking_categories(
    bundle: &serde_json::Value,
    inputs: ReplacementReadinessInputs<'_>,
) -> Vec<String> {
    let mut categories = BTreeSet::new();
    if inputs.covered_business_surface_per_million != Some(1_000_000) {
        categories.insert("scanner_coverage".to_string());
    }
    if inputs.shadow_parity_per_million != Some(1_000_000)
        || inputs.cutover_decision != Some("ready")
        || !inputs.shadow_evidence_ready
    {
        categories.insert("shadow_parity".to_string());
    }
    if inputs.replacement_readiness_per_million != Some(1_000_000) {
        categories.insert("query_family_readiness".to_string());
    }
    if !inputs.family_evidence_ready {
        categories.insert("query_family_readiness".to_string());
    }
    if inputs.migration_gate_decision != Some("ready") {
        categories.insert("migration_gate".to_string());
    }
    if !inputs.cutover_evidence_eligible {
        categories.insert("cutover_evidence".to_string());
    }
    if !inputs.previous_wrapper_contract_ready || !inputs.full_contract_evidence_ready {
        categories.insert("previous_wrapper_contract".to_string());
    }
    if !inputs.dual_engine_evidence_present
        || inputs.dual_engine_evidence_ready != Some(true)
        || !inputs.dual_engine_evidence_consistent
    {
        categories.insert("dual_engine_evidence".to_string());
    }
    if !inputs.search_projection_evidence_ready {
        categories.insert("search_projection_evidence".to_string());
    }
    if !inputs.search_projection_shadow_evidence_ready {
        categories.insert("search_projection_shadow_evidence".to_string());
    }
    if !inputs.search_candidate_shadow_evidence_ready {
        categories.insert("search_candidate_shadow_evidence".to_string());
    }
    if !inputs.search_route_ownership_ready {
        categories.insert("search_route_ownership".to_string());
    }
    if !inputs.active_search_route_ownership_ready {
        categories.insert("active_search_route_ownership".to_string());
    }
    if !inputs.active_search_route_readiness_ready {
        categories.insert("active_search_route_readiness".to_string());
    }
    if !inputs.bounded_read_evidence_ready {
        categories.insert("bounded_read_evidence".to_string());
    }
    if !inputs.graph_route_readiness_ready {
        categories.insert("graph_route_readiness".to_string());
    }
    if !inputs.query_runtime_preflight_ready {
        categories.insert("query_runtime_preflight".to_string());
    }
    if !inputs.workload_fixture_evidence_ready {
        categories.insert("workload_fixture_evidence".to_string());
    }
    if !inputs.source_mutation_readiness_ready {
        categories.insert("source_mutation_dual_write_readiness".to_string());
    }
    if storage_recovery_required(bundle) && !storage_recovery_raw_evidence_ready(bundle) {
        categories.insert("storage_recovery".to_string());
    }
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) == Some(true)
        && (json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_ready"],
        ) != Some(true)
            || inputs.background_graph_delta_evidence_missing)
    {
        categories.insert("background_maintenance".to_string());
    }
    if json_get_u64_path(
        bundle,
        &[
            "cutover_evidence",
            "replacement_readiness_invalid_family_count",
        ],
    )
    .unwrap_or(0)
        > 0
    {
        categories.insert("query_family_readiness".to_string());
    }
    categories.into_iter().collect()
}

fn nowledge_replacement_next_actions(
    bundle: &serde_json::Value,
    inputs: NextActionInputs<'_>,
) -> Vec<serde_json::Value> {
    if inputs.production_cutover_ready {
        return Vec::new();
    }

    let mut actions = Vec::new();
    if inputs.covered_business_surface_per_million != Some(1_000_000) {
        actions.push(next_action(
            "refresh_nowledge_cypher_inventory",
            "scanner coverage is below full Nowledge business-surface coverage",
            [
                "inventory_gate.coverage_per_million",
                "coverage.coverage_per_million",
            ],
        ));
    }
    if inputs.replacement_readiness_per_million != Some(1_000_000) || !inputs.family_evidence_ready
    {
        actions.push(next_action(
            "close_blocked_query_families",
            "one or more required query families are missing or below full replacement readiness",
            [
                "replacement_readiness_per_million",
                "replacement_readiness_by_query_family",
            ],
        ));
    }
    if inputs.shadow_parity_per_million != Some(1_000_000)
        || inputs.cutover_decision != Some("ready")
        || inputs.migration_gate_decision != Some("ready")
        || !inputs.shadow_evidence_ready
    {
        actions.push(next_action(
            "run_previous_wrapper_shadow_gate",
            "shadow parity or migration gate decision is not ready",
            [
                "cutover.matched_per_million",
                "cutover.decision",
                "migration_gate.decision",
                "shadow_run.evidence_kind",
                "shadow_ready.engine_kind",
                "shadow_ready.wrapper_identity",
                "cutover_evidence.ready_wrapper_identity",
                "previous_wrapper_contract_evidence.wrapper_identity",
            ],
        ));
    }
    if !inputs.cutover_evidence_eligible {
        actions.push(next_action(
            "provide_eligible_cutover_evidence",
            "cutover evidence is missing or not eligible for production replacement",
            [
                "cutover_evidence.eligible",
                "cutover_evidence.evidence_kind",
                "cutover_evidence.ready_engine_kind",
                "cutover_evidence.ready_wrapper_identity",
            ],
        ));
    }
    if !inputs.previous_wrapper_contract_ready || !inputs.full_contract_evidence_ready {
        actions.push(next_action(
            "run_full_previous_wrapper_contract_check",
            "previous-wrapper contract evidence is missing or not ready",
            [
                "required_contract_ready",
                "full_contract_checked",
                "full_contract_ready",
                "selected_checks",
                "check_count",
                "previous_wrapper_contract_evidence.ready",
                "previous_wrapper_contract_evidence.wrapper_identity",
                "previous_wrapper_contract_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.dual_engine_evidence_present
        || inputs.dual_engine_evidence_ready != Some(true)
        || !inputs.dual_engine_evidence_consistent
    {
        actions.push(next_action(
            "rerun_dual_engine_shadow_gate",
            "side-by-side dual-engine evidence is missing or not ready",
            [
                "dual_engine_evidence.present",
                "dual_engine_evidence.ready",
                "dual_engine_evidence.consistent",
                "dual_engine_evidence.primary_check_count",
                "dual_engine_evidence.shadow_check_count",
                "dual_engine_evidence.matched_check_count",
                "dual_engine_evidence.primary_only_check_count",
                "dual_engine_evidence.matched_per_million",
            ],
        ));
    }
    if !inputs.source_mutation_readiness_ready {
        actions.push(next_action(
            "attach_source_mutation_dual_write_readiness",
            "Source ingest/create, refresh/reparse, indexed transition, revision edges, and search-projection effects must be covered by durable dual-write readiness",
            [
                "source_mutation_dual_write_readiness.protocol",
                "source_mutation_dual_write_readiness.ready",
                "source_mutation_dual_write_readiness.required_family_count",
                "source_mutation_dual_write_readiness.evidence_family_count",
                "source_mutation_dual_write_readiness.ready_family_count",
                "source_mutation_dual_write_readiness.missing_required_families",
                "source_mutation_dual_write_readiness.blocker_codes",
            ],
        ));
    }
    if !inputs.search_projection_evidence_ready {
        actions.push(next_action(
            "attach_search_projection_replacement_evidence",
            "LanceDB replacement evidence is missing or not ready",
            [
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
                "search_projection_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.search_projection_shadow_evidence_ready {
        actions.push(next_action(
            "run_search_projection_shadow_evidence",
            "LanceDB/Skein search projection side-by-side evidence is missing or not ready",
            [
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
                "search_projection_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.search_candidate_shadow_evidence_ready {
        actions.push(next_action(
            "run_search_candidate_shadow_evidence",
            "LanceDB/Skein search candidate side-by-side evidence is missing or not ready",
            [
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
                "search_candidate_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.search_route_ownership_ready {
        actions.push(next_action(
            "attach_search_route_ownership",
            "search projection route ownership is missing or still routes a projection family to LanceDB",
            [
                "search_route_ownership.protocol",
                "search_route_ownership.ready",
                "search_route_ownership.production_cutover_ready",
                "search_route_ownership.required_route_count",
                "search_route_ownership.explicit_route_count",
                "search_route_ownership.skein_route_count",
                "search_route_ownership.lancedb_route_count",
                "search_route_ownership.missing_required_routes",
                "search_route_ownership.lancedb_routes",
                "search_route_ownership.blocker_codes",
            ],
        ));
    }
    if !inputs.active_search_route_ownership_ready {
        actions.push(next_action(
            "attach_active_search_route_ownership",
            "active search route ownership is missing or still routes a business search path to LanceDB",
            [
                "active_search_route_ownership.protocol",
                "active_search_route_ownership.ready",
                "active_search_route_ownership.production_cutover_ready",
                "active_search_route_ownership.required_route_count",
                "active_search_route_ownership.explicit_route_count",
                "active_search_route_ownership.skein_route_count",
                "active_search_route_ownership.lancedb_route_count",
                "active_search_route_ownership.missing_required_routes",
                "active_search_route_ownership.lancedb_routes",
                "active_search_route_ownership.blocker_codes",
            ],
        ));
    }
    if !inputs.active_search_route_readiness_ready {
        actions.push(next_action(
            "attach_active_search_route_readiness",
            "active search route read evidence is missing or still requires LanceDB for a business search path",
            [
                "active_search_route_readiness.protocol",
                "active_search_route_readiness.ready",
                "active_search_route_readiness.production_cutover_ready",
                "active_search_route_readiness.required_route_count",
                "active_search_route_readiness.evidence_route_count",
                "active_search_route_readiness.ready_route_count",
                "active_search_route_readiness.skein_route_count",
                "active_search_route_readiness.lancedb_handle_required_route_count",
                "active_search_route_readiness.missing_required_routes",
                "active_search_route_readiness.non_skein_routes",
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
                "active_search_route_readiness.blocker_codes",
            ],
        ));
    }
    if !inputs.bounded_read_evidence_ready {
        actions.push(next_action(
            "attach_bounded_read_profile",
            "bounded read execution profile is missing or not ready",
            [
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
                "bounded_read_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.graph_route_readiness_ready {
        actions.push(next_action(
            "attach_graph_route_readiness_evidence",
            "graph route readiness evidence is missing, stale, or does not cover all required graph read routes",
            [
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
                "graph_route_readiness.route_primary_blocker_codes",
            ],
        ));
    }
    if !inputs.query_runtime_preflight_ready {
        actions.push(next_action(
            "attach_query_runtime_preflight_evidence",
            "query runtime preflight evidence is missing or does not cover all required graph read routes",
            [
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
                "query_runtime_preflight.probes",
            ],
        ));
    }
    if !inputs.workload_fixture_evidence_ready {
        actions.push(next_action(
            "run_workload_fixture_evidence",
            "graph route, bounded expansion, metadata-filtered search, or source projection workload fixture evidence is missing or not ready",
            [
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
                "workload_fixture_evidence.blocker_codes",
            ],
        ));
    }
    if storage_recovery_required(bundle) && !storage_recovery_raw_evidence_ready(bundle) {
        actions.push(next_action(
            "attach_storage_recovery_report",
            "required storage recovery evidence is missing or blocked",
            [
                "cutover_evidence.storage_recovery_present",
                "cutover_evidence.storage_recovery_ready",
                "cutover_evidence.storage_recovery_protocol_matches",
                "cutover_evidence.storage_recovery_durable",
                "cutover_evidence.storage_recovery_checkpoint_boundary_present",
                "cutover_evidence.storage_recovery_wal_replay_bounded",
                "cutover_evidence.storage_recovery_replay_boundary_consistent",
                "cutover_evidence.storage_recovery_torn_tail_clean",
                "cutover_evidence.storage_recovery_blocker_codes",
            ],
        ));
    }
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) == Some(true)
        && (json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_ready"],
        ) != Some(true)
            || inputs.background_graph_delta_evidence_missing)
    {
        actions.push(next_action(
            "attach_background_maintenance_report",
            "required background maintenance QoS or graph-delta evidence is missing or blocked",
            [
                "cutover_evidence.background_maintenance_present",
                "cutover_evidence.background_maintenance_ready",
                "cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
                "cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
                "cutover_evidence.background_maintenance_blocker_codes",
            ],
        ));
    }

    actions
}

fn next_action(
    action: &str,
    reason: &str,
    evidence_fields: impl IntoIterator<Item = &'static str>,
) -> serde_json::Value {
    serde_json::json!({
        "action": action,
        "reason": reason,
        "evidence_fields": evidence_fields.into_iter().collect::<Vec<_>>(),
    })
}

fn nowledge_replacement_missing_evidence(bundle: &serde_json::Value) -> Vec<String> {
    let mut missing = Vec::new();
    if bundle.get("cutover_evidence").is_none() {
        missing.push("cutover_evidence".to_string());
    }
    if storage_recovery_required(bundle)
        && json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_present"])
            != Some(true)
    {
        missing.push("storage_recovery".to_string());
    }
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) == Some(true)
        && json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_present"],
        ) != Some(true)
    {
        missing.push("background_maintenance".to_string());
    }
    if background_maintenance_graph_delta_evidence_missing(bundle) {
        missing.push("background_maintenance_search_projection_graph_delta".to_string());
    }
    if bundle
        .get("replacement_readiness_by_query_family")
        .is_none()
    {
        missing.push("replacement_readiness_by_query_family".to_string());
    } else if !replacement_readiness_family_evidence_health_from_bundle(bundle).ready {
        missing.push("replacement_readiness_by_query_family_ready".to_string());
    }
    if bundle.get("previous_wrapper_contract_evidence").is_none() {
        missing.push("previous_wrapper_contract_evidence".to_string());
    }
    if !full_contract_evidence_summary(bundle).ready {
        missing.push("full_contract_evidence".to_string());
    }
    if bundle.get("dual_engine_evidence").is_none()
        && json_get_path(bundle, &["cutover", "dual_engine_evidence"]).is_none()
    {
        missing.push("dual_engine_evidence".to_string());
    }
    if bundle.get("source_mutation_dual_write_readiness").is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "source_mutation_dual_write_readiness"],
        )
        .is_none()
    {
        missing.push("source_mutation_dual_write_readiness".to_string());
    } else if !source_mutation_dual_write_readiness_summary(bundle).ready {
        missing.push("source_mutation_dual_write_readiness_ready".to_string());
    }
    if bundle.get("search_projection_evidence").is_none()
        && json_get_path(bundle, &["cutover_evidence", "search_projection_evidence"]).is_none()
    {
        missing.push("search_projection_evidence".to_string());
    } else if !search_projection_evidence_summary(bundle).ready {
        missing.push("search_projection_evidence_ready".to_string());
    }
    if bundle.get("search_projection_shadow_evidence").is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "search_projection_shadow_evidence"],
        )
        .is_none()
    {
        missing.push("search_projection_shadow_evidence".to_string());
    } else if !search_projection_shadow_evidence_summary(bundle).ready {
        missing.push("search_projection_shadow_evidence_ready".to_string());
    }
    if bundle.get("search_candidate_shadow_evidence").is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "search_candidate_shadow_evidence"],
        )
        .is_none()
    {
        missing.push("search_candidate_shadow_evidence".to_string());
    } else if !search_candidate_shadow_evidence_summary(bundle).ready {
        missing.push("search_candidate_shadow_evidence_ready".to_string());
    }
    if bundle.get("search_route_ownership").is_none()
        && json_get_path(bundle, &["cutover_evidence", "search_route_ownership"]).is_none()
    {
        missing.push("search_route_ownership".to_string());
    } else if !search_route_ownership_summary(bundle).ready {
        missing.push("search_route_ownership_ready".to_string());
    }
    if bundle.get("active_search_route_ownership").is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "active_search_route_ownership"],
        )
        .is_none()
    {
        missing.push("active_search_route_ownership".to_string());
    } else if !active_search_route_ownership_summary(bundle).ready {
        missing.push("active_search_route_ownership_ready".to_string());
    }
    if bundle.get("active_search_route_readiness").is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "active_search_route_readiness"],
        )
        .is_none()
    {
        missing.push("active_search_route_readiness".to_string());
    } else if !active_search_route_readiness_summary(bundle).ready {
        missing.push("active_search_route_readiness_ready".to_string());
    }
    if bundle.get("bounded_read_evidence").is_none()
        && json_get_path(bundle, &["cutover_evidence", "bounded_read_evidence"]).is_none()
    {
        missing.push("bounded_read_evidence".to_string());
    } else if !bounded_read_evidence_summary(bundle).ready {
        missing.push("bounded_read_evidence_ready".to_string());
    }
    if bundle.get("graph_route_readiness").is_none()
        && json_get_path(bundle, &["cutover_evidence", "graph_route_readiness"]).is_none()
    {
        missing.push("graph_route_readiness".to_string());
    } else if !nowledge_graph_route_readiness_summary_from_bundle(bundle).ready {
        missing.push("graph_route_readiness_ready".to_string());
    }
    if bundle.get("query_runtime_preflight").is_none()
        && json_get_path(bundle, &["cutover_evidence", "query_runtime_preflight"]).is_none()
    {
        missing.push("query_runtime_preflight".to_string());
    } else if !query_runtime_preflight_summary(bundle).ready {
        missing.push("query_runtime_preflight_ready".to_string());
    }
    if bundle.get("workload_fixture_evidence").is_none()
        && json_get_path(bundle, &["cutover_evidence", "workload_fixture_evidence"]).is_none()
    {
        missing.push("workload_fixture_evidence".to_string());
    } else if !workload_fixture_evidence_summary(bundle).ready {
        missing.push("workload_fixture_evidence_ready".to_string());
    }
    if bundle.get("shadow_run").is_none() {
        missing.push("shadow_run".to_string());
    }
    if bundle.get("shadow_ready").is_none() {
        missing.push("shadow_ready".to_string());
    }
    missing
}

fn background_maintenance_graph_delta_evidence_missing(bundle: &serde_json::Value) -> bool {
    if json_get_bool_path(
        bundle,
        &["cutover_evidence", "background_maintenance_required"],
    ) != Some(true)
        || json_get_bool_path(
            bundle,
            &["cutover_evidence", "background_maintenance_present"],
        ) != Some(true)
    {
        return false;
    }

    let missing_counter = [
        "background_maintenance_executable_search_projection_graph_delta_count",
        "background_maintenance_admitted_search_projection_graph_delta_count",
        "background_maintenance_deferred_search_projection_graph_delta_count",
        "background_maintenance_rejected_search_projection_graph_delta_count",
        "background_maintenance_executable_search_projection_graph_delta_operations",
        "background_maintenance_admitted_search_projection_graph_delta_operations",
        "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
    ]
    .into_iter()
    .any(|field| json_get_u64_path_from_dynamic(bundle, &["cutover_evidence"], field).is_none());
    missing_counter
        || json_get_bool_path(
            bundle,
            &[
                "cutover_evidence",
                "background_maintenance_foreground_admission_probe_ready",
            ],
        )
        .is_none()
}

fn storage_recovery_required(bundle: &serde_json::Value) -> bool {
    json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]) == Some(true)
}

fn storage_recovery_raw_evidence_ready(bundle: &serde_json::Value) -> bool {
    json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]) == Some(true)
        && json_get_bool_path(
            bundle,
            &["cutover_evidence", "storage_recovery_protocol_matches"],
        ) == Some(true)
        && json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_durable"])
            == Some(true)
        && json_get_bool_path(
            bundle,
            &[
                "cutover_evidence",
                "storage_recovery_checkpoint_boundary_present",
            ],
        ) == Some(true)
        && json_get_bool_path(
            bundle,
            &["cutover_evidence", "storage_recovery_wal_replay_bounded"],
        ) == Some(true)
        && json_get_bool_path(
            bundle,
            &[
                "cutover_evidence",
                "storage_recovery_replay_boundary_consistent",
            ],
        ) == Some(true)
        && json_get_bool_path(
            bundle,
            &["cutover_evidence", "storage_recovery_torn_tail_clean"],
        ) == Some(true)
}

fn nowledge_replacement_blockers(bundle: &serde_json::Value) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    for path in [
        &["inventory_gate", "blockers"][..],
        &["cutover", "blockers"][..],
        &["migration_gate", "blockers"][..],
        &["cutover_evidence", "blockers"][..],
        &["cutover_evidence", "storage_recovery_blockers"][..],
        &["cutover_evidence", "background_maintenance_blockers"][..],
        &["cutover_evidence", "replacement_readiness_blockers"][..],
        &["previous_wrapper_contract_evidence", "blockers"][..],
        &["contract_evidence", "full_contract_blockers"][..],
        &["contract_evidence", "required_contract_blockers"][..],
        &["full_contract_blockers"][..],
        &["required_contract_blockers"][..],
        &["search_projection_shadow_evidence", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "search_projection_shadow_evidence",
            "blocker_codes",
        ][..],
        &["search_candidate_shadow_evidence", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "search_candidate_shadow_evidence",
            "blocker_codes",
        ][..],
        &["source_mutation_dual_write_readiness", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "source_mutation_dual_write_readiness",
            "blocker_codes",
        ][..],
        &["search_route_ownership", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "search_route_ownership",
            "blocker_codes",
        ][..],
        &["active_search_route_ownership", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "active_search_route_ownership",
            "blocker_codes",
        ][..],
        &["active_search_route_readiness", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "active_search_route_readiness",
            "blocker_codes",
        ][..],
        &["bounded_read_evidence", "blocker_codes"][..],
        &["cutover_evidence", "bounded_read_evidence", "blocker_codes"][..],
        &["graph_route_readiness", "route_coverage_blocker_codes"][..],
        &[
            "graph_route_readiness",
            "evidence_route_coverage_blocker_codes",
        ][..],
        &["graph_route_readiness", "route_primary_blocker_codes"][..],
        &[
            "cutover_evidence",
            "graph_route_readiness",
            "route_coverage_blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "graph_route_readiness",
            "evidence_route_coverage_blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "graph_route_readiness",
            "route_primary_blocker_codes",
        ][..],
        &["query_runtime_preflight", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "query_runtime_preflight",
            "blocker_codes",
        ][..],
        &["query_runtime_preflight", "failed_checks"][..],
        &[
            "cutover_evidence",
            "query_runtime_preflight",
            "failed_checks",
        ][..],
        &["workload_fixture_evidence", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "workload_fixture_evidence",
            "blocker_codes",
        ][..],
    ] {
        for blocker in json_get_string_array_path(bundle, path) {
            blockers.insert(blocker);
        }
    }
    blockers.into_iter().collect()
}

#[cfg(test)]
mod tests;
