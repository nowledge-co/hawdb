use crate::{
    graph_route::{NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL, NMEM_GRAPH_ROUTE_READINESS_PROTOCOL},
    graph_summary::{nowledge_graph_route_readiness_summary, GraphRouteReadinessSummary},
    previous_wrapper_preflight::{
        NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL, NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL,
        NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL,
    },
    source_mutation::{
        NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
        REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES,
    },
};
use skein_core::{Result, SkeinError};
use skein_evidence::{
    blackbox::{blackbox_readiness_from_manifest_json, BlackboxReadinessReport},
    inventory::REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
    replacement_contract::{
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
    },
    resource_profile::production_resource_profile_ready,
};
use skein_route_ownership::graph::{
    nowledge_mem_graph_read_route_catalog_digest, nowledge_mem_required_query_families_for_route,
    NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION, NOWLEDGE_MEM_ROUTE_OWNERSHIP_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_ROUTE, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const SKEIN_NOWLEDGE_REPLACEMENT_SUMMARY_PROTOCOL: &str = "skein-nowledge-replacement-summary";
const GRAPH_LAYER_REPLACEMENT_SCOPE: &str = "kuzu_ladybug_graph_layer";
const SEARCH_PROJECTION_REPLACEMENT_SCOPE: &str = "lancedb_search_projection";
const SQLITE_CONTENT_STORE_SCOPE: &str = "sqlite_content_store";
const LARGE_BLOB_VALUE_STORE_SCOPE: &str = "large_blob_value_store";
const SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-evidence";
const SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-shadow-evidence";
const SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE: &str = "skein-rust-library";
const SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-mem-bounded-read-evidence-v2";
const SKEIN_NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL: &str =
    "skein-nowledge-query-runtime-preflight-v1";
const SKEIN_NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL: &str = "skein-nowledge-mem-query-report-v1";
const ROUTE_PARITY_EVIDENCE_SOURCE: &str = "route_parity_evidence";
const ROUTE_PARITY_FULL_MATCH_PER_MILLION: u64 = 1_000_000;
const FINAL_CATEGORY_STARTUP: &str = "startup";
const FINAL_CATEGORY_ROUTE_COVERAGE: &str = "route_coverage";
const FINAL_CATEGORY_GRAPH_REPLACEMENT: &str = "graph_replacement";
const FINAL_CATEGORY_SEARCH_PROJECTION: &str = "search_projection";
const FINAL_CATEGORY_STORAGE_RECOVERY: &str = "storage_recovery";
const FINAL_CATEGORY_BACKGROUND_QOS: &str = "background_qos";
const FINAL_CATEGORY_BLACKBOX: &str = "blackbox";
const FINAL_CATEGORY_LIBRARY_ONLY: &str = "library_only";
const FINAL_CATEGORY_REPLACEMENT_SUMMARY_CUTOVER: &str = "replacement_summary_cutover";

const STARTUP_CHECKS: &[&str] = &[
    "integration_bundle_protocol",
    "replacement_summary_protocol",
    "skein_submodule",
    "legacy_coexistence",
    "content_store_boundary",
    "previous_wrapper_preflight",
];
const ROUTE_COVERAGE_CHECKS: &[&str] = &[
    "bounded_read_evidence",
    "bounded_read_evidence_alignment",
    "graph_route_readiness",
    "graph_route_readiness_alignment",
    "graph_route_parity_alignment",
    "route_ownership",
    "search_route_ownership_alignment",
    "active_search_route_ownership_alignment",
    "active_search_route_readiness_alignment",
    "query_runtime_preflight",
    "query_runtime_preflight_alignment",
];
const GRAPH_REPLACEMENT_CHECKS: &[&str] = &[
    "graph_replacement_evidence",
    "query_family_replacement_evidence",
];
const SEARCH_PROJECTION_CHECKS: &[&str] = &[
    "search_projection_replacement_evidence",
    "search_candidate_primary_evidence",
];
const STORAGE_RECOVERY_CHECKS: &[&str] = &["storage_recovery_evidence", "operations_readiness"];
const BACKGROUND_QOS_CHECKS: &[&str] = &["background_maintenance_evidence"];
const BLACKBOX_CHECKS: &[&str] = &["blackbox_redaction", "blackbox_operational_evidence"];
const LIBRARY_ONLY_CHECKS: &[&str] = &["library_readiness", "cutover_controls"];

pub const NOWLEDGE_MEM_INTEGRATION_READINESS_PROTOCOL: &str =
    "skein-nowledge-mem-integration-readiness";
pub const NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL: &str =
    "nowledge-mem-skein-integration-bundle";
pub const NOWLEDGE_MEM_FINAL_CUTOVER_PREFLIGHT_PROTOCOL: &str =
    "skein-nowledge-mem-final-cutover-preflight-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemIntegrationCheckReport {
    pub name: String,
    pub ready: bool,
    pub evidence_fields: Vec<String>,
    pub failed_evidence_fields: Vec<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemIntegrationCheckReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "evidence_fields": self.evidence_fields,
            "failed_evidence_fields": self.failed_evidence_fields,
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemIntegrationNextAction {
    pub action: String,
    pub reason: String,
    pub evidence_fields: Vec<String>,
}

impl NowledgeMemIntegrationNextAction {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "action": self.action,
            "reason": self.reason,
            "evidence_fields": self.evidence_fields,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemIntegrationReadinessReport {
    pub protocol: String,
    pub ready: bool,
    pub failed_checks: Vec<String>,
    pub checks: Vec<NowledgeMemIntegrationCheckReport>,
    pub blocker_codes: Vec<String>,
    pub next_actions: Vec<NowledgeMemIntegrationNextAction>,
}

impl NowledgeMemIntegrationReadinessReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "failed_checks": self.failed_checks,
            "checks": self.checks.iter().map(NowledgeMemIntegrationCheckReport::json).collect::<Vec<_>>(),
            "blocker_codes": self.blocker_codes,
            "next_actions": self.next_actions.iter().map(NowledgeMemIntegrationNextAction::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemFinalCutoverPreflightReport {
    pub protocol: String,
    pub production_cutover_ready: bool,
    pub integration_ready: bool,
    pub replacement_summary_production_cutover_ready: bool,
    pub startup_ready: bool,
    pub route_coverage_ready: bool,
    pub graph_replacement_ready: bool,
    pub search_projection_ready: bool,
    pub storage_recovery_ready: bool,
    pub background_qos_ready: bool,
    pub blackbox_ready: bool,
    pub library_only_ready: bool,
    pub check_count: usize,
    pub ready_check_count: usize,
    pub failed_check_count: usize,
    pub next_action_count: usize,
    pub next_action_names: Vec<String>,
    pub blocking_categories: Vec<String>,
    pub failed_checks: Vec<String>,
    pub failed_evidence_fields: Vec<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemFinalCutoverPreflightReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "production_cutover_ready": self.production_cutover_ready,
            "integration_ready": self.integration_ready,
            "replacement_summary_production_cutover_ready": self.replacement_summary_production_cutover_ready,
            "startup_ready": self.startup_ready,
            "route_coverage_ready": self.route_coverage_ready,
            "graph_replacement_ready": self.graph_replacement_ready,
            "search_projection_ready": self.search_projection_ready,
            "storage_recovery_ready": self.storage_recovery_ready,
            "background_qos_ready": self.background_qos_ready,
            "blackbox_ready": self.blackbox_ready,
            "library_only_ready": self.library_only_ready,
            "check_count": self.check_count,
            "ready_check_count": self.ready_check_count,
            "failed_check_count": self.failed_check_count,
            "next_action_count": self.next_action_count,
            "next_action_names": self.next_action_names,
            "blocking_categories": self.blocking_categories,
            "failed_checks": self.failed_checks,
            "failed_evidence_fields": self.failed_evidence_fields,
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageRecoveryCutoverReadiness {
    pub required: bool,
    pub ready: bool,
    pub protocol_matches: bool,
    pub durable: bool,
    pub checkpoint_boundary_present: bool,
    pub wal_replay_bounded: bool,
    pub replay_boundary_consistent: bool,
    pub torn_tail_clean: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrationBundleProtocolCutoverReadiness {
    pub protocol_matches: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplacementSummaryProtocolCutoverReadiness {
    pub protocol_matches: bool,
    pub graph_layer_replacement_scope_ready: bool,
    pub search_projection_replacement_scope_ready: bool,
    pub content_store_out_of_scope: bool,
    pub large_blob_store_out_of_scope: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkeinSubmoduleCutoverReadiness {
    pub present: bool,
    pub path_present: bool,
    pub commit_present: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyCoexistenceCutoverReadiness {
    pub old_database_retained: bool,
    pub mode_safe: bool,
    pub old_database_not_deleted: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentStoreBoundaryCutoverReadiness {
    pub present: bool,
    pub engine_sqlite: bool,
    pub messages_available: bool,
    pub source_chunks_available: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviousWrapperPreflightCutoverReadiness {
    pub ready: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundMaintenanceCutoverReadiness {
    pub required: bool,
    pub ready: bool,
    pub protocol_matches: bool,
    pub executable_search_projection_graph_delta_count_present: bool,
    pub admitted_search_projection_graph_delta_count_present: bool,
    pub deferred_search_projection_graph_delta_count_present: bool,
    pub rejected_search_projection_graph_delta_count_present: bool,
    pub executable_search_projection_graph_delta_operations_present: bool,
    pub admitted_search_projection_graph_delta_operations_present: bool,
    pub max_search_projection_graph_delta_complete_through_graph_commit_epoch_present: bool,
    pub foreground_admission_probe_ready: bool,
    pub memory_pressure_ready: bool,
    pub memory_budget_bytes_present: bool,
    pub estimated_memory_bytes_present: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LibraryReadinessCutoverReadiness {
    pub protocol_matches: bool,
    pub present: bool,
    pub ready: bool,
    pub production_path_ready: bool,
    pub production_path_in_process: bool,
    pub production_path_cli_not_required: bool,
    pub production_path_env_control_plane_not_required: bool,
    pub production_path_spawned_helper_not_required: bool,
    pub ready_area_count_present: bool,
    pub blocked_area_count_zero: bool,
    pub redaction_ready: bool,
    pub query_text_redacted: bool,
    pub parameters_redacted: bool,
    pub local_paths_redacted: bool,
    pub graph_opened: bool,
    pub search_projection_opened: bool,
    pub graph_ready: bool,
    pub query_ready: bool,
    pub storage_ready: bool,
    pub background_ready: bool,
    pub query_family_ready: bool,
    pub graph_route_ready: bool,
    pub search_route_ownership_ready: bool,
    pub search_projection_ready: bool,
    pub search_projection_shadow_ready: bool,
    pub search_candidate_shadow_ready: bool,
    pub workload_fixture_ready: bool,
    pub production_resource_profile_ready: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutoverControlsReadiness {
    pub protocol_matches: bool,
    pub ready: bool,
    pub graph_reads_skein: bool,
    pub graph_read_effective: bool,
    pub graph_production_status_effective: bool,
    pub search_reads_skein: bool,
    pub search_read_effective: bool,
    pub search_production_status_effective: bool,
    pub dual_writes_enabled: bool,
    pub projection_catch_up_enabled: bool,
    pub initial_import_safe: bool,
    pub initial_import_inactive_for_cutover: bool,
    pub initial_import_cutover_catch_up_ready: bool,
    pub initial_import_safe_for_read_cutover: bool,
    pub query_text_redacted: bool,
    pub parameters_redacted: bool,
    pub local_paths_redacted: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationsReadinessCutoverReadiness {
    pub protocol_matches: bool,
    pub present: bool,
    pub ready: bool,
    pub graph_open: bool,
    pub graph_writable: bool,
    pub search_projection_open: bool,
    pub search_projection_not_stale: bool,
    pub storage_lifecycle_ready: bool,
    pub storage_lifecycle_action_ready: bool,
    pub storage_recovery_ready: bool,
    pub slow_query_ready: bool,
    pub background_maintenance_ready: bool,
    pub query_text_redacted: bool,
    pub parameters_redacted: bool,
    pub local_paths_redacted: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphReplacementCutoverReadiness {
    pub production_cutover_ready: bool,
    pub shadow_evidence_ready: bool,
    pub dual_engine_evidence_present: bool,
    pub dual_engine_evidence_ready: bool,
    pub dual_engine_evidence_consistent: bool,
    pub source_mutation_protocol_matches: bool,
    pub source_mutation_ready: bool,
    pub source_mutation_required_family_count_matches: bool,
    pub source_mutation_evidence_family_count_matches: bool,
    pub source_mutation_ready_family_count_matches: bool,
    pub source_mutation_missing_required_families_empty: bool,
    pub source_mutation_blocker_codes_empty: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryFamilyReplacementCutoverReadiness {
    pub required_query_families_present: bool,
    pub missing_required_query_families_empty: bool,
    pub blocked_query_families_empty: bool,
    pub min_replacement_readiness_full: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionCutoverReadiness {
    pub evidence_protocol_matches: bool,
    pub evidence_ready: bool,
    pub fts_ready: bool,
    pub vector_ready: bool,
    pub document_identity_ready: bool,
    pub incremental_update_ready: bool,
    pub predicate_pushdown_ready: bool,
    pub production_filter_pruning_ready: bool,
    pub compressed_vector_projection_required: bool,
    pub compressed_vector_projection_ready: bool,
    pub shadow_present: bool,
    pub shadow_protocol_matches: bool,
    pub shadow_evidence_source_matches: bool,
    pub shadow_ready: bool,
    pub shadow_document_count_parity: bool,
    pub shadow_document_identity_parity: bool,
    pub shadow_table_parity_ready: bool,
    pub shadow_embedding_identity_parity: bool,
    pub shadow_incremental_watermark_parity: bool,
    pub shadow_pushdown_ready: bool,
    pub shadow_descriptor_scan_filter_fields_ready: bool,
    pub shadow_document_pruning_ready: bool,
    pub shadow_pruning_candidate_count_ready: bool,
    pub shadow_pruned_document_count_positive: bool,
    pub shadow_scanned_document_count_positive: bool,
    pub primary_scan_filter_fields_ready: bool,
    pub shadow_scan_filter_fields_ready: bool,
    pub shadow_descriptor_field_summaries_ready: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchCandidateCutoverReadiness {
    pub protocol_matches: bool,
    pub evidence_source_matches: bool,
    pub route_matches: bool,
    pub ready: bool,
    pub primary_engine_matches: bool,
    pub candidate_count_parity: bool,
    pub row_count_parity: bool,
    pub text_retriever_ready: bool,
    pub vector_retriever_ready: bool,
    pub fts_top_k_overlap_ready: bool,
    pub vector_top_k_overlap_ready: bool,
    pub source_chunk_identity_ready: bool,
    pub fail_soft_observed: bool,
    pub projection_marker_status_visible: bool,
    pub projection_watermark_ready: bool,
    pub embedding_identity_ready: bool,
    pub candidate_identity_ready: bool,
    pub filter_pushdown_ready: bool,
    pub filter_pushdown_field_summary_present: bool,
    pub filter_pushdown_required_fields_ready: bool,
    pub shadow_scan_filter_pushdown_ready: bool,
    pub shadow_scan_field_pruning_ready: bool,
    pub shadow_scan_field_summary_present: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedReadCutoverReadiness {
    pub present: bool,
    pub protocol_matches: bool,
    pub ready: bool,
    pub max_rows_present: bool,
    pub mode_matches: bool,
    pub execution_cap_matches: bool,
    pub estimated_payload_bytes_present: bool,
    pub max_estimated_payload_bytes_present: bool,
    pub payload_budget_not_exceeded: bool,
    pub row_limit_enforced_before_output: bool,
    pub operator_row_cap_enabled: bool,
    pub blocking_operator_memory_reports_complete: bool,
    pub blocking_operator_memory_within_budget: bool,
    pub spill_within_budget: bool,
    pub streaming_evidence_present: bool,
    pub route_catalog_version_matches: bool,
    pub route_catalog_digest_present: bool,
    pub route_coverage_ready: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedReadAlignmentCutoverReadiness {
    pub evidence_ready: bool,
    pub ready: bool,
    pub alignment_evidence_ready: bool,
    pub summary_ready: bool,
    pub protocol_matches: bool,
    pub readiness_matches: bool,
    pub mode_matches: bool,
    pub max_rows_matches: bool,
    pub estimated_payload_bytes_matches: bool,
    pub max_estimated_payload_bytes_matches: bool,
    pub payload_budget_exceeded_matches: bool,
    pub streaming_matches: bool,
    pub covered_routes_matches: bool,
    pub evidence_route_catalog_version_ready: bool,
    pub summary_route_catalog_version_ready: bool,
    pub evidence_route_catalog_digest_ready: bool,
    pub summary_route_catalog_digest_ready: bool,
    pub route_catalog_version_matches: bool,
    pub route_catalog_digest_matches: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRouteCutoverReadiness {
    pub protocol_matches: bool,
    pub evidence_protocol_matches: bool,
    pub evidence_ready: bool,
    pub route_count_present: bool,
    pub required_route_count_matches: bool,
    pub route_coverage_ready: bool,
    pub missing_required_routes_empty: bool,
    pub query_runtime_route_count_matches: bool,
    pub query_runtime_report_count_ready: bool,
    pub query_plan_profile_summary_ready: bool,
    pub relationship_property_pruning_summary_ready: bool,
    pub missing_query_runtime_routes_empty: bool,
    pub route_query_runtime_ready: bool,
    pub route_primary_ready: bool,
    pub primary_ready_route_count_matches: bool,
    pub route_primary_blocker_codes_empty: bool,
    pub evidence_route_coverage_present: bool,
    pub evidence_route_coverage_matches: bool,
    pub evidence_route_coverage_blocker_codes_empty: bool,
    pub route_query_profiles_ready: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRouteAlignmentCutoverReadiness {
    pub ready: bool,
    pub evidence_protocol_matches: bool,
    pub evidence_ready: bool,
    pub evidence_route_primary_ready: bool,
    pub summary_route_primary_ready: bool,
    pub route_primary_ready_matches: bool,
    pub route_query_plan_evidence_ready_matches: bool,
    pub route_query_profile_evidence_ready_matches: bool,
    pub route_query_api_behavior_evidence_ready_matches: bool,
    pub route_relationship_property_pruning_evidence_ready_matches: bool,
    pub relationship_property_pruning_required_count_matches: bool,
    pub relationship_property_pruning_report_count_matches: bool,
    pub primary_ready_routes_match: bool,
    pub evidence_required_routes_covered: bool,
    pub summary_required_routes_covered: bool,
    pub evidence_route_catalog_version_ready: bool,
    pub summary_route_catalog_version_ready: bool,
    pub evidence_route_catalog_digest_ready: bool,
    pub summary_route_catalog_digest_ready: bool,
    pub route_catalog_version_matches: bool,
    pub route_catalog_digest_matches: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRouteParityAlignmentCutoverReadiness {
    pub ready: bool,
    pub required_route_count_present: bool,
    pub ready_route_count_matches: bool,
    pub missing_routes_empty: bool,
    pub not_ready_routes_empty: bool,
    pub route_mismatch_routes_empty: bool,
    pub protocol_mismatch_routes_empty: bool,
    pub blocker_routes_empty: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteOwnershipCutoverReadiness {
    pub protocol_matches: bool,
    pub ready: bool,
    pub production_cutover_ready: bool,
    pub require_all_skein: bool,
    pub required_route_count_matches: bool,
    pub explicit_route_count_matches: bool,
    pub skein_route_count_matches: bool,
    pub legacy_route_count_zero: bool,
    pub missing_required_routes_empty: bool,
    pub unknown_routes_empty: bool,
    pub duplicate_routes_empty: bool,
    pub conflicting_routes_empty: bool,
    pub skein_not_ready_routes_empty: bool,
    pub route_readiness_present: bool,
    pub route_readiness_ready: bool,
    pub search_route_projection_evidence_present: bool,
    pub route_catalog_version_matches: bool,
    pub route_catalog_digest_matches: bool,
    pub blocker_codes_empty: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRouteOwnershipAlignmentCutoverReadiness {
    pub ready: bool,
    pub evidence_ready: bool,
    pub summary_ready: bool,
    pub protocol_matches: bool,
    pub readiness_matches: bool,
    pub production_cutover_ready_matches: bool,
    pub require_all_skein_matches: bool,
    pub required_route_count_matches: bool,
    pub explicit_route_count_matches: bool,
    pub skein_route_count_matches: bool,
    pub lancedb_route_count_matches: bool,
    pub missing_required_routes_matches: bool,
    pub lancedb_routes_matches: bool,
    pub blocker_codes_match: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveSearchRouteReadinessAlignmentCutoverReadiness {
    pub ready: bool,
    pub evidence_ready: bool,
    pub summary_ready: bool,
    pub protocol_matches: bool,
    pub readiness_matches: bool,
    pub production_cutover_ready_matches: bool,
    pub require_all_skein_matches: bool,
    pub required_route_count_matches: bool,
    pub evidence_route_count_matches: bool,
    pub ready_route_count_matches: bool,
    pub skein_route_count_matches: bool,
    pub lancedb_handle_count_matches: bool,
    pub missing_required_routes_matches: bool,
    pub non_skein_routes_matches: bool,
    pub lancedb_handle_routes_matches: bool,
    pub candidate_not_ready_routes_matches: bool,
    pub candidate_identity_not_ready_routes_matches: bool,
    pub embedding_identity_not_ready_routes_matches: bool,
    pub zero_vector_semantics_not_ready_routes_matches: bool,
    pub cjk_tokenization_not_ready_routes_matches: bool,
    pub metadata_pushdown_not_ready_routes_matches: bool,
    pub ranking_window_not_ready_routes_matches: bool,
    pub ranking_not_ready_routes_matches: bool,
    pub fail_soft_not_ready_routes_matches: bool,
    pub fail_soft_reason_codes_not_ready_routes_matches: bool,
    pub repair_rebuild_markers_not_ready_routes_matches: bool,
    pub blocker_codes_match: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRuntimePreflightCutoverReadiness {
    pub protocol_matches: bool,
    pub ready: bool,
    pub database_opened: bool,
    pub redaction_ready: bool,
    pub rows_redacted: bool,
    pub parameters_redacted: bool,
    pub local_paths_redacted: bool,
    pub raw_errors_redacted: bool,
    pub probe_count_present: bool,
    pub probe_counts_match: bool,
    pub failed_probe_count_zero: bool,
    pub route_coverage_ready: bool,
    pub route_catalog_version_matches: bool,
    pub route_catalog_digest_present: bool,
    pub probe_details_ready: bool,
    pub blocker_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRuntimePreflightAlignmentCutoverReadiness {
    pub ready: bool,
    pub evidence_ready: bool,
    pub summary_ready: bool,
    pub protocol_matches: bool,
    pub readiness_matches: bool,
    pub database_opened_matches: bool,
    pub probe_count_matches: bool,
    pub passed_probe_count_matches: bool,
    pub failed_probe_count_matches: bool,
    pub required_route_count_matches: bool,
    pub covered_route_count_matches: bool,
    pub covered_routes_matches: bool,
    pub required_routes_covered_matches: bool,
    pub route_coverage_ready_matches: bool,
    pub evidence_route_catalog_version_ready: bool,
    pub summary_route_catalog_version_ready: bool,
    pub evidence_route_catalog_digest_ready: bool,
    pub summary_route_catalog_digest_ready: bool,
    pub route_catalog_version_matches: bool,
    pub route_catalog_digest_matches: bool,
    pub blocker_codes: Vec<String>,
}

impl IntegrationBundleProtocolCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.protocol_matches
    }
}

impl ReplacementSummaryProtocolCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.protocol_matches
            && self.graph_layer_replacement_scope_ready
            && self.search_projection_replacement_scope_ready
            && self.content_store_out_of_scope
            && self.large_blob_store_out_of_scope
    }
}

impl SkeinSubmoduleCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.present && self.path_present && self.commit_present
    }
}

impl LegacyCoexistenceCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.old_database_retained && self.mode_safe && self.old_database_not_deleted
    }
}

impl ContentStoreBoundaryCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.present
            && self.engine_sqlite
            && self.messages_available
            && self.source_chunks_available
    }
}

impl PreviousWrapperPreflightCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.ready
    }
}

impl LibraryReadinessCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.protocol_matches
            && self.present
            && self.ready
            && self.production_path_ready
            && self.production_path_in_process
            && self.production_path_cli_not_required
            && self.production_path_env_control_plane_not_required
            && self.production_path_spawned_helper_not_required
            && self.ready_area_count_present
            && self.blocked_area_count_zero
            && self.redaction_ready
            && self.query_text_redacted
            && self.parameters_redacted
            && self.local_paths_redacted
            && self.graph_opened
            && self.search_projection_opened
            && self.graph_ready
            && self.query_ready
            && self.storage_ready
            && self.background_ready
            && self.query_family_ready
            && self.graph_route_ready
            && self.search_route_ownership_ready
            && self.search_projection_ready
            && self.search_projection_shadow_ready
            && self.search_candidate_shadow_ready
            && self.workload_fixture_ready
            && self.production_resource_profile_ready
    }
}

impl CutoverControlsReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.protocol_matches
            && self.ready
            && self.graph_reads_skein
            && self.graph_read_effective
            && self.graph_production_status_effective
            && self.search_reads_skein
            && self.search_read_effective
            && self.search_production_status_effective
            && self.dual_writes_enabled
            && self.projection_catch_up_enabled
            && self.initial_import_safe
            && self.initial_import_safe_for_read_cutover
            && self.query_text_redacted
            && self.parameters_redacted
            && self.local_paths_redacted
    }
}

impl OperationsReadinessCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.protocol_matches
            && self.present
            && self.ready
            && self.graph_open
            && self.graph_writable
            && self.search_projection_open
            && self.search_projection_not_stale
            && self.storage_lifecycle_ready
            && self.storage_lifecycle_action_ready
            && self.storage_recovery_ready
            && self.slow_query_ready
            && self.background_maintenance_ready
            && self.query_text_redacted
            && self.parameters_redacted
            && self.local_paths_redacted
    }
}

impl GraphReplacementCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.production_cutover_ready
            && self.shadow_evidence_ready
            && self.dual_engine_evidence_present
            && self.dual_engine_evidence_ready
            && self.dual_engine_evidence_consistent
            && self.source_mutation_protocol_matches
            && self.source_mutation_ready
            && self.source_mutation_required_family_count_matches
            && self.source_mutation_evidence_family_count_matches
            && self.source_mutation_ready_family_count_matches
            && self.source_mutation_missing_required_families_empty
            && self.source_mutation_blocker_codes_empty
    }
}

impl QueryFamilyReplacementCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.required_query_families_present
            && self.missing_required_query_families_empty
            && self.blocked_query_families_empty
            && self.min_replacement_readiness_full
    }
}

impl SearchProjectionCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.evidence_protocol_matches
            && self.evidence_ready
            && self.fts_ready
            && self.vector_ready
            && self.document_identity_ready
            && self.incremental_update_ready
            && self.predicate_pushdown_ready
            && self.production_filter_pruning_ready
            && self.compressed_vector_projection_required
            && self.compressed_vector_projection_ready
            && self.shadow_present
            && self.shadow_protocol_matches
            && self.shadow_evidence_source_matches
            && self.shadow_ready
            && self.shadow_document_count_parity
            && self.shadow_document_identity_parity
            && self.shadow_table_parity_ready
            && self.shadow_embedding_identity_parity
            && self.shadow_incremental_watermark_parity
            && self.shadow_pushdown_ready
            && self.shadow_descriptor_scan_filter_fields_ready
            && self.shadow_document_pruning_ready
            && self.shadow_pruning_candidate_count_ready
            && self.shadow_pruned_document_count_positive
            && self.shadow_scanned_document_count_positive
            && self.primary_scan_filter_fields_ready
            && self.shadow_scan_filter_fields_ready
            && self.shadow_descriptor_field_summaries_ready
    }
}

impl SearchCandidateCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.protocol_matches
            && self.evidence_source_matches
            && self.route_matches
            && self.ready
            && self.primary_engine_matches
            && self.candidate_count_parity
            && self.row_count_parity
            && self.text_retriever_ready
            && self.vector_retriever_ready
            && self.fts_top_k_overlap_ready
            && self.vector_top_k_overlap_ready
            && self.source_chunk_identity_ready
            && self.fail_soft_observed
            && self.projection_marker_status_visible
            && self.projection_watermark_ready
            && self.embedding_identity_ready
            && self.candidate_identity_ready
            && self.filter_pushdown_ready
            && self.filter_pushdown_field_summary_present
            && self.filter_pushdown_required_fields_ready
            && self.shadow_scan_filter_pushdown_ready
            && self.shadow_scan_field_pruning_ready
            && self.shadow_scan_field_summary_present
    }
}

impl BoundedReadCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.present
            && self.protocol_matches
            && self.ready
            && self.max_rows_present
            && self.mode_matches
            && self.execution_cap_matches
            && self.estimated_payload_bytes_present
            && self.max_estimated_payload_bytes_present
            && self.payload_budget_not_exceeded
            && self.row_limit_enforced_before_output
            && self.operator_row_cap_enabled
            && self.blocking_operator_memory_reports_complete
            && self.blocking_operator_memory_within_budget
            && self.spill_within_budget
            && self.streaming_evidence_present
            && self.route_catalog_version_matches
            && self.route_catalog_digest_present
            && self.route_coverage_ready
    }
}

impl BoundedReadAlignmentCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.evidence_ready
            && self.ready
            && self.alignment_evidence_ready
            && self.summary_ready
            && self.protocol_matches
            && self.readiness_matches
            && self.mode_matches
            && self.max_rows_matches
            && self.estimated_payload_bytes_matches
            && self.max_estimated_payload_bytes_matches
            && self.payload_budget_exceeded_matches
            && self.streaming_matches
            && self.covered_routes_matches
            && self.evidence_route_catalog_version_ready
            && self.summary_route_catalog_version_ready
            && self.evidence_route_catalog_digest_ready
            && self.summary_route_catalog_digest_ready
            && self.route_catalog_version_matches
            && self.route_catalog_digest_matches
    }
}

impl GraphRouteCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.protocol_matches
            && self.evidence_protocol_matches
            && self.evidence_ready
            && self.route_count_present
            && self.required_route_count_matches
            && self.route_coverage_ready
            && self.missing_required_routes_empty
            && self.query_runtime_route_count_matches
            && self.query_runtime_report_count_ready
            && self.query_plan_profile_summary_ready
            && self.relationship_property_pruning_summary_ready
            && self.missing_query_runtime_routes_empty
            && self.route_query_runtime_ready
            && self.route_primary_ready
            && self.primary_ready_route_count_matches
            && self.route_primary_blocker_codes_empty
            && self.evidence_route_coverage_present
            && self.evidence_route_coverage_matches
            && self.evidence_route_coverage_blocker_codes_empty
            && self.route_query_profiles_ready
    }
}

impl GraphRouteAlignmentCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.ready
            && self.evidence_protocol_matches
            && self.evidence_ready
            && self.evidence_route_primary_ready
            && self.summary_route_primary_ready
            && self.route_primary_ready_matches
            && self.route_query_plan_evidence_ready_matches
            && self.route_query_profile_evidence_ready_matches
            && self.route_query_api_behavior_evidence_ready_matches
            && self.route_relationship_property_pruning_evidence_ready_matches
            && self.relationship_property_pruning_required_count_matches
            && self.relationship_property_pruning_report_count_matches
            && self.primary_ready_routes_match
            && self.evidence_required_routes_covered
            && self.summary_required_routes_covered
            && self.evidence_route_catalog_version_ready
            && self.summary_route_catalog_version_ready
            && self.evidence_route_catalog_digest_ready
            && self.summary_route_catalog_digest_ready
            && self.route_catalog_version_matches
            && self.route_catalog_digest_matches
    }
}

impl GraphRouteParityAlignmentCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.ready
            && self.required_route_count_present
            && self.ready_route_count_matches
            && self.missing_routes_empty
            && self.not_ready_routes_empty
            && self.route_mismatch_routes_empty
            && self.protocol_mismatch_routes_empty
            && self.blocker_routes_empty
    }
}

impl SearchRouteOwnershipAlignmentCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.ready
            && self.evidence_ready
            && self.summary_ready
            && self.protocol_matches
            && self.readiness_matches
            && self.production_cutover_ready_matches
            && self.require_all_skein_matches
            && self.required_route_count_matches
            && self.explicit_route_count_matches
            && self.skein_route_count_matches
            && self.lancedb_route_count_matches
            && self.missing_required_routes_matches
            && self.lancedb_routes_matches
            && self.blocker_codes_match
    }
}

impl ActiveSearchRouteReadinessAlignmentCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.ready
            && self.evidence_ready
            && self.summary_ready
            && self.protocol_matches
            && self.readiness_matches
            && self.production_cutover_ready_matches
            && self.require_all_skein_matches
            && self.required_route_count_matches
            && self.evidence_route_count_matches
            && self.ready_route_count_matches
            && self.skein_route_count_matches
            && self.lancedb_handle_count_matches
            && self.missing_required_routes_matches
            && self.non_skein_routes_matches
            && self.lancedb_handle_routes_matches
            && self.candidate_not_ready_routes_matches
            && self.candidate_identity_not_ready_routes_matches
            && self.embedding_identity_not_ready_routes_matches
            && self.zero_vector_semantics_not_ready_routes_matches
            && self.cjk_tokenization_not_ready_routes_matches
            && self.metadata_pushdown_not_ready_routes_matches
            && self.ranking_window_not_ready_routes_matches
            && self.ranking_not_ready_routes_matches
            && self.fail_soft_not_ready_routes_matches
            && self.fail_soft_reason_codes_not_ready_routes_matches
            && self.repair_rebuild_markers_not_ready_routes_matches
            && self.blocker_codes_match
    }
}

impl RouteOwnershipCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.protocol_matches
            && self.ready
            && self.production_cutover_ready
            && self.require_all_skein
            && self.required_route_count_matches
            && self.explicit_route_count_matches
            && self.skein_route_count_matches
            && self.legacy_route_count_zero
            && self.missing_required_routes_empty
            && self.unknown_routes_empty
            && self.duplicate_routes_empty
            && self.conflicting_routes_empty
            && self.skein_not_ready_routes_empty
            && self.route_readiness_present
            && self.route_readiness_ready
            && self.search_route_projection_evidence_present
            && self.route_catalog_version_matches
            && self.route_catalog_digest_matches
            && self.blocker_codes_empty
    }
}

impl QueryRuntimePreflightCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.protocol_matches
            && self.ready
            && self.database_opened
            && self.redaction_ready
            && self.rows_redacted
            && self.parameters_redacted
            && self.local_paths_redacted
            && self.raw_errors_redacted
            && self.probe_count_present
            && self.probe_counts_match
            && self.failed_probe_count_zero
            && self.route_coverage_ready
            && self.route_catalog_version_matches
            && self.route_catalog_digest_present
            && self.probe_details_ready
    }
}

impl QueryRuntimePreflightAlignmentCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.ready
            && self.evidence_ready
            && self.summary_ready
            && self.protocol_matches
            && self.readiness_matches
            && self.database_opened_matches
            && self.probe_count_matches
            && self.passed_probe_count_matches
            && self.failed_probe_count_matches
            && self.required_route_count_matches
            && self.covered_route_count_matches
            && self.covered_routes_matches
            && self.required_routes_covered_matches
            && self.route_coverage_ready_matches
            && self.evidence_route_catalog_version_ready
            && self.summary_route_catalog_version_ready
            && self.evidence_route_catalog_digest_ready
            && self.summary_route_catalog_digest_ready
            && self.route_catalog_version_matches
            && self.route_catalog_digest_matches
    }
}

impl BackgroundMaintenanceCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.required
            && self.ready
            && self.protocol_matches
            && self.executable_search_projection_graph_delta_count_present
            && self.admitted_search_projection_graph_delta_count_present
            && self.deferred_search_projection_graph_delta_count_present
            && self.rejected_search_projection_graph_delta_count_present
            && self.executable_search_projection_graph_delta_operations_present
            && self.admitted_search_projection_graph_delta_operations_present
            && self.max_search_projection_graph_delta_complete_through_graph_commit_epoch_present
            && self.foreground_admission_probe_ready
            && self.memory_pressure_ready
            && self.memory_budget_bytes_present
            && self.estimated_memory_bytes_present
    }
}

impl StorageRecoveryCutoverReadiness {
    pub fn evidence_ready(&self) -> bool {
        self.required
            && self.ready
            && self.protocol_matches
            && self.durable
            && self.checkpoint_boundary_present
            && self.wal_replay_bounded
            && self.replay_boundary_consistent
            && self.torn_tail_clean
    }
}

pub fn nowledge_mem_integration_readiness_usage() -> String {
    "nowledge-mem-integration-readiness requires [--require-ready] <integration-bundle-json>"
        .to_string()
}

pub fn run_nowledge_mem_integration_readiness(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            path => {
                if args.next().is_some() {
                    return Err(SkeinError::Semantic(
                        nowledge_mem_integration_readiness_usage(),
                    ));
                }
                let bundle = read_json_file(Path::new(path))?;
                return Ok((
                    nowledge_mem_integration_readiness_json(&bundle),
                    require_ready,
                ));
            }
        }
    }
    Err(SkeinError::Semantic(
        nowledge_mem_integration_readiness_usage(),
    ))
}

pub fn nowledge_mem_integration_readiness_json(bundle: &serde_json::Value) -> serde_json::Value {
    nowledge_mem_integration_readiness(bundle).json()
}

pub fn nowledge_mem_final_cutover_preflight_json(bundle: &serde_json::Value) -> serde_json::Value {
    nowledge_mem_final_cutover_preflight(bundle).json()
}

pub fn nowledge_mem_final_cutover_preflight(
    bundle: &serde_json::Value,
) -> NowledgeMemFinalCutoverPreflightReport {
    let integration = nowledge_mem_integration_readiness(bundle);
    let graph_replacement = graph_replacement_cutover_readiness(bundle);
    let failed_evidence_fields = integration
        .checks
        .iter()
        .flat_map(|check| check.failed_evidence_fields.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let ready_check_count = integration
        .checks
        .iter()
        .filter(|check| check.ready)
        .count();
    let check_count = integration.checks.len();
    let startup_ready = integration_check_group_ready(&integration, STARTUP_CHECKS);
    let route_coverage_ready = integration_check_group_ready(&integration, ROUTE_COVERAGE_CHECKS);
    let graph_replacement_ready =
        integration_check_group_ready(&integration, GRAPH_REPLACEMENT_CHECKS);
    let search_projection_ready =
        integration_check_group_ready(&integration, SEARCH_PROJECTION_CHECKS);
    let storage_recovery_ready =
        integration_check_group_ready(&integration, STORAGE_RECOVERY_CHECKS);
    let background_qos_ready = integration_check_group_ready(&integration, BACKGROUND_QOS_CHECKS);
    let blackbox_ready = integration_check_group_ready(&integration, BLACKBOX_CHECKS);
    let library_only_ready = integration_check_group_ready(&integration, LIBRARY_ONLY_CHECKS);
    let replacement_summary_production_cutover_ready =
        graph_replacement.production_cutover_ready && graph_replacement.evidence_ready();
    let category_readiness = FinalCutoverCategoryReadiness {
        startup_ready,
        route_coverage_ready,
        graph_replacement_ready,
        search_projection_ready,
        storage_recovery_ready,
        background_qos_ready,
        blackbox_ready,
        library_only_ready,
        replacement_summary_production_cutover_ready,
    };
    let blocking_categories = final_cutover_blocking_categories(&category_readiness);
    let next_action_names = integration
        .next_actions
        .iter()
        .map(|action| action.action.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let next_action_count = next_action_names.len();
    NowledgeMemFinalCutoverPreflightReport {
        protocol: NOWLEDGE_MEM_FINAL_CUTOVER_PREFLIGHT_PROTOCOL.to_string(),
        production_cutover_ready: integration.ready
            && replacement_summary_production_cutover_ready
            && blocking_categories.is_empty(),
        integration_ready: integration.ready,
        replacement_summary_production_cutover_ready,
        startup_ready: category_readiness.startup_ready,
        route_coverage_ready: category_readiness.route_coverage_ready,
        graph_replacement_ready: category_readiness.graph_replacement_ready,
        search_projection_ready: category_readiness.search_projection_ready,
        storage_recovery_ready: category_readiness.storage_recovery_ready,
        background_qos_ready: category_readiness.background_qos_ready,
        blackbox_ready: category_readiness.blackbox_ready,
        library_only_ready: category_readiness.library_only_ready,
        check_count,
        ready_check_count,
        failed_check_count: check_count.saturating_sub(ready_check_count),
        next_action_count,
        next_action_names,
        blocking_categories,
        failed_checks: integration.failed_checks,
        failed_evidence_fields,
        blocker_codes: integration.blocker_codes,
    }
}

struct FinalCutoverCategoryReadiness {
    startup_ready: bool,
    route_coverage_ready: bool,
    graph_replacement_ready: bool,
    search_projection_ready: bool,
    storage_recovery_ready: bool,
    background_qos_ready: bool,
    blackbox_ready: bool,
    library_only_ready: bool,
    replacement_summary_production_cutover_ready: bool,
}

fn integration_check_group_ready(
    report: &NowledgeMemIntegrationReadinessReport,
    names: &[&str],
) -> bool {
    names.iter().all(|name| {
        report
            .checks
            .iter()
            .find(|check| check.name == *name)
            .is_some_and(|check| check.ready)
    })
}

fn final_cutover_blocking_categories(readiness: &FinalCutoverCategoryReadiness) -> Vec<String> {
    [
        (FINAL_CATEGORY_STARTUP, readiness.startup_ready),
        (
            FINAL_CATEGORY_ROUTE_COVERAGE,
            readiness.route_coverage_ready,
        ),
        (
            FINAL_CATEGORY_GRAPH_REPLACEMENT,
            readiness.graph_replacement_ready,
        ),
        (
            FINAL_CATEGORY_SEARCH_PROJECTION,
            readiness.search_projection_ready,
        ),
        (
            FINAL_CATEGORY_STORAGE_RECOVERY,
            readiness.storage_recovery_ready,
        ),
        (
            FINAL_CATEGORY_BACKGROUND_QOS,
            readiness.background_qos_ready,
        ),
        (FINAL_CATEGORY_BLACKBOX, readiness.blackbox_ready),
        (FINAL_CATEGORY_LIBRARY_ONLY, readiness.library_only_ready),
        (
            FINAL_CATEGORY_REPLACEMENT_SUMMARY_CUTOVER,
            readiness.replacement_summary_production_cutover_ready,
        ),
    ]
    .into_iter()
    .filter_map(|(category, ready)| (!ready).then_some(category.to_string()))
    .collect()
}

pub fn nowledge_mem_integration_readiness(
    bundle: &serde_json::Value,
) -> NowledgeMemIntegrationReadinessReport {
    let blackbox_manifest = bundle
        .get("blackbox_manifest")
        .unwrap_or(&serde_json::Value::Null);
    let blackbox_readiness = blackbox_readiness_from_manifest_json(blackbox_manifest);
    let integration_bundle_protocol_readiness =
        integration_bundle_protocol_cutover_readiness(bundle);
    let replacement_summary_protocol_readiness =
        replacement_summary_protocol_cutover_readiness(bundle);
    let skein_submodule_readiness = skein_submodule_cutover_readiness(bundle);
    let legacy_coexistence_readiness = legacy_coexistence_cutover_readiness(bundle);
    let content_store_readiness = content_store_boundary_cutover_readiness(bundle);
    let previous_wrapper_readiness = previous_wrapper_preflight_cutover_readiness(bundle);
    let library_readiness = library_readiness_cutover_readiness(bundle);
    let cutover_controls = cutover_controls_readiness(bundle);
    let graph_replacement_readiness = graph_replacement_cutover_readiness(bundle);
    let query_family_readiness = query_family_replacement_cutover_readiness(bundle);
    let search_projection_readiness = search_projection_cutover_readiness(bundle);
    let search_candidate_readiness = search_candidate_cutover_readiness(bundle);
    let bounded_read_readiness = bounded_read_cutover_readiness(bundle);
    let bounded_read_alignment_readiness = bounded_read_alignment_cutover_readiness(bundle);
    let graph_route_readiness = graph_route_cutover_readiness(bundle);
    let graph_route_alignment_readiness = graph_route_alignment_cutover_readiness(bundle);
    let graph_route_parity_alignment_readiness =
        graph_route_parity_alignment_cutover_readiness(bundle);
    let route_ownership_readiness = route_ownership_cutover_readiness(bundle);
    let search_route_ownership_alignment_readiness =
        search_route_ownership_alignment_cutover_readiness(
            bundle,
            "replacement_summary_search_route_ownership_alignment",
        );
    let active_search_route_ownership_alignment_readiness =
        search_route_ownership_alignment_cutover_readiness(
            bundle,
            "replacement_summary_active_search_route_ownership_alignment",
        );
    let active_search_route_readiness_alignment_readiness =
        active_search_route_readiness_alignment_cutover_readiness(bundle);
    let query_runtime_readiness = query_runtime_preflight_cutover_readiness(bundle);
    let query_runtime_alignment_readiness =
        query_runtime_preflight_alignment_cutover_readiness(bundle);
    let storage_recovery_readiness = storage_recovery_cutover_readiness(bundle);
    let operations_readiness = operations_readiness_cutover_readiness(bundle);
    let background_maintenance_readiness = background_maintenance_cutover_readiness(bundle);
    let checks = vec![
        check_named_conditions(
            "integration_bundle_protocol",
            integration_bundle_protocol_cutover_conditions(&integration_bundle_protocol_readiness),
            Vec::new(),
        ),
        check_named_conditions(
            "replacement_summary_protocol",
            replacement_summary_protocol_cutover_conditions(
                &replacement_summary_protocol_readiness,
            ),
            Vec::new(),
        ),
        check_named_conditions(
            "skein_submodule",
            skein_submodule_cutover_conditions(&skein_submodule_readiness),
            skein_submodule_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "legacy_coexistence",
            legacy_coexistence_cutover_conditions(&legacy_coexistence_readiness),
            legacy_coexistence_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "content_store_boundary",
            content_store_boundary_cutover_conditions(&content_store_readiness),
            content_store_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "previous_wrapper_preflight",
            previous_wrapper_preflight_cutover_conditions(&previous_wrapper_readiness),
            previous_wrapper_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "graph_replacement_evidence",
            graph_replacement_cutover_conditions(&graph_replacement_readiness),
            graph_replacement_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "query_family_replacement_evidence",
            query_family_replacement_cutover_conditions(&query_family_readiness),
            query_family_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "search_projection_replacement_evidence",
            search_projection_cutover_conditions(&search_projection_readiness),
            search_projection_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "search_candidate_primary_evidence",
            search_candidate_cutover_conditions(&search_candidate_readiness),
            search_candidate_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "bounded_read_evidence",
            bounded_read_cutover_conditions(&bounded_read_readiness),
            bounded_read_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "bounded_read_evidence_alignment",
            bounded_read_alignment_cutover_conditions(&bounded_read_alignment_readiness),
            bounded_read_alignment_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "graph_route_readiness",
            graph_route_cutover_conditions(&graph_route_readiness),
            graph_route_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "graph_route_readiness_alignment",
            graph_route_alignment_cutover_conditions(&graph_route_alignment_readiness),
            graph_route_alignment_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "graph_route_parity_alignment",
            graph_route_parity_alignment_cutover_conditions(
                &graph_route_parity_alignment_readiness,
            ),
            graph_route_parity_alignment_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "route_ownership",
            route_ownership_cutover_conditions(&route_ownership_readiness),
            route_ownership_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "search_route_ownership_alignment",
            search_route_ownership_alignment_cutover_conditions(
                &search_route_ownership_alignment_readiness,
                "replacement_summary_search_route_ownership_alignment",
            ),
            search_route_ownership_alignment_readiness
                .blocker_codes
                .clone(),
        ),
        check_named_conditions(
            "active_search_route_ownership_alignment",
            search_route_ownership_alignment_cutover_conditions(
                &active_search_route_ownership_alignment_readiness,
                "replacement_summary_active_search_route_ownership_alignment",
            ),
            active_search_route_ownership_alignment_readiness
                .blocker_codes
                .clone(),
        ),
        check_named_conditions(
            "active_search_route_readiness_alignment",
            active_search_route_readiness_alignment_cutover_conditions(
                &active_search_route_readiness_alignment_readiness,
            ),
            active_search_route_readiness_alignment_readiness
                .blocker_codes
                .clone(),
        ),
        check_named_conditions(
            "query_runtime_preflight",
            query_runtime_preflight_cutover_conditions(&query_runtime_readiness),
            query_runtime_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "query_runtime_preflight_alignment",
            query_runtime_preflight_alignment_cutover_conditions(
                &query_runtime_alignment_readiness,
            ),
            query_runtime_alignment_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "library_readiness",
            library_readiness_cutover_conditions(&library_readiness),
            library_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "cutover_controls",
            cutover_controls_conditions(&cutover_controls),
            cutover_controls.blocker_codes.clone(),
        ),
        check_named_conditions(
            "background_maintenance_evidence",
            background_maintenance_cutover_conditions(&background_maintenance_readiness),
            background_maintenance_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "storage_recovery_evidence",
            storage_recovery_cutover_conditions(&storage_recovery_readiness),
            storage_recovery_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "operations_readiness",
            operations_readiness_cutover_conditions(&operations_readiness),
            operations_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "blackbox_redaction",
            blackbox_redaction_cutover_conditions(&blackbox_readiness),
            blackbox_readiness.blocker_codes.clone(),
        ),
        check_named_conditions(
            "blackbox_operational_evidence",
            blackbox_operational_cutover_conditions(&blackbox_readiness),
            blackbox_readiness.blocker_codes.clone(),
        ),
    ];
    let ready = checks.iter().all(|check| check.ready);
    let failed_checks = checks
        .iter()
        .filter(|check| !check.ready)
        .map(|check| check.name.clone())
        .collect::<Vec<_>>();
    let blocker_codes = checks
        .iter()
        .flat_map(|check| check.blocker_codes.iter().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    NowledgeMemIntegrationReadinessReport {
        protocol: NOWLEDGE_MEM_INTEGRATION_READINESS_PROTOCOL.to_string(),
        ready,
        failed_checks,
        checks,
        blocker_codes,
        next_actions: next_actions(
            ready,
            IntegrationGateReadiness {
                integration_bundle: &integration_bundle_protocol_readiness,
                replacement_summary_protocol: &replacement_summary_protocol_readiness,
                submodule: &skein_submodule_readiness,
                legacy_coexistence: &legacy_coexistence_readiness,
                content_store: &content_store_readiness,
                previous_wrapper: &previous_wrapper_readiness,
                library: &library_readiness,
                cutover_controls: &cutover_controls,
                graph_replacement: &graph_replacement_readiness,
                query_family: &query_family_readiness,
                search_projection: &search_projection_readiness,
                search_candidate: &search_candidate_readiness,
                bounded_read: &bounded_read_readiness,
                bounded_read_alignment: &bounded_read_alignment_readiness,
                graph_route: &graph_route_readiness,
                graph_route_alignment: &graph_route_alignment_readiness,
                graph_route_parity_alignment: &graph_route_parity_alignment_readiness,
                route_ownership: &route_ownership_readiness,
                search_route_ownership_alignment: &search_route_ownership_alignment_readiness,
                active_search_route_ownership_alignment:
                    &active_search_route_ownership_alignment_readiness,
                active_search_route_readiness_alignment:
                    &active_search_route_readiness_alignment_readiness,
                query_runtime: &query_runtime_readiness,
                query_runtime_alignment: &query_runtime_alignment_readiness,
                blackbox: &blackbox_readiness,
                storage_recovery: &storage_recovery_readiness,
                operations: &operations_readiness,
                background_maintenance: &background_maintenance_readiness,
            },
        ),
    }
}

fn check_named_conditions(
    name: &'static str,
    conditions: impl IntoIterator<Item = (&'static str, bool)>,
    blocker_codes: Vec<String>,
) -> NowledgeMemIntegrationCheckReport {
    let conditions = conditions.into_iter().collect::<Vec<_>>();
    let evidence_fields = conditions
        .iter()
        .map(|(field, _)| (*field).to_string())
        .collect::<Vec<_>>();
    let failed_evidence_fields = conditions
        .iter()
        .filter_map(|(field, ready)| (!*ready).then_some((*field).to_string()))
        .collect::<Vec<_>>();
    NowledgeMemIntegrationCheckReport {
        name: name.to_string(),
        ready: failed_evidence_fields.is_empty(),
        evidence_fields,
        failed_evidence_fields,
        blocker_codes,
    }
}

fn integration_bundle_protocol_cutover_conditions(
    readiness: &IntegrationBundleProtocolCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![("protocol", readiness.protocol_matches)]
}

fn replacement_summary_protocol_cutover_conditions(
    readiness: &ReplacementSummaryProtocolCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        ("replacement_summary.protocol", readiness.protocol_matches),
        (
            "replacement_summary.replacement_boundaries.graph_layer",
            readiness.graph_layer_replacement_scope_ready,
        ),
        (
            "replacement_summary.replacement_boundaries.search_projection",
            readiness.search_projection_replacement_scope_ready,
        ),
        (
            "replacement_summary.replacement_boundaries.content_store",
            readiness.content_store_out_of_scope,
        ),
        (
            "replacement_summary.replacement_boundaries.large_blob_store",
            readiness.large_blob_store_out_of_scope,
        ),
    ]
}

fn skein_submodule_cutover_conditions(
    readiness: &SkeinSubmoduleCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        ("submodule.present", readiness.present),
        ("submodule.path", readiness.path_present),
        ("submodule.commit", readiness.commit_present),
    ]
}

fn legacy_coexistence_cutover_conditions(
    readiness: &LegacyCoexistenceCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "coexistence.old_database_retained",
            readiness.old_database_retained,
        ),
        ("coexistence.mode", readiness.mode_safe),
        (
            "coexistence.old_database_deleted",
            readiness.old_database_not_deleted,
        ),
    ]
}

fn content_store_boundary_cutover_conditions(
    readiness: &ContentStoreBoundaryCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        ("content_store.present", readiness.present),
        ("content_store.engine", readiness.engine_sqlite),
        (
            "content_store.messages_available",
            readiness.messages_available,
        ),
        (
            "content_store.source_chunks_available",
            readiness.source_chunks_available,
        ),
    ]
}

fn previous_wrapper_preflight_cutover_conditions(
    readiness: &PreviousWrapperPreflightCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![("previous_wrapper_preflight.ready", readiness.ready)]
}

fn route_ownership_cutover_conditions(
    readiness: &RouteOwnershipCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        ("route_ownership.protocol", readiness.protocol_matches),
        ("route_ownership.ready", readiness.ready),
        (
            "route_ownership.production_cutover_ready",
            readiness.production_cutover_ready,
        ),
        (
            "route_ownership.require_all_skein",
            readiness.require_all_skein,
        ),
        (
            "route_ownership.required_route_count",
            readiness.required_route_count_matches,
        ),
        (
            "route_ownership.explicit_route_count",
            readiness.explicit_route_count_matches,
        ),
        (
            "route_ownership.skein_route_count",
            readiness.skein_route_count_matches,
        ),
        (
            "route_ownership.legacy_route_count",
            readiness.legacy_route_count_zero,
        ),
        (
            "route_ownership.missing_required_routes",
            readiness.missing_required_routes_empty,
        ),
        (
            "route_ownership.unknown_routes",
            readiness.unknown_routes_empty,
        ),
        (
            "route_ownership.duplicate_routes",
            readiness.duplicate_routes_empty,
        ),
        (
            "route_ownership.conflicting_routes",
            readiness.conflicting_routes_empty,
        ),
        (
            "route_ownership.skein_not_ready_routes",
            readiness.skein_not_ready_routes_empty,
        ),
        (
            "route_ownership.route_readiness_present",
            readiness.route_readiness_present,
        ),
        (
            "route_ownership.route_readiness_ready",
            readiness.route_readiness_ready,
        ),
        (
            "route_ownership.search_route_projection_evidence_present",
            readiness.search_route_projection_evidence_present,
        ),
        (
            "route_ownership.route_catalog_version",
            readiness.route_catalog_version_matches,
        ),
        (
            "route_ownership.route_catalog_digest",
            readiness.route_catalog_digest_matches,
        ),
        (
            "route_ownership.blocker_codes",
            readiness.blocker_codes_empty,
        ),
    ]
}

fn blackbox_redaction_cutover_conditions(
    readiness: &BlackboxReadinessReport,
) -> Vec<(&'static str, bool)> {
    vec![
        ("blackbox_manifest.protocol", readiness.protocol_ready),
        (
            "blackbox_manifest.artifact_dir_present",
            readiness.artifact_dir_present,
        ),
        (
            "blackbox_manifest.artifact_count",
            readiness.artifact_count_present,
        ),
        ("blackbox_manifest.events_path", readiness.events_path_ready),
        (
            "blackbox_manifest.redaction.raw_query_text_copied",
            readiness.raw_query_text_redacted,
        ),
        (
            "blackbox_manifest.redaction.raw_parameters_copied",
            readiness.raw_parameters_redacted,
        ),
        (
            "blackbox_manifest.redaction.raw_artifact_payloads_copied",
            readiness.raw_artifact_payloads_redacted,
        ),
        (
            "blackbox_manifest.redaction.artifact_paths_are_relative",
            readiness.artifact_paths_relative,
        ),
    ]
}

fn blackbox_operational_cutover_conditions(
    readiness: &BlackboxReadinessReport,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "blackbox_manifest.artifacts.slow-query-log.jsonl",
            readiness.slow_query_log_present,
        ),
        (
            "blackbox_manifest.artifacts.slow-query-log.jsonl.jsonl",
            readiness.slow_query_log_jsonl_summary_present,
        ),
        (
            "blackbox_manifest.artifacts.background-maintenance.json",
            readiness.background_maintenance_present,
        ),
        (
            "blackbox_manifest.artifacts.background-maintenance.json.background_qos",
            readiness.background_qos_summary_ready,
        ),
    ]
}

struct IntegrationGateReadiness<'a> {
    integration_bundle: &'a IntegrationBundleProtocolCutoverReadiness,
    replacement_summary_protocol: &'a ReplacementSummaryProtocolCutoverReadiness,
    submodule: &'a SkeinSubmoduleCutoverReadiness,
    legacy_coexistence: &'a LegacyCoexistenceCutoverReadiness,
    content_store: &'a ContentStoreBoundaryCutoverReadiness,
    previous_wrapper: &'a PreviousWrapperPreflightCutoverReadiness,
    library: &'a LibraryReadinessCutoverReadiness,
    cutover_controls: &'a CutoverControlsReadiness,
    graph_replacement: &'a GraphReplacementCutoverReadiness,
    query_family: &'a QueryFamilyReplacementCutoverReadiness,
    search_projection: &'a SearchProjectionCutoverReadiness,
    search_candidate: &'a SearchCandidateCutoverReadiness,
    bounded_read: &'a BoundedReadCutoverReadiness,
    bounded_read_alignment: &'a BoundedReadAlignmentCutoverReadiness,
    graph_route: &'a GraphRouteCutoverReadiness,
    graph_route_alignment: &'a GraphRouteAlignmentCutoverReadiness,
    graph_route_parity_alignment: &'a GraphRouteParityAlignmentCutoverReadiness,
    route_ownership: &'a RouteOwnershipCutoverReadiness,
    search_route_ownership_alignment: &'a SearchRouteOwnershipAlignmentCutoverReadiness,
    active_search_route_ownership_alignment: &'a SearchRouteOwnershipAlignmentCutoverReadiness,
    active_search_route_readiness_alignment:
        &'a ActiveSearchRouteReadinessAlignmentCutoverReadiness,
    query_runtime: &'a QueryRuntimePreflightCutoverReadiness,
    query_runtime_alignment: &'a QueryRuntimePreflightAlignmentCutoverReadiness,
    blackbox: &'a BlackboxReadinessReport,
    storage_recovery: &'a StorageRecoveryCutoverReadiness,
    operations: &'a OperationsReadinessCutoverReadiness,
    background_maintenance: &'a BackgroundMaintenanceCutoverReadiness,
}

fn next_actions(
    ready: bool,
    readiness: IntegrationGateReadiness<'_>,
) -> Vec<NowledgeMemIntegrationNextAction> {
    if ready {
        return Vec::new();
    }
    let mut actions = Vec::new();
    if !readiness.integration_bundle.evidence_ready() {
        actions.push(next_action(
            "regenerate_skein_integration_bundle",
            "Nowledge Mem integration readiness requires the versioned integration bundle protocol",
            ["protocol"],
        ));
    }
    if !readiness.submodule.evidence_ready() {
        actions.push(next_action(
            "add_skein_submodule",
            "Nowledge Mem must depend on Skein as a submodule instead of copying sources",
            ["submodule.present", "submodule.path", "submodule.commit"],
        ));
    }
    if !readiness.legacy_coexistence.evidence_ready() {
        actions.push(next_action(
            "enable_side_by_side_coexistence",
            "Kuzu/Ladybug and LanceDB must remain available while Skein runs in shadow",
            [
                "coexistence.old_database_retained",
                "coexistence.mode",
                "coexistence.old_database_deleted",
            ],
        ));
    }
    if !readiness.content_store.evidence_ready() {
        actions.push(next_action(
            "attach_content_store_evidence",
            "messages and source chunks still come from content.db during replacement validation",
            [
                "content_store.present",
                "content_store.engine",
                "content_store.messages_available",
                "content_store.source_chunks_available",
            ],
        ));
    }
    if !readiness.previous_wrapper.evidence_ready() {
        actions.push(next_action(
            "run_previous_wrapper_preflight",
            "the previous-wrapper release bundle must pass before Mem cutover",
            ["previous_wrapper_preflight.ready"],
        ));
    }
    if !readiness.graph_replacement.evidence_ready() {
        actions.push(next_action(
            "produce_replacement_summary",
            "graph and search replacement evidence must be production-ready",
            [
                "replacement_summary.production_cutover_ready",
                "replacement_summary.blocking_categories",
                "replacement_summary.missing_evidence",
            ],
        ));
    }
    if !readiness.replacement_summary_protocol.evidence_ready() {
        actions.push(next_action(
            "produce_replacement_summary",
            "replacement summary must use the Skein protocol and explicit replacement boundaries",
            [
                "replacement_summary.protocol",
                "replacement_summary.replacement_boundaries.graph_layer",
                "replacement_summary.replacement_boundaries.search_projection",
                "replacement_summary.replacement_boundaries.content_store",
                "replacement_summary.replacement_boundaries.large_blob_store",
            ],
        ));
    }
    if !readiness.query_family.evidence_ready() {
        actions.push(next_action(
            "close_required_query_families",
            "Nowledge Mem cutover requires explicit readiness for every required query family",
            [
                "replacement_summary.replacement_readiness_family_summary.required_query_families",
                "replacement_summary.replacement_readiness_family_summary.missing_required_query_families",
                "replacement_summary.replacement_readiness_family_summary.blocked_query_families",
                "replacement_summary.replacement_readiness_family_summary.min_replacement_readiness_per_million",
            ],
        ));
    }
    if !readiness.search_projection.evidence_ready() {
        actions.push(next_action(
            "attach_search_projection_replacement_evidence",
            "LanceDB replacement evidence must prove FTS, vector, incremental, predicate pushdown, compressed projection, and shadow parity",
            [
                "replacement_summary.search_projection_evidence.ready",
                "replacement_summary.search_projection_evidence.protocol",
                "replacement_summary.search_projection_evidence.fts_ready",
                "replacement_summary.search_projection_evidence.vector_ready",
                "replacement_summary.search_projection_evidence.document_identity_ready",
                "replacement_summary.search_projection_evidence.incremental_update_ready",
                "replacement_summary.search_projection_evidence.predicate_pushdown_ready",
                "replacement_summary.search_projection_evidence.production_filter_pruning_ready",
                "replacement_summary.search_projection_evidence.compressed_vector_projection_required",
                "replacement_summary.search_projection_evidence.compressed_vector_projection_ready",
                "replacement_summary.search_projection_shadow_evidence.protocol",
                "replacement_summary.search_projection_shadow_evidence.evidence_source",
                "replacement_summary.search_projection_shadow_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.document_count_parity",
                "replacement_summary.search_projection_shadow_evidence.document_identity_parity",
                "replacement_summary.search_projection_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if !readiness.search_candidate.evidence_ready() {
        actions.push(next_action(
            "enable_skein_search_candidate_primary_reads",
            "LanceDB replacement must prove memory-hybrid candidate reads are served by Skein before Mem cutover",
            [
                "search_candidate_shadow_evidence.protocol",
                "search_candidate_shadow_evidence.evidence_source",
                "search_candidate_shadow_evidence.route",
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
                "search_candidate_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if !readiness.bounded_read.evidence_ready() {
        actions.push(next_action(
            "attach_bounded_read_profile",
            "Skein read replacement must prove bounded execution before Mem cutover",
            [
                "replacement_summary.bounded_read_evidence.present",
                "replacement_summary.bounded_read_evidence.protocol",
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.mode",
                "replacement_summary.bounded_read_evidence.max_rows",
                "replacement_summary.bounded_read_evidence.execution_row_cap",
                "replacement_summary.bounded_read_evidence.estimated_payload_bytes",
                "replacement_summary.bounded_read_evidence.max_estimated_payload_bytes",
                "replacement_summary.bounded_read_evidence.payload_budget_exceeded",
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output",
                "replacement_summary.bounded_read_evidence.operator_row_cap_enabled",
                "replacement_summary.bounded_read_evidence.blocking_operator_count",
                "replacement_summary.bounded_read_evidence.streaming",
                "replacement_summary.bounded_read_evidence.blocker_codes",
            ],
        ));
    }
    if !readiness.bounded_read_alignment.evidence_ready() {
        actions.push(next_action(
            "regenerate_bounded_read_alignment",
            "live bounded-read evidence must match the replacement summary before Mem cutover",
            [
                "bounded_read_evidence.ready",
                "replacement_summary_bounded_read_alignment.ready",
                "replacement_summary_bounded_read_alignment.evidence_ready",
                "replacement_summary_bounded_read_alignment.summary_ready",
                "replacement_summary_bounded_read_alignment.blocker_codes",
            ],
        ));
    }
    if !readiness.graph_route.evidence_ready() {
        actions.push(next_action(
            "attach_graph_route_readiness_evidence",
            "Nowledge Mem graph cutover requires route-level primary-read readiness evidence",
            [
                "graph_route_readiness.protocol",
                "graph_route_readiness.evidence_protocol",
                "graph_route_readiness.evidence_ready",
                "graph_route_readiness.route_count",
                "graph_route_readiness.required_route_count",
                "graph_route_readiness.route_coverage",
                "graph_route_readiness.missing_required_routes",
                "graph_route_readiness.query_runtime_route_count",
                "graph_route_readiness.query_runtime_report_count",
                "graph_route_readiness.query_runtime_plan_profile_counts",
                "graph_route_readiness.relationship_property_pruning_counts",
                "graph_route_readiness.missing_query_runtime_routes",
                "graph_route_readiness.route_query_runtime_ready",
                "graph_route_readiness.route_primary_ready",
                "graph_route_readiness.primary_ready_route_count",
                "graph_route_readiness.route_primary_blocker_codes",
                "graph_route_readiness.evidence_route_coverage_present",
                "graph_route_readiness.evidence_route_coverage_matches",
                "graph_route_readiness.evidence_route_coverage_blocker_codes",
                "graph_route_readiness.routes",
            ],
        ));
    }
    if !readiness.graph_route_alignment.evidence_ready() {
        actions.push(next_action(
            "regenerate_graph_route_readiness_alignment",
            "live graph route primary-read readiness must match the replacement summary before Mem cutover",
            [
                "replacement_summary_graph_route_alignment.ready",
                "replacement_summary_graph_route_alignment.evidence_protocol_matches",
                "replacement_summary_graph_route_alignment.evidence_ready",
                "replacement_summary_graph_route_alignment.evidence_route_primary_ready",
                "replacement_summary_graph_route_alignment.summary_route_primary_ready",
                "replacement_summary_graph_route_alignment.route_primary_ready_matches",
                "replacement_summary_graph_route_alignment.route_query_plan_evidence_ready_matches",
                "replacement_summary_graph_route_alignment.route_query_profile_evidence_ready_matches",
                "replacement_summary_graph_route_alignment.route_relationship_property_pruning_evidence_ready_matches",
                "replacement_summary_graph_route_alignment.relationship_property_pruning_required_count_matches",
                "replacement_summary_graph_route_alignment.relationship_property_pruning_report_count_matches",
                "replacement_summary_graph_route_alignment.primary_ready_routes_match",
                "replacement_summary_graph_route_alignment.blocker_codes",
            ],
        ));
    }
    if !readiness.graph_route_parity_alignment.evidence_ready() {
        actions.push(next_action(
            "attach_graph_route_parity_evidence",
            "Nowledge Mem graph cutover requires route-level shadow parity evidence for graph reads",
            [
                "graph_route_parity_alignment.ready",
                "graph_route_parity_alignment.required_route_count",
                "graph_route_parity_alignment.ready_route_count",
                "graph_route_parity_alignment.missing_routes",
                "graph_route_parity_alignment.not_ready_routes",
                "graph_route_parity_alignment.route_mismatch_routes",
                "graph_route_parity_alignment.protocol_mismatch_routes",
                "graph_route_parity_alignment.blocker_routes",
            ],
        ));
    }
    if !readiness.route_ownership.evidence_ready() {
        actions.push(next_action(
            "attach_route_ownership_evidence",
            "Nowledge Mem cutover requires every active read route to be owned by Skein through the embedded library runtime",
            [
                "route_ownership.protocol",
                "route_ownership.ready",
                "route_ownership.production_cutover_ready",
                "route_ownership.require_all_skein",
                "route_ownership.required_route_count",
                "route_ownership.explicit_route_count",
                "route_ownership.skein_route_count",
                "route_ownership.legacy_route_count",
                "route_ownership.missing_required_routes",
                "route_ownership.unknown_routes",
                "route_ownership.duplicate_routes",
                "route_ownership.conflicting_routes",
                "route_ownership.skein_not_ready_routes",
                "route_ownership.route_readiness_present",
                "route_ownership.route_readiness_ready",
                "route_ownership.route_catalog_version",
                "route_ownership.route_catalog_digest",
                "route_ownership.blocker_codes",
            ],
        ));
    }
    if !readiness.search_route_ownership_alignment.evidence_ready() {
        actions.push(next_action(
            "regenerate_search_route_ownership_alignment",
            "live search projection route ownership must match the replacement summary before Mem cutover",
            [
                "search_route_ownership.ready",
                "search_route_ownership.production_cutover_ready",
                "replacement_summary.search_route_ownership.ready",
                "replacement_summary_search_route_ownership_alignment.ready",
                "replacement_summary_search_route_ownership_alignment.evidence_ready",
                "replacement_summary_search_route_ownership_alignment.summary_ready",
                "replacement_summary_search_route_ownership_alignment.lancedb_route_count_matches",
                "replacement_summary_search_route_ownership_alignment.lancedb_routes_matches",
                "replacement_summary_search_route_ownership_alignment.blocker_codes",
            ],
        ));
    }
    if !readiness
        .active_search_route_ownership_alignment
        .evidence_ready()
    {
        actions.push(next_action(
            "regenerate_active_search_route_ownership_alignment",
            "live active search route ownership must match the replacement summary before Mem cutover",
            [
                "active_search_route_ownership.ready",
                "active_search_route_ownership.production_cutover_ready",
                "replacement_summary.active_search_route_ownership.ready",
                "replacement_summary_active_search_route_ownership_alignment.ready",
                "replacement_summary_active_search_route_ownership_alignment.evidence_ready",
                "replacement_summary_active_search_route_ownership_alignment.summary_ready",
                "replacement_summary_active_search_route_ownership_alignment.lancedb_route_count_matches",
                "replacement_summary_active_search_route_ownership_alignment.lancedb_routes_matches",
                "replacement_summary_active_search_route_ownership_alignment.blocker_codes",
            ],
        ));
    }
    if !readiness
        .active_search_route_readiness_alignment
        .evidence_ready()
    {
        actions.push(next_action(
            "regenerate_active_search_route_readiness_alignment",
            "live active search route read evidence must match the replacement summary before Mem cutover",
            [
                "active_search_route_readiness.ready",
                "active_search_route_readiness.production_cutover_ready",
                "active_search_route_readiness.lancedb_handle_required_route_count",
                "active_search_route_readiness.lancedb_handle_required_routes",
                "active_search_route_readiness.embedding_identity_not_ready_routes",
                "active_search_route_readiness.zero_vector_semantics_not_ready_routes",
                "active_search_route_readiness.cjk_tokenization_not_ready_routes",
                "active_search_route_readiness.ranking_window_not_ready_routes",
                "active_search_route_readiness.fail_soft_reason_codes_not_ready_routes",
                "active_search_route_readiness.repair_rebuild_markers_not_ready_routes",
                "replacement_summary.active_search_route_readiness.ready",
                "replacement_summary_active_search_route_readiness_alignment.ready",
                "replacement_summary_active_search_route_readiness_alignment.evidence_present",
                "replacement_summary_active_search_route_readiness_alignment.summary_present",
                "replacement_summary_active_search_route_readiness_alignment.lancedb_handle_count_matches",
                "replacement_summary_active_search_route_readiness_alignment.lancedb_handle_routes_matches",
                "replacement_summary_active_search_route_readiness_alignment.embedding_identity_not_ready_routes_matches",
                "replacement_summary_active_search_route_readiness_alignment.zero_vector_semantics_not_ready_routes_matches",
                "replacement_summary_active_search_route_readiness_alignment.cjk_tokenization_not_ready_routes_matches",
                "replacement_summary_active_search_route_readiness_alignment.ranking_window_not_ready_routes_matches",
                "replacement_summary_active_search_route_readiness_alignment.fail_soft_reason_codes_not_ready_routes_matches",
                "replacement_summary_active_search_route_readiness_alignment.repair_rebuild_markers_not_ready_routes_matches",
                "replacement_summary_active_search_route_readiness_alignment.blocker_codes",
            ],
        ));
    }
    if !readiness.query_runtime.evidence_ready() {
        actions.push(next_action(
            "attach_query_runtime_preflight_evidence",
            "Nowledge Mem cutover requires read-only query runtime EXPLAIN ANALYZE preflight evidence",
            [
                "query_runtime_preflight.protocol",
                "query_runtime_preflight.ready",
                "query_runtime_preflight.database_opened",
                "query_runtime_preflight.redaction.ready",
                "query_runtime_preflight.redaction.rows_copied",
                "query_runtime_preflight.redaction.parameters_copied",
                "query_runtime_preflight.redaction.local_paths_copied",
                "query_runtime_preflight.redaction.raw_errors_copied",
                "query_runtime_preflight.probe_count",
                "query_runtime_preflight.passed_probe_count",
                "query_runtime_preflight.failed_probe_count",
                "query_runtime_preflight.route_coverage",
                "query_runtime_preflight.blocker_codes",
                "query_runtime_preflight.probes",
            ],
        ));
    }
    if !readiness.query_runtime_alignment.evidence_ready() {
        actions.push(next_action(
            "regenerate_query_runtime_preflight_alignment",
            "live query runtime preflight evidence must match the replacement summary before Mem cutover",
            [
                "query_runtime_preflight.ready",
                "replacement_summary.query_runtime_preflight.ready",
                "replacement_summary_query_runtime_alignment.ready",
                "replacement_summary_query_runtime_alignment.evidence_ready",
                "replacement_summary_query_runtime_alignment.summary_ready",
                "replacement_summary_query_runtime_alignment.covered_routes_matches",
                "replacement_summary_query_runtime_alignment.blocker_codes",
            ],
        ));
    }
    if !readiness.library.evidence_ready() {
        actions.push(next_action(
            "attach_library_readiness_evidence",
            "Nowledge Mem cutover requires the Skein Rust library to open graph, search projection, and required evidence areas",
            [
                "library_readiness.protocol",
                "library_readiness.ready",
                "library_readiness.blocked_area_count",
                "library_readiness.open_report.graph_opened",
                "library_readiness.open_report.search_projection_opened",
                "library_readiness.readiness_by_area",
            ],
        ));
    }
    if !readiness.cutover_controls.evidence_ready() {
        actions.push(next_action(
            "attach_cutover_controls_evidence",
            "Nowledge Mem cutover requires host-owned graph/search read controls to select effective Skein reads with dual writes and projection catch-up enabled",
            [
                "cutover_controls.protocol",
                "cutover_controls.ready",
                "cutover_controls.controls.graph_reads",
                "cutover_controls.graph.read_effective",
                "cutover_controls.production_status.graph.skein_cutover_effective",
                "cutover_controls.controls.search_reads",
                "cutover_controls.search.read_effective",
                "cutover_controls.production_status.search.skein_cutover_effective",
                "cutover_controls.work.dual_writes_enabled",
                "cutover_controls.work.projection_catch_up_enabled",
                "cutover_controls.work.initial_import_safe_for_read_cutover",
                "cutover_controls.blocker_codes",
            ],
        ));
    }
    if !readiness.blackbox.redaction_ready {
        actions.push(next_action(
            "attach_blackbox_redaction_report",
            "Nowledge Mem cutover requires redacted blackbox evidence before diagnostics can be retained",
            [
                "blackbox_manifest.protocol",
                "blackbox_manifest.artifact_dir_present",
                "blackbox_manifest.artifact_count",
                "blackbox_manifest.events_path",
                "blackbox_manifest.redaction.raw_query_text_copied",
                "blackbox_manifest.redaction.raw_parameters_copied",
                "blackbox_manifest.redaction.raw_artifact_payloads_copied",
                "blackbox_manifest.redaction.artifact_paths_are_relative",
            ],
        ));
    }
    if !readiness.blackbox.operational_evidence_ready {
        actions.push(next_action(
            "attach_blackbox_operational_evidence",
            "Nowledge Mem cutover requires blackbox slow-query and background QoS operational evidence",
            [
                "blackbox_manifest.artifacts.slow-query-log.jsonl",
                "blackbox_manifest.artifacts.slow-query-log.jsonl.jsonl",
                "blackbox_manifest.artifacts.background-maintenance.json",
                "blackbox_manifest.artifacts.background-maintenance.json.background_qos",
            ],
        ));
    }
    if !readiness.storage_recovery.evidence_ready() {
        actions.push(next_action(
            "attach_storage_recovery_report",
            "storage recovery evidence must prove durable bounded WAL replay before Mem cutover",
            [
                "replacement_summary.cutover_evidence.storage_recovery_required",
                "replacement_summary.cutover_evidence.storage_recovery_ready",
                "replacement_summary.cutover_evidence.storage_recovery_protocol_matches",
                "replacement_summary.cutover_evidence.storage_recovery_durable",
                "replacement_summary.cutover_evidence.storage_recovery_checkpoint_boundary_present",
                "replacement_summary.cutover_evidence.storage_recovery_wal_replay_bounded",
                "replacement_summary.cutover_evidence.storage_recovery_replay_boundary_consistent",
                "replacement_summary.cutover_evidence.storage_recovery_torn_tail_clean",
                "replacement_summary.cutover_evidence.storage_recovery_blocker_codes",
            ],
        ));
    }
    if !readiness.operations.evidence_ready() {
        actions.push(next_action(
            "attach_operations_readiness_report",
            "Nowledge Mem cutover requires the embedded Skein library to expose ready lifecycle, recovery, slow-query, background, and projection freshness status",
            [
                "operations_readiness.protocol",
                "operations_readiness.present",
                "operations_readiness.ready",
                "operations_readiness.graph.open",
                "operations_readiness.graph.read_only",
                "operations_readiness.search_projection.open",
                "operations_readiness.search_projection.stale",
                "operations_readiness.storage_lifecycle.ready",
                "operations_readiness.storage_lifecycle.action",
                "operations_readiness.readiness.storage_recovery_ready",
                "operations_readiness.readiness.slow_query_ready",
                "operations_readiness.readiness.background_maintenance_ready",
                "operations_readiness.blocker_codes",
            ],
        ));
    }
    if !readiness.background_maintenance.evidence_ready() {
        actions.push(next_action(
            "attach_background_maintenance_report",
            "background maintenance QoS and search-projection graph-delta evidence must be ready before Mem cutover",
            [
                "replacement_summary.cutover_evidence.background_maintenance_required",
                "replacement_summary.cutover_evidence.background_maintenance_ready",
                "replacement_summary.cutover_evidence.background_maintenance_protocol_matches",
                "replacement_summary.cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_deferred_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_rejected_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_executable_search_projection_graph_delta_operations",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_operations",
                "replacement_summary.cutover_evidence.background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
                "replacement_summary.cutover_evidence.background_maintenance_foreground_admission_probe_ready",
                "replacement_summary.cutover_evidence.background_maintenance_memory_pressure_ready",
                "replacement_summary.cutover_evidence.background_maintenance_memory_budget_bytes",
                "replacement_summary.cutover_evidence.background_maintenance_estimated_memory_bytes",
                "replacement_summary.cutover_evidence.background_maintenance_blocker_codes",
            ],
        ));
    }
    actions
}

fn next_action(
    action: &str,
    reason: &str,
    evidence_fields: impl IntoIterator<Item = &'static str>,
) -> NowledgeMemIntegrationNextAction {
    NowledgeMemIntegrationNextAction {
        action: action.to_string(),
        reason: reason.to_string(),
        evidence_fields: evidence_fields.into_iter().map(str::to_string).collect(),
    }
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|_| {
        SkeinError::Execution(
            "failed to read Nowledge Mem integration bundle: io_error".to_string(),
        )
    })?;
    serde_json::from_str(&raw).map_err(|_| {
        SkeinError::Execution(
            "failed to parse Nowledge Mem integration bundle: invalid_json".to_string(),
        )
    })
}

pub fn integration_bundle_protocol_cutover_readiness(
    bundle: &serde_json::Value,
) -> IntegrationBundleProtocolCutoverReadiness {
    IntegrationBundleProtocolCutoverReadiness {
        protocol_matches: str_path(bundle, &["protocol"])
            == Some(NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL),
    }
}

pub fn replacement_summary_protocol_cutover_readiness(
    bundle: &serde_json::Value,
) -> ReplacementSummaryProtocolCutoverReadiness {
    ReplacementSummaryProtocolCutoverReadiness {
        protocol_matches: str_path(bundle, &["replacement_summary", "protocol"])
            == Some(SKEIN_NOWLEDGE_REPLACEMENT_SUMMARY_PROTOCOL),
        graph_layer_replacement_scope_ready: replacement_boundary_matches(
            bundle,
            "graph_layer",
            GRAPH_LAYER_REPLACEMENT_SCOPE,
            "primary_replacement",
        ),
        search_projection_replacement_scope_ready: replacement_boundary_matches(
            bundle,
            "search_projection",
            SEARCH_PROJECTION_REPLACEMENT_SCOPE,
            "rebuildable_projection",
        ),
        content_store_out_of_scope: replacement_boundary_matches(
            bundle,
            "content_store",
            SQLITE_CONTENT_STORE_SCOPE,
            "external_out_of_scope",
        ),
        large_blob_store_out_of_scope: replacement_boundary_matches(
            bundle,
            "large_blob_store",
            LARGE_BLOB_VALUE_STORE_SCOPE,
            "external_out_of_scope",
        ),
    }
}

fn replacement_boundary_matches(
    bundle: &serde_json::Value,
    boundary: &str,
    scope: &str,
    replacement_role: &str,
) -> bool {
    str_path(
        bundle,
        &[
            "replacement_summary",
            "replacement_boundaries",
            boundary,
            "scope",
        ],
    ) == Some(scope)
        && str_path(
            bundle,
            &[
                "replacement_summary",
                "replacement_boundaries",
                boundary,
                "replacement_role",
            ],
        ) == Some(replacement_role)
}

pub fn skein_submodule_cutover_readiness(
    bundle: &serde_json::Value,
) -> SkeinSubmoduleCutoverReadiness {
    SkeinSubmoduleCutoverReadiness {
        present: bool_path(bundle, &["submodule", "present"]) == Some(true),
        path_present: non_empty_str_path(bundle, &["submodule", "path"]),
        commit_present: non_empty_str_path(bundle, &["submodule", "commit"]),
        blocker_codes: blocker_codes(bundle, &[&["submodule", "blocker_codes"][..]]),
    }
}

pub fn legacy_coexistence_cutover_readiness(
    bundle: &serde_json::Value,
) -> LegacyCoexistenceCutoverReadiness {
    LegacyCoexistenceCutoverReadiness {
        old_database_retained: bool_path(bundle, &["coexistence", "old_database_retained"])
            == Some(true),
        mode_safe: coexistence_mode_is_safe(bundle),
        old_database_not_deleted: bool_path(bundle, &["coexistence", "old_database_deleted"])
            != Some(true),
        blocker_codes: blocker_codes(bundle, &[&["coexistence", "blocker_codes"][..]]),
    }
}

pub fn content_store_boundary_cutover_readiness(
    bundle: &serde_json::Value,
) -> ContentStoreBoundaryCutoverReadiness {
    ContentStoreBoundaryCutoverReadiness {
        present: bool_path(bundle, &["content_store", "present"]) == Some(true),
        engine_sqlite: str_path(bundle, &["content_store", "engine"]) == Some("sqlite"),
        messages_available: bool_path(bundle, &["content_store", "messages_available"])
            == Some(true),
        source_chunks_available: bool_path(bundle, &["content_store", "source_chunks_available"])
            == Some(true),
        blocker_codes: blocker_codes(bundle, &[&["content_store", "blocker_codes"][..]]),
    }
}

pub fn previous_wrapper_preflight_cutover_readiness(
    bundle: &serde_json::Value,
) -> PreviousWrapperPreflightCutoverReadiness {
    PreviousWrapperPreflightCutoverReadiness {
        ready: bool_path(bundle, &["previous_wrapper_preflight", "ready"]) == Some(true),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &["previous_wrapper_preflight", "blocker_codes"][..],
                &["previous_wrapper_preflight", "failed_checks"][..],
            ],
        ),
    }
}

pub fn route_ownership_cutover_readiness(
    bundle: &serde_json::Value,
) -> RouteOwnershipCutoverReadiness {
    let route_ownership =
        json_get_path(bundle, &["route_ownership"]).unwrap_or(&serde_json::Value::Null);
    let route_count_summary = route_ownership_count_summary(route_ownership);
    let search_route_skein_owned =
        route_ownership_skein_route_present(route_ownership, NOWLEDGE_MEM_SEARCH_ROUTE);
    let search_route_projection_evidence_present =
        !search_route_skein_owned || search_projection_replacement_evidence_present(bundle);
    RouteOwnershipCutoverReadiness {
        protocol_matches: str_path(bundle, &["route_ownership", "protocol"])
            == Some(NOWLEDGE_MEM_ROUTE_OWNERSHIP_PROTOCOL),
        ready: bool_path(bundle, &["route_ownership", "ready"]) == Some(true),
        production_cutover_ready: bool_path(
            bundle,
            &["route_ownership", "production_cutover_ready"],
        ) == Some(true),
        require_all_skein: bool_path(bundle, &["route_ownership", "require_all_skein"])
            == Some(true),
        required_route_count_matches: u64_path(
            bundle,
            &["route_ownership", "required_route_count"],
        ) == Some(
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64
        ),
        explicit_route_count_matches: u64_path(
            bundle,
            &["route_ownership", "explicit_route_count"],
        ) == Some(
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64
        ) && route_count_summary.explicit_required_route_count
            == REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        skein_route_count_matches: u64_path(bundle, &["route_ownership", "skein_route_count"])
            == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
            && route_count_summary.skein_route_count
                == REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        legacy_route_count_zero: u64_path(bundle, &["route_ownership", "legacy_route_count"])
            == Some(0)
            && route_count_summary.legacy_route_count == 0,
        missing_required_routes_empty: string_array_path(
            bundle,
            &["route_ownership", "missing_required_routes"],
        )
        .is_empty(),
        unknown_routes_empty: string_array_path(bundle, &["route_ownership", "unknown_routes"])
            .is_empty(),
        duplicate_routes_empty: string_array_path(bundle, &["route_ownership", "duplicate_routes"])
            .is_empty(),
        conflicting_routes_empty: string_array_path(
            bundle,
            &["route_ownership", "conflicting_routes"],
        )
        .is_empty(),
        skein_not_ready_routes_empty: string_array_path(
            bundle,
            &["route_ownership", "skein_not_ready_routes"],
        )
        .is_empty(),
        route_readiness_present: bool_path(bundle, &["route_ownership", "route_readiness_present"])
            == Some(true),
        route_readiness_ready: bool_path(bundle, &["route_ownership", "route_readiness_ready"])
            == Some(true),
        search_route_projection_evidence_present,
        route_catalog_version_matches: str_path(
            bundle,
            &["route_ownership", "route_catalog_version"],
        ) == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION),
        route_catalog_digest_matches: str_path(
            bundle,
            &["route_ownership", "route_catalog_digest"],
        ) == Some(
            nowledge_mem_graph_read_route_catalog_digest().as_str(),
        ),
        blocker_codes_empty: string_array_path(bundle, &["route_ownership", "blocker_codes"])
            .is_empty(),
        blocker_codes: blocker_codes(bundle, &[&["route_ownership", "blocker_codes"][..]]),
    }
}

fn route_ownership_skein_route_present(route_ownership: &serde_json::Value, route: &str) -> bool {
    route_ownership
        .get("routes")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|routes| {
            routes.iter().any(|entry| {
                str_path(entry, &["route"]) == Some(route)
                    && str_path(entry, &["read_engine"]) == Some("skein")
            })
        })
}

fn search_projection_replacement_evidence_present(bundle: &serde_json::Value) -> bool {
    json_get_path(
        bundle,
        &["replacement_summary", "search_projection_evidence"],
    )
    .is_some()
        && json_get_path(
            bundle,
            &["replacement_summary", "search_projection_shadow_evidence"],
        )
        .is_some()
}

pub fn search_route_ownership_alignment_cutover_readiness(
    bundle: &serde_json::Value,
    field: &'static str,
) -> SearchRouteOwnershipAlignmentCutoverReadiness {
    SearchRouteOwnershipAlignmentCutoverReadiness {
        ready: search_route_ownership_alignment_bool(bundle, field, "ready"),
        evidence_ready: search_route_ownership_alignment_bool(bundle, field, "evidence_present"),
        summary_ready: search_route_ownership_alignment_bool(bundle, field, "summary_present"),
        protocol_matches: search_route_ownership_alignment_bool(bundle, field, "protocol_matches"),
        readiness_matches: search_route_ownership_alignment_bool(bundle, field, "ready_matches"),
        production_cutover_ready_matches: search_route_ownership_alignment_bool(
            bundle,
            field,
            "production_cutover_ready_matches",
        ),
        require_all_skein_matches: search_route_ownership_alignment_bool(
            bundle,
            field,
            "require_all_skein_matches",
        ),
        required_route_count_matches: search_route_ownership_alignment_bool(
            bundle,
            field,
            "required_route_count_matches",
        ),
        explicit_route_count_matches: search_route_ownership_alignment_bool(
            bundle,
            field,
            "explicit_route_count_matches",
        ),
        skein_route_count_matches: search_route_ownership_alignment_bool(
            bundle,
            field,
            "skein_route_count_matches",
        ),
        lancedb_route_count_matches: search_route_ownership_alignment_bool(
            bundle,
            field,
            "lancedb_route_count_matches",
        ),
        missing_required_routes_matches: search_route_ownership_alignment_bool(
            bundle,
            field,
            "missing_required_routes_matches",
        ),
        lancedb_routes_matches: search_route_ownership_alignment_bool(
            bundle,
            field,
            "lancedb_routes_matches",
        ),
        blocker_codes_match: search_route_ownership_alignment_bool(
            bundle,
            field,
            "blocker_codes_match",
        ),
        blocker_codes: blocker_codes(bundle, &[&[field, "blocker_codes"][..]]),
    }
}

fn search_route_ownership_alignment_bool(
    bundle: &serde_json::Value,
    field: &'static str,
    property: &'static str,
) -> bool {
    bool_path(bundle, &[field, property]) == Some(true)
}

fn search_route_ownership_alignment_cutover_conditions(
    readiness: &SearchRouteOwnershipAlignmentCutoverReadiness,
    field: &'static str,
) -> Vec<(&'static str, bool)> {
    let fields = search_route_ownership_alignment_condition_fields(field);
    vec![
        (fields.ready, readiness.ready),
        (fields.evidence_ready, readiness.evidence_ready),
        (fields.summary_ready, readiness.summary_ready),
        (fields.protocol_matches, readiness.protocol_matches),
        (fields.readiness_matches, readiness.readiness_matches),
        (
            fields.production_cutover_ready_matches,
            readiness.production_cutover_ready_matches,
        ),
        (
            fields.require_all_skein_matches,
            readiness.require_all_skein_matches,
        ),
        (
            fields.required_route_count_matches,
            readiness.required_route_count_matches,
        ),
        (
            fields.explicit_route_count_matches,
            readiness.explicit_route_count_matches,
        ),
        (
            fields.skein_route_count_matches,
            readiness.skein_route_count_matches,
        ),
        (
            fields.lancedb_route_count_matches,
            readiness.lancedb_route_count_matches,
        ),
        (
            fields.missing_required_routes_matches,
            readiness.missing_required_routes_matches,
        ),
        (
            fields.lancedb_routes_matches,
            readiness.lancedb_routes_matches,
        ),
        (fields.blocker_codes_match, readiness.blocker_codes_match),
    ]
}

struct SearchRouteOwnershipAlignmentConditionFields {
    ready: &'static str,
    evidence_ready: &'static str,
    summary_ready: &'static str,
    protocol_matches: &'static str,
    readiness_matches: &'static str,
    production_cutover_ready_matches: &'static str,
    require_all_skein_matches: &'static str,
    required_route_count_matches: &'static str,
    explicit_route_count_matches: &'static str,
    skein_route_count_matches: &'static str,
    lancedb_route_count_matches: &'static str,
    missing_required_routes_matches: &'static str,
    lancedb_routes_matches: &'static str,
    blocker_codes_match: &'static str,
}

fn search_route_ownership_alignment_condition_fields(
    field: &'static str,
) -> SearchRouteOwnershipAlignmentConditionFields {
    match field {
        "replacement_summary_search_route_ownership_alignment" => {
            SearchRouteOwnershipAlignmentConditionFields {
                ready: "replacement_summary_search_route_ownership_alignment.ready",
                evidence_ready: "replacement_summary_search_route_ownership_alignment.evidence_present",
                summary_ready: "replacement_summary_search_route_ownership_alignment.summary_present",
                protocol_matches: "replacement_summary_search_route_ownership_alignment.protocol_matches",
                readiness_matches: "replacement_summary_search_route_ownership_alignment.ready_matches",
                production_cutover_ready_matches: "replacement_summary_search_route_ownership_alignment.production_cutover_ready_matches",
                require_all_skein_matches: "replacement_summary_search_route_ownership_alignment.require_all_skein_matches",
                required_route_count_matches: "replacement_summary_search_route_ownership_alignment.required_route_count_matches",
                explicit_route_count_matches: "replacement_summary_search_route_ownership_alignment.explicit_route_count_matches",
                skein_route_count_matches: "replacement_summary_search_route_ownership_alignment.skein_route_count_matches",
                lancedb_route_count_matches: "replacement_summary_search_route_ownership_alignment.lancedb_route_count_matches",
                missing_required_routes_matches: "replacement_summary_search_route_ownership_alignment.missing_required_routes_matches",
                lancedb_routes_matches: "replacement_summary_search_route_ownership_alignment.lancedb_routes_matches",
                blocker_codes_match: "replacement_summary_search_route_ownership_alignment.blocker_codes_match",
            }
        }
        "replacement_summary_active_search_route_ownership_alignment" => {
            SearchRouteOwnershipAlignmentConditionFields {
                ready: "replacement_summary_active_search_route_ownership_alignment.ready",
                evidence_ready: "replacement_summary_active_search_route_ownership_alignment.evidence_present",
                summary_ready: "replacement_summary_active_search_route_ownership_alignment.summary_present",
                protocol_matches: "replacement_summary_active_search_route_ownership_alignment.protocol_matches",
                readiness_matches: "replacement_summary_active_search_route_ownership_alignment.ready_matches",
                production_cutover_ready_matches: "replacement_summary_active_search_route_ownership_alignment.production_cutover_ready_matches",
                require_all_skein_matches: "replacement_summary_active_search_route_ownership_alignment.require_all_skein_matches",
                required_route_count_matches: "replacement_summary_active_search_route_ownership_alignment.required_route_count_matches",
                explicit_route_count_matches: "replacement_summary_active_search_route_ownership_alignment.explicit_route_count_matches",
                skein_route_count_matches: "replacement_summary_active_search_route_ownership_alignment.skein_route_count_matches",
                lancedb_route_count_matches: "replacement_summary_active_search_route_ownership_alignment.lancedb_route_count_matches",
                missing_required_routes_matches: "replacement_summary_active_search_route_ownership_alignment.missing_required_routes_matches",
                lancedb_routes_matches: "replacement_summary_active_search_route_ownership_alignment.lancedb_routes_matches",
                blocker_codes_match: "replacement_summary_active_search_route_ownership_alignment.blocker_codes_match",
            }
        }
        _ => SearchRouteOwnershipAlignmentConditionFields {
            ready: "unknown_search_route_ownership_alignment.ready",
            evidence_ready: "unknown_search_route_ownership_alignment.evidence_present",
            summary_ready: "unknown_search_route_ownership_alignment.summary_present",
            protocol_matches: "unknown_search_route_ownership_alignment.protocol_matches",
            readiness_matches: "unknown_search_route_ownership_alignment.ready_matches",
            production_cutover_ready_matches:
                "unknown_search_route_ownership_alignment.production_cutover_ready_matches",
            require_all_skein_matches:
                "unknown_search_route_ownership_alignment.require_all_skein_matches",
            required_route_count_matches:
                "unknown_search_route_ownership_alignment.required_route_count_matches",
            explicit_route_count_matches:
                "unknown_search_route_ownership_alignment.explicit_route_count_matches",
            skein_route_count_matches:
                "unknown_search_route_ownership_alignment.skein_route_count_matches",
            lancedb_route_count_matches:
                "unknown_search_route_ownership_alignment.lancedb_route_count_matches",
            missing_required_routes_matches:
                "unknown_search_route_ownership_alignment.missing_required_routes_matches",
            lancedb_routes_matches: "unknown_search_route_ownership_alignment.lancedb_routes_matches",
            blocker_codes_match: "unknown_search_route_ownership_alignment.blocker_codes_match",
        },
    }
}

pub fn active_search_route_readiness_alignment_cutover_readiness(
    bundle: &serde_json::Value,
) -> ActiveSearchRouteReadinessAlignmentCutoverReadiness {
    const FIELD: &str = "replacement_summary_active_search_route_readiness_alignment";
    ActiveSearchRouteReadinessAlignmentCutoverReadiness {
        ready: bool_path(bundle, &[FIELD, "ready"]) == Some(true),
        evidence_ready: bool_path(bundle, &[FIELD, "evidence_present"]) == Some(true),
        summary_ready: bool_path(bundle, &[FIELD, "summary_present"]) == Some(true),
        protocol_matches: bool_path(bundle, &[FIELD, "protocol_matches"]) == Some(true),
        readiness_matches: bool_path(bundle, &[FIELD, "ready_matches"]) == Some(true),
        production_cutover_ready_matches: bool_path(
            bundle,
            &[FIELD, "production_cutover_ready_matches"],
        ) == Some(true),
        require_all_skein_matches: bool_path(bundle, &[FIELD, "require_all_skein_matches"])
            == Some(true),
        required_route_count_matches: bool_path(bundle, &[FIELD, "required_route_count_matches"])
            == Some(true),
        evidence_route_count_matches: bool_path(bundle, &[FIELD, "evidence_route_count_matches"])
            == Some(true),
        ready_route_count_matches: bool_path(bundle, &[FIELD, "ready_route_count_matches"])
            == Some(true),
        skein_route_count_matches: bool_path(bundle, &[FIELD, "skein_route_count_matches"])
            == Some(true),
        lancedb_handle_count_matches: bool_path(bundle, &[FIELD, "lancedb_handle_count_matches"])
            == Some(true),
        missing_required_routes_matches: bool_path(
            bundle,
            &[FIELD, "missing_required_routes_matches"],
        ) == Some(true),
        non_skein_routes_matches: bool_path(bundle, &[FIELD, "non_skein_routes_matches"])
            == Some(true),
        lancedb_handle_routes_matches: bool_path(bundle, &[FIELD, "lancedb_handle_routes_matches"])
            == Some(true),
        candidate_not_ready_routes_matches: bool_path(
            bundle,
            &[FIELD, "candidate_not_ready_routes_matches"],
        ) == Some(true),
        candidate_identity_not_ready_routes_matches: bool_path(
            bundle,
            &[FIELD, "candidate_identity_not_ready_routes_matches"],
        ) == Some(true),
        embedding_identity_not_ready_routes_matches: bool_path(
            bundle,
            &[FIELD, "embedding_identity_not_ready_routes_matches"],
        ) == Some(true),
        zero_vector_semantics_not_ready_routes_matches: bool_path(
            bundle,
            &[FIELD, "zero_vector_semantics_not_ready_routes_matches"],
        ) == Some(true),
        cjk_tokenization_not_ready_routes_matches: bool_path(
            bundle,
            &[FIELD, "cjk_tokenization_not_ready_routes_matches"],
        ) == Some(true),
        metadata_pushdown_not_ready_routes_matches: bool_path(
            bundle,
            &[FIELD, "metadata_pushdown_not_ready_routes_matches"],
        ) == Some(true),
        ranking_window_not_ready_routes_matches: bool_path(
            bundle,
            &[FIELD, "ranking_window_not_ready_routes_matches"],
        ) == Some(true),
        ranking_not_ready_routes_matches: bool_path(
            bundle,
            &[FIELD, "ranking_not_ready_routes_matches"],
        ) == Some(true),
        fail_soft_not_ready_routes_matches: bool_path(
            bundle,
            &[FIELD, "fail_soft_not_ready_routes_matches"],
        ) == Some(true),
        fail_soft_reason_codes_not_ready_routes_matches: bool_path(
            bundle,
            &[FIELD, "fail_soft_reason_codes_not_ready_routes_matches"],
        ) == Some(true),
        repair_rebuild_markers_not_ready_routes_matches: bool_path(
            bundle,
            &[FIELD, "repair_rebuild_markers_not_ready_routes_matches"],
        ) == Some(true),
        blocker_codes_match: bool_path(bundle, &[FIELD, "blocker_codes_match"]) == Some(true),
        blocker_codes: blocker_codes(bundle, &[&[FIELD, "blocker_codes"][..]]),
    }
}

fn active_search_route_readiness_alignment_cutover_conditions(
    readiness: &ActiveSearchRouteReadinessAlignmentCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "replacement_summary_active_search_route_readiness_alignment.ready",
            readiness.ready,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.evidence_present",
            readiness.evidence_ready,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.summary_present",
            readiness.summary_ready,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.protocol_matches",
            readiness.protocol_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.ready_matches",
            readiness.readiness_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.production_cutover_ready_matches",
            readiness.production_cutover_ready_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.require_all_skein_matches",
            readiness.require_all_skein_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.required_route_count_matches",
            readiness.required_route_count_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.evidence_route_count_matches",
            readiness.evidence_route_count_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.ready_route_count_matches",
            readiness.ready_route_count_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.skein_route_count_matches",
            readiness.skein_route_count_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.lancedb_handle_count_matches",
            readiness.lancedb_handle_count_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.missing_required_routes_matches",
            readiness.missing_required_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.non_skein_routes_matches",
            readiness.non_skein_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.lancedb_handle_routes_matches",
            readiness.lancedb_handle_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.candidate_not_ready_routes_matches",
            readiness.candidate_not_ready_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.candidate_identity_not_ready_routes_matches",
            readiness.candidate_identity_not_ready_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.embedding_identity_not_ready_routes_matches",
            readiness.embedding_identity_not_ready_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.zero_vector_semantics_not_ready_routes_matches",
            readiness.zero_vector_semantics_not_ready_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.cjk_tokenization_not_ready_routes_matches",
            readiness.cjk_tokenization_not_ready_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.metadata_pushdown_not_ready_routes_matches",
            readiness.metadata_pushdown_not_ready_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.ranking_window_not_ready_routes_matches",
            readiness.ranking_window_not_ready_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.ranking_not_ready_routes_matches",
            readiness.ranking_not_ready_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.fail_soft_not_ready_routes_matches",
            readiness.fail_soft_not_ready_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.fail_soft_reason_codes_not_ready_routes_matches",
            readiness.fail_soft_reason_codes_not_ready_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.repair_rebuild_markers_not_ready_routes_matches",
            readiness.repair_rebuild_markers_not_ready_routes_matches,
        ),
        (
            "replacement_summary_active_search_route_readiness_alignment.blocker_codes_match",
            readiness.blocker_codes_match,
        ),
    ]
}

#[derive(Debug, Default)]
struct RouteOwnershipCountSummary {
    explicit_required_route_count: usize,
    skein_route_count: usize,
    legacy_route_count: usize,
}

fn route_ownership_count_summary(value: &serde_json::Value) -> RouteOwnershipCountSummary {
    let Some(routes) = value.get("routes").and_then(serde_json::Value::as_array) else {
        return RouteOwnershipCountSummary::default();
    };
    let required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut explicit_required_routes = BTreeSet::new();
    let mut skein_routes = BTreeSet::new();
    let mut legacy_routes = BTreeSet::new();
    for route in routes {
        let Some(route_name) = str_path(route, &["route"]) else {
            continue;
        };
        if required_routes.contains(route_name) {
            explicit_required_routes.insert(route_name);
        }
        match str_path(route, &["read_engine"]) {
            Some("skein") if required_routes.contains(route_name) => {
                skein_routes.insert(route_name);
            }
            Some("legacy") if required_routes.contains(route_name) => {
                legacy_routes.insert(route_name);
            }
            _ => {}
        }
    }
    RouteOwnershipCountSummary {
        explicit_required_route_count: explicit_required_routes.len(),
        skein_route_count: skein_routes.len(),
        legacy_route_count: legacy_routes.len(),
    }
}

fn coexistence_mode_is_safe(bundle: &serde_json::Value) -> bool {
    matches!(
        str_path(bundle, &["coexistence", "mode"]),
        Some("shadow") | Some("side_by_side")
    )
}

fn blocker_codes(value: &serde_json::Value, paths: &[&[&str]]) -> Vec<String> {
    paths
        .iter()
        .flat_map(|path| string_array_path(value, path))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn replacement_summary_required_query_families_present(bundle: &serde_json::Value) -> bool {
    let families = string_array_path(
        bundle,
        &[
            "replacement_summary",
            "replacement_readiness_family_summary",
            "required_query_families",
        ],
    );
    REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES
        .iter()
        .all(|required| families.iter().any(|family| family == required))
}

pub fn graph_replacement_cutover_readiness(
    bundle: &serde_json::Value,
) -> GraphReplacementCutoverReadiness {
    GraphReplacementCutoverReadiness {
        production_cutover_ready: bool_path(
            bundle,
            &["replacement_summary", "production_cutover_ready"],
        ) == Some(true),
        shadow_evidence_ready: bool_path(
            bundle,
            &["replacement_summary", "shadow_evidence", "ready"],
        ) == Some(true),
        dual_engine_evidence_present: bool_path(
            bundle,
            &["replacement_summary", "dual_engine_evidence", "present"],
        ) == Some(true),
        dual_engine_evidence_ready: bool_path(
            bundle,
            &["replacement_summary", "dual_engine_evidence", "ready"],
        ) == Some(true),
        dual_engine_evidence_consistent: bool_path(
            bundle,
            &["replacement_summary", "dual_engine_evidence", "consistent"],
        ) == Some(true),
        source_mutation_protocol_matches: str_path(
            bundle,
            &[
                "replacement_summary",
                "source_mutation_dual_write_readiness",
                "protocol",
            ],
        ) == Some(
            NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
        ),
        source_mutation_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "source_mutation_dual_write_readiness",
                "ready",
            ],
        ) == Some(true),
        source_mutation_required_family_count_matches: u64_path(
            bundle,
            &[
                "replacement_summary",
                "source_mutation_dual_write_readiness",
                "required_family_count",
            ],
        ) == Some(
            REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len() as u64,
        ),
        source_mutation_evidence_family_count_matches: u64_path(
            bundle,
            &[
                "replacement_summary",
                "source_mutation_dual_write_readiness",
                "evidence_family_count",
            ],
        ) == Some(
            REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len() as u64,
        ),
        source_mutation_ready_family_count_matches: u64_path(
            bundle,
            &[
                "replacement_summary",
                "source_mutation_dual_write_readiness",
                "ready_family_count",
            ],
        ) == Some(
            REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len() as u64,
        ),
        source_mutation_missing_required_families_empty: string_array_path(
            bundle,
            &[
                "replacement_summary",
                "source_mutation_dual_write_readiness",
                "missing_required_families",
            ],
        )
        .is_empty(),
        source_mutation_blocker_codes_empty: string_array_path(
            bundle,
            &[
                "replacement_summary",
                "source_mutation_dual_write_readiness",
                "blocker_codes",
            ],
        )
        .is_empty(),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &["replacement_summary", "blocking_categories"][..],
                &["replacement_summary", "missing_evidence"][..],
                &[
                    "replacement_summary",
                    "dual_engine_evidence",
                    "blocker_codes",
                ][..],
                &[
                    "replacement_summary",
                    "source_mutation_dual_write_readiness",
                    "blocker_codes",
                ][..],
            ],
        ),
    }
}

fn graph_replacement_cutover_conditions(
    readiness: &GraphReplacementCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "replacement_summary.production_cutover_ready",
            readiness.production_cutover_ready,
        ),
        (
            "replacement_summary.shadow_evidence.ready",
            readiness.shadow_evidence_ready,
        ),
        (
            "replacement_summary.dual_engine_evidence.present",
            readiness.dual_engine_evidence_present,
        ),
        (
            "replacement_summary.dual_engine_evidence.ready",
            readiness.dual_engine_evidence_ready,
        ),
        (
            "replacement_summary.dual_engine_evidence.consistent",
            readiness.dual_engine_evidence_consistent,
        ),
        (
            "replacement_summary.source_mutation_dual_write_readiness.protocol",
            readiness.source_mutation_protocol_matches,
        ),
        (
            "replacement_summary.source_mutation_dual_write_readiness.ready",
            readiness.source_mutation_ready,
        ),
        (
            "replacement_summary.source_mutation_dual_write_readiness.required_family_count",
            readiness.source_mutation_required_family_count_matches,
        ),
        (
            "replacement_summary.source_mutation_dual_write_readiness.evidence_family_count",
            readiness.source_mutation_evidence_family_count_matches,
        ),
        (
            "replacement_summary.source_mutation_dual_write_readiness.ready_family_count",
            readiness.source_mutation_ready_family_count_matches,
        ),
        (
            "replacement_summary.source_mutation_dual_write_readiness.missing_required_families",
            readiness.source_mutation_missing_required_families_empty,
        ),
        (
            "replacement_summary.source_mutation_dual_write_readiness.blocker_codes",
            readiness.source_mutation_blocker_codes_empty,
        ),
    ]
}

pub fn query_family_replacement_cutover_readiness(
    bundle: &serde_json::Value,
) -> QueryFamilyReplacementCutoverReadiness {
    QueryFamilyReplacementCutoverReadiness {
        required_query_families_present: replacement_summary_required_query_families_present(
            bundle,
        ),
        missing_required_query_families_empty: string_array_path(
            bundle,
            &[
                "replacement_summary",
                "replacement_readiness_family_summary",
                "missing_required_query_families",
            ],
        )
        .is_empty(),
        blocked_query_families_empty: string_array_path(
            bundle,
            &[
                "replacement_summary",
                "replacement_readiness_family_summary",
                "blocked_query_families",
            ],
        )
        .is_empty(),
        min_replacement_readiness_full: u64_path(
            bundle,
            &[
                "replacement_summary",
                "replacement_readiness_family_summary",
                "min_replacement_readiness_per_million",
            ],
        ) == Some(1_000_000),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &["replacement_summary", "blocking_categories"][..],
                &["replacement_summary", "missing_evidence"][..],
            ],
        ),
    }
}

fn query_family_replacement_cutover_conditions(
    readiness: &QueryFamilyReplacementCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "replacement_summary.replacement_readiness_family_summary.required_query_families",
            readiness.required_query_families_present,
        ),
        (
            "replacement_summary.replacement_readiness_family_summary.missing_required_query_families",
            readiness.missing_required_query_families_empty,
        ),
        (
            "replacement_summary.replacement_readiness_family_summary.blocked_query_families",
            readiness.blocked_query_families_empty,
        ),
        (
            "replacement_summary.replacement_readiness_family_summary.min_replacement_readiness_per_million",
            readiness.min_replacement_readiness_full,
        ),
    ]
}

pub fn search_projection_cutover_readiness(
    bundle: &serde_json::Value,
) -> SearchProjectionCutoverReadiness {
    let pushdown_path = &[
        "replacement_summary",
        "search_projection_shadow_evidence",
        "pushdown_evidence",
    ];
    SearchProjectionCutoverReadiness {
        evidence_protocol_matches: str_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_evidence",
                "protocol",
            ],
        ) == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL),
        evidence_ready: bool_path(
            bundle,
            &["replacement_summary", "search_projection_evidence", "ready"],
        ) == Some(true),
        fts_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_evidence",
                "fts_ready",
            ],
        ) == Some(true),
        vector_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_evidence",
                "vector_ready",
            ],
        ) == Some(true),
        document_identity_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_evidence",
                "document_identity_ready",
            ],
        ) == Some(true),
        incremental_update_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_evidence",
                "incremental_update_ready",
            ],
        ) == Some(true),
        predicate_pushdown_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_evidence",
                "predicate_pushdown_ready",
            ],
        ) == Some(true),
        production_filter_pruning_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_evidence",
                "production_filter_pruning_ready",
            ],
        ) == Some(true),
        compressed_vector_projection_required: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_evidence",
                "compressed_vector_projection_required",
            ],
        ) == Some(true),
        compressed_vector_projection_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_evidence",
                "compressed_vector_projection_ready",
            ],
        ) == Some(true),
        shadow_present: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "present",
            ],
        ) == Some(true),
        shadow_protocol_matches: str_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "protocol",
            ],
        ) == Some(
            SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL,
        ),
        shadow_evidence_source_matches: str_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "evidence_source",
            ],
        ) == Some(SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE),
        shadow_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "ready",
            ],
        ) == Some(true),
        shadow_document_count_parity: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "document_count_parity",
            ],
        ) == Some(true),
        shadow_document_identity_parity: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "document_identity_parity",
            ],
        ) == Some(true),
        shadow_table_parity_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "table_parity_ready",
            ],
        ) == Some(true),
        shadow_embedding_identity_parity: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "embedding_identity_parity",
            ],
        ) == Some(true),
        shadow_incremental_watermark_parity: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "incremental_watermark_parity",
            ],
        ) == Some(true),
        shadow_pushdown_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "ready",
            ],
        ) == Some(true),
        shadow_descriptor_scan_filter_fields_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "shadow_segment_descriptor_scan_filter_fields_ready",
            ],
        ) == Some(true),
        shadow_document_pruning_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "shadow_segment_document_pruning_ready",
            ],
        ) == Some(true),
        shadow_pruning_candidate_count_ready:
            search_projection_segment_pruning_candidate_count_ready(bundle, pushdown_path),
        shadow_pruned_document_count_positive: search_projection_segment_pruning_count_is_positive(
            bundle,
            pushdown_path,
            "shadow_segment_pruned_document_count",
        ),
        shadow_scanned_document_count_positive: search_projection_segment_pruning_count_is_positive(
            bundle,
            pushdown_path,
            "shadow_segment_scanned_document_count",
        ),
        primary_scan_filter_fields_ready: search_projection_scan_filter_fields_cover_required(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "primary_scan_filter_fields",
            ],
        ),
        shadow_scan_filter_fields_ready: search_projection_scan_filter_fields_cover_required(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "pushdown_evidence",
                "shadow_scan_filter_fields",
            ],
        ),
        shadow_descriptor_field_summaries_ready:
            search_projection_segment_descriptor_summaries_cover_required(
                bundle,
                &[
                    "replacement_summary",
                    "search_projection_shadow_evidence",
                    "pushdown_evidence",
                    "shadow_segment_descriptor_field_summaries",
                ],
            ),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &[
                    "replacement_summary",
                    "search_projection_evidence",
                    "blocker_codes",
                ][..],
                &[
                    "replacement_summary",
                    "search_projection_shadow_evidence",
                    "blocker_codes",
                ][..],
            ],
        ),
    }
}

fn search_projection_cutover_conditions(
    readiness: &SearchProjectionCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "replacement_summary.search_projection_evidence.ready",
            readiness.evidence_ready,
        ),
        (
            "replacement_summary.search_projection_evidence.protocol",
            readiness.evidence_protocol_matches,
        ),
        (
            "replacement_summary.search_projection_evidence.fts_ready",
            readiness.fts_ready,
        ),
        (
            "replacement_summary.search_projection_evidence.vector_ready",
            readiness.vector_ready,
        ),
        (
            "replacement_summary.search_projection_evidence.document_identity_ready",
            readiness.document_identity_ready,
        ),
        (
            "replacement_summary.search_projection_evidence.incremental_update_ready",
            readiness.incremental_update_ready,
        ),
        (
            "replacement_summary.search_projection_evidence.predicate_pushdown_ready",
            readiness.predicate_pushdown_ready,
        ),
        (
            "replacement_summary.search_projection_evidence.production_filter_pruning_ready",
            readiness.production_filter_pruning_ready,
        ),
        (
            "replacement_summary.search_projection_evidence.compressed_vector_projection_required",
            readiness.compressed_vector_projection_required,
        ),
        (
            "replacement_summary.search_projection_evidence.compressed_vector_projection_ready",
            readiness.compressed_vector_projection_ready,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.present",
            readiness.shadow_present,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.protocol",
            readiness.shadow_protocol_matches,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.evidence_source",
            readiness.shadow_evidence_source_matches,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.ready",
            readiness.shadow_ready,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.document_count_parity",
            readiness.shadow_document_count_parity,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.document_identity_parity",
            readiness.shadow_document_identity_parity,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.table_parity_ready",
            readiness.shadow_table_parity_ready,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.embedding_identity_parity",
            readiness.shadow_embedding_identity_parity,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.incremental_watermark_parity",
            readiness.shadow_incremental_watermark_parity,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.ready",
            readiness.shadow_pushdown_ready,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_scan_filter_fields_ready",
            readiness.shadow_descriptor_scan_filter_fields_ready,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_document_pruning_ready",
            readiness.shadow_document_pruning_ready,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_pruning_candidate_document_count",
            readiness.shadow_pruning_candidate_count_ready,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_pruned_document_count",
            readiness.shadow_pruned_document_count_positive,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_scanned_document_count",
            readiness.shadow_scanned_document_count_positive,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.primary_scan_filter_fields",
            readiness.primary_scan_filter_fields_ready,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_scan_filter_fields",
            readiness.shadow_scan_filter_fields_ready,
        ),
        (
            "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_field_summaries",
            readiness.shadow_descriptor_field_summaries_ready,
        ),
    ]
}

pub fn search_candidate_cutover_readiness(
    bundle: &serde_json::Value,
) -> SearchCandidateCutoverReadiness {
    SearchCandidateCutoverReadiness {
        protocol_matches: str_path(bundle, &["search_candidate_shadow_evidence", "protocol"])
            == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL),
        evidence_source_matches: str_path(
            bundle,
            &["search_candidate_shadow_evidence", "evidence_source"],
        ) == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_SOURCE),
        route_matches: str_path(bundle, &["search_candidate_shadow_evidence", "route"])
            == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE),
        ready: bool_path(bundle, &["search_candidate_shadow_evidence", "ready"]) == Some(true),
        primary_engine_matches: str_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "candidate_primary_engine",
            ],
        ) == Some(NOWLEDGE_MEM_SEARCH_CANDIDATE_PRIMARY_ENGINE),
        candidate_count_parity: search_candidate_shadow_counts_ready(bundle),
        row_count_parity: bool_path(
            bundle,
            &["search_candidate_shadow_evidence", "row_count_parity"],
        ) == Some(true),
        text_retriever_ready: bool_path(
            bundle,
            &["search_candidate_shadow_evidence", "text_retriever_ready"],
        ) == Some(true),
        vector_retriever_ready: bool_path(
            bundle,
            &["search_candidate_shadow_evidence", "vector_retriever_ready"],
        ) == Some(true),
        fts_top_k_overlap_ready: bool_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "fts_top_k_overlap_ready",
            ],
        ) == Some(true),
        vector_top_k_overlap_ready: bool_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "vector_top_k_overlap_ready",
            ],
        ) == Some(true),
        source_chunk_identity_ready: bool_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "candidate_readiness",
                "source_chunk_identity_ready",
            ],
        ) == Some(true),
        fail_soft_observed: bool_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "candidate_readiness",
                "fail_soft_observed",
            ],
        ) == Some(true),
        projection_marker_status_visible: bool_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "candidate_readiness",
                "projection_marker_status_visible",
            ],
        ) == Some(true),
        projection_watermark_ready: bool_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "candidate_readiness",
                "projection_watermark_ready",
            ],
        ) == Some(true),
        embedding_identity_ready: bool_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "candidate_readiness",
                "embedding_identity_ready",
            ],
        ) == Some(true),
        candidate_identity_ready: bool_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "candidate_identity",
                "ready",
            ],
        ) == Some(true),
        filter_pushdown_ready: bool_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "filter_pushdown",
                "ready",
            ],
        ) == Some(true),
        filter_pushdown_field_summary_present: u64_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "filter_pushdown",
                "field_summary_count",
            ],
        )
        .is_some_and(|count| count > 0),
        filter_pushdown_required_fields_ready:
            search_candidate_filter_pushdown_required_fields_ready(bundle),
        shadow_scan_filter_pushdown_ready: bool_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "shadow_scan_filter_pushdown_ready",
            ],
        ) == Some(true),
        shadow_scan_field_pruning_ready: bool_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "shadow_scan_field_pruning_ready",
            ],
        ) == Some(true),
        shadow_scan_field_summary_present: u64_path(
            bundle,
            &[
                "search_candidate_shadow_evidence",
                "shadow_scan_field_summary_count",
            ],
        )
        .is_some_and(|count| count > 0),
        blocker_codes: blocker_codes(
            bundle,
            &[&["search_candidate_shadow_evidence", "blocker_codes"][..]],
        ),
    }
}

fn search_candidate_cutover_conditions(
    readiness: &SearchCandidateCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "search_candidate_shadow_evidence.protocol",
            readiness.protocol_matches,
        ),
        (
            "search_candidate_shadow_evidence.evidence_source",
            readiness.evidence_source_matches,
        ),
        (
            "search_candidate_shadow_evidence.route",
            readiness.route_matches,
        ),
        ("search_candidate_shadow_evidence.ready", readiness.ready),
        (
            "search_candidate_shadow_evidence.candidate_primary_engine",
            readiness.primary_engine_matches,
        ),
        (
            "search_candidate_shadow_evidence.candidate_counts",
            readiness.candidate_count_parity,
        ),
        (
            "search_candidate_shadow_evidence.row_count_parity",
            readiness.row_count_parity,
        ),
        (
            "search_candidate_shadow_evidence.text_retriever_ready",
            readiness.text_retriever_ready,
        ),
        (
            "search_candidate_shadow_evidence.vector_retriever_ready",
            readiness.vector_retriever_ready,
        ),
        (
            "search_candidate_shadow_evidence.fts_top_k_overlap_ready",
            readiness.fts_top_k_overlap_ready,
        ),
        (
            "search_candidate_shadow_evidence.vector_top_k_overlap_ready",
            readiness.vector_top_k_overlap_ready,
        ),
        (
            "search_candidate_shadow_evidence.candidate_readiness.source_chunk_identity_ready",
            readiness.source_chunk_identity_ready,
        ),
        (
            "search_candidate_shadow_evidence.candidate_readiness.fail_soft_observed",
            readiness.fail_soft_observed,
        ),
        (
            "search_candidate_shadow_evidence.candidate_readiness.projection_marker_status_visible",
            readiness.projection_marker_status_visible,
        ),
        (
            "search_candidate_shadow_evidence.candidate_readiness.projection_watermark_ready",
            readiness.projection_watermark_ready,
        ),
        (
            "search_candidate_shadow_evidence.candidate_readiness.embedding_identity_ready",
            readiness.embedding_identity_ready,
        ),
        (
            "search_candidate_shadow_evidence.candidate_identity.ready",
            readiness.candidate_identity_ready,
        ),
        (
            "search_candidate_shadow_evidence.filter_pushdown.ready",
            readiness.filter_pushdown_ready,
        ),
        (
            "search_candidate_shadow_evidence.filter_pushdown.field_summary_count",
            readiness.filter_pushdown_field_summary_present,
        ),
        (
            "search_candidate_shadow_evidence.filter_pushdown.missing_required_fields",
            readiness.filter_pushdown_required_fields_ready,
        ),
        (
            "search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready",
            readiness.shadow_scan_filter_pushdown_ready,
        ),
        (
            "search_candidate_shadow_evidence.shadow_scan_field_pruning_ready",
            readiness.shadow_scan_field_pruning_ready,
        ),
        (
            "search_candidate_shadow_evidence.shadow_scan_field_summary_count",
            readiness.shadow_scan_field_summary_present,
        ),
    ]
}

fn search_candidate_filter_pushdown_required_fields_ready(bundle: &serde_json::Value) -> bool {
    let path = &[
        "search_candidate_shadow_evidence",
        "filter_pushdown",
        "missing_required_fields",
    ];
    json_get_path(bundle, path).is_some_and(serde_json::Value::is_array)
        && string_array_path(bundle, path).is_empty()
}

fn search_candidate_shadow_counts_ready(bundle: &serde_json::Value) -> bool {
    let request_count = u64_path(
        bundle,
        &["search_candidate_shadow_evidence", "request_count"],
    );
    let primary_candidate_count = u64_path(
        bundle,
        &[
            "search_candidate_shadow_evidence",
            "primary_candidate_count",
        ],
    );
    let shadow_candidate_count = u64_path(
        bundle,
        &["search_candidate_shadow_evidence", "shadow_candidate_count"],
    );
    let matched_candidate_count = u64_path(
        bundle,
        &[
            "search_candidate_shadow_evidence",
            "matched_candidate_count",
        ],
    );
    let primary_only_candidate_count = u64_path(
        bundle,
        &[
            "search_candidate_shadow_evidence",
            "primary_only_candidate_count",
        ],
    );
    request_count.is_some_and(|count| count > 0)
        && primary_candidate_count.is_some()
        && primary_candidate_count == shadow_candidate_count
        && matched_candidate_count == shadow_candidate_count
        && primary_only_candidate_count == Some(0)
}

pub fn bounded_read_cutover_readiness(bundle: &serde_json::Value) -> BoundedReadCutoverReadiness {
    BoundedReadCutoverReadiness {
        present: bool_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "present"],
        ) == Some(true),
        protocol_matches: str_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "protocol"],
        ) == Some(SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL),
        ready: bool_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "ready"],
        ) == Some(true),
        max_rows_present: u64_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "max_rows"],
        )
        .is_some_and(|value| value > 0),
        mode_matches: str_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "mode"],
        ) == Some("shadow_read_only"),
        execution_cap_matches: bounded_read_execution_cap_matches(bundle),
        estimated_payload_bytes_present: u64_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "estimated_payload_bytes",
            ],
        )
        .is_some(),
        max_estimated_payload_bytes_present: u64_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "max_estimated_payload_bytes",
            ],
        )
        .is_some_and(|value| value > 0),
        payload_budget_not_exceeded: bool_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "payload_budget_exceeded",
            ],
        ) == Some(false),
        row_limit_enforced_before_output: bool_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "row_limit_enforced_before_output",
            ],
        ) == Some(true),
        operator_row_cap_enabled: bool_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "operator_row_cap_enabled",
            ],
        ) == Some(true),
        blocking_operator_memory_reports_complete: bool_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "blocking_operator_memory_reports_complete",
            ],
        ) == Some(true),
        blocking_operator_memory_within_budget: bool_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "blocking_operator_memory_within_budget",
            ],
        ) == Some(true),
        spill_within_budget: bool_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "spill_within_budget",
            ],
        ) == Some(true),
        streaming_evidence_present: bool_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "streaming"],
        )
        .is_some(),
        route_catalog_version_matches: str_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "route_catalog_version",
            ],
        ) == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION),
        route_catalog_digest_present: str_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "route_catalog_digest",
            ],
        )
        .is_some(),
        route_coverage_ready: bounded_read_route_coverage_ready(bundle),
        blocker_codes: blocker_codes(
            bundle,
            &[&[
                "replacement_summary",
                "bounded_read_evidence",
                "blocker_codes",
            ][..]],
        ),
    }
}

fn bounded_read_cutover_conditions(
    readiness: &BoundedReadCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "replacement_summary.bounded_read_evidence.present",
            readiness.present,
        ),
        (
            "replacement_summary.bounded_read_evidence.protocol",
            readiness.protocol_matches,
        ),
        (
            "replacement_summary.bounded_read_evidence.ready",
            readiness.ready,
        ),
        (
            "replacement_summary.bounded_read_evidence.max_rows",
            readiness.max_rows_present,
        ),
        (
            "replacement_summary.bounded_read_evidence.mode",
            readiness.mode_matches,
        ),
        (
            "replacement_summary.bounded_read_evidence.execution_row_cap",
            readiness.execution_cap_matches,
        ),
        (
            "replacement_summary.bounded_read_evidence.estimated_payload_bytes",
            readiness.estimated_payload_bytes_present,
        ),
        (
            "replacement_summary.bounded_read_evidence.max_estimated_payload_bytes",
            readiness.max_estimated_payload_bytes_present,
        ),
        (
            "replacement_summary.bounded_read_evidence.payload_budget_exceeded",
            readiness.payload_budget_not_exceeded,
        ),
        (
            "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output",
            readiness.row_limit_enforced_before_output,
        ),
        (
            "replacement_summary.bounded_read_evidence.operator_row_cap_enabled",
            readiness.operator_row_cap_enabled,
        ),
        (
            "replacement_summary.bounded_read_evidence.blocking_operator_memory_reports_complete",
            readiness.blocking_operator_memory_reports_complete,
        ),
        (
            "replacement_summary.bounded_read_evidence.blocking_operator_memory_within_budget",
            readiness.blocking_operator_memory_within_budget,
        ),
        (
            "replacement_summary.bounded_read_evidence.spill_within_budget",
            readiness.spill_within_budget,
        ),
        (
            "replacement_summary.bounded_read_evidence.streaming",
            readiness.streaming_evidence_present,
        ),
        (
            "replacement_summary.bounded_read_evidence.route_catalog_version",
            readiness.route_catalog_version_matches,
        ),
        (
            "replacement_summary.bounded_read_evidence.route_catalog_digest",
            readiness.route_catalog_digest_present,
        ),
        (
            "replacement_summary.bounded_read_evidence.covered_routes",
            readiness.route_coverage_ready,
        ),
    ]
}

pub fn bounded_read_alignment_cutover_readiness(
    bundle: &serde_json::Value,
) -> BoundedReadAlignmentCutoverReadiness {
    BoundedReadAlignmentCutoverReadiness {
        evidence_ready: bool_path(bundle, &["bounded_read_evidence", "ready"]) == Some(true),
        ready: bool_path(
            bundle,
            &["replacement_summary_bounded_read_alignment", "ready"],
        ) == Some(true),
        alignment_evidence_ready: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "evidence_ready",
            ],
        ) == Some(true),
        summary_ready: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "summary_ready",
            ],
        ) == Some(true),
        protocol_matches: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "protocol_matches",
            ],
        ) == Some(true),
        readiness_matches: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "readiness_matches",
            ],
        ) == Some(true),
        mode_matches: bool_path(
            bundle,
            &["replacement_summary_bounded_read_alignment", "mode_matches"],
        ) == Some(true),
        max_rows_matches: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "max_rows_matches",
            ],
        ) == Some(true),
        estimated_payload_bytes_matches: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "estimated_payload_bytes_matches",
            ],
        ) == Some(true),
        max_estimated_payload_bytes_matches: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "max_estimated_payload_bytes_matches",
            ],
        ) == Some(true),
        payload_budget_exceeded_matches: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "payload_budget_exceeded_matches",
            ],
        ) == Some(true),
        streaming_matches: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "streaming_matches",
            ],
        ) == Some(true),
        covered_routes_matches: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "covered_routes_matches",
            ],
        ) == Some(true),
        evidence_route_catalog_version_ready: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "evidence_route_catalog_version_ready",
            ],
        ) == Some(true),
        summary_route_catalog_version_ready: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "summary_route_catalog_version_ready",
            ],
        ) == Some(true),
        evidence_route_catalog_digest_ready: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "evidence_route_catalog_digest_ready",
            ],
        ) == Some(true),
        summary_route_catalog_digest_ready: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "summary_route_catalog_digest_ready",
            ],
        ) == Some(true),
        route_catalog_version_matches: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "route_catalog_version_matches",
            ],
        ) == Some(true),
        route_catalog_digest_matches: bool_path(
            bundle,
            &[
                "replacement_summary_bounded_read_alignment",
                "route_catalog_digest_matches",
            ],
        ) == Some(true),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &["bounded_read_evidence", "blocker_codes"][..],
                &[
                    "replacement_summary_bounded_read_alignment",
                    "blocker_codes",
                ][..],
            ],
        ),
    }
}

fn bounded_read_alignment_cutover_conditions(
    readiness: &BoundedReadAlignmentCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        ("bounded_read_evidence.ready", readiness.evidence_ready),
        (
            "replacement_summary_bounded_read_alignment.ready",
            readiness.ready,
        ),
        (
            "replacement_summary_bounded_read_alignment.evidence_ready",
            readiness.alignment_evidence_ready,
        ),
        (
            "replacement_summary_bounded_read_alignment.summary_ready",
            readiness.summary_ready,
        ),
        (
            "replacement_summary_bounded_read_alignment.protocol_matches",
            readiness.protocol_matches,
        ),
        (
            "replacement_summary_bounded_read_alignment.readiness_matches",
            readiness.readiness_matches,
        ),
        (
            "replacement_summary_bounded_read_alignment.mode_matches",
            readiness.mode_matches,
        ),
        (
            "replacement_summary_bounded_read_alignment.max_rows_matches",
            readiness.max_rows_matches,
        ),
        (
            "replacement_summary_bounded_read_alignment.estimated_payload_bytes_matches",
            readiness.estimated_payload_bytes_matches,
        ),
        (
            "replacement_summary_bounded_read_alignment.max_estimated_payload_bytes_matches",
            readiness.max_estimated_payload_bytes_matches,
        ),
        (
            "replacement_summary_bounded_read_alignment.payload_budget_exceeded_matches",
            readiness.payload_budget_exceeded_matches,
        ),
        (
            "replacement_summary_bounded_read_alignment.streaming_matches",
            readiness.streaming_matches,
        ),
        (
            "replacement_summary_bounded_read_alignment.covered_routes_matches",
            readiness.covered_routes_matches,
        ),
        (
            "replacement_summary_bounded_read_alignment.evidence_route_catalog_version_ready",
            readiness.evidence_route_catalog_version_ready,
        ),
        (
            "replacement_summary_bounded_read_alignment.summary_route_catalog_version_ready",
            readiness.summary_route_catalog_version_ready,
        ),
        (
            "replacement_summary_bounded_read_alignment.evidence_route_catalog_digest_ready",
            readiness.evidence_route_catalog_digest_ready,
        ),
        (
            "replacement_summary_bounded_read_alignment.summary_route_catalog_digest_ready",
            readiness.summary_route_catalog_digest_ready,
        ),
        (
            "replacement_summary_bounded_read_alignment.route_catalog_version_matches",
            readiness.route_catalog_version_matches,
        ),
        (
            "replacement_summary_bounded_read_alignment.route_catalog_digest_matches",
            readiness.route_catalog_digest_matches,
        ),
    ]
}

fn bounded_read_route_coverage_ready(bundle: &serde_json::Value) -> bool {
    let covered_routes = string_array_path(
        bundle,
        &[
            "replacement_summary",
            "bounded_read_evidence",
            "covered_routes",
        ],
    );
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .all(|route| covered_routes.iter().any(|covered| covered == route))
        && string_array_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "missing_covered_routes",
            ],
        )
        .is_empty()
}

pub fn graph_route_cutover_readiness(bundle: &serde_json::Value) -> GraphRouteCutoverReadiness {
    GraphRouteCutoverReadiness {
        protocol_matches: graph_route_protocol_ready(bundle),
        evidence_protocol_matches: graph_route_evidence_protocol_ready(bundle),
        evidence_ready: graph_route_evidence_ready(bundle),
        route_count_present: u64_path(bundle, &["graph_route_readiness", "route_count"])
            .is_some_and(|value| value > 0),
        required_route_count_matches: graph_route_required_route_count_ready(bundle),
        route_coverage_ready: graph_route_readiness_coverage_ready(bundle),
        missing_required_routes_empty: graph_route_missing_required_routes_empty(bundle),
        query_runtime_route_count_matches: u64_path(
            bundle,
            &["graph_route_readiness", "query_runtime_route_count"],
        ) == Some(
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64,
        ),
        query_runtime_report_count_ready: u64_path(
            bundle,
            &["graph_route_readiness", "query_runtime_report_count"],
        )
        .is_some_and(|value| value >= REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64),
        query_plan_profile_summary_ready: graph_route_query_plan_profile_summary_ready(bundle),
        relationship_property_pruning_summary_ready:
            graph_route_relationship_property_pruning_summary_ready(bundle),
        missing_query_runtime_routes_empty: string_array_path_is_empty(
            bundle,
            &["graph_route_readiness", "missing_query_runtime_routes"],
        ),
        route_query_runtime_ready: graph_route_query_runtime_ready(bundle),
        route_primary_ready: graph_route_primary_ready(bundle),
        primary_ready_route_count_matches: graph_route_primary_ready_count_ready(bundle),
        route_primary_blocker_codes_empty: string_array_path(
            bundle,
            &["graph_route_readiness", "route_primary_blocker_codes"],
        )
        .is_empty(),
        evidence_route_coverage_present: graph_route_evidence_route_coverage_present(bundle),
        evidence_route_coverage_matches: graph_route_evidence_route_coverage_matches(bundle),
        evidence_route_coverage_blocker_codes_empty: string_array_path(
            bundle,
            &[
                "graph_route_readiness",
                "evidence_route_coverage_blocker_codes",
            ],
        )
        .is_empty(),
        route_query_profiles_ready: graph_route_query_profiles_ready(bundle),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &["graph_route_readiness", "blocker_codes"][..],
                &["graph_route_readiness", "route_primary_blocker_codes"][..],
                &[
                    "graph_route_readiness",
                    "evidence_route_coverage_blocker_codes",
                ][..],
            ],
        ),
    }
}

fn graph_route_cutover_conditions(
    readiness: &GraphRouteCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        ("graph_route_readiness.protocol", readiness.protocol_matches),
        (
            "graph_route_readiness.evidence_protocol",
            readiness.evidence_protocol_matches,
        ),
        (
            "graph_route_readiness.evidence_ready",
            readiness.evidence_ready,
        ),
        (
            "graph_route_readiness.route_count",
            readiness.route_count_present,
        ),
        (
            "graph_route_readiness.required_route_count",
            readiness.required_route_count_matches,
        ),
        (
            "graph_route_readiness.route_coverage",
            readiness.route_coverage_ready,
        ),
        (
            "graph_route_readiness.missing_required_routes",
            readiness.missing_required_routes_empty,
        ),
        (
            "graph_route_readiness.query_runtime_route_count",
            readiness.query_runtime_route_count_matches,
        ),
        (
            "graph_route_readiness.query_runtime_report_count",
            readiness.query_runtime_report_count_ready,
        ),
        (
            "graph_route_readiness.query_runtime_plan_profile_counts",
            readiness.query_plan_profile_summary_ready,
        ),
        (
            "graph_route_readiness.relationship_property_pruning_counts",
            readiness.relationship_property_pruning_summary_ready,
        ),
        (
            "graph_route_readiness.missing_query_runtime_routes",
            readiness.missing_query_runtime_routes_empty,
        ),
        (
            "graph_route_readiness.route_query_runtime_ready",
            readiness.route_query_runtime_ready,
        ),
        (
            "graph_route_readiness.route_primary_ready",
            readiness.route_primary_ready,
        ),
        (
            "graph_route_readiness.primary_ready_route_count",
            readiness.primary_ready_route_count_matches,
        ),
        (
            "graph_route_readiness.route_primary_blocker_codes",
            readiness.route_primary_blocker_codes_empty,
        ),
        (
            "graph_route_readiness.evidence_route_coverage_present",
            readiness.evidence_route_coverage_present,
        ),
        (
            "graph_route_readiness.evidence_route_coverage_matches",
            readiness.evidence_route_coverage_matches,
        ),
        (
            "graph_route_readiness.evidence_route_coverage_blocker_codes",
            readiness.evidence_route_coverage_blocker_codes_empty,
        ),
        (
            "graph_route_readiness.routes",
            readiness.route_query_profiles_ready,
        ),
    ]
}

fn graph_route_readiness_summary(bundle: &serde_json::Value) -> GraphRouteReadinessSummary {
    let value =
        json_get_path(bundle, &["graph_route_readiness"]).unwrap_or(&serde_json::Value::Null);
    nowledge_graph_route_readiness_summary(value)
}

fn graph_route_protocol_ready(bundle: &serde_json::Value) -> bool {
    graph_route_readiness_summary(bundle).protocol.as_deref()
        == Some(NMEM_GRAPH_ROUTE_READINESS_PROTOCOL)
}

fn graph_route_evidence_protocol_ready(bundle: &serde_json::Value) -> bool {
    graph_route_readiness_summary(bundle)
        .evidence_protocol
        .as_deref()
        == Some(NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL)
}

fn graph_route_evidence_ready(bundle: &serde_json::Value) -> bool {
    graph_route_readiness_summary(bundle).evidence_ready == Some(true)
}

fn graph_route_required_route_count_ready(bundle: &serde_json::Value) -> bool {
    graph_route_readiness_summary(bundle).required_route_count
        == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
}

fn graph_route_readiness_coverage_ready(bundle: &serde_json::Value) -> bool {
    let summary = graph_route_readiness_summary(bundle);
    summary.covered_route_count == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && summary.missing_required_routes.is_empty()
        && summary.unknown_routes.is_empty()
        && summary.duplicate_routes.is_empty()
        && summary.route_coverage_ready == Some(true)
}

fn graph_route_missing_required_routes_empty(bundle: &serde_json::Value) -> bool {
    graph_route_readiness_summary(bundle)
        .missing_required_routes
        .is_empty()
}

fn graph_route_query_runtime_ready(bundle: &serde_json::Value) -> bool {
    graph_route_readiness_summary(bundle).route_query_runtime_ready == Some(true)
}

fn graph_route_primary_ready(bundle: &serde_json::Value) -> bool {
    graph_route_readiness_summary(bundle).route_primary_ready == Some(true)
}

fn graph_route_primary_ready_count_ready(bundle: &serde_json::Value) -> bool {
    graph_route_readiness_summary(bundle).primary_ready_route_count
        == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
}

fn graph_route_evidence_route_coverage_present(bundle: &serde_json::Value) -> bool {
    graph_route_readiness_summary(bundle).evidence_route_coverage_present == Some(true)
}

fn graph_route_evidence_route_coverage_matches(bundle: &serde_json::Value) -> bool {
    graph_route_readiness_summary(bundle).evidence_route_coverage_matches == Some(true)
}

fn graph_route_query_plan_profile_summary_ready(bundle: &serde_json::Value) -> bool {
    let report_count = u64_path(
        bundle,
        &["graph_route_readiness", "query_runtime_report_count"],
    );
    report_count
        .is_some_and(|value| value >= REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && u64_path(
            bundle,
            &["graph_route_readiness", "query_runtime_plan_report_count"],
        ) == report_count
        && u64_path(
            bundle,
            &[
                "graph_route_readiness",
                "query_runtime_profile_report_count",
            ],
        ) == report_count
        && u64_path(
            bundle,
            &["graph_route_readiness", "query_runtime_failed_query_count"],
        ) == Some(0)
        && u64_path(
            bundle,
            &[
                "graph_route_readiness",
                "query_runtime_missing_plan_evidence_count",
            ],
        ) == Some(0)
        && u64_path(
            bundle,
            &[
                "graph_route_readiness",
                "query_runtime_missing_profile_evidence_count",
            ],
        ) == Some(0)
        && bool_path(
            bundle,
            &["graph_route_readiness", "route_query_plan_evidence_ready"],
        ) == Some(true)
        && bool_path(
            bundle,
            &[
                "graph_route_readiness",
                "route_query_profile_evidence_ready",
            ],
        ) == Some(true)
}

fn graph_route_relationship_property_pruning_summary_ready(bundle: &serde_json::Value) -> bool {
    let Some(required_count) = u64_path(
        bundle,
        &[
            "graph_route_readiness",
            "relationship_property_pruning_required_count",
        ],
    ) else {
        return false;
    };
    u64_path(
        bundle,
        &[
            "graph_route_readiness",
            "relationship_property_pruning_report_count",
        ],
    ) == Some(required_count)
        && bool_path(
            bundle,
            &[
                "graph_route_readiness",
                "route_relationship_property_pruning_evidence_ready",
            ],
        ) == Some(true)
}

fn graph_route_query_profiles_ready(bundle: &serde_json::Value) -> bool {
    let Some(routes) = json_get_path(bundle, &["graph_route_readiness", "routes"])
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    if routes.len() != REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() {
        return false;
    }

    let mut observed_routes = BTreeSet::new();
    for route in routes {
        let Some(route_name) = str_path(route, &["route"]) else {
            return false;
        };
        if !REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(&route_name) {
            return false;
        }
        if !observed_routes.insert(route_name) {
            return false;
        }
        if !graph_route_query_families_ready(route, route_name) {
            return false;
        }
        if bool_path(route, &["primary_ready"]) != Some(true)
            || bool_path(route, &["query_runtime_ready"]) != Some(true)
            || bool_path(route, &["query_plan_evidence_ready"]) != Some(true)
            || bool_path(route, &["query_profile_evidence_ready"]) != Some(true)
            || bool_path(route, &["relationship_property_pruning_evidence_ready"]) != Some(true)
            || !graph_route_shadow_compare_ready(route)
            || u64_path(route, &["query_report_count"]).is_none_or(|value| value == 0)
        {
            return false;
        }
        let Some(query_reports) =
            json_get_path(route, &["query_reports"]).and_then(serde_json::Value::as_array)
        else {
            return false;
        };
        if query_reports.is_empty()
            || u64_path(route, &["query_report_count"]) != Some(query_reports.len() as u64)
            || u64_path(route, &["query_runtime_report_count"]) != Some(query_reports.len() as u64)
            || u64_path(route, &["query_runtime_plan_report_count"])
                != Some(query_reports.len() as u64)
            || u64_path(route, &["query_runtime_profile_report_count"])
                != Some(query_reports.len() as u64)
            || u64_path(route, &["query_runtime_failed_query_count"]) != Some(0)
            || u64_path(route, &["query_runtime_missing_plan_evidence_count"]) != Some(0)
            || u64_path(route, &["query_runtime_missing_profile_evidence_count"]) != Some(0)
            || !graph_route_relationship_property_pruning_route_ready(route, query_reports)
            || !query_reports.iter().all(graph_route_query_report_ready)
        {
            return false;
        }
    }

    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .all(|route| observed_routes.contains(route))
}

fn graph_route_relationship_property_pruning_route_ready(
    route: &serde_json::Value,
    query_reports: &[serde_json::Value],
) -> bool {
    let Some(required_count) = u64_path(route, &["relationship_property_pruning_required_count"])
    else {
        return false;
    };
    let Some(reported_count) = u64_path(route, &["relationship_property_pruning_report_count"])
    else {
        return false;
    };
    let actual_count = query_reports
        .iter()
        .filter(|report| graph_route_query_report_has_relationship_property_pruning(report))
        .count() as u64;
    reported_count == required_count
        && actual_count == reported_count
        && bool_path(route, &["relationship_property_pruning_evidence_ready"]) == Some(true)
}

fn graph_route_query_families_ready(route: &serde_json::Value, route_name: &str) -> bool {
    let expected = nowledge_mem_required_query_families_for_route(route_name)
        .iter()
        .map(|family| (*family).to_string())
        .collect::<Vec<_>>();
    if string_array_path(route, &["required_query_families"]) != expected
        || string_array_path(route, &["computed_required_query_families"]) != expected
        || !string_array_path(route, &["query_family_blocker_codes"]).is_empty()
    {
        return false;
    }
    let Some(query_reports) =
        json_get_path(route, &["query_reports"]).and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    if expected.is_empty() {
        return true;
    }
    query_reports.iter().any(|report| {
        str_path(report, &["query_family"])
            .is_some_and(|family| expected.iter().any(|expected| expected == family))
    })
}

fn graph_route_shadow_compare_ready(route: &serde_json::Value) -> bool {
    bool_path(route, &["shadow_compare_ready"]) == Some(true)
        && str_path(route, &["shadow_compare_evidence_source"])
            == Some(ROUTE_PARITY_EVIDENCE_SOURCE)
        && str_path(route, &["shadow_compare", "source"]) == Some(ROUTE_PARITY_EVIDENCE_SOURCE)
        && bool_path(route, &["shadow_compare", "ready"]) == Some(true)
        && u64_path(route, &["shadow_compare", "matched_per_million"])
            == Some(ROUTE_PARITY_FULL_MATCH_PER_MILLION)
        && str_path(route, &["shadow_compare", "primary_engine"])
            .is_some_and(is_legacy_graph_engine)
        && str_path(route, &["shadow_compare", "shadow_engine"]) == Some("skein")
        && string_array_path(route, &["shadow_compare", "blocker_codes"]).is_empty()
        && string_array_path(route, &["shadow_compare", "computed_blocker_codes"]).is_empty()
}

fn is_legacy_graph_engine(engine: &str) -> bool {
    matches!(engine, "kuzu" | "ladybug" | "kuzu/ladybug")
}

fn graph_route_query_report_ready(report: &serde_json::Value) -> bool {
    non_empty_str_path(report, &["query_name"])
        && u64_path(report, &["query_index"]).is_some()
        && str_path(report, &["query_family"])
            .is_some_and(|family| REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.contains(&family))
        && str_path(report, &["protocol"]) == Some(SKEIN_NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL)
        && bool_path(report, &["ready"]) == Some(true)
        && string_array_path(report, &["blocker_codes"]).is_empty()
        && non_empty_str_path(report, &["statement_kind"])
        && matches!(
            str_path(report, &["execution_path"]),
            Some("fast_path" | "optimized_path")
        )
        && bool_path(report, &["fast_path_selected"]).is_some()
        && bool_path(report, &["slow_log_candidate"]).is_some()
        && bool_path(report, &["physical_plan_captured"]).is_some()
        && u64_path(report, &["elapsed_micros"]).is_some()
        && graph_route_query_report_physical_operators_present(report)
        && u64_path(report, &["optimizer_decision_count"]).is_some()
        && u64_path(report, &["optimizer_rule_event_count"]).is_some()
        && u64_path(report, &["scan_pruning_report_count"]).is_some()
        && graph_route_query_report_scan_pruning_present(report)
        && non_empty_str_path(report, &["plan_cache", "lookup"])
        && bool_path(report, &["plan_cache", "cacheable"]).is_some()
        && bool_path(report, &["plan_cache", "hit"]).is_some()
        && bool_path(report, &["plan_cache", "miss"]).is_some()
        && bool_path(report, &["plan_cache", "bypassed"]) == Some(false)
}

fn graph_route_query_report_physical_operators_present(report: &serde_json::Value) -> bool {
    bool_path(report, &["physical_operator_counts_present"]) == Some(true)
        || json_get_path(report, &["physical_operator_counts"])
            .is_some_and(serde_json::Value::is_object)
}

fn graph_route_query_report_scan_pruning_present(report: &serde_json::Value) -> bool {
    let Some(report_count) = u64_path(report, &["scan_pruning_report_count"]) else {
        return false;
    };
    let Some(reports) =
        json_get_path(report, &["scan_pruning_reports"]).and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    report_count == reports.len() as u64
        && !reports.is_empty()
        && reports.iter().all(query_runtime_scan_pruning_report_ready)
}

fn graph_route_query_report_has_relationship_property_pruning(report: &serde_json::Value) -> bool {
    json_get_path(report, &["scan_pruning_reports"])
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .any(|scan_report| {
            query_runtime_scan_pruning_report_ready(scan_report)
                && str_path(scan_report, &["target_kind"]) == Some("relationship")
                && str_path(scan_report, &["strategy", "kind"]) == Some("relationship_property")
        })
}

pub fn graph_route_alignment_cutover_readiness(
    bundle: &serde_json::Value,
) -> GraphRouteAlignmentCutoverReadiness {
    GraphRouteAlignmentCutoverReadiness {
        ready: graph_route_alignment_bool(bundle, "ready"),
        evidence_protocol_matches: graph_route_alignment_bool(bundle, "evidence_protocol_matches"),
        evidence_ready: graph_route_alignment_bool(bundle, "evidence_ready"),
        evidence_route_primary_ready: graph_route_alignment_bool(
            bundle,
            "evidence_route_primary_ready",
        ),
        summary_route_primary_ready: graph_route_alignment_bool(
            bundle,
            "summary_route_primary_ready",
        ),
        route_primary_ready_matches: graph_route_alignment_bool(
            bundle,
            "route_primary_ready_matches",
        ),
        route_query_plan_evidence_ready_matches: graph_route_alignment_bool(
            bundle,
            "route_query_plan_evidence_ready_matches",
        ),
        route_query_profile_evidence_ready_matches: graph_route_alignment_bool(
            bundle,
            "route_query_profile_evidence_ready_matches",
        ),
        route_query_api_behavior_evidence_ready_matches: graph_route_alignment_bool(
            bundle,
            "route_query_api_behavior_evidence_ready_matches",
        ),
        route_relationship_property_pruning_evidence_ready_matches: graph_route_alignment_bool(
            bundle,
            "route_relationship_property_pruning_evidence_ready_matches",
        ),
        relationship_property_pruning_required_count_matches: graph_route_alignment_bool(
            bundle,
            "relationship_property_pruning_required_count_matches",
        ),
        relationship_property_pruning_report_count_matches: graph_route_alignment_bool(
            bundle,
            "relationship_property_pruning_report_count_matches",
        ),
        primary_ready_routes_match: graph_route_alignment_bool(
            bundle,
            "primary_ready_routes_match",
        ),
        evidence_required_routes_covered: graph_route_alignment_bool(
            bundle,
            "evidence_required_routes_covered",
        ),
        summary_required_routes_covered: graph_route_alignment_bool(
            bundle,
            "summary_required_routes_covered",
        ),
        evidence_route_catalog_version_ready: graph_route_alignment_bool(
            bundle,
            "evidence_route_catalog_version_ready",
        ),
        summary_route_catalog_version_ready: graph_route_alignment_bool(
            bundle,
            "summary_route_catalog_version_ready",
        ),
        evidence_route_catalog_digest_ready: graph_route_alignment_bool(
            bundle,
            "evidence_route_catalog_digest_ready",
        ),
        summary_route_catalog_digest_ready: graph_route_alignment_bool(
            bundle,
            "summary_route_catalog_digest_ready",
        ),
        route_catalog_version_matches: graph_route_alignment_bool(
            bundle,
            "route_catalog_version_matches",
        ),
        route_catalog_digest_matches: graph_route_alignment_bool(
            bundle,
            "route_catalog_digest_matches",
        ),
        blocker_codes: blocker_codes(
            bundle,
            &[&["replacement_summary_graph_route_alignment", "blocker_codes"][..]],
        ),
    }
}

fn graph_route_alignment_bool(bundle: &serde_json::Value, field: &str) -> bool {
    bool_path(
        bundle,
        &["replacement_summary_graph_route_alignment", field],
    ) == Some(true)
}

fn graph_route_alignment_cutover_conditions(
    readiness: &GraphRouteAlignmentCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "replacement_summary_graph_route_alignment.ready",
            readiness.ready,
        ),
        (
            "replacement_summary_graph_route_alignment.evidence_protocol_matches",
            readiness.evidence_protocol_matches,
        ),
        (
            "replacement_summary_graph_route_alignment.evidence_ready",
            readiness.evidence_ready,
        ),
        (
            "replacement_summary_graph_route_alignment.evidence_route_primary_ready",
            readiness.evidence_route_primary_ready,
        ),
        (
            "replacement_summary_graph_route_alignment.summary_route_primary_ready",
            readiness.summary_route_primary_ready,
        ),
        (
            "replacement_summary_graph_route_alignment.route_primary_ready_matches",
            readiness.route_primary_ready_matches,
        ),
        (
            "replacement_summary_graph_route_alignment.route_query_plan_evidence_ready_matches",
            readiness.route_query_plan_evidence_ready_matches,
        ),
        (
            "replacement_summary_graph_route_alignment.route_query_profile_evidence_ready_matches",
            readiness.route_query_profile_evidence_ready_matches,
        ),
        (
            "replacement_summary_graph_route_alignment.route_query_api_behavior_evidence_ready_matches",
            readiness.route_query_api_behavior_evidence_ready_matches,
        ),
        (
            "replacement_summary_graph_route_alignment.route_relationship_property_pruning_evidence_ready_matches",
            readiness.route_relationship_property_pruning_evidence_ready_matches,
        ),
        (
            "replacement_summary_graph_route_alignment.relationship_property_pruning_required_count_matches",
            readiness.relationship_property_pruning_required_count_matches,
        ),
        (
            "replacement_summary_graph_route_alignment.relationship_property_pruning_report_count_matches",
            readiness.relationship_property_pruning_report_count_matches,
        ),
        (
            "replacement_summary_graph_route_alignment.primary_ready_routes_match",
            readiness.primary_ready_routes_match,
        ),
        (
            "replacement_summary_graph_route_alignment.evidence_required_routes_covered",
            readiness.evidence_required_routes_covered,
        ),
        (
            "replacement_summary_graph_route_alignment.summary_required_routes_covered",
            readiness.summary_required_routes_covered,
        ),
        (
            "replacement_summary_graph_route_alignment.evidence_route_catalog_version_ready",
            readiness.evidence_route_catalog_version_ready,
        ),
        (
            "replacement_summary_graph_route_alignment.summary_route_catalog_version_ready",
            readiness.summary_route_catalog_version_ready,
        ),
        (
            "replacement_summary_graph_route_alignment.evidence_route_catalog_digest_ready",
            readiness.evidence_route_catalog_digest_ready,
        ),
        (
            "replacement_summary_graph_route_alignment.summary_route_catalog_digest_ready",
            readiness.summary_route_catalog_digest_ready,
        ),
        (
            "replacement_summary_graph_route_alignment.route_catalog_version_matches",
            readiness.route_catalog_version_matches,
        ),
        (
            "replacement_summary_graph_route_alignment.route_catalog_digest_matches",
            readiness.route_catalog_digest_matches,
        ),
    ]
}

pub fn graph_route_parity_alignment_cutover_readiness(
    bundle: &serde_json::Value,
) -> GraphRouteParityAlignmentCutoverReadiness {
    let required_route_count = u64_path(
        bundle,
        &["graph_route_parity_alignment", "required_route_count"],
    );
    GraphRouteParityAlignmentCutoverReadiness {
        ready: bool_path(bundle, &["graph_route_parity_alignment", "ready"]) == Some(true),
        required_route_count_present: required_route_count.is_some_and(|value| value > 0),
        ready_route_count_matches: u64_path(
            bundle,
            &["graph_route_parity_alignment", "ready_route_count"],
        ) == required_route_count,
        missing_routes_empty: string_array_path(
            bundle,
            &["graph_route_parity_alignment", "missing_routes"],
        )
        .is_empty(),
        not_ready_routes_empty: string_array_path(
            bundle,
            &["graph_route_parity_alignment", "not_ready_routes"],
        )
        .is_empty(),
        route_mismatch_routes_empty: string_array_path(
            bundle,
            &["graph_route_parity_alignment", "route_mismatch_routes"],
        )
        .is_empty(),
        protocol_mismatch_routes_empty: string_array_path(
            bundle,
            &["graph_route_parity_alignment", "protocol_mismatch_routes"],
        )
        .is_empty(),
        blocker_routes_empty: string_array_path(
            bundle,
            &["graph_route_parity_alignment", "blocker_routes"],
        )
        .is_empty(),
        blocker_codes: blocker_codes(
            bundle,
            &[&["graph_route_parity_alignment", "blocker_codes"][..]],
        ),
    }
}

fn graph_route_parity_alignment_cutover_conditions(
    readiness: &GraphRouteParityAlignmentCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        ("graph_route_parity_alignment.ready", readiness.ready),
        (
            "graph_route_parity_alignment.required_route_count",
            readiness.required_route_count_present,
        ),
        (
            "graph_route_parity_alignment.ready_route_count",
            readiness.ready_route_count_matches,
        ),
        (
            "graph_route_parity_alignment.missing_routes",
            readiness.missing_routes_empty,
        ),
        (
            "graph_route_parity_alignment.not_ready_routes",
            readiness.not_ready_routes_empty,
        ),
        (
            "graph_route_parity_alignment.route_mismatch_routes",
            readiness.route_mismatch_routes_empty,
        ),
        (
            "graph_route_parity_alignment.protocol_mismatch_routes",
            readiness.protocol_mismatch_routes_empty,
        ),
        (
            "graph_route_parity_alignment.blocker_routes",
            readiness.blocker_routes_empty,
        ),
    ]
}

pub fn query_runtime_preflight_cutover_readiness(
    bundle: &serde_json::Value,
) -> QueryRuntimePreflightCutoverReadiness {
    QueryRuntimePreflightCutoverReadiness {
        protocol_matches: str_path(bundle, &["query_runtime_preflight", "protocol"])
            == Some(SKEIN_NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL),
        ready: bool_path(bundle, &["query_runtime_preflight", "ready"]) == Some(true),
        database_opened: bool_path(bundle, &["query_runtime_preflight", "database_opened"])
            == Some(true),
        redaction_ready: bool_path(bundle, &["query_runtime_preflight", "redaction", "ready"])
            == Some(true),
        rows_redacted: bool_path(
            bundle,
            &["query_runtime_preflight", "redaction", "rows_copied"],
        ) == Some(false),
        parameters_redacted: bool_path(
            bundle,
            &["query_runtime_preflight", "redaction", "parameters_copied"],
        ) == Some(false),
        local_paths_redacted: bool_path(
            bundle,
            &["query_runtime_preflight", "redaction", "local_paths_copied"],
        ) == Some(false),
        raw_errors_redacted: bool_path(
            bundle,
            &["query_runtime_preflight", "redaction", "raw_errors_copied"],
        ) == Some(false),
        probe_count_present: u64_path(bundle, &["query_runtime_preflight", "probe_count"])
            .is_some_and(|value| value > 0),
        probe_counts_match: query_runtime_preflight_counts_match(bundle),
        failed_probe_count_zero: u64_path(
            bundle,
            &["query_runtime_preflight", "failed_probe_count"],
        ) == Some(0),
        route_coverage_ready: query_runtime_preflight_route_coverage_ready(bundle),
        route_catalog_version_matches: str_path(
            bundle,
            &["query_runtime_preflight", "route_catalog_version"],
        ) == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION),
        route_catalog_digest_present: str_path(
            bundle,
            &["query_runtime_preflight", "route_catalog_digest"],
        )
        .is_some(),
        probe_details_ready: query_runtime_preflight_probe_details_ready(bundle),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &["query_runtime_preflight", "blocker_codes"][..],
                &["query_runtime_preflight", "failed_checks"][..],
                &["query_runtime_preflight", "route_coverage_blocker_codes"][..],
            ],
        ),
    }
}

fn query_runtime_preflight_cutover_conditions(
    readiness: &QueryRuntimePreflightCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "query_runtime_preflight.protocol",
            readiness.protocol_matches,
        ),
        ("query_runtime_preflight.ready", readiness.ready),
        (
            "query_runtime_preflight.database_opened",
            readiness.database_opened,
        ),
        (
            "query_runtime_preflight.redaction.ready",
            readiness.redaction_ready,
        ),
        (
            "query_runtime_preflight.redaction.rows_copied",
            readiness.rows_redacted,
        ),
        (
            "query_runtime_preflight.redaction.parameters_copied",
            readiness.parameters_redacted,
        ),
        (
            "query_runtime_preflight.redaction.local_paths_copied",
            readiness.local_paths_redacted,
        ),
        (
            "query_runtime_preflight.redaction.raw_errors_copied",
            readiness.raw_errors_redacted,
        ),
        (
            "query_runtime_preflight.probe_count",
            readiness.probe_count_present,
        ),
        (
            "query_runtime_preflight.passed_probe_count",
            readiness.probe_counts_match,
        ),
        (
            "query_runtime_preflight.failed_probe_count",
            readiness.failed_probe_count_zero,
        ),
        (
            "query_runtime_preflight.route_coverage",
            readiness.route_coverage_ready,
        ),
        (
            "query_runtime_preflight.route_catalog_version",
            readiness.route_catalog_version_matches,
        ),
        (
            "query_runtime_preflight.route_catalog_digest",
            readiness.route_catalog_digest_present,
        ),
        (
            "query_runtime_preflight.probes",
            readiness.probe_details_ready,
        ),
    ]
}

fn query_runtime_preflight_counts_match(bundle: &serde_json::Value) -> bool {
    let probe_count = u64_path(bundle, &["query_runtime_preflight", "probe_count"]);
    let passed_probe_count = u64_path(bundle, &["query_runtime_preflight", "passed_probe_count"]);
    probe_count.is_some_and(|value| value > 0) && probe_count == passed_probe_count
}

fn query_runtime_preflight_probe_details_ready(bundle: &serde_json::Value) -> bool {
    let Some(probes) = json_get_path(bundle, &["query_runtime_preflight", "probes"])
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    !probes.is_empty() && probes.iter().all(query_runtime_preflight_probe_ready)
}

fn query_runtime_preflight_route_coverage_ready(bundle: &serde_json::Value) -> bool {
    let value = match json_get_path(bundle, &["query_runtime_preflight"]) {
        Some(value) => value,
        None => return false,
    };
    let Some(probes) = value
        .get("probes")
        .and_then(serde_json::Value::as_array)
        .filter(|probes| !probes.is_empty())
    else {
        return false;
    };
    let observed_routes = probes
        .iter()
        .filter_map(|probe| str_path(probe, &["route"]))
        .collect::<Vec<_>>();
    let observed_route_set = observed_routes.iter().copied().collect::<BTreeSet<_>>();
    let duplicate_routes = duplicate_routes(&observed_routes);
    let unknown_routes = observed_route_set
        .iter()
        .filter(|route| !REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(route))
        .count();
    u64_path(value, &["required_route_count"])
        == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && u64_path(value, &["covered_route_count"])
            == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
        && bool_path(value, &["required_routes_covered"]) == Some(true)
        && string_array_path(value, &["missing_required_routes"]).is_empty()
        && string_array_path(value, &["unknown_routes"]).is_empty()
        && string_array_path(value, &["duplicate_routes"]).is_empty()
        && str_path(value, &["route_catalog_version"])
            == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION)
        && str_path(value, &["route_catalog_digest"])
            == Some(nowledge_mem_graph_read_route_catalog_digest().as_str())
        && bool_path(value, &["route_coverage_ready"]) == Some(true)
        && string_array_path(value, &["route_coverage_blocker_codes"]).is_empty()
        && unknown_routes == 0
        && duplicate_routes.is_empty()
        && REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .all(|route| observed_route_set.contains(route))
}

fn duplicate_routes(routes: &[&str]) -> Vec<String> {
    let mut counts = BTreeMap::<&str, usize>::new();
    for route in routes {
        *counts.entry(*route).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(route, _)| route.to_string())
        .collect()
}

fn query_runtime_preflight_probe_ready(probe: &serde_json::Value) -> bool {
    query_runtime_preflight_probe_identity_ready(probe)
        && bool_path(probe, &["ready"]) == Some(true)
        && bool_path(probe, &["success"]) == Some(true)
        && non_empty_str_path(probe, &["selected_plan_fingerprint"])
        && u64_path(probe, &["output_row_count"]).is_some()
        && json_object_path_is_non_empty(probe, &["selected_plan_operator_counts"])
        && json_object_path_is_non_empty(probe, &["selected_plan_class_counts"])
        && u64_path(probe, &["optimizer_decision_count"]).is_some()
        && u64_path(probe, &["optimizer_rule_event_count"]).is_some()
        && query_runtime_preflight_probe_plan_cache_ready(probe)
        && query_runtime_preflight_probe_scan_pruning_ready(probe)
        && string_array_path(probe, &["blocker_codes"]).is_empty()
}

fn query_runtime_preflight_probe_identity_ready(probe: &serde_json::Value) -> bool {
    str_path(probe, &["name"]).is_some_and(|name| {
        let name = name.trim();
        !name.is_empty() && name != "unnamed"
    }) && str_path(probe, &["route"])
        .is_some_and(|route| REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(&route))
        && str_path(probe, &["query_family"])
            .is_some_and(|family| REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.contains(&family))
}

fn query_runtime_preflight_probe_plan_cache_ready(probe: &serde_json::Value) -> bool {
    non_empty_str_path(probe, &["plan_cache_lookup"])
        && non_empty_str_path(probe, &["plan_cache", "lookup"])
        && bool_path(probe, &["plan_cache", "cacheable"]).is_some()
        && bool_path(probe, &["plan_cache", "hit"]).is_some()
        && bool_path(probe, &["plan_cache", "miss"]).is_some()
        && bool_path(probe, &["plan_cache", "bypassed"]) == Some(false)
}

fn query_runtime_preflight_probe_scan_pruning_ready(probe: &serde_json::Value) -> bool {
    let Some(report_count) = u64_path(probe, &["execution_profile", "scan_pruning_report_count"])
    else {
        return false;
    };
    let Some(reports) = json_get_path(probe, &["execution_profile", "scan_pruning_reports"])
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    report_count == reports.len() as u64
        && u64_path(probe, &["execution_profile", "pruned_scan_count"]).is_some()
        && reports.iter().all(query_runtime_scan_pruning_report_ready)
}

pub fn query_runtime_preflight_alignment_cutover_readiness(
    bundle: &serde_json::Value,
) -> QueryRuntimePreflightAlignmentCutoverReadiness {
    QueryRuntimePreflightAlignmentCutoverReadiness {
        ready: query_runtime_preflight_alignment_bool(bundle, "ready"),
        evidence_ready: query_runtime_preflight_alignment_bool(bundle, "evidence_ready"),
        summary_ready: query_runtime_preflight_alignment_bool(bundle, "summary_ready"),
        protocol_matches: query_runtime_preflight_alignment_bool(bundle, "protocol_matches"),
        readiness_matches: query_runtime_preflight_alignment_bool(bundle, "readiness_matches"),
        database_opened_matches: query_runtime_preflight_alignment_bool(
            bundle,
            "database_opened_matches",
        ),
        probe_count_matches: query_runtime_preflight_alignment_bool(bundle, "probe_count_matches"),
        passed_probe_count_matches: query_runtime_preflight_alignment_bool(
            bundle,
            "passed_probe_count_matches",
        ),
        failed_probe_count_matches: query_runtime_preflight_alignment_bool(
            bundle,
            "failed_probe_count_matches",
        ),
        required_route_count_matches: query_runtime_preflight_alignment_bool(
            bundle,
            "required_route_count_matches",
        ),
        covered_route_count_matches: query_runtime_preflight_alignment_bool(
            bundle,
            "covered_route_count_matches",
        ),
        covered_routes_matches: query_runtime_preflight_alignment_bool(
            bundle,
            "covered_routes_matches",
        ),
        required_routes_covered_matches: query_runtime_preflight_alignment_bool(
            bundle,
            "required_routes_covered_matches",
        ),
        route_coverage_ready_matches: query_runtime_preflight_alignment_bool(
            bundle,
            "route_coverage_ready_matches",
        ),
        evidence_route_catalog_version_ready: query_runtime_preflight_alignment_bool(
            bundle,
            "evidence_route_catalog_version_ready",
        ),
        summary_route_catalog_version_ready: query_runtime_preflight_alignment_bool(
            bundle,
            "summary_route_catalog_version_ready",
        ),
        evidence_route_catalog_digest_ready: query_runtime_preflight_alignment_bool(
            bundle,
            "evidence_route_catalog_digest_ready",
        ),
        summary_route_catalog_digest_ready: query_runtime_preflight_alignment_bool(
            bundle,
            "summary_route_catalog_digest_ready",
        ),
        route_catalog_version_matches: query_runtime_preflight_alignment_bool(
            bundle,
            "route_catalog_version_matches",
        ),
        route_catalog_digest_matches: query_runtime_preflight_alignment_bool(
            bundle,
            "route_catalog_digest_matches",
        ),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &["query_runtime_preflight", "blocker_codes"][..],
                &[
                    "replacement_summary",
                    "query_runtime_preflight",
                    "blocker_codes",
                ][..],
                &[
                    "replacement_summary_query_runtime_alignment",
                    "blocker_codes",
                ][..],
            ],
        ),
    }
}

fn query_runtime_preflight_alignment_bool(bundle: &serde_json::Value, field: &str) -> bool {
    bool_path(
        bundle,
        &["replacement_summary_query_runtime_alignment", field],
    ) == Some(true)
}

fn query_runtime_preflight_alignment_cutover_conditions(
    readiness: &QueryRuntimePreflightAlignmentCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "replacement_summary_query_runtime_alignment.ready",
            readiness.ready,
        ),
        (
            "replacement_summary_query_runtime_alignment.evidence_ready",
            readiness.evidence_ready,
        ),
        (
            "replacement_summary_query_runtime_alignment.summary_ready",
            readiness.summary_ready,
        ),
        (
            "replacement_summary_query_runtime_alignment.protocol_matches",
            readiness.protocol_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.readiness_matches",
            readiness.readiness_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.database_opened_matches",
            readiness.database_opened_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.probe_count_matches",
            readiness.probe_count_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.passed_probe_count_matches",
            readiness.passed_probe_count_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.failed_probe_count_matches",
            readiness.failed_probe_count_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.required_route_count_matches",
            readiness.required_route_count_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.covered_route_count_matches",
            readiness.covered_route_count_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.covered_routes_matches",
            readiness.covered_routes_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.required_routes_covered_matches",
            readiness.required_routes_covered_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.route_coverage_ready_matches",
            readiness.route_coverage_ready_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.evidence_route_catalog_version_ready",
            readiness.evidence_route_catalog_version_ready,
        ),
        (
            "replacement_summary_query_runtime_alignment.summary_route_catalog_version_ready",
            readiness.summary_route_catalog_version_ready,
        ),
        (
            "replacement_summary_query_runtime_alignment.evidence_route_catalog_digest_ready",
            readiness.evidence_route_catalog_digest_ready,
        ),
        (
            "replacement_summary_query_runtime_alignment.summary_route_catalog_digest_ready",
            readiness.summary_route_catalog_digest_ready,
        ),
        (
            "replacement_summary_query_runtime_alignment.route_catalog_version_matches",
            readiness.route_catalog_version_matches,
        ),
        (
            "replacement_summary_query_runtime_alignment.route_catalog_digest_matches",
            readiness.route_catalog_digest_matches,
        ),
    ]
}

fn query_runtime_scan_pruning_report_ready(report: &serde_json::Value) -> bool {
    scan_pruning_target_kind_ready(report)
        && json_get_path(report, &["strategy"]).is_some_and(serde_json::Value::is_object)
        && bool_path(report, &["pruned"]).is_some()
        && bool_path(report, &["exact_empty"]).is_some()
        && u64_path(report, &["candidate_count_before_pruning"]).is_some()
        && u64_path(report, &["pruned_candidate_count"]).is_some()
        && u64_path(report, &["candidate_count_before_filter"]).is_some()
        && u64_path(report, &["output_count"]).is_some()
        && u64_path(report, &["filtered_out_count"]).is_some()
}

fn scan_pruning_target_kind_ready(report: &serde_json::Value) -> bool {
    matches!(
        str_path(report, &["target_kind"]),
        Some("node" | "relationship")
    )
}

pub fn library_readiness_cutover_readiness(
    bundle: &serde_json::Value,
) -> LibraryReadinessCutoverReadiness {
    LibraryReadinessCutoverReadiness {
        protocol_matches: str_path(bundle, &["library_readiness", "protocol"])
            == Some(NOWLEDGE_MEM_LIBRARY_READINESS_PROTOCOL),
        present: bool_path(bundle, &["library_readiness", "present"]) == Some(true),
        ready: bool_path(bundle, &["library_readiness", "ready"]) == Some(true),
        production_path_ready: bool_path(
            bundle,
            &["library_readiness", "production_path", "ready"],
        ) == Some(true),
        production_path_in_process: bool_path(
            bundle,
            &["library_readiness", "production_path", "in_process"],
        ) == Some(true),
        production_path_cli_not_required: bool_path(
            bundle,
            &["library_readiness", "production_path", "cli_required"],
        ) == Some(false),
        production_path_env_control_plane_not_required: bool_path(
            bundle,
            &[
                "library_readiness",
                "production_path",
                "env_control_plane_required",
            ],
        ) == Some(false),
        production_path_spawned_helper_not_required: bool_path(
            bundle,
            &[
                "library_readiness",
                "production_path",
                "spawned_helper_required",
            ],
        ) == Some(false),
        ready_area_count_present: u64_path(bundle, &["library_readiness", "ready_area_count"])
            .is_some_and(|value| value > 0),
        blocked_area_count_zero: u64_path(bundle, &["library_readiness", "blocked_area_count"])
            == Some(0),
        redaction_ready: bool_path(bundle, &["library_readiness", "redaction", "ready"])
            == Some(true),
        query_text_redacted: bool_path(
            bundle,
            &["library_readiness", "redaction", "query_text_copied"],
        ) == Some(false),
        parameters_redacted: bool_path(
            bundle,
            &["library_readiness", "redaction", "parameters_copied"],
        ) == Some(false),
        local_paths_redacted: bool_path(
            bundle,
            &["library_readiness", "redaction", "local_paths_copied"],
        ) == Some(false),
        graph_opened: bool_path(
            bundle,
            &["library_readiness", "open_report", "graph_opened"],
        ) == Some(true),
        search_projection_opened: bool_path(
            bundle,
            &[
                "library_readiness",
                "open_report",
                "search_projection_opened",
            ],
        ) == Some(true),
        graph_ready: library_readiness_area_ready(bundle, "graph"),
        query_ready: library_readiness_area_ready(bundle, "query"),
        storage_ready: library_readiness_area_ready(bundle, "storage"),
        background_ready: library_readiness_area_ready(bundle, "background"),
        query_family_ready: library_readiness_area_ready(bundle, "query_family"),
        graph_route_ready: library_readiness_area_ready(bundle, "graph_route"),
        search_route_ownership_ready: library_readiness_area_ready(
            bundle,
            "search_route_ownership",
        ),
        search_projection_ready: library_readiness_area_ready(bundle, "search_projection"),
        search_projection_shadow_ready: library_readiness_area_ready(
            bundle,
            "search_projection_shadow",
        ),
        search_candidate_shadow_ready: library_readiness_area_ready(
            bundle,
            "search_candidate_shadow",
        ),
        workload_fixture_ready: library_readiness_area_ready(bundle, "workload_fixture"),
        production_resource_profile_ready: json_get_path(
            bundle,
            &["library_readiness", "production_resource_profile"],
        )
        .is_some_and(production_resource_profile_ready),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &["library_readiness", "blocker_codes"][..],
                &[
                    "library_readiness",
                    "readiness_by_area",
                    "graph",
                    "blocker_codes",
                ][..],
                &[
                    "library_readiness",
                    "readiness_by_area",
                    "query",
                    "blocker_codes",
                ][..],
                &[
                    "library_readiness",
                    "readiness_by_area",
                    "storage",
                    "blocker_codes",
                ][..],
                &[
                    "library_readiness",
                    "readiness_by_area",
                    "background",
                    "blocker_codes",
                ][..],
                &[
                    "library_readiness",
                    "readiness_by_area",
                    "query_family",
                    "blocker_codes",
                ][..],
                &[
                    "library_readiness",
                    "readiness_by_area",
                    "graph_route",
                    "blocker_codes",
                ][..],
                &[
                    "library_readiness",
                    "readiness_by_area",
                    "search_route_ownership",
                    "blocker_codes",
                ][..],
                &[
                    "library_readiness",
                    "readiness_by_area",
                    "search_projection",
                    "blocker_codes",
                ][..],
                &[
                    "library_readiness",
                    "readiness_by_area",
                    "search_projection_shadow",
                    "blocker_codes",
                ][..],
                &[
                    "library_readiness",
                    "readiness_by_area",
                    "search_candidate_shadow",
                    "blocker_codes",
                ][..],
                &[
                    "library_readiness",
                    "readiness_by_area",
                    "workload_fixture",
                    "blocker_codes",
                ][..],
            ],
        ),
    }
}

fn library_readiness_cutover_conditions(
    readiness: &LibraryReadinessCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        ("library_readiness.protocol", readiness.protocol_matches),
        ("library_readiness.present", readiness.present),
        ("library_readiness.ready", readiness.ready),
        (
            "library_readiness.production_path.ready",
            readiness.production_path_ready,
        ),
        (
            "library_readiness.production_path.in_process",
            readiness.production_path_in_process,
        ),
        (
            "library_readiness.production_path.cli_required",
            readiness.production_path_cli_not_required,
        ),
        (
            "library_readiness.production_path.env_control_plane_required",
            readiness.production_path_env_control_plane_not_required,
        ),
        (
            "library_readiness.production_path.spawned_helper_required",
            readiness.production_path_spawned_helper_not_required,
        ),
        (
            "library_readiness.ready_area_count",
            readiness.ready_area_count_present,
        ),
        (
            "library_readiness.blocked_area_count",
            readiness.blocked_area_count_zero,
        ),
        (
            "library_readiness.redaction.ready",
            readiness.redaction_ready,
        ),
        (
            "library_readiness.redaction.query_text_copied",
            readiness.query_text_redacted,
        ),
        (
            "library_readiness.redaction.parameters_copied",
            readiness.parameters_redacted,
        ),
        (
            "library_readiness.redaction.local_paths_copied",
            readiness.local_paths_redacted,
        ),
        (
            "library_readiness.open_report.graph_opened",
            readiness.graph_opened,
        ),
        (
            "library_readiness.open_report.search_projection_opened",
            readiness.search_projection_opened,
        ),
        (
            "library_readiness.readiness_by_area.graph.ready",
            readiness.graph_ready,
        ),
        (
            "library_readiness.readiness_by_area.query.ready",
            readiness.query_ready,
        ),
        (
            "library_readiness.readiness_by_area.storage.ready",
            readiness.storage_ready,
        ),
        (
            "library_readiness.readiness_by_area.background.ready",
            readiness.background_ready,
        ),
        (
            "library_readiness.readiness_by_area.query_family.ready",
            readiness.query_family_ready,
        ),
        (
            "library_readiness.readiness_by_area.graph_route.ready",
            readiness.graph_route_ready,
        ),
        (
            "library_readiness.readiness_by_area.search_route_ownership.ready",
            readiness.search_route_ownership_ready,
        ),
        (
            "library_readiness.readiness_by_area.search_projection.ready",
            readiness.search_projection_ready,
        ),
        (
            "library_readiness.readiness_by_area.search_projection_shadow.ready",
            readiness.search_projection_shadow_ready,
        ),
        (
            "library_readiness.readiness_by_area.search_candidate_shadow.ready",
            readiness.search_candidate_shadow_ready,
        ),
        (
            "library_readiness.readiness_by_area.workload_fixture.ready",
            readiness.workload_fixture_ready,
        ),
        (
            "library_readiness.production_resource_profile",
            readiness.production_resource_profile_ready,
        ),
    ]
}

fn library_readiness_area_ready(bundle: &serde_json::Value, area: &str) -> bool {
    json_get_path(
        bundle,
        &["library_readiness", "readiness_by_area", area, "ready"],
    )
    .and_then(serde_json::Value::as_bool)
        == Some(true)
}

pub fn cutover_controls_readiness(bundle: &serde_json::Value) -> CutoverControlsReadiness {
    let initial_import_enabled = bool_path(
        bundle,
        &["cutover_controls", "work", "initial_import_enabled"],
    ) == Some(true);
    let dual_writes_enabled =
        bool_path(bundle, &["cutover_controls", "work", "dual_writes_enabled"]) == Some(true);
    let initial_import_inactive_for_cutover = bool_path(
        bundle,
        &[
            "cutover_controls",
            "work",
            "initial_import_inactive_for_cutover",
        ],
    ) == Some(true);
    let initial_import_cutover_catch_up_ready = bool_path(
        bundle,
        &[
            "cutover_controls",
            "work",
            "initial_import_cutover_catch_up_ready",
        ],
    ) == Some(true);
    CutoverControlsReadiness {
        protocol_matches: str_path(bundle, &["cutover_controls", "protocol"])
            == Some(NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL),
        ready: bool_path(bundle, &["cutover_controls", "ready"]) == Some(true),
        graph_reads_skein: str_path(bundle, &["cutover_controls", "controls", "graph_reads"])
            == Some("skein"),
        graph_read_effective: bool_path(bundle, &["cutover_controls", "graph", "read_effective"])
            == Some(true),
        graph_production_status_effective: bool_path(
            bundle,
            &[
                "cutover_controls",
                "production_status",
                "graph",
                "skein_cutover_effective",
            ],
        ) == Some(true),
        search_reads_skein: str_path(bundle, &["cutover_controls", "controls", "search_reads"])
            == Some("skein"),
        search_read_effective: bool_path(bundle, &["cutover_controls", "search", "read_effective"])
            == Some(true),
        search_production_status_effective: bool_path(
            bundle,
            &[
                "cutover_controls",
                "production_status",
                "search",
                "skein_cutover_effective",
            ],
        ) == Some(true),
        dual_writes_enabled,
        projection_catch_up_enabled: bool_path(
            bundle,
            &["cutover_controls", "work", "projection_catch_up_enabled"],
        ) == Some(true),
        initial_import_safe: !initial_import_enabled || dual_writes_enabled,
        initial_import_inactive_for_cutover,
        initial_import_cutover_catch_up_ready,
        initial_import_safe_for_read_cutover: initial_import_inactive_for_cutover
            || initial_import_cutover_catch_up_ready,
        query_text_redacted: bool_path(
            bundle,
            &["cutover_controls", "redaction", "query_text_copied"],
        ) == Some(false),
        parameters_redacted: bool_path(
            bundle,
            &["cutover_controls", "redaction", "parameters_copied"],
        ) == Some(false),
        local_paths_redacted: bool_path(
            bundle,
            &["cutover_controls", "redaction", "local_paths_copied"],
        ) == Some(false),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &["cutover_controls", "blocker_codes"][..],
                &["cutover_controls", "production_status", "blocker_codes"][..],
            ],
        ),
    }
}

fn cutover_controls_conditions(readiness: &CutoverControlsReadiness) -> Vec<(&'static str, bool)> {
    vec![
        ("cutover_controls.protocol", readiness.protocol_matches),
        ("cutover_controls.ready", readiness.ready),
        (
            "cutover_controls.controls.graph_reads",
            readiness.graph_reads_skein,
        ),
        (
            "cutover_controls.graph.read_effective",
            readiness.graph_read_effective,
        ),
        (
            "cutover_controls.production_status.graph.skein_cutover_effective",
            readiness.graph_production_status_effective,
        ),
        (
            "cutover_controls.controls.search_reads",
            readiness.search_reads_skein,
        ),
        (
            "cutover_controls.search.read_effective",
            readiness.search_read_effective,
        ),
        (
            "cutover_controls.production_status.search.skein_cutover_effective",
            readiness.search_production_status_effective,
        ),
        (
            "cutover_controls.work.dual_writes_enabled",
            readiness.dual_writes_enabled,
        ),
        (
            "cutover_controls.work.projection_catch_up_enabled",
            readiness.projection_catch_up_enabled,
        ),
        (
            "cutover_controls.work.initial_import_enabled",
            readiness.initial_import_safe,
        ),
        (
            "cutover_controls.work.initial_import_safe_for_read_cutover",
            readiness.initial_import_safe_for_read_cutover,
        ),
        (
            "cutover_controls.redaction.query_text_copied",
            readiness.query_text_redacted,
        ),
        (
            "cutover_controls.redaction.parameters_copied",
            readiness.parameters_redacted,
        ),
        (
            "cutover_controls.redaction.local_paths_copied",
            readiness.local_paths_redacted,
        ),
    ]
}

pub fn operations_readiness_cutover_readiness(
    bundle: &serde_json::Value,
) -> OperationsReadinessCutoverReadiness {
    OperationsReadinessCutoverReadiness {
        protocol_matches: str_path(bundle, &["operations_readiness", "protocol"])
            == Some(NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL),
        present: bool_path(bundle, &["operations_readiness", "present"]) == Some(true),
        ready: bool_path(bundle, &["operations_readiness", "ready"]) == Some(true),
        graph_open: bool_path(bundle, &["operations_readiness", "graph", "open"]) == Some(true),
        graph_writable: bool_path(bundle, &["operations_readiness", "graph", "read_only"])
            == Some(false),
        search_projection_open: bool_path(
            bundle,
            &["operations_readiness", "search_projection", "open"],
        ) == Some(true),
        search_projection_not_stale: bool_path(
            bundle,
            &["operations_readiness", "search_projection", "stale"],
        ) == Some(false),
        storage_lifecycle_ready: bool_path(
            bundle,
            &["operations_readiness", "storage_lifecycle", "ready"],
        ) == Some(true),
        storage_lifecycle_action_ready: str_path(
            bundle,
            &["operations_readiness", "storage_lifecycle", "action"],
        ) == Some("ready"),
        storage_recovery_ready: bool_path(
            bundle,
            &[
                "operations_readiness",
                "readiness",
                "storage_recovery_ready",
            ],
        ) == Some(true),
        slow_query_ready: bool_path(
            bundle,
            &["operations_readiness", "readiness", "slow_query_ready"],
        ) == Some(true),
        background_maintenance_ready: bool_path(
            bundle,
            &[
                "operations_readiness",
                "readiness",
                "background_maintenance_ready",
            ],
        ) == Some(true),
        query_text_redacted: bool_path(
            bundle,
            &["operations_readiness", "redaction", "query_text_copied"],
        ) == Some(false),
        parameters_redacted: bool_path(
            bundle,
            &["operations_readiness", "redaction", "parameters_copied"],
        ) == Some(false),
        local_paths_redacted: bool_path(
            bundle,
            &["operations_readiness", "redaction", "local_paths_copied"],
        ) == Some(false),
        blocker_codes: blocker_codes(bundle, &[&["operations_readiness", "blocker_codes"][..]]),
    }
}

fn operations_readiness_cutover_conditions(
    readiness: &OperationsReadinessCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        ("operations_readiness.protocol", readiness.protocol_matches),
        ("operations_readiness.present", readiness.present),
        ("operations_readiness.ready", readiness.ready),
        ("operations_readiness.graph.open", readiness.graph_open),
        (
            "operations_readiness.graph.read_only",
            readiness.graph_writable,
        ),
        (
            "operations_readiness.search_projection.open",
            readiness.search_projection_open,
        ),
        (
            "operations_readiness.search_projection.stale",
            readiness.search_projection_not_stale,
        ),
        (
            "operations_readiness.storage_lifecycle.ready",
            readiness.storage_lifecycle_ready,
        ),
        (
            "operations_readiness.storage_lifecycle.action",
            readiness.storage_lifecycle_action_ready,
        ),
        (
            "operations_readiness.readiness.storage_recovery_ready",
            readiness.storage_recovery_ready,
        ),
        (
            "operations_readiness.readiness.slow_query_ready",
            readiness.slow_query_ready,
        ),
        (
            "operations_readiness.readiness.background_maintenance_ready",
            readiness.background_maintenance_ready,
        ),
        (
            "operations_readiness.redaction.query_text_copied",
            readiness.query_text_redacted,
        ),
        (
            "operations_readiness.redaction.parameters_copied",
            readiness.parameters_redacted,
        ),
        (
            "operations_readiness.redaction.local_paths_copied",
            readiness.local_paths_redacted,
        ),
    ]
}

pub fn storage_recovery_cutover_readiness(
    bundle: &serde_json::Value,
) -> StorageRecoveryCutoverReadiness {
    StorageRecoveryCutoverReadiness {
        required: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "storage_recovery_required",
            ],
        ) == Some(true),
        ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "storage_recovery_ready",
            ],
        ) == Some(true),
        protocol_matches: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "storage_recovery_protocol_matches",
            ],
        ) == Some(true),
        durable: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "storage_recovery_durable",
            ],
        ) == Some(true),
        checkpoint_boundary_present: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "storage_recovery_checkpoint_boundary_present",
            ],
        ) == Some(true),
        wal_replay_bounded: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "storage_recovery_wal_replay_bounded",
            ],
        ) == Some(true),
        replay_boundary_consistent: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "storage_recovery_replay_boundary_consistent",
            ],
        ) == Some(true),
        torn_tail_clean: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "storage_recovery_torn_tail_clean",
            ],
        ) == Some(true),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &[
                    "replacement_summary",
                    "cutover_evidence",
                    "storage_recovery_blocker_codes",
                ][..],
                &[
                    "replacement_summary",
                    "cutover_evidence",
                    "storage_recovery_blockers",
                ][..],
            ],
        ),
    }
}

fn storage_recovery_cutover_conditions(
    readiness: &StorageRecoveryCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "replacement_summary.cutover_evidence.storage_recovery_required",
            readiness.required,
        ),
        (
            "replacement_summary.cutover_evidence.storage_recovery_ready",
            readiness.ready,
        ),
        (
            "replacement_summary.cutover_evidence.storage_recovery_protocol_matches",
            readiness.protocol_matches,
        ),
        (
            "replacement_summary.cutover_evidence.storage_recovery_durable",
            readiness.durable,
        ),
        (
            "replacement_summary.cutover_evidence.storage_recovery_checkpoint_boundary_present",
            readiness.checkpoint_boundary_present,
        ),
        (
            "replacement_summary.cutover_evidence.storage_recovery_wal_replay_bounded",
            readiness.wal_replay_bounded,
        ),
        (
            "replacement_summary.cutover_evidence.storage_recovery_replay_boundary_consistent",
            readiness.replay_boundary_consistent,
        ),
        (
            "replacement_summary.cutover_evidence.storage_recovery_torn_tail_clean",
            readiness.torn_tail_clean,
        ),
    ]
}

fn bounded_read_execution_cap_matches(bundle: &serde_json::Value) -> bool {
    let max_rows = u64_path(
        bundle,
        &["replacement_summary", "bounded_read_evidence", "max_rows"],
    );
    let execution_row_cap = u64_path(
        bundle,
        &[
            "replacement_summary",
            "bounded_read_evidence",
            "execution_row_cap",
        ],
    );
    max_rows.and_then(|value| value.checked_add(1)) == execution_row_cap
}

pub fn background_maintenance_cutover_readiness(
    bundle: &serde_json::Value,
) -> BackgroundMaintenanceCutoverReadiness {
    BackgroundMaintenanceCutoverReadiness {
        required: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_required",
            ],
        ) == Some(true),
        ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_ready",
            ],
        ) == Some(true),
        protocol_matches: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_protocol_matches",
            ],
        ) == Some(true),
        executable_search_projection_graph_delta_count_present: u64_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_executable_search_projection_graph_delta_count",
            ],
        )
        .is_some(),
        admitted_search_projection_graph_delta_count_present: u64_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_admitted_search_projection_graph_delta_count",
            ],
        )
        .is_some(),
        deferred_search_projection_graph_delta_count_present: u64_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_deferred_search_projection_graph_delta_count",
            ],
        )
        .is_some(),
        rejected_search_projection_graph_delta_count_present: u64_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_rejected_search_projection_graph_delta_count",
            ],
        )
        .is_some(),
        executable_search_projection_graph_delta_operations_present: u64_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_executable_search_projection_graph_delta_operations",
            ],
        )
        .is_some(),
        admitted_search_projection_graph_delta_operations_present: u64_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_admitted_search_projection_graph_delta_operations",
            ],
        )
        .is_some(),
        max_search_projection_graph_delta_complete_through_graph_commit_epoch_present: u64_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            ],
        )
        .is_some(),
        foreground_admission_probe_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_foreground_admission_probe_ready",
            ],
        ) == Some(true),
        memory_pressure_ready: bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_memory_pressure_ready",
            ],
        ) == Some(true),
        memory_budget_bytes_present: u64_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_memory_budget_bytes",
            ],
        )
        .is_some(),
        estimated_memory_bytes_present: u64_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_estimated_memory_bytes",
            ],
        )
        .is_some(),
        blocker_codes: blocker_codes(
            bundle,
            &[
                &[
                    "replacement_summary",
                    "cutover_evidence",
                    "background_maintenance_blocker_codes",
                ][..],
                &[
                    "replacement_summary",
                    "cutover_evidence",
                    "background_maintenance_blockers",
                ][..],
            ],
        ),
    }
}

fn background_maintenance_cutover_conditions(
    readiness: &BackgroundMaintenanceCutoverReadiness,
) -> Vec<(&'static str, bool)> {
    vec![
        (
            "replacement_summary.cutover_evidence.background_maintenance_required",
            readiness.required,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_ready",
            readiness.ready,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_protocol_matches",
            readiness.protocol_matches,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_executable_search_projection_graph_delta_count",
            readiness.executable_search_projection_graph_delta_count_present,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count",
            readiness.admitted_search_projection_graph_delta_count_present,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_deferred_search_projection_graph_delta_count",
            readiness.deferred_search_projection_graph_delta_count_present,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_rejected_search_projection_graph_delta_count",
            readiness.rejected_search_projection_graph_delta_count_present,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_executable_search_projection_graph_delta_operations",
            readiness.executable_search_projection_graph_delta_operations_present,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_operations",
            readiness.admitted_search_projection_graph_delta_operations_present,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            readiness.max_search_projection_graph_delta_complete_through_graph_commit_epoch_present,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_foreground_admission_probe_ready",
            readiness.foreground_admission_probe_ready,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_memory_pressure_ready",
            readiness.memory_pressure_ready,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_memory_budget_bytes",
            readiness.memory_budget_bytes_present,
        ),
        (
            "replacement_summary.cutover_evidence.background_maintenance_estimated_memory_bytes",
            readiness.estimated_memory_bytes_present,
        ),
    ]
}

fn string_array_path(value: &serde_json::Value, path: &[&str]) -> Vec<String> {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn string_array_path_is_empty(value: &serde_json::Value, path: &[&str]) -> bool {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_array)
        .is_some_and(Vec::is_empty)
}

fn search_projection_scan_filter_fields_cover_required(
    value: &serde_json::Value,
    path: &[&str],
) -> bool {
    let fields = string_array_path(value, path);
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .all(|required| fields.iter().any(|field| field == required))
}

fn search_projection_segment_descriptor_summaries_cover_required(
    value: &serde_json::Value,
    path: &[&str],
) -> bool {
    let Some(summaries) = json_get_path(value, path).and_then(serde_json::Value::as_array) else {
        return false;
    };
    if summaries.is_empty() {
        return false;
    }
    let fields = summaries
        .iter()
        .filter_map(|summary| str_path(summary, &["field"]))
        .collect::<BTreeSet<_>>();
    NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .all(|required| fields.contains(required))
        && ["importance", "confidence"].iter().all(|field| {
            summaries.iter().any(|summary| {
                str_path(summary, &["field"]) == Some(*field)
                    && bool_path(summary, &["numeric_range_summary_used"]) == Some(true)
            })
        })
        && ["created_at", "updated_at", "event_start", "event_end"]
            .iter()
            .all(|field| {
                summaries.iter().any(|summary| {
                    str_path(summary, &["field"]) == Some(*field)
                        && bool_path(summary, &["timestamp_range_summary_used"]) == Some(true)
                })
            })
        && summaries.iter().any(|summary| {
            str_path(summary, &["field"]) == Some("document_id")
                && bool_path(summary, &["unique_key_summary_used"]) == Some(true)
                && u64_path(summary, &["unique_key_summary_segment_count"])
                    .is_some_and(|count| count > 0)
        })
}

fn search_projection_segment_pruning_candidate_count_ready(
    value: &serde_json::Value,
    path: &[&str],
) -> bool {
    let mut candidate_path = path.to_vec();
    candidate_path.push("shadow_segment_pruning_candidate_document_count");
    let mut pruned_path = path.to_vec();
    pruned_path.push("shadow_segment_pruned_document_count");
    let mut scanned_path = path.to_vec();
    scanned_path.push("shadow_segment_scanned_document_count");
    matches!(
        (
            u64_path(value, &candidate_path),
            u64_path(value, &pruned_path),
            u64_path(value, &scanned_path),
        ),
        (Some(candidate), Some(pruned), Some(scanned))
            if candidate > 0 && pruned.checked_add(scanned) == Some(candidate)
    )
}

fn search_projection_segment_pruning_count_is_positive(
    value: &serde_json::Value,
    path: &[&str],
    field: &str,
) -> bool {
    let mut count_path = path.to_vec();
    count_path.push(field);
    u64_path(value, &count_path).is_some_and(|count| count > 0)
}

fn non_empty_str_path(value: &serde_json::Value, path: &[&str]) -> bool {
    str_path(value, path).is_some_and(|s| !s.trim().is_empty())
}

fn json_object_path_is_non_empty(value: &serde_json::Value, path: &[&str]) -> bool {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_object)
        .is_some_and(|object| !object.is_empty())
}

fn bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    json_get_path(value, path).and_then(serde_json::Value::as_bool)
}

fn u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    json_get_path(value, path).and_then(serde_json::Value::as_u64)
}

fn str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    json_get_path(value, path).and_then(serde_json::Value::as_str)
}

fn json_get_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_mem_integration_readiness_json, NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL,
        NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL,
        NOWLEDGE_MEM_SOURCE_MUTATION_DUAL_WRITE_READINESS_PROTOCOL,
        REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES,
        SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
    };
    use skein_evidence::replacement_contract::{
        NOWLEDGE_MEM_SEARCH_CANDIDATE_EVIDENCE_ROUTE, NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_ENGINE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_EVIDENCE_SOURCE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_PRIMARY_ENGINE,
        NOWLEDGE_MEM_SEARCH_CANDIDATE_TRACE_SHADOW_ENGINE,
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
    };
    use skein_route_ownership::graph::{
        nowledge_mem_graph_read_route_catalog_digest, nowledge_mem_graph_read_route_spec,
        nowledge_mem_graph_read_route_specs_json, nowledge_mem_route_ownership_all_skein,
        nowledge_mem_route_ownership_readiness, NowledgeMemRouteOwnershipPolicy,
        NowledgeMemRouteReadinessSummary, NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
    };
    use skein_route_ownership::{
        nowledge_mem_active_search_route_ownership_all_skein,
        nowledge_mem_active_search_route_ownership_readiness,
        nowledge_mem_active_search_route_read_evidence_all_skein_ready,
        nowledge_mem_active_search_route_readiness, nowledge_mem_search_route_ownership_all_skein,
        nowledge_mem_search_route_ownership_readiness, NowledgeMemSearchRouteOwnershipPolicy,
        REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES,
    };
    use skein_search::candidate_evidence::{
        nowledge_mem_search_candidate_shadow_evidence_json,
        NowledgeMemSearchCandidateShadowAccumulator,
    };
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn reports_ready_when_mem_integration_evidence_is_complete() {
        let report = nowledge_mem_integration_readiness_json(&ready_bundle());

        assert_eq!(report["ready"], true);
        assert_eq!(report["failed_checks"], serde_json::json!([]));
        assert_eq!(report["blocker_codes"], serde_json::json!([]));
        assert_eq!(report["next_actions"], serde_json::json!([]));
    }

    #[test]
    fn integration_bundle_read_errors_are_redacted_by_default() {
        let secret_path = unique_test_path("integration-secret-path-do-not-emit")
            .join("missing-secret-bundle.json");

        let error = super::read_json_file(&secret_path).unwrap_err().to_string();

        assert_eq!(
            error,
            "execution error: failed to read Nowledge Mem integration bundle: io_error"
        );
        assert!(!error.contains("integration-secret-path-do-not-emit"));
        assert!(!error.contains("missing-secret-bundle"));
    }

    #[test]
    fn integration_bundle_parse_errors_are_redacted_by_default() {
        let root = unique_test_path("integration-parse-redaction");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("secret-bundle-path-do-not-emit.json");
        std::fs::write(
            &path,
            "{ \"secret\": \"bundle-parse-secret-do-not-emit\", \"unterminated\": ",
        )
        .unwrap();

        let error = super::read_json_file(&path).unwrap_err().to_string();

        assert_eq!(
            error,
            "execution error: failed to parse Nowledge Mem integration bundle: invalid_json"
        );
        assert!(!error.contains("secret-bundle-path-do-not-emit"));
        assert!(!error.contains("bundle-parse-secret-do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn typed_integration_readiness_api_exposes_structured_gate() {
        let report = super::nowledge_mem_integration_readiness(&ready_bundle());

        assert!(report.ready);
        assert_eq!(
            report.protocol,
            super::NOWLEDGE_MEM_INTEGRATION_READINESS_PROTOCOL
        );
        assert!(report.failed_checks.is_empty());
        assert!(report.blocker_codes.is_empty());
        assert!(report.next_actions.is_empty());
        assert!(report.checks.iter().all(|check| check.ready));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "query_runtime_preflight"));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "blackbox_redaction"));
        assert!(report
            .checks
            .iter()
            .any(|check| check.name == "blackbox_operational_evidence"));
        assert_eq!(report.json()["ready"], true);
    }

    #[test]
    fn requires_blackbox_redaction_manifest() {
        let mut bundle = ready_bundle();
        bundle.as_object_mut().unwrap().remove("blackbox_manifest");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert!(report["failed_checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check == "blackbox_redaction"));
        let blackbox_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "blackbox_redaction")
            .unwrap();
        assert_eq!(
            blackbox_check["failed_evidence_fields"],
            serde_json::json!([
                "blackbox_manifest.protocol",
                "blackbox_manifest.artifact_dir_present",
                "blackbox_manifest.artifact_count",
                "blackbox_manifest.events_path",
                "blackbox_manifest.redaction.raw_query_text_copied",
                "blackbox_manifest.redaction.raw_parameters_copied",
                "blackbox_manifest.redaction.raw_artifact_payloads_copied",
                "blackbox_manifest.redaction.artifact_paths_are_relative"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_blackbox_redaction_report"));
    }

    #[test]
    fn rejects_blackbox_manifest_that_copies_raw_query_text() {
        let mut bundle = ready_bundle();
        bundle["blackbox_manifest"]["redaction"]["raw_query_text_copied"] = serde_json::json!(true);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        let blackbox_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "blackbox_redaction")
            .unwrap();
        assert_eq!(
            blackbox_check["failed_evidence_fields"],
            serde_json::json!(["blackbox_manifest.redaction.raw_query_text_copied"])
        );
    }

    #[test]
    fn requires_blackbox_slow_query_artifact_summary() {
        let mut bundle = ready_bundle();
        bundle["blackbox_manifest"]["artifacts"] =
            serde_json::json!([ready_blackbox_background_maintenance_artifact()]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        let blackbox_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "blackbox_operational_evidence")
            .unwrap();
        assert_eq!(
            blackbox_check["failed_evidence_fields"],
            serde_json::json!([
                "blackbox_manifest.artifacts.slow-query-log.jsonl",
                "blackbox_manifest.artifacts.slow-query-log.jsonl.jsonl"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_blackbox_operational_evidence"));
    }

    #[test]
    fn requires_blackbox_background_qos_summary() {
        let mut bundle = ready_bundle();
        bundle["blackbox_manifest"]["artifacts"] = serde_json::json!([
            ready_blackbox_slow_query_artifact(),
            {
                "name": "background-maintenance.json",
                "format": "json",
                "byte_len": 256,
                "checksum": 3,
                "json": {
                    "parse_ready": true,
                    "protocol": "skein-background-maintenance-report",
                    "ready": true,
                    "blocker_codes": []
                }
            }
        ]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        let blackbox_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "blackbox_operational_evidence")
            .unwrap();
        assert_eq!(
            blackbox_check["failed_evidence_fields"],
            serde_json::json!([
                "blackbox_manifest.artifacts.background-maintenance.json.background_qos"
            ])
        );
    }

    #[test]
    fn requires_blackbox_background_qos_memory_pressure_evidence() {
        let mut bundle = ready_bundle();
        let artifacts = bundle["blackbox_manifest"]["artifacts"]
            .as_array_mut()
            .unwrap();
        let background_artifact = artifacts
            .iter_mut()
            .find(|artifact| artifact["name"] == "background-maintenance.json")
            .unwrap();
        let background_qos = background_artifact["background_qos"]
            .as_object_mut()
            .unwrap();
        background_qos.remove("memory_pressure_ready");
        background_qos.remove("memory_budget_bytes");
        background_qos.remove("estimated_memory_bytes");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["blackbox_operational_evidence"])
        );
        assert!(report["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "blackbox_background_qos_summary_missing"));
        let blackbox_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "blackbox_operational_evidence")
            .unwrap();
        assert_eq!(
            blackbox_check["failed_evidence_fields"],
            serde_json::json!([
                "blackbox_manifest.artifacts.background-maintenance.json.background_qos"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_blackbox_operational_evidence"));
    }

    #[test]
    fn requires_versioned_integration_bundle_protocol() {
        let mut bundle = ready_bundle();
        bundle.as_object_mut().unwrap().remove("protocol");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["integration_bundle_protocol"])
        );
        let protocol_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "integration_bundle_protocol")
            .unwrap();
        assert_eq!(
            protocol_check["failed_evidence_fields"],
            serde_json::json!(["protocol"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "regenerate_skein_integration_bundle"));
    }

    #[test]
    fn requires_replacement_summary_protocol() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]
            .as_object_mut()
            .unwrap()
            .remove("protocol");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary_protocol"])
        );
        let summary_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "replacement_summary_protocol")
            .unwrap();
        assert_eq!(
            summary_check["failed_evidence_fields"],
            serde_json::json!(["replacement_summary.protocol"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "produce_replacement_summary"
                && action["evidence_fields"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field == "replacement_summary.protocol")));
    }

    #[test]
    fn fails_closed_without_submodule_and_coexistence() {
        let mut bundle = ready_bundle();
        bundle["submodule"]["present"] = serde_json::json!(false);
        bundle["submodule"]["commit"] = serde_json::json!("");
        bundle["coexistence"]["old_database_retained"] = serde_json::json!(false);
        bundle["coexistence"]["old_database_deleted"] = serde_json::json!(true);
        bundle["coexistence"]["mode"] = serde_json::json!("replace_in_place");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["skein_submodule", "legacy_coexistence"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "add_skein_submodule"));
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "enable_side_by_side_coexistence"));
    }

    #[test]
    fn exposes_typed_protocol_cutover_readiness() {
        let mut bundle = ready_bundle();

        let integration = super::integration_bundle_protocol_cutover_readiness(&bundle);
        let replacement = super::replacement_summary_protocol_cutover_readiness(&bundle);
        assert!(integration.evidence_ready());
        assert!(integration.protocol_matches);
        assert!(replacement.evidence_ready());
        assert!(replacement.protocol_matches);
        assert!(replacement.graph_layer_replacement_scope_ready);
        assert!(replacement.search_projection_replacement_scope_ready);
        assert!(replacement.content_store_out_of_scope);
        assert!(replacement.large_blob_store_out_of_scope);

        bundle["protocol"] = serde_json::json!("legacy");
        bundle["replacement_summary"]["protocol"] = serde_json::json!("legacy");
        bundle["replacement_summary"]["replacement_boundaries"]["content_store"]
            ["replacement_role"] = serde_json::json!("primary_replacement");
        let integration = super::integration_bundle_protocol_cutover_readiness(&bundle);
        let replacement = super::replacement_summary_protocol_cutover_readiness(&bundle);
        assert!(!integration.evidence_ready());
        assert!(!replacement.evidence_ready());
        assert!(!replacement.protocol_matches);
        assert!(!replacement.content_store_out_of_scope);
    }

    #[test]
    fn requires_replacement_summary_boundaries() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]
            .as_object_mut()
            .unwrap()
            .remove("replacement_boundaries");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["replacement_summary_protocol"])
        );
        let summary_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "replacement_summary_protocol")
            .unwrap();
        assert_eq!(
            summary_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.replacement_boundaries.graph_layer",
                "replacement_summary.replacement_boundaries.search_projection",
                "replacement_summary.replacement_boundaries.content_store",
                "replacement_summary.replacement_boundaries.large_blob_store"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "produce_replacement_summary"
                && action["evidence_fields"].as_array().unwrap().iter().any(
                    |field| field == "replacement_summary.replacement_boundaries.content_store"
                )));
    }

    #[test]
    fn exposes_typed_submodule_and_coexistence_cutover_readiness() {
        let mut bundle = ready_bundle();
        let submodule = super::skein_submodule_cutover_readiness(&bundle);
        let coexistence = super::legacy_coexistence_cutover_readiness(&bundle);
        assert!(submodule.evidence_ready());
        assert!(submodule.present);
        assert!(submodule.path_present);
        assert!(submodule.commit_present);
        assert!(coexistence.evidence_ready());
        assert!(coexistence.old_database_retained);
        assert!(coexistence.mode_safe);
        assert!(coexistence.old_database_not_deleted);

        bundle["submodule"]["commit"] = serde_json::json!("");
        bundle["coexistence"]["mode"] = serde_json::json!("replace_in_place");
        bundle["coexistence"]["old_database_deleted"] = serde_json::json!(true);
        let submodule = super::skein_submodule_cutover_readiness(&bundle);
        let coexistence = super::legacy_coexistence_cutover_readiness(&bundle);
        assert!(!submodule.evidence_ready());
        assert!(submodule.present);
        assert!(submodule.path_present);
        assert!(!submodule.commit_present);
        assert!(!coexistence.evidence_ready());
        assert!(!coexistence.mode_safe);
        assert!(!coexistence.old_database_not_deleted);
    }

    #[test]
    fn typed_content_store_boundary_rejects_missing_source_chunks() {
        let mut bundle = ready_bundle();
        let typed = super::content_store_boundary_cutover_readiness(&bundle);
        assert!(typed.evidence_ready());
        assert!(typed.present);
        assert!(typed.engine_sqlite);
        assert!(typed.messages_available);
        assert!(typed.source_chunks_available);

        bundle["content_store"]["source_chunks_available"] = serde_json::json!(false);
        let typed = super::content_store_boundary_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(!typed.source_chunks_available);

        let report = nowledge_mem_integration_readiness_json(&bundle);
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "content_store_boundary")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!(["content_store.source_chunks_available"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_content_store_evidence"));
    }

    #[test]
    fn typed_previous_wrapper_preflight_collects_failed_checks_as_blockers() {
        let mut bundle = ready_bundle();
        bundle["previous_wrapper_preflight"]["ready"] = serde_json::json!(false);
        bundle["previous_wrapper_preflight"]["failed_checks"] =
            serde_json::json!(["route_inventory_missing"]);

        let typed = super::previous_wrapper_preflight_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert_eq!(typed.blocker_codes, vec!["route_inventory_missing"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "previous_wrapper_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!(["previous_wrapper_preflight.ready"])
        );
        assert_eq!(
            check["blocker_codes"],
            serde_json::json!(["route_inventory_missing"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_previous_wrapper_preflight"));
    }

    #[test]
    fn graph_replacement_requires_source_mutation_dual_write_readiness() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["production_cutover_ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["source_mutation_dual_write_readiness"]["ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["source_mutation_dual_write_readiness"]
            ["ready_family_count"] =
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_SOURCE_MUTATION_FAMILIES.len() - 1);
        bundle["replacement_summary"]["source_mutation_dual_write_readiness"]
            ["missing_required_families"] = serde_json::json!(["source_ingest_create"]);
        bundle["replacement_summary"]["source_mutation_dual_write_readiness"]["blocker_codes"] =
            serde_json::json!(["source_mutation_dual_write_missing_required_families"]);
        bundle["replacement_summary"]["blocking_categories"] =
            serde_json::json!(["source_mutation_dual_write_readiness"]);
        bundle["replacement_summary"]["missing_evidence"] =
            serde_json::json!(["source_mutation_dual_write_readiness_ready"]);

        let typed = super::graph_replacement_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(!typed.production_cutover_ready);
        assert!(!typed.source_mutation_ready);
        assert!(!typed.source_mutation_ready_family_count_matches);
        assert!(!typed.source_mutation_missing_required_families_empty);
        assert_eq!(
            typed.blocker_codes,
            vec![
                "source_mutation_dual_write_missing_required_families".to_string(),
                "source_mutation_dual_write_readiness".to_string(),
                "source_mutation_dual_write_readiness_ready".to_string()
            ]
        );

        let report = nowledge_mem_integration_readiness_json(&bundle);
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_replacement_evidence")
            .unwrap();
        assert_eq!(check["ready"], false);
        assert!(
            check["failed_evidence_fields"]
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field
                    == "replacement_summary.source_mutation_dual_write_readiness.ready")
        );
        assert!(check["failed_evidence_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field
                == "replacement_summary.source_mutation_dual_write_readiness.ready_family_count"));

        let final_report = super::nowledge_mem_final_cutover_preflight(&bundle);
        assert!(!final_report.production_cutover_ready);
        assert!(!final_report.graph_replacement_ready);
        assert!(final_report
            .blocking_categories
            .contains(&"graph_replacement".to_string()));
    }

    #[test]
    fn typed_route_ownership_cutover_readiness_requires_all_skein_routes() {
        let mut bundle = ready_bundle();
        let typed = super::route_ownership_cutover_readiness(&bundle);
        assert!(typed.evidence_ready());
        assert!(typed.production_cutover_ready);
        assert!(typed.require_all_skein);
        assert!(typed.legacy_route_count_zero);

        bundle["route_ownership"]["routes"][0]["read_engine"] = serde_json::json!("legacy");
        bundle["route_ownership"]["legacy_route_count"] = serde_json::json!(1);
        bundle["route_ownership"]["skein_route_count"] =
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() - 1);
        bundle["route_ownership"]["legacy_routes"] =
            serde_json::json!([REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0]]);
        bundle["route_ownership"]["skein_routes"] =
            serde_json::json!(&REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[1..]);
        bundle["route_ownership"]["ready"] = serde_json::json!(false);
        bundle["route_ownership"]["production_cutover_ready"] = serde_json::json!(false);
        bundle["route_ownership"]["blocker_codes"] =
            serde_json::json!(["route_ownership_legacy_routes_remaining"]);

        let typed = super::route_ownership_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(!typed.ready);
        assert!(!typed.production_cutover_ready);
        assert!(!typed.skein_route_count_matches);
        assert!(!typed.legacy_route_count_zero);
        assert_eq!(
            typed.blocker_codes,
            vec!["route_ownership_legacy_routes_remaining".to_string()]
        );
    }

    #[test]
    fn route_ownership_rejects_skein_search_route_without_projection_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]
            .as_object_mut()
            .unwrap()
            .remove("search_projection_evidence");

        let typed = super::route_ownership_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(!typed.search_route_projection_evidence_present);

        let report = nowledge_mem_integration_readiness_json(&bundle);
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "route_ownership")
            .unwrap();
        assert!(check["failed_evidence_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "route_ownership.search_route_projection_evidence_present"));
    }

    #[test]
    fn integration_readiness_fails_closed_without_route_ownership() {
        let mut bundle = ready_bundle();
        bundle.as_object_mut().unwrap().remove("route_ownership");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert!(report["failed_checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check == "route_ownership"));
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "route_ownership")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "route_ownership.protocol",
                "route_ownership.ready",
                "route_ownership.production_cutover_ready",
                "route_ownership.require_all_skein",
                "route_ownership.required_route_count",
                "route_ownership.explicit_route_count",
                "route_ownership.skein_route_count",
                "route_ownership.legacy_route_count",
                "route_ownership.route_readiness_present",
                "route_ownership.route_readiness_ready",
                "route_ownership.route_catalog_version",
                "route_ownership.route_catalog_digest"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_route_ownership_evidence"));
    }

    #[test]
    fn integration_readiness_blocks_active_search_reads_that_still_require_lancedb() {
        let mut bundle = ready_bundle();
        bundle["active_search_route_readiness"]["ready"] = serde_json::json!(false);
        bundle["active_search_route_readiness"]["production_cutover_ready"] =
            serde_json::json!(false);
        bundle["active_search_route_readiness"]["ready_route_count"] =
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len() - 1);
        bundle["active_search_route_readiness"]["lancedb_handle_required_route_count"] =
            serde_json::json!(1);
        bundle["active_search_route_readiness"]["lancedb_handle_required_routes"] =
            serde_json::json!([REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES[0]]);
        bundle["active_search_route_readiness"]["blocker_codes"] =
            serde_json::json!(["active_search_route_readiness_lancedb_handle_required"]);
        bundle["replacement_summary_active_search_route_readiness_alignment"]["ready"] =
            serde_json::json!(false);
        bundle["replacement_summary_active_search_route_readiness_alignment"]
            ["ready_route_count_matches"] = serde_json::json!(false);
        bundle["replacement_summary_active_search_route_readiness_alignment"]
            ["lancedb_handle_count_matches"] = serde_json::json!(false);
        bundle["replacement_summary_active_search_route_readiness_alignment"]
            ["lancedb_handle_routes_matches"] = serde_json::json!(false);
        bundle["replacement_summary_active_search_route_readiness_alignment"]
            ["blocker_codes_match"] = serde_json::json!(false);
        bundle["replacement_summary_active_search_route_readiness_alignment"]["blocker_codes"] =
            serde_json::json!([
                "active_search_route_readiness_ready_count_mismatch",
                "active_search_route_readiness_lancedb_handle_count_mismatch",
                "active_search_route_readiness_lancedb_handle_routes_mismatch",
                "active_search_route_readiness_blocker_codes_mismatch"
            ]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert!(report["failed_checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check == "active_search_route_readiness_alignment"));
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "active_search_route_readiness_alignment")
            .unwrap();
        assert!(check["failed_evidence_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field
                == "replacement_summary_active_search_route_readiness_alignment.lancedb_handle_count_matches"));
        assert!(
            report["next_actions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|action| action["action"]
                    == "regenerate_active_search_route_readiness_alignment")
        );
    }

    #[test]
    fn final_cutover_preflight_fails_closed_on_legacy_route_ownership() {
        let mut bundle = ready_bundle();
        bundle["route_ownership"]["routes"][0]["read_engine"] = serde_json::json!("legacy");
        bundle["route_ownership"]["legacy_route_count"] = serde_json::json!(1);
        bundle["route_ownership"]["skein_route_count"] =
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() - 1);
        bundle["route_ownership"]["legacy_routes"] =
            serde_json::json!([REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0]]);
        bundle["route_ownership"]["skein_routes"] =
            serde_json::json!(&REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[1..]);
        bundle["route_ownership"]["ready"] = serde_json::json!(false);
        bundle["route_ownership"]["production_cutover_ready"] = serde_json::json!(false);
        bundle["route_ownership"]["blocker_codes"] =
            serde_json::json!(["route_ownership_legacy_routes_remaining"]);

        let report = super::nowledge_mem_final_cutover_preflight(&bundle);

        assert!(!report.production_cutover_ready);
        assert!(!report.integration_ready);
        assert!(!report.route_coverage_ready);
        assert!(report
            .blocking_categories
            .contains(&"route_coverage".to_string()));
        assert!(report
            .failed_checks
            .contains(&"route_ownership".to_string()));
        assert!(report
            .next_action_names
            .contains(&"attach_route_ownership_evidence".to_string()));
    }

    #[test]
    fn exposes_typed_final_cutover_preflight_report() {
        let bundle = ready_bundle();

        let report = super::nowledge_mem_final_cutover_preflight(&bundle);

        assert_eq!(
            report.protocol,
            super::NOWLEDGE_MEM_FINAL_CUTOVER_PREFLIGHT_PROTOCOL
        );
        assert!(report.production_cutover_ready);
        assert!(report.integration_ready);
        assert!(report.replacement_summary_production_cutover_ready);
        assert!(report.startup_ready);
        assert!(report.route_coverage_ready);
        assert!(report.graph_replacement_ready);
        assert!(report.search_projection_ready);
        assert!(report.storage_recovery_ready);
        assert!(report.background_qos_ready);
        assert!(report.blackbox_ready);
        assert!(report.library_only_ready);
        assert!(report.check_count > 0);
        assert_eq!(report.ready_check_count, report.check_count);
        assert_eq!(report.failed_check_count, 0);
        assert_eq!(report.next_action_count, 0);
        assert!(report.next_action_names.is_empty());
        assert!(report.blocking_categories.is_empty());
        assert!(report.failed_checks.is_empty());
        assert!(report.failed_evidence_fields.is_empty());
        assert!(report.blocker_codes.is_empty());

        let json = report.json();
        assert_eq!(json["production_cutover_ready"], true);
        assert_eq!(json["route_coverage_ready"], true);
        assert_eq!(json["blocking_categories"], serde_json::json!([]));
        assert!(json.get("checks").is_none());
        assert!(json.get("next_actions").is_none());
    }

    #[test]
    fn final_cutover_preflight_fails_closed_on_missing_integration_evidence() {
        let mut bundle = ready_bundle();
        bundle["content_store"]["source_chunks_available"] = serde_json::json!(false);

        let report = super::nowledge_mem_final_cutover_preflight(&bundle);

        assert!(!report.production_cutover_ready);
        assert!(!report.integration_ready);
        assert!(report.replacement_summary_production_cutover_ready);
        assert!(!report.startup_ready);
        assert!(report.route_coverage_ready);
        assert!(report.blocking_categories.contains(&"startup".to_string()));
        assert_eq!(
            report.next_action_names,
            vec!["attach_content_store_evidence"]
        );
        assert!(report
            .failed_checks
            .contains(&"content_store_boundary".to_string()));
        assert!(report
            .failed_evidence_fields
            .contains(&"content_store.source_chunks_available".to_string()));
        assert_eq!(report.failed_check_count, 1);
    }

    #[test]
    fn final_cutover_preflight_fails_closed_on_replacement_summary_cutover() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["production_cutover_ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["blocking_categories"] =
            serde_json::json!(["graph_replacement"]);

        let report = super::nowledge_mem_final_cutover_preflight(&bundle);

        assert!(!report.production_cutover_ready);
        assert!(!report.integration_ready);
        assert!(!report.replacement_summary_production_cutover_ready);
        assert!(!report.graph_replacement_ready);
        assert!(report
            .blocking_categories
            .contains(&"graph_replacement".to_string()));
        assert!(report
            .blocking_categories
            .contains(&"replacement_summary_cutover".to_string()));
        assert!(report
            .failed_checks
            .contains(&"graph_replacement_evidence".to_string()));
        assert!(report
            .failed_evidence_fields
            .contains(&"replacement_summary.production_cutover_ready".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"graph_replacement".to_string()));
    }

    #[test]
    fn requires_search_projection_shadow_parity() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]
            ["document_count_parity"] = serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["document_count_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["document_count_mismatch"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.document_count_parity"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_search_projection_replacement_evidence"));
    }

    #[test]
    fn exposes_typed_search_projection_cutover_readiness() {
        let bundle = ready_bundle();

        let typed = super::search_projection_cutover_readiness(&bundle);

        assert!(typed.evidence_ready());
        assert!(typed.evidence_protocol_matches);
        assert!(typed.shadow_protocol_matches);
        assert!(typed.shadow_evidence_source_matches);
        assert!(typed.shadow_pruning_candidate_count_ready);
        assert!(typed.primary_scan_filter_fields_ready);
        assert!(typed.shadow_scan_filter_fields_ready);
        assert!(typed.shadow_descriptor_field_summaries_ready);
        assert!(typed.blocker_codes.is_empty());
    }

    #[test]
    fn typed_search_projection_cutover_recomputes_pushdown_fields() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["ready"] =
            serde_json::json!(true);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["ready"] = serde_json::json!(true);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["primary_scan_filter_fields"] = serde_json::json!(["unit_type", "importance"]);

        let typed = super::search_projection_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.primary_scan_filter_fields_ready);
    }

    #[test]
    fn typed_search_projection_cutover_requires_production_filter_pruning() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_evidence"]["ready"] =
            serde_json::json!(true);
        bundle["replacement_summary"]["search_projection_evidence"]
            ["production_filter_pruning_ready"] = serde_json::json!(false);

        let typed = super::search_projection_cutover_readiness(&bundle);
        let conditions = super::search_projection_cutover_conditions(&typed);

        assert!(!typed.evidence_ready());
        assert!(!typed.production_filter_pruning_ready);
        assert!(conditions.iter().any(|(field, ready)| {
            *field
                == "replacement_summary.search_projection_evidence.production_filter_pruning_ready"
                && !*ready
        }));
    }

    #[test]
    fn requires_search_projection_document_identity() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_evidence"]["ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_evidence"]["document_identity_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_evidence"]["blocker_codes"] =
            serde_json::json!(["document_identity_not_ready"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["document_identity_not_ready"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_evidence.ready",
                "replacement_summary.search_projection_evidence.document_identity_ready"
            ])
        );
    }

    #[test]
    fn requires_search_projection_shadow_document_identity_parity() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]
            ["document_identity_parity"] = serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["document_identity_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["document_identity_mismatch"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.document_identity_parity"
            ])
        );
    }

    #[test]
    fn requires_skein_search_candidate_primary_engine() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["candidate_primary_engine"] =
            serde_json::json!("lancedb");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!(["search_candidate_shadow_evidence.candidate_primary_engine"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "enable_skein_search_candidate_primary_reads"));
    }

    #[test]
    fn requires_search_candidate_shadow_count_parity() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["primary_candidate_count"] =
            serde_json::json!(3);
        bundle["search_candidate_shadow_evidence"]["shadow_candidate_count"] = serde_json::json!(3);
        bundle["search_candidate_shadow_evidence"]["matched_candidate_count"] =
            serde_json::json!(2);
        bundle["search_candidate_shadow_evidence"]["primary_only_candidate_count"] =
            serde_json::json!(1);
        bundle["search_candidate_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["search_candidate_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["search_candidate_mismatch"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!(["search_candidate_shadow_evidence.candidate_counts"])
        );
    }

    #[test]
    fn requires_search_candidate_shadow_identity_parity() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["candidate_identity"]["ready"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["candidate_identity"]["parity"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["search_candidate_identity_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["search_candidate_identity_mismatch"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!(["search_candidate_shadow_evidence.candidate_identity.ready"])
        );
    }

    #[test]
    fn requires_search_candidate_filter_pushdown_evidence() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["ready"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["field_summary_count"] =
            serde_json::json!(0);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["missing_required_fields"] =
            serde_json::json!(["unit_type"]);
        bundle["search_candidate_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["search_candidate_field_pruning_missing"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["search_candidate_field_pruning_missing"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.filter_pushdown.ready",
                "search_candidate_shadow_evidence.filter_pushdown.field_summary_count",
                "search_candidate_shadow_evidence.filter_pushdown.missing_required_fields"
            ])
        );
    }

    #[test]
    fn requires_search_candidate_shadow_scan_fields() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["row_count_parity"] = serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["shadow_scan_filter_pushdown_ready"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["shadow_scan_field_summary_count"] =
            serde_json::json!(0);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.row_count_parity",
                "search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready",
                "search_candidate_shadow_evidence.shadow_scan_field_pruning_ready",
                "search_candidate_shadow_evidence.shadow_scan_field_summary_count"
            ])
        );
    }

    #[test]
    fn requires_search_candidate_retriever_leg_evidence() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["text_retriever_ready"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["vector_retriever_ready"] =
            serde_json::json!(false);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.text_retriever_ready",
                "search_candidate_shadow_evidence.vector_retriever_ready"
            ])
        );
    }

    #[test]
    fn requires_search_candidate_top_k_overlap_evidence() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["fts_top_k_overlap_ready"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["vector_top_k_overlap_ready"] =
            serde_json::json!(false);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.fts_top_k_overlap_ready",
                "search_candidate_shadow_evidence.vector_top_k_overlap_ready"
            ])
        );
    }

    #[test]
    fn requires_search_candidate_readiness_evidence() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["candidate_readiness"] = serde_json::json!({
            "source_chunk_identity_ready": false,
            "fail_soft_observed": false,
            "projection_marker_status_visible": false,
            "projection_watermark_ready": false,
            "embedding_identity_ready": false,
        });

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.candidate_readiness.source_chunk_identity_ready",
                "search_candidate_shadow_evidence.candidate_readiness.fail_soft_observed",
                "search_candidate_shadow_evidence.candidate_readiness.projection_marker_status_visible",
                "search_candidate_shadow_evidence.candidate_readiness.projection_watermark_ready",
                "search_candidate_shadow_evidence.candidate_readiness.embedding_identity_ready"
            ])
        );
    }

    #[test]
    fn rejects_search_candidate_trace_evidence_for_library_readiness() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"] = ready_search_candidate_trace_evidence();

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.evidence_source",
                "search_candidate_shadow_evidence.candidate_primary_engine",
                "search_candidate_shadow_evidence.candidate_counts",
                "search_candidate_shadow_evidence.text_retriever_ready",
                "search_candidate_shadow_evidence.vector_retriever_ready",
                "search_candidate_shadow_evidence.candidate_readiness.source_chunk_identity_ready",
                "search_candidate_shadow_evidence.candidate_readiness.fail_soft_observed",
                "search_candidate_shadow_evidence.candidate_readiness.projection_marker_status_visible",
                "search_candidate_shadow_evidence.candidate_readiness.projection_watermark_ready",
                "search_candidate_shadow_evidence.candidate_readiness.embedding_identity_ready",
                "search_candidate_shadow_evidence.candidate_identity.ready",
                "search_candidate_shadow_evidence.filter_pushdown.ready",
                "search_candidate_shadow_evidence.filter_pushdown.field_summary_count",
                "search_candidate_shadow_evidence.filter_pushdown.missing_required_fields"
            ])
        );
    }

    #[test]
    fn exposes_typed_search_candidate_cutover_readiness() {
        let bundle = ready_bundle();

        let typed = super::search_candidate_cutover_readiness(&bundle);

        assert!(typed.evidence_ready());
        assert!(typed.protocol_matches);
        assert!(typed.evidence_source_matches);
        assert!(typed.route_matches);
        assert!(typed.primary_engine_matches);
        assert!(typed.candidate_count_parity);
        assert!(typed.row_count_parity);
        assert!(typed.text_retriever_ready);
        assert!(typed.vector_retriever_ready);
        assert!(typed.fts_top_k_overlap_ready);
        assert!(typed.vector_top_k_overlap_ready);
        assert!(typed.source_chunk_identity_ready);
        assert!(typed.fail_soft_observed);
        assert!(typed.projection_marker_status_visible);
        assert!(typed.projection_watermark_ready);
        assert!(typed.embedding_identity_ready);
        assert!(typed.candidate_identity_ready);
        assert!(typed.filter_pushdown_ready);
        assert!(typed.filter_pushdown_field_summary_present);
        assert!(typed.filter_pushdown_required_fields_ready);
        assert!(typed.blocker_codes.is_empty());
    }

    #[test]
    fn typed_search_candidate_cutover_requires_rust_bridge_evidence() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"] = ready_search_candidate_trace_evidence();

        let typed = super::search_candidate_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.evidence_source_matches);
        assert!(!typed.primary_engine_matches);
        assert!(!typed.candidate_count_parity);
    }

    #[test]
    fn requires_search_candidate_shadow_evidence_presence() {
        let mut bundle = ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("search_candidate_shadow_evidence");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!([
                "search_candidate_shadow_evidence.protocol",
                "search_candidate_shadow_evidence.evidence_source",
                "search_candidate_shadow_evidence.route",
                "search_candidate_shadow_evidence.ready",
                "search_candidate_shadow_evidence.candidate_primary_engine",
                "search_candidate_shadow_evidence.candidate_counts",
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
                "search_candidate_shadow_evidence.filter_pushdown.ready",
                "search_candidate_shadow_evidence.filter_pushdown.field_summary_count",
                "search_candidate_shadow_evidence.filter_pushdown.missing_required_fields",
                "search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready",
                "search_candidate_shadow_evidence.shadow_scan_field_pruning_ready",
                "search_candidate_shadow_evidence.shadow_scan_field_summary_count"
            ])
        );
    }

    #[test]
    fn requires_search_candidate_shadow_evidence_source() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["evidence_source"] =
            serde_json::json!("manual-json");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!(["search_candidate_shadow_evidence.evidence_source"])
        );
    }

    #[test]
    fn requires_search_candidate_shadow_evidence_route() {
        let mut bundle = ready_bundle();
        bundle["search_candidate_shadow_evidence"]["route"] = serde_json::json!("/manual");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_candidate_primary_evidence"])
        );
        let candidate_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_candidate_primary_evidence")
            .unwrap();
        assert_eq!(
            candidate_check["failed_evidence_fields"],
            serde_json::json!(["search_candidate_shadow_evidence.route"])
        );
    }

    #[test]
    fn requires_search_projection_shadow_descriptor_field_coverage() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_scan_filter_fields_ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["skein_search_projection_segment_descriptor_fields_missing"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["skein_search_projection_segment_descriptor_fields_missing"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_scan_filter_fields_ready"
            ])
        );
    }

    #[test]
    fn recomputes_search_projection_shadow_descriptor_field_coverage() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["ready"] =
            serde_json::json!(true);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["ready"] = serde_json::json!(true);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_scan_filter_fields_ready"] = serde_json::json!(true);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["primary_scan_filter_fields"] = serde_json::json!(["unit_type", "importance"]);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_scan_filter_fields"] = serde_json::json!(["unit_type", "importance"]);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_descriptor_field_summaries"] =
            serde_json::json!([{ "field": "unit_type" }, { "field": "importance" }]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.primary_scan_filter_fields",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_scan_filter_fields",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_field_summaries"
            ])
        );
    }

    #[test]
    fn recomputes_search_projection_shadow_descriptor_range_capabilities() {
        let mut bundle = ready_bundle();
        let summaries = bundle["replacement_summary"]["search_projection_shadow_evidence"]
            ["pushdown_evidence"]["shadow_segment_descriptor_field_summaries"]
            .as_array_mut()
            .unwrap();
        for summary in summaries {
            if summary["field"] == "importance" {
                summary["numeric_range_summary_used"] = serde_json::json!(false);
            }
            if summary["field"] == "created_at" {
                summary["timestamp_range_summary_used"] = serde_json::json!(false);
            }
        }

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_field_summaries"
            ])
        );
    }

    #[test]
    fn recomputes_search_projection_shadow_descriptor_unique_key_capability() {
        let mut bundle = ready_bundle();
        let summaries = bundle["replacement_summary"]["search_projection_shadow_evidence"]
            ["pushdown_evidence"]["shadow_segment_descriptor_field_summaries"]
            .as_array_mut()
            .unwrap();
        let document_id = summaries
            .iter_mut()
            .find(|summary| summary["field"] == "document_id")
            .unwrap();
        document_id["unique_key_summary_used"] = serde_json::json!(false);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_descriptor_field_summaries"
            ])
        );
    }

    #[test]
    fn recomputes_search_projection_shadow_document_pruning_counts() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_document_pruning_ready"] = serde_json::json!(true);
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["pushdown_evidence"]
            ["shadow_segment_pruned_document_count"] = serde_json::json!(0);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_pruning_candidate_document_count",
                "replacement_summary.search_projection_shadow_evidence.pushdown_evidence.shadow_segment_pruned_document_count"
            ])
        );
    }

    #[test]
    fn requires_search_projection_shadow_evidence_source() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["evidence_source"] =
            serde_json::json!("manual-json");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_shadow_evidence.evidence_source"
            ])
        );
    }

    #[test]
    fn requires_compressed_vector_projection_readiness() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_evidence"]
            ["compressed_vector_projection_ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["search_projection_evidence"]["blocker_codes"] =
            serde_json::json!(["compressed_vector_projection_not_ready"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["compressed_vector_projection_not_ready"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_evidence.compressed_vector_projection_ready"
            ])
        );
    }

    #[test]
    fn requires_search_projection_evidence_protocols() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_projection_evidence"]["protocol"] =
            serde_json::json!("handwritten");
        bundle["replacement_summary"]["search_projection_shadow_evidence"]["protocol"] =
            serde_json::json!("handwritten");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        let search_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "search_projection_replacement_evidence")
            .unwrap();
        assert_eq!(
            search_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.search_projection_evidence.protocol",
                "replacement_summary.search_projection_shadow_evidence.protocol"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "attach_search_projection_replacement_evidence"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| {
                            field == "replacement_summary.search_projection_evidence.protocol"
                        })
            }));
    }

    #[test]
    fn exposes_typed_graph_replacement_cutover_readiness() {
        let bundle = ready_bundle();

        let typed = super::graph_replacement_cutover_readiness(&bundle);

        assert!(typed.evidence_ready());
        assert!(typed.production_cutover_ready);
        assert!(typed.shadow_evidence_ready);
        assert!(typed.dual_engine_evidence_present);
        assert!(typed.dual_engine_evidence_ready);
        assert!(typed.dual_engine_evidence_consistent);
        assert!(typed.blocker_codes.is_empty());
    }

    #[test]
    fn typed_graph_replacement_cutover_readiness_rejects_inconsistent_dual_engine() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["dual_engine_evidence"]["consistent"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["dual_engine_evidence"]["blocker_codes"] =
            serde_json::json!(["dual_engine_inconsistent"]);

        let typed = super::graph_replacement_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.dual_engine_evidence_consistent);
        assert_eq!(
            typed.blocker_codes,
            vec!["dual_engine_inconsistent".to_string()]
        );
    }

    #[test]
    fn exposes_typed_query_family_replacement_cutover_readiness() {
        let bundle = ready_bundle();

        let typed = super::query_family_replacement_cutover_readiness(&bundle);

        assert!(typed.evidence_ready());
        assert!(typed.required_query_families_present);
        assert!(typed.missing_required_query_families_empty);
        assert!(typed.blocked_query_families_empty);
        assert!(typed.min_replacement_readiness_full);
        assert!(typed.blocker_codes.is_empty());
    }

    #[test]
    fn typed_query_family_replacement_cutover_readiness_rejects_missing_family() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["replacement_readiness_family_summary"]
            ["missing_required_query_families"] = serde_json::json!(["projected_graph"]);
        bundle["replacement_summary"]["missing_evidence"] =
            serde_json::json!(["query_family.projected_graph"]);

        let typed = super::query_family_replacement_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.missing_required_query_families_empty);
        assert_eq!(
            typed.blocker_codes,
            vec!["query_family.projected_graph".to_string()]
        );
    }

    #[test]
    fn requires_explicit_required_query_family_readiness() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]
            .as_object_mut()
            .unwrap()
            .remove("replacement_readiness_family_summary");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_family_replacement_evidence"])
        );
        let family_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_family_replacement_evidence")
            .unwrap();
        assert_eq!(
            family_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.replacement_readiness_family_summary.required_query_families",
                "replacement_summary.replacement_readiness_family_summary.min_replacement_readiness_per_million"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "close_required_query_families"));
    }

    #[test]
    fn rejects_missing_required_query_family_even_if_cutover_flag_is_true() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["replacement_readiness_family_summary"]
            ["missing_required_query_families"] = serde_json::json!(["projected_graph"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_family_replacement_evidence"])
        );
        let family_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_family_replacement_evidence")
            .unwrap();
        assert_eq!(
            family_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.replacement_readiness_family_summary.missing_required_query_families"
            ])
        );
    }

    #[test]
    fn requires_bounded_read_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["bounded_read_evidence"]
            ["row_limit_enforced_before_output"] = serde_json::json!(false);
        bundle["replacement_summary"]["bounded_read_evidence"]["blocker_codes"] =
            serde_json::json!(["row_cap_not_enforced"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["row_cap_not_enforced"])
        );
        let read_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence")
            .unwrap();
        assert_eq!(
            read_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_bounded_read_profile"));
    }

    #[test]
    fn exposes_typed_bounded_read_cutover_readiness() {
        let bundle = ready_bundle();

        let typed = super::bounded_read_cutover_readiness(&bundle);

        assert!(typed.evidence_ready());
        assert!(typed.present);
        assert!(typed.protocol_matches);
        assert!(typed.max_rows_present);
        assert!(typed.execution_cap_matches);
        assert!(typed.estimated_payload_bytes_present);
        assert!(typed.max_estimated_payload_bytes_present);
        assert!(typed.payload_budget_not_exceeded);
        assert!(typed.row_limit_enforced_before_output);
        assert!(typed.operator_row_cap_enabled);
        assert!(typed.blocking_operator_memory_reports_complete);
        assert!(typed.blocking_operator_memory_within_budget);
        assert!(typed.spill_within_budget);
        assert!(typed.streaming_evidence_present);
        assert!(typed.route_catalog_version_matches);
        assert!(typed.route_catalog_digest_present);
        assert!(typed.route_coverage_ready);
        assert!(typed.blocker_codes.is_empty());
    }

    #[test]
    fn typed_bounded_read_cutover_requires_route_coverage() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["covered_routes"] =
            serde_json::json!(["/graph/overview"]);
        bundle["replacement_summary"]["bounded_read_evidence"]["missing_covered_routes"] =
            serde_json::json!(["/graph/explore"]);

        let typed = super::bounded_read_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.route_coverage_ready);
    }

    #[test]
    fn requires_bounded_read_payload_budget() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["bounded_read_evidence"]["estimated_payload_bytes"] =
            serde_json::json!(8192);
        bundle["replacement_summary"]["bounded_read_evidence"]["max_estimated_payload_bytes"] =
            serde_json::json!(4096);
        bundle["replacement_summary"]["bounded_read_evidence"]["payload_budget_exceeded"] =
            serde_json::json!(true);
        bundle["replacement_summary"]["bounded_read_evidence"]["blocker_codes"] =
            serde_json::json!(["payload_budget_exceeded"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["payload_budget_exceeded"])
        );
        let read_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence")
            .unwrap();
        assert_eq!(
            read_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.payload_budget_exceeded"
            ])
        );
    }

    #[test]
    fn requires_bounded_read_route_coverage() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["covered_routes"] =
            serde_json::json!(["/graph/overview"]);
        bundle["replacement_summary"]["bounded_read_evidence"]["missing_covered_routes"] =
            serde_json::json!(["/graph/explore"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence"])
        );
        let read_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence")
            .unwrap();
        assert_eq!(
            read_check["failed_evidence_fields"],
            serde_json::json!(["replacement_summary.bounded_read_evidence.covered_routes"])
        );
    }

    #[test]
    fn rejects_stale_bounded_read_summary_when_live_evidence_is_not_ready() {
        let mut bundle = ready_bundle();
        bundle["bounded_read_evidence"]["ready"] = serde_json::json!(false);
        bundle["bounded_read_evidence"]["blocker_codes"] =
            serde_json::json!(["skein_shadow_runtime_not_open"]);
        bundle["replacement_summary_bounded_read_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["evidence_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["readiness_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["blocker_codes"] =
            serde_json::json!(["replacement_summary_bounded_read_evidence_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence_alignment"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!([
                "replacement_summary_bounded_read_evidence_mismatch",
                "skein_shadow_runtime_not_open"
            ])
        );
        let alignment_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence_alignment")
            .unwrap();
        assert_eq!(
            alignment_check["failed_evidence_fields"],
            serde_json::json!([
                "bounded_read_evidence.ready",
                "replacement_summary_bounded_read_alignment.ready",
                "replacement_summary_bounded_read_alignment.evidence_ready",
                "replacement_summary_bounded_read_alignment.readiness_matches"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "regenerate_bounded_read_alignment"));
    }

    #[test]
    fn exposes_typed_bounded_read_alignment_cutover_readiness() {
        let bundle = ready_bundle();

        let typed = super::bounded_read_alignment_cutover_readiness(&bundle);

        assert!(typed.evidence_ready());
        assert!(typed.ready);
        assert!(typed.alignment_evidence_ready);
        assert!(typed.summary_ready);
        assert!(typed.protocol_matches);
        assert!(typed.readiness_matches);
        assert!(typed.mode_matches);
        assert!(typed.max_rows_matches);
        assert!(typed.estimated_payload_bytes_matches);
        assert!(typed.max_estimated_payload_bytes_matches);
        assert!(typed.payload_budget_exceeded_matches);
        assert!(typed.streaming_matches);
        assert!(typed.covered_routes_matches);
        assert!(typed.evidence_route_catalog_version_ready);
        assert!(typed.summary_route_catalog_version_ready);
        assert!(typed.evidence_route_catalog_digest_ready);
        assert!(typed.summary_route_catalog_digest_ready);
        assert!(typed.route_catalog_version_matches);
        assert!(typed.route_catalog_digest_matches);
        assert!(typed.blocker_codes.is_empty());
    }

    #[test]
    fn typed_bounded_read_alignment_detects_stale_route_coverage() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_bounded_read_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["covered_routes_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["blocker_codes"] =
            serde_json::json!(["replacement_summary_bounded_read_evidence_mismatch"]);

        let typed = super::bounded_read_alignment_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.ready);
        assert!(!typed.covered_routes_matches);
        assert_eq!(
            typed.blocker_codes,
            vec!["replacement_summary_bounded_read_evidence_mismatch".to_string()]
        );
    }

    #[test]
    fn rejects_stale_bounded_read_summary_when_route_coverage_differs() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_bounded_read_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["covered_routes_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["blocker_codes"] =
            serde_json::json!(["replacement_summary_bounded_read_evidence_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence_alignment"])
        );
        let alignment_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence_alignment")
            .unwrap();
        assert_eq!(
            alignment_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_bounded_read_alignment.ready",
                "replacement_summary_bounded_read_alignment.covered_routes_matches"
            ])
        );
    }

    #[test]
    fn rejects_stale_bounded_read_summary_when_payload_budget_differs() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_bounded_read_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["estimated_payload_bytes_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["payload_budget_exceeded_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_bounded_read_alignment"]["blocker_codes"] =
            serde_json::json!([
                "bounded_read_estimated_payload_bytes_mismatch",
                "bounded_read_payload_budget_exceeded_mismatch"
            ]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence_alignment"])
        );
        let alignment_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence_alignment")
            .unwrap();
        assert_eq!(
            alignment_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_bounded_read_alignment.ready",
                "replacement_summary_bounded_read_alignment.estimated_payload_bytes_matches",
                "replacement_summary_bounded_read_alignment.payload_budget_exceeded_matches"
            ])
        );
    }

    #[test]
    fn rejects_inconsistent_bounded_read_summary_even_if_ready_flag_is_true() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["mode"] =
            serde_json::json!("writable_cutover");
        bundle["replacement_summary"]["bounded_read_evidence"]["execution_row_cap"] =
            serde_json::json!(512);
        bundle["replacement_summary"]["bounded_read_evidence"]["blocking_operator_count"] =
            serde_json::json!(1);
        bundle["replacement_summary"]["bounded_read_evidence"]["streaming"] =
            serde_json::json!(true);
        bundle["replacement_summary"]["bounded_read_evidence"]
            ["blocking_operator_memory_reports_complete"] = serde_json::json!(false);
        bundle["replacement_summary"]["bounded_read_evidence"]
            ["blocking_operator_memory_within_budget"] = serde_json::json!(false);
        bundle["replacement_summary"]["bounded_read_evidence"]["spill_within_budget"] =
            serde_json::json!(false);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence"])
        );
        let read_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence")
            .unwrap();
        assert_eq!(
            read_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.bounded_read_evidence.mode",
                "replacement_summary.bounded_read_evidence.execution_row_cap",
                "replacement_summary.bounded_read_evidence.blocking_operator_memory_reports_complete",
                "replacement_summary.bounded_read_evidence.blocking_operator_memory_within_budget",
                "replacement_summary.bounded_read_evidence.spill_within_budget"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_bounded_read_profile"));
    }

    #[test]
    fn requires_bounded_read_evidence_protocol() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["protocol"] =
            serde_json::json!("handwritten");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["bounded_read_evidence"])
        );
        let read_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "bounded_read_evidence")
            .unwrap();
        assert_eq!(
            read_check["failed_evidence_fields"],
            serde_json::json!(["replacement_summary.bounded_read_evidence.protocol"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "attach_bounded_read_profile"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "replacement_summary.bounded_read_evidence.protocol")
            }));
    }

    #[test]
    fn requires_graph_route_readiness_evidence() {
        let mut bundle = ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("graph_route_readiness");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!([
                "graph_route_readiness.protocol",
                "graph_route_readiness.evidence_protocol",
                "graph_route_readiness.evidence_ready",
                "graph_route_readiness.route_count",
                "graph_route_readiness.required_route_count",
                "graph_route_readiness.route_coverage",
                "graph_route_readiness.missing_required_routes",
                "graph_route_readiness.query_runtime_route_count",
                "graph_route_readiness.query_runtime_report_count",
                "graph_route_readiness.query_runtime_plan_profile_counts",
                "graph_route_readiness.relationship_property_pruning_counts",
                "graph_route_readiness.missing_query_runtime_routes",
                "graph_route_readiness.route_query_runtime_ready",
                "graph_route_readiness.route_primary_ready",
                "graph_route_readiness.primary_ready_route_count",
                "graph_route_readiness.evidence_route_coverage_present",
                "graph_route_readiness.evidence_route_coverage_matches",
                "graph_route_readiness.routes"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_graph_route_readiness_evidence"));
    }

    #[test]
    fn rejects_graph_route_readiness_without_route_coverage_envelope() {
        let mut bundle = ready_bundle();
        for field in [
            "covered_route_count",
            "covered_routes",
            "required_routes_covered",
            "unknown_routes",
            "duplicate_routes",
            "route_coverage_ready",
            "route_coverage_blocker_codes",
            "evidence_route_coverage_present",
            "evidence_route_coverage_matches",
            "evidence_route_coverage_blocker_codes",
        ] {
            bundle["graph_route_readiness"]
                .as_object_mut()
                .unwrap()
                .remove(field);
        }

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert!(route_check["failed_evidence_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "graph_route_readiness.route_coverage"));
        assert!(route_check["failed_evidence_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "graph_route_readiness.evidence_route_coverage_present"));
    }

    #[test]
    fn rejects_graph_route_readiness_with_stale_route_coverage_envelope() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["covered_routes"] = serde_json::json!(["/graph/overview"]);
        bundle["graph_route_readiness"]["covered_route_count"] = serde_json::json!(1);
        bundle["graph_route_readiness"]["evidence_route_coverage_matches"] =
            serde_json::json!(false);
        bundle["graph_route_readiness"]["evidence_route_coverage_blocker_codes"] =
            serde_json::json!(["route_coverage_evidence_mismatch"]);
        bundle["graph_route_readiness"]["route_primary_blocker_codes"] =
            serde_json::json!(["route_coverage_evidence_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert!(route_check["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "route_coverage_evidence_mismatch"));
        assert!(route_check["failed_evidence_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "graph_route_readiness.route_coverage"));
        assert!(route_check["failed_evidence_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "graph_route_readiness.evidence_route_coverage_matches"));
    }

    #[test]
    fn requires_query_runtime_preflight_evidence() {
        let mut bundle = ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("query_runtime_preflight");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "query_runtime_preflight.protocol",
                "query_runtime_preflight.ready",
                "query_runtime_preflight.database_opened",
                "query_runtime_preflight.redaction.ready",
                "query_runtime_preflight.redaction.rows_copied",
                "query_runtime_preflight.redaction.parameters_copied",
                "query_runtime_preflight.redaction.local_paths_copied",
                "query_runtime_preflight.redaction.raw_errors_copied",
                "query_runtime_preflight.probe_count",
                "query_runtime_preflight.passed_probe_count",
                "query_runtime_preflight.failed_probe_count",
                "query_runtime_preflight.route_coverage",
                "query_runtime_preflight.route_catalog_version",
                "query_runtime_preflight.route_catalog_digest",
                "query_runtime_preflight.probes"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_query_runtime_preflight_evidence"));
    }

    #[test]
    fn exposes_typed_query_runtime_preflight_cutover_readiness() {
        let bundle = ready_bundle();

        let typed = super::query_runtime_preflight_cutover_readiness(&bundle);

        assert!(typed.evidence_ready());
        assert!(typed.protocol_matches);
        assert!(typed.ready);
        assert!(typed.database_opened);
        assert!(typed.redaction_ready);
        assert!(typed.rows_redacted);
        assert!(typed.parameters_redacted);
        assert!(typed.local_paths_redacted);
        assert!(typed.raw_errors_redacted);
        assert!(typed.probe_count_present);
        assert!(typed.probe_counts_match);
        assert!(typed.failed_probe_count_zero);
        assert!(typed.route_coverage_ready);
        assert!(typed.route_catalog_version_matches);
        assert!(typed.route_catalog_digest_present);
        assert!(typed.probe_details_ready);
        assert!(typed.blocker_codes.is_empty());
    }

    #[test]
    fn typed_query_runtime_preflight_recomputes_probe_details() {
        let mut bundle = ready_bundle();
        bundle["query_runtime_preflight"]["probes"][0]
            .as_object_mut()
            .unwrap()
            .remove("selected_plan_fingerprint");

        let typed = super::query_runtime_preflight_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.probe_details_ready);
    }

    #[test]
    fn rejects_query_runtime_preflight_that_copies_sensitive_fields() {
        let mut bundle = ready_bundle();
        bundle["query_runtime_preflight"]["redaction"]["ready"] = serde_json::json!(false);
        bundle["query_runtime_preflight"]["redaction"]["rows_copied"] = serde_json::json!(true);
        bundle["query_runtime_preflight"]["redaction"]["parameters_copied"] =
            serde_json::json!(true);
        bundle["query_runtime_preflight"]["redaction"]["local_paths_copied"] =
            serde_json::json!(true);
        bundle["query_runtime_preflight"]["redaction"]["raw_errors_copied"] =
            serde_json::json!(true);

        let report = nowledge_mem_integration_readiness_json(&bundle);
        let typed = super::query_runtime_preflight_cutover_readiness(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        assert!(!typed.evidence_ready());
        assert!(!typed.redaction_ready);
        assert!(!typed.rows_redacted);
        assert!(!typed.parameters_redacted);
        assert!(!typed.local_paths_redacted);
        assert!(!typed.raw_errors_redacted);
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "query_runtime_preflight.redaction.ready",
                "query_runtime_preflight.redaction.rows_copied",
                "query_runtime_preflight.redaction.parameters_copied",
                "query_runtime_preflight.redaction.local_paths_copied",
                "query_runtime_preflight.redaction.raw_errors_copied"
            ])
        );
    }

    #[test]
    fn rejects_weak_query_runtime_preflight_even_if_ready_flag_is_true() {
        let mut bundle = ready_bundle();
        bundle["query_runtime_preflight"]["probes"][0]
            .as_object_mut()
            .unwrap()
            .remove("selected_plan_fingerprint");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.probes"])
        );
    }

    #[test]
    fn rejects_query_runtime_preflight_without_probe_identity() {
        let mut bundle = ready_bundle();
        bundle["query_runtime_preflight"]["probes"][0]
            .as_object_mut()
            .unwrap()
            .remove("query_family");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let query_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            query_check["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.probes"])
        );
    }

    #[test]
    fn rejects_query_runtime_preflight_without_scan_pruning_reports() {
        let mut bundle = ready_bundle();
        bundle["query_runtime_preflight"]["probes"][0]["execution_profile"]
            .as_object_mut()
            .unwrap()
            .remove("scan_pruning_reports");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.probes"])
        );
    }

    #[test]
    fn rejects_query_runtime_preflight_without_route_coverage() {
        let mut bundle = ready_bundle();
        bundle["query_runtime_preflight"]["probes"]
            .as_array_mut()
            .unwrap()
            .pop();

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.route_coverage"])
        );
    }

    #[test]
    fn rejects_query_runtime_preflight_with_unknown_route() {
        let mut bundle = ready_bundle();
        let mut probe = bundle["query_runtime_preflight"]["probes"][0].clone();
        probe["route"] = serde_json::json!("/graph/stale-route");
        bundle["query_runtime_preflight"]["probes"]
            .as_array_mut()
            .unwrap()
            .push(probe);
        bundle["query_runtime_preflight"]["unknown_routes"] =
            serde_json::json!(["/graph/stale-route"]);
        bundle["query_runtime_preflight"]["route_coverage_ready"] = serde_json::json!(false);
        bundle["query_runtime_preflight"]["route_coverage_blocker_codes"] =
            serde_json::json!(["query_runtime_unknown_routes"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "query_runtime_preflight.route_coverage",
                "query_runtime_preflight.probes"
            ])
        );
        assert!(check["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_runtime_unknown_routes"));
    }

    #[test]
    fn rejects_query_runtime_preflight_with_duplicate_route() {
        let mut bundle = ready_bundle();
        let probe = bundle["query_runtime_preflight"]["probes"][0].clone();
        bundle["query_runtime_preflight"]["probes"]
            .as_array_mut()
            .unwrap()
            .push(probe);
        bundle["query_runtime_preflight"]["duplicate_routes"] =
            serde_json::json!([REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0]]);
        bundle["query_runtime_preflight"]["route_coverage_ready"] = serde_json::json!(false);
        bundle["query_runtime_preflight"]["route_coverage_blocker_codes"] =
            serde_json::json!(["query_runtime_duplicate_routes"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!(["query_runtime_preflight.route_coverage"])
        );
        assert!(check["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_runtime_duplicate_routes"));
    }

    #[test]
    fn exposes_typed_query_runtime_preflight_alignment_cutover_readiness() {
        let bundle = ready_bundle();

        let typed = super::query_runtime_preflight_alignment_cutover_readiness(&bundle);

        assert!(typed.evidence_ready());
        assert!(typed.ready);
        assert!(typed.evidence_ready);
        assert!(typed.summary_ready);
        assert!(typed.protocol_matches);
        assert!(typed.readiness_matches);
        assert!(typed.database_opened_matches);
        assert!(typed.probe_count_matches);
        assert!(typed.passed_probe_count_matches);
        assert!(typed.failed_probe_count_matches);
        assert!(typed.required_route_count_matches);
        assert!(typed.covered_route_count_matches);
        assert!(typed.covered_routes_matches);
        assert!(typed.required_routes_covered_matches);
        assert!(typed.route_coverage_ready_matches);
        assert!(typed.evidence_route_catalog_version_ready);
        assert!(typed.summary_route_catalog_version_ready);
        assert!(typed.evidence_route_catalog_digest_ready);
        assert!(typed.summary_route_catalog_digest_ready);
        assert!(typed.route_catalog_version_matches);
        assert!(typed.route_catalog_digest_matches);
        assert!(typed.blocker_codes.is_empty());
    }

    #[test]
    fn typed_query_runtime_preflight_alignment_detects_stale_route_coverage() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_query_runtime_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_query_runtime_alignment"]["covered_routes_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_query_runtime_alignment"]["blocker_codes"] =
            serde_json::json!(["query_runtime_preflight_covered_routes_mismatch"]);

        let typed = super::query_runtime_preflight_alignment_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.ready);
        assert!(!typed.covered_routes_matches);
        assert_eq!(
            typed.blocker_codes,
            vec!["query_runtime_preflight_covered_routes_mismatch".to_string()]
        );
    }

    #[test]
    fn requires_query_runtime_preflight_alignment() {
        let mut bundle = ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("replacement_summary_query_runtime_alignment");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight_alignment"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight_alignment")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_query_runtime_alignment.ready",
                "replacement_summary_query_runtime_alignment.evidence_ready",
                "replacement_summary_query_runtime_alignment.summary_ready",
                "replacement_summary_query_runtime_alignment.protocol_matches",
                "replacement_summary_query_runtime_alignment.readiness_matches",
                "replacement_summary_query_runtime_alignment.database_opened_matches",
                "replacement_summary_query_runtime_alignment.probe_count_matches",
                "replacement_summary_query_runtime_alignment.passed_probe_count_matches",
                "replacement_summary_query_runtime_alignment.failed_probe_count_matches",
                "replacement_summary_query_runtime_alignment.required_route_count_matches",
                "replacement_summary_query_runtime_alignment.covered_route_count_matches",
                "replacement_summary_query_runtime_alignment.covered_routes_matches",
                "replacement_summary_query_runtime_alignment.required_routes_covered_matches",
                "replacement_summary_query_runtime_alignment.route_coverage_ready_matches",
                "replacement_summary_query_runtime_alignment.evidence_route_catalog_version_ready",
                "replacement_summary_query_runtime_alignment.summary_route_catalog_version_ready",
                "replacement_summary_query_runtime_alignment.evidence_route_catalog_digest_ready",
                "replacement_summary_query_runtime_alignment.summary_route_catalog_digest_ready",
                "replacement_summary_query_runtime_alignment.route_catalog_version_matches",
                "replacement_summary_query_runtime_alignment.route_catalog_digest_matches"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "regenerate_query_runtime_preflight_alignment"));
    }

    #[test]
    fn rejects_stale_query_runtime_summary_when_route_coverage_differs() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_query_runtime_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_query_runtime_alignment"]["covered_routes_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_query_runtime_alignment"]["blocker_codes"] =
            serde_json::json!(["query_runtime_preflight_covered_routes_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["query_runtime_preflight_alignment"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["query_runtime_preflight_covered_routes_mismatch"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "query_runtime_preflight_alignment")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_query_runtime_alignment.ready",
                "replacement_summary_query_runtime_alignment.covered_routes_matches"
            ])
        );
    }

    #[test]
    fn rejects_weak_graph_route_query_profiles_even_if_summary_is_ready() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]
            .as_object_mut()
            .unwrap()
            .remove("scan_pruning_reports");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_graph_route_readiness_evidence"));
    }

    #[test]
    fn exposes_typed_graph_route_cutover_readiness() {
        let bundle = ready_bundle();

        let typed = super::graph_route_cutover_readiness(&bundle);

        assert!(typed.evidence_ready());
        assert!(typed.protocol_matches);
        assert!(typed.evidence_protocol_matches);
        assert!(typed.route_count_present);
        assert!(typed.required_route_count_matches);
        assert!(typed.route_coverage_ready);
        assert!(typed.query_runtime_route_count_matches);
        assert!(typed.query_runtime_report_count_ready);
        assert!(typed.query_plan_profile_summary_ready);
        assert!(typed.relationship_property_pruning_summary_ready);
        assert!(typed.route_query_runtime_ready);
        assert!(typed.route_primary_ready);
        assert!(typed.primary_ready_route_count_matches);
        assert!(typed.evidence_route_coverage_present);
        assert!(typed.evidence_route_coverage_matches);
        assert!(typed.route_query_profiles_ready);
        assert!(typed.blocker_codes.is_empty());
    }

    #[test]
    fn typed_graph_route_cutover_recomputes_route_profile_details() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]
            .as_object_mut()
            .unwrap()
            .remove("scan_pruning_reports");

        let typed = super::graph_route_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.route_query_profiles_ready);
    }

    #[test]
    fn rejects_graph_route_readiness_without_plan_profile_summary_counts() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]
            .as_object_mut()
            .unwrap()
            .remove("query_runtime_plan_report_count");
        bundle["graph_route_readiness"]["query_runtime_missing_profile_evidence_count"] =
            serde_json::json!(1);
        bundle["graph_route_readiness"]["route_query_profile_evidence_ready"] =
            serde_json::json!(false);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.query_runtime_plan_profile_counts"])
        );
    }

    #[test]
    fn rejects_graph_route_readiness_without_relationship_property_pruning_counts() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]
            .as_object_mut()
            .unwrap()
            .remove("relationship_property_pruning_required_count");
        bundle["graph_route_readiness"]["route_relationship_property_pruning_evidence_ready"] =
            serde_json::json!(false);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.relationship_property_pruning_counts"])
        );
    }

    #[test]
    fn rejects_graph_route_query_profiles_with_only_scan_pruning_presence_flag() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]
            .as_object_mut()
            .unwrap()
            .remove("scan_pruning_reports");
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]
            ["scan_pruning_reports_present"] = serde_json::json!(true);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
    }

    #[test]
    fn rejects_graph_route_relationship_pruning_with_legacy_record_kind_only() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["relationship_property_pruning_required_count"] =
            serde_json::json!(1);
        bundle["graph_route_readiness"]["relationship_property_pruning_report_count"] =
            serde_json::json!(1);
        bundle["graph_route_readiness"]["route_relationship_property_pruning_evidence_ready"] =
            serde_json::json!(true);
        bundle["graph_route_readiness"]["routes"][0]
            ["relationship_property_pruning_required_count"] = serde_json::json!(1);
        bundle["graph_route_readiness"]["routes"][0]
            ["relationship_property_pruning_report_count"] = serde_json::json!(1);
        bundle["graph_route_readiness"]["routes"][0]
            ["relationship_property_pruning_evidence_ready"] = serde_json::json!(true);
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]["scan_pruning_reports"]
            [0]["record_kind"] = serde_json::json!("relationship");
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]["scan_pruning_reports"]
            [0]["target_kind"] = serde_json::json!("node");
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]["scan_pruning_reports"]
            [0]["strategy"] = serde_json::json!({
            "kind": "relationship_property",
            "property": "type"
        });

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
    }

    #[test]
    fn rejects_graph_route_query_profiles_without_query_identity() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["routes"][0]["query_reports"][0]
            .as_object_mut()
            .unwrap()
            .remove("query_name");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
    }

    #[test]
    fn rejects_graph_route_profiles_without_required_query_family_evidence() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["routes"][0]
            .as_object_mut()
            .unwrap()
            .remove("required_query_families");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
    }

    #[test]
    fn rejects_graph_route_profiles_when_query_family_does_not_match_route() {
        let mut bundle = ready_bundle();
        let route = bundle["graph_route_readiness"]["routes"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|route| route["route"] == "/graph/shortest-path")
            .unwrap();
        route["query_reports"][0]["query_family"] = serde_json::json!("memory_lookup");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
    }

    #[test]
    fn rejects_graph_route_profiles_without_route_parity_source() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["routes"][0]
            .as_object_mut()
            .unwrap()
            .remove("shadow_compare_evidence_source");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
    }

    #[test]
    fn rejects_graph_route_profiles_with_weak_route_parity_detail() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["routes"][0]["shadow_compare"]["matched_per_million"] =
            serde_json::json!(999999);
        bundle["graph_route_readiness"]["routes"][0]["shadow_compare"]["primary_engine"] =
            serde_json::json!("skein");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.routes"])
        );
    }

    #[test]
    fn rejects_graph_route_readiness_without_primary_route_coverage() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["route_primary_ready"] = serde_json::json!(false);
        bundle["graph_route_readiness"]["primary_ready_route_count"] = serde_json::json!(13);
        bundle["graph_route_readiness"]["route_primary_blocker_codes"] =
            serde_json::json!(["graph_route_primary_not_enabled"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["graph_route_primary_not_enabled"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!([
                "graph_route_readiness.route_primary_ready",
                "graph_route_readiness.primary_ready_route_count",
                "graph_route_readiness.route_primary_blocker_codes"
            ])
        );
    }

    #[test]
    fn requires_graph_route_readiness_protocol() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["protocol"] = serde_json::json!("handwritten");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.protocol"])
        );
    }

    #[test]
    fn requires_graph_route_readiness_evidence_protocol() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["evidence_protocol"] = serde_json::json!("handwritten");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!(["graph_route_readiness.evidence_protocol"])
        );
    }

    #[test]
    fn rejects_graph_route_readiness_when_route_evidence_is_not_ready() {
        let mut bundle = ready_bundle();
        bundle["graph_route_readiness"]["evidence_ready"] = serde_json::json!(false);
        bundle["graph_route_readiness"]["route_primary_blocker_codes"] =
            serde_json::json!(["graph_route_evidence_not_ready"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["graph_route_evidence_not_ready"])
        );
        let route_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness")
            .unwrap();
        assert_eq!(
            route_check["failed_evidence_fields"],
            serde_json::json!([
                "graph_route_readiness.evidence_ready",
                "graph_route_readiness.route_primary_blocker_codes"
            ])
        );
    }

    #[test]
    fn rejects_stale_graph_route_readiness_summary() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_graph_route_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["summary_route_primary_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["route_primary_ready_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["primary_ready_routes_match"] =
            serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["blocker_codes"] =
            serde_json::json!(["replacement_summary_graph_route_readiness_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness_alignment"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["replacement_summary_graph_route_readiness_mismatch"])
        );
        let alignment_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness_alignment")
            .unwrap();
        assert_eq!(
            alignment_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_graph_route_alignment.ready",
                "replacement_summary_graph_route_alignment.summary_route_primary_ready",
                "replacement_summary_graph_route_alignment.route_primary_ready_matches",
                "replacement_summary_graph_route_alignment.primary_ready_routes_match"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "regenerate_graph_route_readiness_alignment"));
    }

    #[test]
    fn rejects_graph_route_alignment_without_api_behavior_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_graph_route_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]
            ["route_query_api_behavior_evidence_ready_matches"] = serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["blocker_codes"] =
            serde_json::json!(["graph_route_query_api_behavior_evidence_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness_alignment"])
        );
        let alignment_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness_alignment")
            .unwrap();
        assert_eq!(
            alignment_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_graph_route_alignment.ready",
                "replacement_summary_graph_route_alignment.route_query_api_behavior_evidence_ready_matches"
            ])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["graph_route_query_api_behavior_evidence_mismatch"])
        );
    }

    #[test]
    fn rejects_graph_route_alignment_without_route_evidence_envelope() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_graph_route_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["evidence_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["evidence_protocol_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["blocker_codes"] = serde_json::json!([
            "graph_route_evidence_not_ready",
            "graph_route_evidence_protocol_mismatch"
        ]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_readiness_alignment"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!([
                "graph_route_evidence_not_ready",
                "graph_route_evidence_protocol_mismatch"
            ])
        );
        let alignment_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_readiness_alignment")
            .unwrap();
        assert_eq!(
            alignment_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary_graph_route_alignment.ready",
                "replacement_summary_graph_route_alignment.evidence_protocol_matches",
                "replacement_summary_graph_route_alignment.evidence_ready"
            ])
        );
    }

    #[test]
    fn exposes_typed_graph_route_alignment_cutover_readiness() {
        let bundle = ready_bundle();

        let typed = super::graph_route_alignment_cutover_readiness(&bundle);

        assert!(typed.evidence_ready());
        assert!(typed.ready);
        assert!(typed.evidence_protocol_matches);
        assert!(typed.evidence_ready);
        assert!(typed.evidence_route_primary_ready);
        assert!(typed.summary_route_primary_ready);
        assert!(typed.route_primary_ready_matches);
        assert!(typed.route_query_plan_evidence_ready_matches);
        assert!(typed.route_query_profile_evidence_ready_matches);
        assert!(typed.route_query_api_behavior_evidence_ready_matches);
        assert!(typed.route_relationship_property_pruning_evidence_ready_matches);
        assert!(typed.relationship_property_pruning_required_count_matches);
        assert!(typed.relationship_property_pruning_report_count_matches);
        assert!(typed.primary_ready_routes_match);
        assert!(typed.evidence_required_routes_covered);
        assert!(typed.summary_required_routes_covered);
        assert!(typed.evidence_route_catalog_version_ready);
        assert!(typed.summary_route_catalog_version_ready);
        assert!(typed.evidence_route_catalog_digest_ready);
        assert!(typed.summary_route_catalog_digest_ready);
        assert!(typed.route_catalog_version_matches);
        assert!(typed.route_catalog_digest_matches);
        assert!(typed.blocker_codes.is_empty());
    }

    #[test]
    fn typed_graph_route_alignment_detects_stale_route_catalog_metadata() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary_graph_route_alignment"]["ready"] = serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["route_catalog_digest_matches"] =
            serde_json::json!(false);
        bundle["replacement_summary_graph_route_alignment"]["blocker_codes"] =
            serde_json::json!(["graph_route_catalog_digest_mismatch"]);

        let typed = super::graph_route_alignment_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.ready);
        assert!(!typed.route_catalog_digest_matches);
        assert_eq!(
            typed.blocker_codes,
            vec!["graph_route_catalog_digest_mismatch".to_string()]
        );
    }

    #[test]
    fn requires_graph_route_parity_alignment() {
        let mut bundle = ready_bundle();
        bundle["graph_route_parity_alignment"]["ready"] = serde_json::json!(false);
        bundle["graph_route_parity_alignment"]["ready_route_count"] = serde_json::json!(17);
        bundle["graph_route_parity_alignment"]["missing_routes"] =
            serde_json::json!(["agent_evolves"]);
        bundle["graph_route_parity_alignment"]["observed_blocker_codes"] =
            serde_json::json!(["agent_evolves_parity_evidence_missing"]);
        bundle["graph_route_parity_alignment"]["blocker_codes"] =
            serde_json::json!(["graph_route_parity_evidence_missing"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_parity_alignment"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["graph_route_parity_evidence_missing"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_parity_alignment")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "graph_route_parity_alignment.ready",
                "graph_route_parity_alignment.ready_route_count",
                "graph_route_parity_alignment.missing_routes"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_graph_route_parity_evidence"));
    }

    #[test]
    fn exposes_typed_graph_route_parity_alignment_cutover_readiness() {
        let bundle = ready_bundle();

        let typed = super::graph_route_parity_alignment_cutover_readiness(&bundle);

        assert!(typed.evidence_ready());
        assert!(typed.ready);
        assert!(typed.required_route_count_present);
        assert!(typed.ready_route_count_matches);
        assert!(typed.missing_routes_empty);
        assert!(typed.not_ready_routes_empty);
        assert!(typed.route_mismatch_routes_empty);
        assert!(typed.protocol_mismatch_routes_empty);
        assert!(typed.blocker_routes_empty);
        assert!(typed.blocker_codes.is_empty());
    }

    #[test]
    fn typed_graph_route_parity_alignment_detects_missing_routes() {
        let mut bundle = ready_bundle();
        bundle["graph_route_parity_alignment"]["ready"] = serde_json::json!(false);
        bundle["graph_route_parity_alignment"]["ready_route_count"] = serde_json::json!(17);
        bundle["graph_route_parity_alignment"]["missing_routes"] =
            serde_json::json!(["agent_evolves"]);
        bundle["graph_route_parity_alignment"]["blocker_codes"] =
            serde_json::json!(["graph_route_parity_evidence_missing"]);

        let typed = super::graph_route_parity_alignment_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.ready);
        assert!(!typed.ready_route_count_matches);
        assert!(!typed.missing_routes_empty);
        assert_eq!(
            typed.blocker_codes,
            vec!["graph_route_parity_evidence_missing".to_string()]
        );
    }

    #[test]
    fn requires_library_readiness_evidence() {
        let mut bundle = ready_bundle();
        bundle.as_object_mut().unwrap().remove("library_readiness");

        let typed = super::library_readiness_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(!typed.protocol_matches);
        assert!(!typed.present);
        assert!(!typed.ready);
        assert!(!typed.graph_ready);
        assert!(typed.blocker_codes.is_empty());

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        let library_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "library_readiness")
            .unwrap();
        assert_eq!(
            library_check["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.protocol",
                "library_readiness.present",
                "library_readiness.ready",
                "library_readiness.production_path.ready",
                "library_readiness.production_path.in_process",
                "library_readiness.production_path.cli_required",
                "library_readiness.production_path.env_control_plane_required",
                "library_readiness.production_path.spawned_helper_required",
                "library_readiness.ready_area_count",
                "library_readiness.blocked_area_count",
                "library_readiness.redaction.ready",
                "library_readiness.redaction.query_text_copied",
                "library_readiness.redaction.parameters_copied",
                "library_readiness.redaction.local_paths_copied",
                "library_readiness.open_report.graph_opened",
                "library_readiness.open_report.search_projection_opened",
                "library_readiness.readiness_by_area.graph.ready",
                "library_readiness.readiness_by_area.query.ready",
                "library_readiness.readiness_by_area.storage.ready",
                "library_readiness.readiness_by_area.background.ready",
                "library_readiness.readiness_by_area.query_family.ready",
                "library_readiness.readiness_by_area.graph_route.ready",
                "library_readiness.readiness_by_area.search_route_ownership.ready",
                "library_readiness.readiness_by_area.search_projection.ready",
                "library_readiness.readiness_by_area.search_projection_shadow.ready",
                "library_readiness.readiness_by_area.search_candidate_shadow.ready",
                "library_readiness.readiness_by_area.workload_fixture.ready",
                "library_readiness.production_resource_profile"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_library_readiness_evidence"));
    }

    #[test]
    fn rejects_library_readiness_with_over_budget_production_resource_profile() {
        let mut bundle = ready_bundle();
        bundle["library_readiness"]["production_resource_profile"]["execution"]
            ["steady_resident_bytes"] = serde_json::json!(536870913u64);

        let typed = super::library_readiness_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.production_resource_profile_ready);
        let report = nowledge_mem_integration_readiness_json(&bundle);
        let library_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "library_readiness")
            .unwrap();
        assert_eq!(report["ready"], false);
        assert_eq!(
            library_check["failed_evidence_fields"],
            serde_json::json!(["library_readiness.production_resource_profile"])
        );
    }

    #[test]
    fn rejects_blocked_library_readiness_area() {
        let mut bundle = ready_bundle();
        bundle["library_readiness"]["ready"] = serde_json::json!(false);
        bundle["library_readiness"]["blocked_area_count"] = serde_json::json!(1);
        bundle["library_readiness"]["blocker_codes"] =
            serde_json::json!(["search_projection_not_ready"]);
        bundle["library_readiness"]["readiness_by_area"]["search_projection"]["ready"] =
            serde_json::json!(false);
        bundle["library_readiness"]["readiness_by_area"]["search_projection"]["blocker_codes"] =
            serde_json::json!(["search_projection_probe_missing"]);

        let typed = super::library_readiness_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.protocol_matches);
        assert!(typed.present);
        assert!(!typed.ready);
        assert!(!typed.blocked_area_count_zero);
        assert!(!typed.search_projection_ready);
        assert_eq!(
            typed.blocker_codes,
            vec![
                "search_projection_not_ready".to_string(),
                "search_projection_probe_missing".to_string()
            ]
        );

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!([
                "search_projection_not_ready",
                "search_projection_probe_missing"
            ])
        );
        let library_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "library_readiness")
            .unwrap();
        assert_eq!(
            library_check["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.ready",
                "library_readiness.blocked_area_count",
                "library_readiness.readiness_by_area.search_projection.ready"
            ])
        );
    }

    #[test]
    fn rejects_library_readiness_that_requires_cli_on_production_path() {
        let mut bundle = ready_bundle();
        bundle["library_readiness"]["ready"] = serde_json::json!(false);
        bundle["library_readiness"]["production_path"]["ready"] = serde_json::json!(false);
        bundle["library_readiness"]["production_path"]["cli_required"] = serde_json::json!(true);
        bundle["library_readiness"]["production_path"]["spawned_helper_required"] =
            serde_json::json!(true);
        bundle["library_readiness"]["blocker_codes"] =
            serde_json::json!(["library_production_path_not_embedded"]);

        let typed = super::library_readiness_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.protocol_matches);
        assert!(typed.present);
        assert!(!typed.ready);
        assert!(!typed.production_path_ready);
        assert!(!typed.production_path_cli_not_required);
        assert!(!typed.production_path_spawned_helper_not_required);
        assert_eq!(
            typed.blocker_codes,
            vec!["library_production_path_not_embedded".to_string()]
        );

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "library_readiness")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.ready",
                "library_readiness.production_path.ready",
                "library_readiness.production_path.cli_required",
                "library_readiness.production_path.spawned_helper_required"
            ])
        );
    }

    #[test]
    fn typed_library_readiness_requires_production_path_contract() {
        let mut bundle = ready_bundle();
        bundle["library_readiness"]
            .as_object_mut()
            .unwrap()
            .remove("production_path");

        let typed = super::library_readiness_cutover_readiness(&bundle);

        assert!(!typed.evidence_ready());
        assert!(!typed.production_path_ready);
        assert!(!typed.production_path_in_process);
        assert!(!typed.production_path_cli_not_required);
        assert!(!typed.production_path_env_control_plane_not_required);
        assert!(!typed.production_path_spawned_helper_not_required);
    }

    #[test]
    fn requires_cutover_controls_evidence() {
        let mut bundle = ready_bundle();
        bundle.as_object_mut().unwrap().remove("cutover_controls");

        let typed = super::cutover_controls_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(!typed.protocol_matches);
        assert!(!typed.ready);
        assert!(!typed.graph_reads_skein);
        assert!(!typed.search_reads_skein);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["cutover_controls"])
        );
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "cutover_controls")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "cutover_controls.protocol",
                "cutover_controls.ready",
                "cutover_controls.controls.graph_reads",
                "cutover_controls.graph.read_effective",
                "cutover_controls.production_status.graph.skein_cutover_effective",
                "cutover_controls.controls.search_reads",
                "cutover_controls.search.read_effective",
                "cutover_controls.production_status.search.skein_cutover_effective",
                "cutover_controls.work.dual_writes_enabled",
                "cutover_controls.work.projection_catch_up_enabled",
                "cutover_controls.work.initial_import_safe_for_read_cutover",
                "cutover_controls.redaction.query_text_copied",
                "cutover_controls.redaction.parameters_copied",
                "cutover_controls.redaction.local_paths_copied"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_cutover_controls_evidence"));
    }

    #[test]
    fn cutover_controls_reject_legacy_or_ineffective_skein_reads() {
        let mut bundle = ready_bundle();
        bundle["cutover_controls"]["ready"] = serde_json::json!(false);
        bundle["cutover_controls"]["controls"]["graph_reads"] = serde_json::json!("legacy");
        bundle["cutover_controls"]["graph"]["read_effective"] = serde_json::json!(false);
        bundle["cutover_controls"]["production_status"]["graph"]["skein_cutover_effective"] =
            serde_json::json!(false);
        bundle["cutover_controls"]["work"]["projection_catch_up_enabled"] =
            serde_json::json!(false);
        bundle["cutover_controls"]["blocker_codes"] = serde_json::json!([
            "graph_read_selected_skein_but_not_effective",
            "search_read_selected_skein_without_projection_catch_up"
        ]);

        let typed = super::cutover_controls_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.protocol_matches);
        assert!(!typed.ready);
        assert!(!typed.graph_reads_skein);
        assert!(!typed.graph_read_effective);
        assert!(!typed.graph_production_status_effective);
        assert!(!typed.projection_catch_up_enabled);
        assert_eq!(
            typed.blocker_codes,
            vec![
                "graph_read_selected_skein_but_not_effective".to_string(),
                "search_read_selected_skein_without_projection_catch_up".to_string()
            ]
        );

        let report = nowledge_mem_integration_readiness_json(&bundle);
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "cutover_controls")
            .unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["cutover_controls"])
        );
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "cutover_controls.ready",
                "cutover_controls.controls.graph_reads",
                "cutover_controls.graph.read_effective",
                "cutover_controls.production_status.graph.skein_cutover_effective",
                "cutover_controls.work.projection_catch_up_enabled"
            ])
        );
    }

    #[test]
    fn cutover_controls_reject_active_initial_import_for_read_cutover() {
        let mut bundle = ready_bundle();
        bundle["cutover_controls"]["ready"] = serde_json::json!(false);
        bundle["cutover_controls"]["controls"]["initial_import"] = serde_json::json!("enabled");
        bundle["cutover_controls"]["work"]["initial_import_enabled"] = serde_json::json!(true);
        bundle["cutover_controls"]["work"]["initial_import_inactive_for_cutover"] =
            serde_json::json!(false);
        bundle["cutover_controls"]["blocker_codes"] =
            serde_json::json!(["initial_import_active_blocks_read_cutover"]);

        let typed = super::cutover_controls_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.dual_writes_enabled);
        assert!(typed.initial_import_safe);
        assert!(!typed.initial_import_inactive_for_cutover);
        assert!(!typed.initial_import_cutover_catch_up_ready);
        assert!(!typed.initial_import_safe_for_read_cutover);
        assert_eq!(
            typed.blocker_codes,
            vec!["initial_import_active_blocks_read_cutover".to_string()]
        );

        let report = nowledge_mem_integration_readiness_json(&bundle);
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "cutover_controls")
            .unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["cutover_controls"])
        );
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "cutover_controls.ready",
                "cutover_controls.work.initial_import_safe_for_read_cutover"
            ])
        );
        assert_eq!(
            check["blocker_codes"],
            serde_json::json!(["initial_import_active_blocks_read_cutover"])
        );
    }

    #[test]
    fn cutover_controls_accept_active_initial_import_with_catch_up_proof() {
        let mut bundle = ready_bundle();
        bundle["cutover_controls"]["controls"]["initial_import"] = serde_json::json!("enabled");
        bundle["cutover_controls"]["work"]["initial_import_enabled"] = serde_json::json!(true);
        bundle["cutover_controls"]["work"]["initial_import_inactive_for_cutover"] =
            serde_json::json!(false);
        bundle["cutover_controls"]["work"]["initial_import_cutover_catch_up_ready"] =
            serde_json::json!(true);
        bundle["cutover_controls"]["work"]["initial_import_safe_for_read_cutover"] =
            serde_json::json!(true);

        let typed = super::cutover_controls_readiness(&bundle);
        assert!(typed.evidence_ready());
        assert!(typed.initial_import_safe);
        assert!(!typed.initial_import_inactive_for_cutover);
        assert!(typed.initial_import_cutover_catch_up_ready);
        assert!(typed.initial_import_safe_for_read_cutover);

        let report = nowledge_mem_integration_readiness_json(&bundle);
        assert_eq!(report["ready"], true);
    }

    #[test]
    fn requires_operations_readiness_evidence() {
        let mut bundle = ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("operations_readiness");

        let typed = super::operations_readiness_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(!typed.protocol_matches);
        assert!(!typed.present);
        assert!(!typed.ready);
        assert!(!typed.graph_open);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["operations_readiness"])
        );
        assert_eq!(report["blocking_categories"], serde_json::Value::Null);
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "operations_readiness")
            .unwrap();
        assert_eq!(
            check["failed_evidence_fields"],
            serde_json::json!([
                "operations_readiness.protocol",
                "operations_readiness.present",
                "operations_readiness.ready",
                "operations_readiness.graph.open",
                "operations_readiness.graph.read_only",
                "operations_readiness.search_projection.open",
                "operations_readiness.search_projection.stale",
                "operations_readiness.storage_lifecycle.ready",
                "operations_readiness.storage_lifecycle.action",
                "operations_readiness.readiness.storage_recovery_ready",
                "operations_readiness.readiness.slow_query_ready",
                "operations_readiness.readiness.background_maintenance_ready",
                "operations_readiness.redaction.query_text_copied",
                "operations_readiness.redaction.parameters_copied",
                "operations_readiness.redaction.local_paths_copied"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_operations_readiness_report"));
    }

    #[test]
    fn operations_readiness_blocks_stale_projection_and_recovery_actions() {
        let mut bundle = ready_bundle();
        bundle["operations_readiness"]["ready"] = serde_json::json!(false);
        bundle["operations_readiness"]["search_projection"]["stale"] = serde_json::json!(true);
        bundle["operations_readiness"]["storage_lifecycle"]["ready"] = serde_json::json!(false);
        bundle["operations_readiness"]["storage_lifecycle"]["action"] =
            serde_json::json!("run_checkpoint");
        bundle["operations_readiness"]["readiness"]["storage_recovery_ready"] =
            serde_json::json!(false);
        bundle["operations_readiness"]["blocker_codes"] =
            serde_json::json!(["storage_recovery_not_ready", "search_projection_stale"]);

        let typed = super::operations_readiness_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.protocol_matches);
        assert!(typed.present);
        assert!(!typed.ready);
        assert!(!typed.search_projection_not_stale);
        assert!(!typed.storage_lifecycle_ready);
        assert!(!typed.storage_lifecycle_action_ready);
        assert!(!typed.storage_recovery_ready);
        assert_eq!(
            typed.blocker_codes,
            vec![
                "search_projection_stale".to_string(),
                "storage_recovery_not_ready".to_string()
            ]
        );

        let preflight = super::nowledge_mem_final_cutover_preflight(&bundle);
        assert!(!preflight.production_cutover_ready);
        assert!(!preflight.storage_recovery_ready);
        assert!(preflight
            .failed_checks
            .contains(&"operations_readiness".to_string()));
        assert!(preflight
            .blocking_categories
            .contains(&"storage_recovery".to_string()));
    }

    #[test]
    fn rejects_library_readiness_that_copies_sensitive_fields() {
        let mut bundle = ready_bundle();
        bundle["library_readiness"]["ready"] = serde_json::json!(false);
        bundle["library_readiness"]["redaction"]["ready"] = serde_json::json!(false);
        bundle["library_readiness"]["redaction"]["query_text_copied"] = serde_json::json!(true);
        bundle["library_readiness"]["redaction"]["parameters_copied"] = serde_json::json!(true);
        bundle["library_readiness"]["redaction"]["local_paths_copied"] = serde_json::json!(true);
        bundle["library_readiness"]["blocker_codes"] =
            serde_json::json!(["library_readiness_redaction_not_ready"]);

        let typed = super::library_readiness_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(!typed.redaction_ready);
        assert!(!typed.query_text_redacted);
        assert!(!typed.parameters_redacted);
        assert!(!typed.local_paths_redacted);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["library_readiness_redaction_not_ready"])
        );
        let library_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "library_readiness")
            .unwrap();
        assert_eq!(
            library_check["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.ready",
                "library_readiness.redaction.ready",
                "library_readiness.redaction.query_text_copied",
                "library_readiness.redaction.parameters_copied",
                "library_readiness.redaction.local_paths_copied"
            ])
        );
    }

    #[test]
    fn rejects_blocked_library_graph_route_readiness_area() {
        let mut bundle = ready_bundle();
        bundle["library_readiness"]["ready"] = serde_json::json!(false);
        bundle["library_readiness"]["ready_area_count"] = serde_json::json!(8);
        bundle["library_readiness"]["blocked_area_count"] = serde_json::json!(1);
        bundle["library_readiness"]["blocker_codes"] =
            serde_json::json!(["graph_route_readiness_not_ready"]);
        bundle["library_readiness"]["readiness_by_area"]["graph_route"]["ready"] =
            serde_json::json!(false);
        bundle["library_readiness"]["readiness_by_area"]["graph_route"]["blocker_codes"] =
            serde_json::json!(["graph_route_readiness_missing"]);

        let typed = super::library_readiness_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.protocol_matches);
        assert!(typed.present);
        assert!(!typed.ready);
        assert!(!typed.blocked_area_count_zero);
        assert!(!typed.graph_route_ready);
        assert_eq!(
            typed.blocker_codes,
            vec![
                "graph_route_readiness_missing".to_string(),
                "graph_route_readiness_not_ready".to_string()
            ]
        );

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!([
                "graph_route_readiness_missing",
                "graph_route_readiness_not_ready"
            ])
        );
        let library_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "library_readiness")
            .unwrap();
        assert_eq!(
            library_check["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.ready",
                "library_readiness.blocked_area_count",
                "library_readiness.readiness_by_area.graph_route.ready"
            ])
        );
    }

    #[test]
    fn rejects_blocked_library_search_route_ownership_area() {
        let mut bundle = ready_bundle();
        bundle["library_readiness"]["ready"] = serde_json::json!(false);
        bundle["library_readiness"]["ready_area_count"] = serde_json::json!(10);
        bundle["library_readiness"]["blocked_area_count"] = serde_json::json!(1);
        bundle["library_readiness"]["blocker_codes"] =
            serde_json::json!(["search_route_ownership_not_ready"]);
        bundle["library_readiness"]["search_route_ownership"]["ready"] = serde_json::json!(false);
        bundle["library_readiness"]["search_route_ownership"]["production_cutover_ready"] =
            serde_json::json!(false);
        bundle["library_readiness"]["search_route_ownership"]["lancedb_route_count"] =
            serde_json::json!(1);
        bundle["library_readiness"]["search_route_ownership"]["lancedb_routes"] =
            serde_json::json!(["memory"]);
        bundle["library_readiness"]["search_route_ownership"]["blocker_codes"] =
            serde_json::json!(["search_routes_still_lancedb"]);
        bundle["library_readiness"]["readiness_by_area"]["search_route_ownership"]["ready"] =
            serde_json::json!(false);
        bundle["library_readiness"]["readiness_by_area"]["search_route_ownership"]
            ["blocker_codes"] = serde_json::json!(["search_routes_still_lancedb"]);

        let typed = super::library_readiness_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(!typed.ready);
        assert!(!typed.blocked_area_count_zero);
        assert!(!typed.search_route_ownership_ready);
        assert_eq!(
            typed.blocker_codes,
            vec![
                "search_route_ownership_not_ready".to_string(),
                "search_routes_still_lancedb".to_string()
            ]
        );

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!([
                "search_route_ownership_not_ready",
                "search_routes_still_lancedb"
            ])
        );
        let library_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "library_readiness")
            .unwrap();
        assert_eq!(
            library_check["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.ready",
                "library_readiness.blocked_area_count",
                "library_readiness.readiness_by_area.search_route_ownership.ready"
            ])
        );
    }

    #[test]
    fn rejects_blocked_library_workload_fixture_readiness_area() {
        let mut bundle = ready_bundle();
        bundle["library_readiness"]["ready"] = serde_json::json!(false);
        bundle["library_readiness"]["ready_area_count"] = serde_json::json!(10);
        bundle["library_readiness"]["blocked_area_count"] = serde_json::json!(1);
        bundle["library_readiness"]["blocker_codes"] =
            serde_json::json!(["workload_fixture_evidence_not_ready"]);
        bundle["library_readiness"]["readiness_by_area"]["workload_fixture"]["ready"] =
            serde_json::json!(false);
        bundle["library_readiness"]["readiness_by_area"]["workload_fixture"]["blocker_codes"] =
            serde_json::json!(["workload_fixture_search_metadata_not_ready"]);

        let typed = super::library_readiness_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.protocol_matches);
        assert!(typed.present);
        assert!(!typed.ready);
        assert!(!typed.blocked_area_count_zero);
        assert!(!typed.workload_fixture_ready);
        assert_eq!(
            typed.blocker_codes,
            vec![
                "workload_fixture_evidence_not_ready".to_string(),
                "workload_fixture_search_metadata_not_ready".to_string()
            ]
        );

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["library_readiness"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!([
                "workload_fixture_evidence_not_ready",
                "workload_fixture_search_metadata_not_ready"
            ])
        );
        let library_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "library_readiness")
            .unwrap();
        assert_eq!(
            library_check["failed_evidence_fields"],
            serde_json::json!([
                "library_readiness.ready",
                "library_readiness.blocked_area_count",
                "library_readiness.readiness_by_area.workload_fixture.ready"
            ])
        );
    }

    #[test]
    fn requires_background_maintenance_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]["background_maintenance_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_protocol_matches"] = serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_admitted_search_projection_graph_delta_count"] =
            serde_json::Value::Null;
        bundle["replacement_summary"]["cutover_evidence"]["background_maintenance_blocker_codes"] =
            serde_json::json!(["background_disabled"]);

        let typed = super::background_maintenance_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.required);
        assert!(!typed.ready);
        assert!(!typed.protocol_matches);
        assert!(typed.executable_search_projection_graph_delta_count_present);
        assert!(!typed.admitted_search_projection_graph_delta_count_present);
        assert_eq!(typed.blocker_codes, vec!["background_disabled".to_string()]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["background_maintenance_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["background_disabled"])
        );
        let maintenance_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "background_maintenance_evidence")
            .unwrap();
        assert_eq!(
            maintenance_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.background_maintenance_ready",
                "replacement_summary.cutover_evidence.background_maintenance_protocol_matches",
                "replacement_summary.cutover_evidence.background_maintenance_admitted_search_projection_graph_delta_count"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_background_maintenance_report"));
    }

    #[test]
    fn rejects_incomplete_background_maintenance_graph_delta_summary() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_deferred_search_projection_graph_delta_count"] =
            serde_json::Value::Null;
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_rejected_search_projection_graph_delta_count"] =
            serde_json::Value::Null;
        bundle["replacement_summary"]["cutover_evidence"]["background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"] =
            serde_json::Value::Null;

        let typed = super::background_maintenance_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.required);
        assert!(typed.ready);
        assert!(typed.protocol_matches);
        assert!(!typed.deferred_search_projection_graph_delta_count_present);
        assert!(!typed.rejected_search_projection_graph_delta_count_present);
        assert!(
            !typed.max_search_projection_graph_delta_complete_through_graph_commit_epoch_present
        );
        assert!(typed.memory_pressure_ready);
        assert!(typed.memory_budget_bytes_present);
        assert!(typed.estimated_memory_bytes_present);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["background_maintenance_evidence"])
        );
        let maintenance_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "background_maintenance_evidence")
            .unwrap();
        assert_eq!(
            maintenance_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.background_maintenance_deferred_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_rejected_search_projection_graph_delta_count",
                "replacement_summary.cutover_evidence.background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_background_maintenance_report"));
    }

    #[test]
    fn rejects_missing_background_maintenance_memory_pressure_summary() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_memory_pressure_ready"] = serde_json::Value::Null;
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_memory_budget_bytes"] = serde_json::Value::Null;
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_estimated_memory_bytes"] = serde_json::Value::Null;

        let typed = super::background_maintenance_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.required);
        assert!(typed.ready);
        assert!(typed.protocol_matches);
        assert!(!typed.memory_pressure_ready);
        assert!(!typed.memory_budget_bytes_present);
        assert!(!typed.estimated_memory_bytes_present);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["background_maintenance_evidence"])
        );
        let maintenance_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "background_maintenance_evidence")
            .unwrap();
        assert_eq!(
            maintenance_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.background_maintenance_memory_pressure_ready",
                "replacement_summary.cutover_evidence.background_maintenance_memory_budget_bytes",
                "replacement_summary.cutover_evidence.background_maintenance_estimated_memory_bytes"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_background_maintenance_report"));
    }

    #[test]
    fn requires_storage_recovery_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_wal_replay_bounded"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_blocker_codes"] =
            serde_json::json!(["wal_replay_unbounded"]);

        let typed = super::storage_recovery_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.required);
        assert!(!typed.ready);
        assert!(!typed.wal_replay_bounded);
        assert_eq!(
            typed.blocker_codes,
            vec!["wal_replay_unbounded".to_string()]
        );

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["storage_recovery_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["wal_replay_unbounded"])
        );
        let storage_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "storage_recovery_evidence")
            .unwrap();
        assert_eq!(
            storage_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.storage_recovery_ready",
                "replacement_summary.cutover_evidence.storage_recovery_wal_replay_bounded"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_storage_recovery_report"));
    }

    #[test]
    fn rejects_inconsistent_storage_recovery_summary_even_if_ready_flag_is_true() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_durable"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]
            ["storage_recovery_checkpoint_boundary_present"] = serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]
            ["storage_recovery_replay_boundary_consistent"] = serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_torn_tail_clean"] =
            serde_json::json!(false);

        let typed = super::storage_recovery_cutover_readiness(&bundle);
        assert!(!typed.evidence_ready());
        assert!(typed.ready);
        assert!(!typed.durable);
        assert!(!typed.checkpoint_boundary_present);
        assert!(!typed.replay_boundary_consistent);
        assert!(!typed.torn_tail_clean);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["storage_recovery_evidence"])
        );
        let storage_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "storage_recovery_evidence")
            .unwrap();
        assert_eq!(
            storage_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.cutover_evidence.storage_recovery_durable",
                "replacement_summary.cutover_evidence.storage_recovery_checkpoint_boundary_present",
                "replacement_summary.cutover_evidence.storage_recovery_replay_boundary_consistent",
                "replacement_summary.cutover_evidence.storage_recovery_torn_tail_clean"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_storage_recovery_report"));
    }

    fn ready_bundle() -> serde_json::Value {
        let mut bundle = serde_json::json!({
            "protocol": "nowledge-mem-skein-integration-bundle",
            "submodule": {
                "present": true,
                "path": "vendor/skein",
                "commit": "46f8bfb",
                "blocker_codes": []
            },
            "coexistence": {
                "old_database_retained": true,
                "old_database_deleted": false,
                "mode": "shadow",
                "blocker_codes": []
            },
            "content_store": {
                "present": true,
                "engine": "sqlite",
                "messages_available": true,
                "source_chunks_available": true,
                "blocker_codes": []
            },
            "previous_wrapper_preflight": {
                "ready": true,
                "blocker_codes": [],
                "failed_checks": []
            },
            "bounded_read_evidence": {
                "protocol": "skein-nowledge-mem-bounded-read-evidence-v2",
                "ready": true,
                "mode": "shadow_read_only",
                "max_rows": 512,
                "execution_row_cap": 513,
                "estimated_payload_bytes": 128,
                "max_estimated_payload_bytes": 4194304,
                "payload_budget_exceeded": false,
                "row_limit_enforced_before_output": true,
                "operator_row_cap_enabled": true,
                "blocking_operator_count": 0,
                "blocking_operator_memory_reports_complete": true,
                "blocking_operator_memory_within_budget": true,
                "spill_within_budget": true,
                "streaming": false,
                "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
                "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
                "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
                "blocker_codes": []
            },
            "replacement_summary_bounded_read_alignment": {
                "ready": true,
                "evidence_present": true,
                "summary_present": true,
                "evidence_ready": true,
                "summary_ready": true,
                "protocol_matches": true,
                "readiness_matches": true,
                "mode_matches": true,
                "max_rows_matches": true,
                "estimated_payload_bytes_matches": true,
                "max_estimated_payload_bytes_matches": true,
                "payload_budget_exceeded_matches": true,
                "streaming_matches": true,
                "covered_routes_matches": true,
                "evidence_route_catalog_version_ready": true,
                "summary_route_catalog_version_ready": true,
                "evidence_route_catalog_digest_ready": true,
                "summary_route_catalog_digest_ready": true,
                "route_catalog_version_matches": true,
                "route_catalog_digest_matches": true,
                "blocker_codes": []
            },
            "replacement_summary": {
                "protocol": "skein-nowledge-replacement-summary",
                "production_cutover_ready": true,
                "blocking_categories": [],
                "missing_evidence": [],
                "shadow_evidence": {
                    "ready": true
                },
                "dual_engine_evidence": {
                    "present": true,
                    "ready": true,
                    "consistent": true
                },
                "replacement_readiness_family_summary": {
                    "total_count": 5,
                    "ready_count": 5,
                    "blocked_count": 0,
                    "omitted_count": 0,
                    "min_replacement_readiness_per_million": 1_000_000,
                    "blocked_query_families": [],
                    "required_query_families": [
                        "memory_lookup",
                        "graph_traversal",
                        "projected_graph",
                        "label_stats_read",
                        "search_projection"
                    ],
                    "missing_required_query_families": []
                },
                "search_projection_evidence": {
                    "protocol": "skein-nowledge-search-projection-evidence",
                    "ready": true,
                    "fts_ready": true,
                    "vector_ready": true,
                    "document_identity_ready": true,
                    "incremental_update_ready": true,
                    "predicate_pushdown_ready": true,
                    "production_filter_pruning_ready": true,
                    "compressed_vector_projection_required": true,
                    "compressed_vector_projection_ready": true,
                    "blocker_codes": []
                },
                "search_projection_shadow_evidence": {
                    "protocol": "skein-nowledge-search-projection-shadow-evidence",
                    "evidence_source": SKEIN_SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
                    "present": true,
                    "ready": true,
                    "document_count_parity": true,
                    "document_identity_parity": true,
                    "table_parity_ready": true,
                    "embedding_identity_parity": true,
                    "incremental_watermark_parity": true,
                    "pushdown_evidence": {
                        "ready": true,
                        "shadow_segment_descriptor_scan_filter_fields_ready": true,
                        "shadow_segment_document_pruning_ready": true,
                        "shadow_segment_pruning_candidate_document_count": 4,
                        "shadow_segment_pruned_document_count": 2,
                        "shadow_segment_scanned_document_count": 2,
                        "primary_scan_filter_fields": scan_filter_fields_json(),
                        "shadow_scan_filter_fields": scan_filter_fields_json(),
                        "shadow_segment_descriptor_field_summaries": scan_filter_field_summaries_json()
                    },
                    "blocker_codes": []
                },
                "bounded_read_evidence": {
                    "protocol": "skein-nowledge-mem-bounded-read-evidence-v2",
                    "present": true,
                    "ready": true,
                    "mode": "shadow_read_only",
                    "max_rows": 512,
                    "execution_row_cap": 513,
                    "estimated_payload_bytes": 128,
                    "max_estimated_payload_bytes": 4194304,
                    "payload_budget_exceeded": false,
                    "row_limit_enforced_before_output": true,
                    "operator_row_cap_enabled": true,
                    "blocking_operator_count": 0,
                    "blocking_operator_memory_reports_complete": true,
                    "blocking_operator_memory_within_budget": true,
                    "spill_within_budget": true,
                    "streaming": false,
                    "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
                    "missing_covered_routes": [],
                    "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
                    "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
                    "blocker_codes": []
                },
                "cutover_evidence": {
                    "storage_recovery_required": true,
                    "storage_recovery_ready": true,
                    "storage_recovery_protocol_matches": true,
                    "storage_recovery_durable": true,
                    "storage_recovery_checkpoint_boundary_present": true,
                    "storage_recovery_wal_replay_bounded": true,
                    "storage_recovery_torn_tail_clean": true,
                    "storage_recovery_blocker_codes": [],
                    "storage_recovery_blockers": [],
                    "background_maintenance_required": true,
                    "background_maintenance_ready": true,
                    "background_maintenance_protocol_matches": true,
                    "background_maintenance_executable_search_projection_graph_delta_count": 1,
                    "background_maintenance_admitted_search_projection_graph_delta_count": 1,
                    "background_maintenance_deferred_search_projection_graph_delta_count": 0,
                    "background_maintenance_rejected_search_projection_graph_delta_count": 0,
                    "background_maintenance_executable_search_projection_graph_delta_operations": 2,
                    "background_maintenance_admitted_search_projection_graph_delta_operations": 2,
                    "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch": 7,
                    "background_maintenance_blocker_codes": [],
                    "background_maintenance_blockers": []
                }
            }
        });
        bundle["replacement_summary"]["source_mutation_dual_write_readiness"] =
            ready_source_mutation_dual_write_readiness();
        bundle["replacement_summary"]["cutover_evidence"]
            ["storage_recovery_replay_boundary_consistent"] = serde_json::json!(true);
        bundle["replacement_summary"]["replacement_boundaries"] = serde_json::json!({
            "graph_layer": {
                "scope": "kuzu_ladybug_graph_layer",
                "replacement_role": "primary_replacement",
                "storage_owner": "skein"
            },
            "search_projection": {
                "scope": "lancedb_search_projection",
                "replacement_role": "rebuildable_projection",
                "storage_owner": "skein"
            },
            "content_store": {
                "scope": "sqlite_content_store",
                "replacement_role": "external_out_of_scope",
                "storage_owner": "nowledge_mem"
            },
            "large_blob_store": {
                "scope": "large_blob_value_store",
                "replacement_role": "external_out_of_scope",
                "storage_owner": "nowledge_mem"
            }
        });
        bundle["replacement_summary"]["cutover_evidence"]
            ["storage_recovery_replay_boundary_consistent"] = serde_json::json!(true);
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_foreground_admission_probe_ready"] = serde_json::json!(true);
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_foreground_admission_probe_admission"] =
            serde_json::json!("admit");
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_memory_pressure_ready"] = serde_json::json!(true);
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_memory_budget_bytes"] = serde_json::json!(4096);
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_estimated_memory_bytes"] = serde_json::json!(1024);
        bundle["replacement_summary"]["query_runtime_preflight"] = ready_query_runtime_summary();
        bundle["replacement_summary_graph_route_alignment"] = serde_json::json!({
            "ready": true,
            "evidence_present": true,
            "summary_present": true,
            "protocol_matches": true,
            "evidence_protocol_matches": true,
            "evidence_ready": true,
            "evidence_route_primary_ready": true,
            "summary_route_primary_ready": true,
            "route_primary_ready_matches": true,
            "evidence_route_query_plan_evidence_ready": true,
            "summary_route_query_plan_evidence_ready": true,
            "route_query_plan_evidence_ready_matches": true,
            "evidence_route_query_profile_evidence_ready": true,
            "summary_route_query_profile_evidence_ready": true,
            "route_query_profile_evidence_ready_matches": true,
            "evidence_route_query_api_behavior_evidence_ready": true,
            "summary_route_query_api_behavior_evidence_ready": true,
            "route_query_api_behavior_evidence_ready_matches": true,
            "evidence_route_relationship_property_pruning_evidence_ready": true,
            "summary_route_relationship_property_pruning_evidence_ready": true,
            "route_relationship_property_pruning_evidence_ready_matches": true,
            "evidence_relationship_property_pruning_required_count": 0,
            "summary_relationship_property_pruning_required_count": 0,
            "relationship_property_pruning_required_count_matches": true,
            "evidence_relationship_property_pruning_report_count": 0,
            "summary_relationship_property_pruning_report_count": 0,
            "relationship_property_pruning_report_count_matches": true,
            "evidence_route_catalog_metadata_ready": true,
            "summary_route_catalog_metadata_ready": true,
            "route_catalog_metadata_ready_matches": true,
            "evidence_route_catalog_version_ready": true,
            "summary_route_catalog_version_ready": true,
            "evidence_route_catalog_digest_ready": true,
            "summary_route_catalog_digest_ready": true,
            "route_catalog_version_matches": true,
            "route_catalog_digest_matches": true,
            "primary_ready_routes_match": true,
            "evidence_required_routes_covered": true,
            "summary_required_routes_covered": true,
            "blocker_codes": []
        });
        bundle["replacement_summary_query_runtime_alignment"] = serde_json::json!({
            "ready": true,
            "evidence_present": true,
            "summary_present": true,
            "evidence_ready": true,
            "summary_ready": true,
            "protocol_matches": true,
            "readiness_matches": true,
            "database_opened_matches": true,
            "probe_count_matches": true,
            "passed_probe_count_matches": true,
            "failed_probe_count_matches": true,
            "required_route_count_matches": true,
            "covered_route_count_matches": true,
            "covered_routes_matches": true,
            "required_routes_covered_matches": true,
            "route_coverage_ready_matches": true,
            "evidence_route_catalog_version_ready": true,
            "summary_route_catalog_version_ready": true,
            "evidence_route_catalog_digest_ready": true,
            "summary_route_catalog_digest_ready": true,
            "route_catalog_version_matches": true,
            "route_catalog_digest_matches": true,
            "evidence_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "summary_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "blocker_codes": []
        });
        bundle["graph_route_readiness"] = serde_json::json!({
            "protocol": "nmem-graph-route-readiness-v1",
            "evidence_protocol": "nmem-graph-route-evidence-v1",
            "evidence_ready": true,
            "route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": [],
            "required_routes_covered": true,
            "unknown_routes": [],
            "duplicate_routes": [],
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
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
            "query_runtime_api_behavior_report_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
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
            "routes": ready_graph_route_profile_routes()
        });
        bundle["route_ownership"] = ready_route_ownership();
        bundle["search_route_ownership"] = ready_search_route_ownership();
        bundle["active_search_route_ownership"] = ready_active_search_route_ownership();
        bundle["active_search_route_readiness"] = ready_active_search_route_readiness();
        bundle["replacement_summary"]["search_route_ownership"] = ready_search_route_ownership();
        bundle["replacement_summary"]["active_search_route_ownership"] =
            ready_active_search_route_ownership();
        bundle["replacement_summary"]["active_search_route_readiness"] =
            ready_active_search_route_readiness();
        bundle["replacement_summary_search_route_ownership_alignment"] =
            ready_search_route_ownership_alignment();
        bundle["replacement_summary_active_search_route_ownership_alignment"] =
            ready_search_route_ownership_alignment();
        bundle["replacement_summary_active_search_route_readiness_alignment"] =
            ready_active_search_route_readiness_alignment();
        bundle["graph_route_parity_alignment"] = serde_json::json!({
            "ready": true,
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "ready_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "ready_routes": [
                "augmentation_state",
                "pagerank_plan",
                "overview",
                "graph_search",
                "explore",
                "expand",
                "live_preview",
                "live_preview_node",
                "node_details",
                "source_detail",
                "orphans",
                "shortest_path",
                "community_members",
                "community_subgraph",
                "community_recent_memories",
                "related_communities",
                "graph_analysis",
                "agent_evolves"
            ],
            "missing_routes": [],
            "not_ready_routes": [],
            "route_mismatch_routes": [],
            "protocol_mismatch_routes": [],
            "blocker_routes": [],
            "observed_blocker_codes": [],
            "blocker_codes": []
        });
        bundle["query_runtime_preflight"] = serde_json::json!({
            "protocol": "skein-nowledge-query-runtime-preflight-v1",
            "ready": true,
            "database_opened": true,
            "redaction": {
                "ready": true,
                "rows_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
                "raw_errors_copied": false
            },
            "probe_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "passed_probe_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "failed_probe_count": 0,
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": [],
            "required_routes_covered": true,
            "unknown_routes": [],
            "duplicate_routes": [],
            "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
            "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
            "route_coverage_ready": true,
            "route_coverage_blocker_codes": [],
            "blocker_codes": [],
            "probes": ready_query_runtime_preflight_probes()
        });
        let mut search_candidate_evidence = NowledgeMemSearchCandidateShadowAccumulator::new();
        search_candidate_evidence.record_compare_candidate_ids(
            &["mem_1", "mem_2", "mem_3"],
            &["mem_1", "mem_2", "mem_3"],
        );
        search_candidate_evidence.record_filter_pushdown_fields(
            1,
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
                .iter()
                .copied(),
        );
        bundle["search_candidate_shadow_evidence"] =
            nowledge_mem_search_candidate_shadow_evidence_json(
                &search_candidate_evidence.evidence(),
            );
        bundle["search_candidate_shadow_evidence"]["text_retriever_ready"] =
            serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["vector_retriever_ready"] =
            serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["retriever_leg_candidate_counts"] = serde_json::json!({
            "text": 3,
            "vector": 3,
        });
        bundle["search_candidate_shadow_evidence"]["fts_top_k_overlap_ready"] =
            serde_json::json!(true);
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
        bundle["library_readiness"] = ready_library_readiness();
        bundle["cutover_controls"] = ready_cutover_controls();
        bundle["operations_readiness"] = ready_operations_readiness();
        bundle["blackbox_manifest"] = ready_blackbox_manifest();
        bundle
    }

    fn scan_filter_fields_json() -> serde_json::Value {
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS)
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
        serde_json::json!(NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .copied()
            .chain(std::iter::once("document_id"))
            .map(|field| {
                serde_json::json!({
                    "field": field,
                    "segment_count": 1,
                    "present_document_count": 1,
                    "value_summary_used": true,
                    "value_summary_segment_count": 1,
                    "numeric_range_summary_used": matches!(field, "importance" | "confidence"),
                    "numeric_range_segment_count": usize::from(matches!(
                        field,
                        "importance" | "confidence"
                    )),
                    "timestamp_range_summary_used": matches!(
                        field,
                        "created_at" | "updated_at" | "event_start" | "event_end"
                    ),
                    "timestamp_range_segment_count": usize::from(matches!(
                        field,
                        "created_at" | "updated_at" | "event_start" | "event_end"
                    )),
                    "unique_key_summary_used": field == "document_id",
                    "unique_key_summary_segment_count": usize::from(field == "document_id"),
                })
            })
            .collect::<Vec<_>>())
    }

    fn ready_route_ownership() -> serde_json::Value {
        nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_skein(),
            Some(&ready_route_readiness_summary()),
            NowledgeMemRouteOwnershipPolicy::production_cutover(),
        )
        .json()
    }

    fn ready_search_route_ownership() -> serde_json::Value {
        nowledge_mem_search_route_ownership_readiness(
            &nowledge_mem_search_route_ownership_all_skein(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        )
        .json()
    }

    fn ready_active_search_route_ownership() -> serde_json::Value {
        nowledge_mem_active_search_route_ownership_readiness(
            &nowledge_mem_active_search_route_ownership_all_skein(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        )
        .json()
    }

    fn ready_active_search_route_readiness() -> serde_json::Value {
        nowledge_mem_active_search_route_readiness(
            &nowledge_mem_active_search_route_read_evidence_all_skein_ready(),
            NowledgeMemSearchRouteOwnershipPolicy::production_cutover(),
        )
        .json()
    }

    fn ready_search_route_ownership_alignment() -> serde_json::Value {
        serde_json::json!({
            "ready": true,
            "evidence_present": true,
            "summary_present": true,
            "protocol_matches": true,
            "ready_matches": true,
            "production_cutover_ready_matches": true,
            "require_all_skein_matches": true,
            "required_route_count_matches": true,
            "explicit_route_count_matches": true,
            "skein_route_count_matches": true,
            "lancedb_route_count_matches": true,
            "missing_required_routes_matches": true,
            "lancedb_routes_matches": true,
            "blocker_codes_match": true,
            "evidence_lancedb_routes": [],
            "summary_lancedb_routes": [],
            "blocker_codes": []
        })
    }

    fn ready_active_search_route_readiness_alignment() -> serde_json::Value {
        serde_json::json!({
            "ready": true,
            "evidence_present": true,
            "summary_present": true,
            "protocol_matches": true,
            "ready_matches": true,
            "production_cutover_ready_matches": true,
            "require_all_skein_matches": true,
            "required_route_count_matches": true,
            "evidence_route_count_matches": true,
            "ready_route_count_matches": true,
            "skein_route_count_matches": true,
            "lancedb_handle_count_matches": true,
            "missing_required_routes_matches": true,
            "non_skein_routes_matches": true,
            "lancedb_handle_routes_matches": true,
            "candidate_not_ready_routes_matches": true,
            "candidate_identity_not_ready_routes_matches": true,
            "embedding_identity_not_ready_routes_matches": true,
            "zero_vector_semantics_not_ready_routes_matches": true,
            "cjk_tokenization_not_ready_routes_matches": true,
            "metadata_pushdown_not_ready_routes_matches": true,
            "ranking_window_not_ready_routes_matches": true,
            "ranking_not_ready_routes_matches": true,
            "fail_soft_not_ready_routes_matches": true,
            "fail_soft_reason_codes_not_ready_routes_matches": true,
            "repair_rebuild_markers_not_ready_routes_matches": true,
            "blocker_codes_match": true,
            "evidence_lancedb_handle_required_routes": [],
            "summary_lancedb_handle_required_routes": [],
            "blocker_codes": []
        })
    }

    fn ready_route_readiness_summary() -> NowledgeMemRouteReadinessSummary {
        NowledgeMemRouteReadinessSummary {
            route_primary_ready: true,
            primary_ready_routes: REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
                .iter()
                .map(|route| (*route).to_string())
                .collect(),
            route_query_plan_evidence_ready: true,
            route_query_profile_evidence_ready: true,
            route_query_api_behavior_evidence_ready: true,
            relationship_property_pruning_required_count: 0,
            relationship_property_pruning_report_count: 0,
            route_relationship_property_pruning_evidence_ready: true,
        }
    }

    fn ready_graph_route_profile_routes() -> Vec<serde_json::Value> {
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                let required_query_families =
                    skein_route_ownership::graph::nowledge_mem_required_query_families_for_route(
                        route,
                    );
                let query_family = required_query_families
                    .first()
                    .copied()
                    .unwrap_or("memory_lookup");
                let spec = nowledge_mem_graph_read_route_spec(route).unwrap();
                serde_json::json!({
                    "route": route,
                    "owner": spec.owner.as_str(),
                    "required_evidence_kind": spec.required_evidence_kind.as_str(),
                    "stale_on_catalog_change": spec.stale_on_catalog_change,
                    "shadow_compare_ready": true,
                    "shadow_compare_evidence_source": "route_parity_evidence",
                    "shadow_compare": {
                        "source": "route_parity_evidence",
                        "ready": true,
                        "matched_per_million": 1000000,
                        "primary_engine": "kuzu",
                        "shadow_engine": "skein",
                        "blocker_codes": [],
                        "computed_blocker_codes": []
                    },
                    "primary_ready": true,
                    "required_query_families": required_query_families,
                    "computed_required_query_families": required_query_families,
                    "query_family_blocker_codes": [],
                    "query_runtime_ready": true,
                    "query_report_count": 1,
                    "query_runtime_report_count": 1,
                    "query_runtime_plan_report_count": 1,
                    "query_runtime_profile_report_count": 1,
                    "query_runtime_failed_query_count": 0,
                    "query_runtime_missing_plan_evidence_count": 0,
                    "query_runtime_missing_profile_evidence_count": 0,
                    "relationship_property_pruning_required_count": 0,
                    "relationship_property_pruning_report_count": 0,
                    "relationship_property_pruning_evidence_ready": true,
                    "query_plan_evidence_ready": true,
                    "query_profile_evidence_ready": true,
                    "query_reports": [ready_graph_route_query_report(query_family)],
                    "blocker_codes": []
                })
            })
            .collect()
    }

    fn ready_query_runtime_preflight_probes() -> Vec<serde_json::Value> {
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                serde_json::json!({
                    "name": format!("probe:{route}"),
                    "route": route,
                    "query_family": "memory_lookup",
                    "ready": true,
                    "success": true,
                    "output_row_count": 1,
                    "selected_plan_fingerprint": "IndexNodeSeek(1:m:6:Memory)",
                    "selected_plan_operator_counts": {
                        "IndexNodeSeek": 1,
                        "ProjectExec": 1
                    },
                    "selected_plan_class_counts": {
                        "access": 1,
                        "relational": 1
                    },
                    "optimizer_decision_count": 2,
                    "optimizer_rule_event_count": 1,
                    "plan_cache_lookup": "miss",
                    "plan_cache": {
                        "lookup": "miss",
                        "bypass_reason": null,
                        "cacheable": true,
                        "hit": false,
                        "miss": true,
                        "bypassed": false
                    },
                    "execution_profile": {
                        "scan_pruning_report_count": 1,
                        "pruned_scan_count": 1,
                        "scan_pruning_reports": [
                            {
                                "target_kind": "node",
                                "label_id": 1,
                                "rel_type_id": null,
                                "strategy": {
                                    "kind": "property_eq",
                                    "property": "id"
                                },
                                "pruned": true,
                                "exact_empty": false,
                                "candidate_count_before_pruning": 2,
                                "pruned_candidate_count": 1,
                                "candidate_count_before_filter": 1,
                                "output_count": 1,
                                "filtered_out_count": 0
                            }
                        ]
                    },
                    "blocker_codes": []
                })
            })
            .collect()
    }

    fn ready_query_runtime_summary() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-query-runtime-preflight-v1",
            "present": true,
            "ready": true,
            "database_opened": true,
            "redaction": {
                "ready": true,
                "rows_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false,
                "raw_errors_copied": false
            },
            "probe_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "passed_probe_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "failed_probe_count": 0,
            "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
            "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": [],
            "required_routes_covered": true,
            "unknown_routes": [],
            "duplicate_routes": [],
            "route_coverage_ready": true,
            "route_coverage_blocker_codes": [],
            "probe_details_ready": true,
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

    fn ready_graph_route_query_report(query_family: &str) -> serde_json::Value {
        serde_json::json!({
            "query_name": "overview-memory-lookup",
            "query_index": 0,
            "query_family": query_family,
            "protocol": "skein-nowledge-mem-query-report-v1",
            "statement_kind": "match_return",
            "execution_path": "fast_path",
            "fast_path_selected": true,
            "slow_log_candidate": false,
            "physical_plan_captured": false,
            "elapsed_micros": 12,
            "physical_operator_counts_present": true,
            "optimizer_decision_count": 2,
            "optimizer_rule_event_count": 1,
            "scan_pruning_report_count": 1,
            "scan_pruning_reports_present": true,
            "scan_pruning_reports": [
                {
                    "target_kind": "node",
                    "label_id": 1,
                    "rel_type_id": null,
                    "strategy": {
                        "kind": "property_eq",
                        "property": "id"
                    },
                    "pruned": true,
                    "exact_empty": false,
                    "candidate_count_before_pruning": 2,
                    "pruned_candidate_count": 1,
                    "candidate_count_before_filter": 1,
                    "output_count": 1,
                    "filtered_out_count": 0
                }
            ],
            "plan_cache_lookup": "miss",
            "plan_cache": {
                "lookup": "miss",
                "cacheable": true,
                "hit": false,
                "miss": true,
                "bypassed": false
            },
            "ready": true,
            "blocker_codes": []
        })
    }

    fn ready_library_readiness() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-mem-library-readiness-v1",
            "present": true,
            "ready": true,
            "mode": "shadow_read_only",
            "ready_area_count": 11,
            "blocked_area_count": 0,
            "blocker_codes": [],
            "redaction": {
                "ready": true,
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            },
            "production_path": {
                "ready": true,
                "in_process": true,
                "cli_required": false,
                "env_control_plane_required": false,
                "spawned_helper_required": false
            },
            "open_report": {
                "protocol": "skein-nowledge-mem-open-report",
                "mode": "shadow_read_only",
                "graph_configured": true,
                "search_projection_configured": true,
                "compressed_vector_search_mode": "disabled",
                "graph_opened": true,
                "search_projection_opened": true
            },
            "graph": {
                "open": true,
                "mode": "shadow_read_only",
                "read_only": true
            },
            "readiness_by_area": {
                "graph": {
                    "ready": true,
                    "blocker_codes": []
                },
                "query": {
                    "ready": true,
                    "blocker_codes": []
                },
                "storage": {
                    "ready": true,
                    "blocker_codes": []
                },
                "background": {
                    "ready": true,
                    "blocker_codes": []
                },
                "query_family": {
                    "ready": true,
                    "blocker_codes": []
                },
                "graph_route": {
                    "ready": true,
                    "blocker_codes": []
                },
                "search_route_ownership": {
                    "ready": true,
                    "blocker_codes": []
                },
                "search_projection": {
                    "ready": true,
                    "blocker_codes": []
                },
                "search_projection_shadow": {
                    "ready": true,
                    "blocker_codes": []
                },
                "search_candidate_shadow": {
                    "ready": true,
                    "blocker_codes": []
                },
                "workload_fixture": {
                    "ready": true,
                    "blocker_codes": []
                }
            },
            "bounded_read_evidence": {
                "ready": true,
                "blocker_codes": []
            },
            "search_route_ownership": ready_search_route_ownership(),
            "active_search_route_ownership": ready_active_search_route_ownership(),
            "query_family_evidence": {
                "ready": true,
                "blocker_codes": []
            },
            "storage_recovery": {
                "ready": true,
                "blocker_codes": []
            },
            "background_maintenance": {
                "blocker_codes": []
            },
            "production_resource_profile": ready_production_resource_profile(),
            "search_projection_evidence": {
                "ready": true,
                "blocker_codes": []
            },
            "search_projection_shadow_evidence": {
                "ready": true,
                "blocker_codes": []
            }
        })
    }

    fn ready_production_resource_profile() -> serde_json::Value {
        serde_json::json!({
            "protocol": skein_evidence::resource_profile::STORAGE_RESOURCE_PROFILE_PROTOCOL,
            "protocol_version": 2,
            "present": true,
            "resource_ready": true,
            "ready": true,
            "blocker_codes": [],
            "evidence_binding": {
                "identity": production_identity(),
                "generated_at_unix_seconds": 1
            },
            "expected_identity": production_identity(),
            "canonical_graph_commit_epoch": 42,
            "identity_matches_expected": true,
            "limits": {
                "min_canonical_artifact_bytes": 536870912u64,
                "max_steady_resident_bytes": 536870912u64,
                "max_peak_resident_bytes": 805306368u64,
                "max_total_page_faults": 1010,
                "max_minor_page_faults": 1000,
                "max_major_page_faults": 10,
                "max_intermediate_rows": 1000,
                "max_intermediate_payload_bytes": 1048576,
                "max_output_rows": 100,
                "max_output_payload_bytes": 1048576,
                "require_fully_streamed": true
            },
            "storage": {
                "durable": true,
                "out_of_core": true,
                "canonical_artifact_bytes": 1073741824u64,
                "canonical_exceeds_cache": true,
                "segment_cache_capacity_bytes": 67108864,
                "segment_cache_resident_bytes_after": 33554432,
                "delta_within_budget": true
            },
            "execution": {
                "fully_streamed": true,
                "start_resident_bytes": 251658240,
                "start_peak_resident_bytes": 377487360,
                "steady_resident_bytes": 268435456,
                "peak_resident_bytes": 402653184,
                "steady_resident_growth_bytes": 16777216,
                "lifetime_peak_resident_growth_bytes": 25165824,
                "total_page_faults": 100,
                "minor_page_faults": 100,
                "major_page_faults": 0,
                "metric_capabilities": {
                    "resident_memory": true,
                    "total_page_faults": true,
                    "split_page_faults": true
                },
                "intermediate_rows": 200,
                "intermediate_payload_bytes": 524288,
                "output_rows": 100,
                "output_payload_bytes": 262144
            }
        })
    }

    fn production_identity() -> serde_json::Value {
        serde_json::json!({
            "source_revision": "test-revision",
            "rust_toolchain": "test-toolchain",
            "target_os": "linux",
            "target_arch": "x86_64",
            "enabled_features": ["full-text-search", "vector-search"],
            "durable_format_version": 1,
            "schema_version": 1,
            "configuration_digest": "test-config",
            "deployment_profile": "production-replica",
            "dataset_fingerprint": "test-dataset",
            "canonical_graph_commit_epoch": 42,
            "policy_version": skein_evidence::PRODUCTION_QUALIFICATION_POLICY_VERSION
        })
    }

    fn ready_cutover_controls() -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_CUTOVER_CONTROLS_PROTOCOL,
            "ready": true,
            "controls": {
                "graph_reads": "skein",
                "search_reads": "skein",
                "dual_writes": "enabled",
                "initial_import": "disabled",
                "projection_catch_up": "enabled"
            },
            "graph": {
                "read_selected_skein": true,
                "read_effective": true
            },
            "search": {
                "read_selected_skein": true,
                "read_effective": true
            },
            "work": {
                "dual_writes_enabled": true,
                "initial_import_enabled": false,
                "initial_import_inactive_for_cutover": true,
                "initial_import_cutover_catch_up_ready": false,
                "initial_import_safe_for_read_cutover": true,
                "projection_catch_up_enabled": true
            },
            "blocker_codes": [],
            "production_status": {
                "graph": {
                    "skein_cutover_effective": true
                },
                "search": {
                    "skein_cutover_effective": true
                },
                "blocker_codes": []
            },
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            }
        })
    }

    fn ready_operations_readiness() -> serde_json::Value {
        serde_json::json!({
            "protocol": NOWLEDGE_MEM_OPERATIONS_READINESS_PROTOCOL,
            "present": true,
            "ready": true,
            "graph": {
                "open": true,
                "read_only": false,
                "commit_epoch": 7
            },
            "search_projection": {
                "open": true,
                "commit_lag": 0,
                "stale": false
            },
            "storage_lifecycle": {
                "ready": true,
                "action": "ready"
            },
            "readiness": {
                "storage_lifecycle_ready": true,
                "storage_recovery_ready": true,
                "slow_query_ready": true,
                "background_maintenance_ready": true
            },
            "blocker_codes": [],
            "redaction": {
                "query_text_copied": false,
                "parameters_copied": false,
                "local_paths_copied": false
            }
        })
    }

    fn ready_blackbox_manifest() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-blackbox-report-v1",
            "protocol_version": 1,
            "run_id": "integration-ready",
            "run_status": "completed",
            "exit_code": 0,
            "generated_unix_seconds": 1,
            "artifact_dir_present": true,
            "artifact_count": 3,
            "events_path": "events.jsonl",
            "artifacts": [
                ready_blackbox_slow_query_artifact(),
                ready_blackbox_background_maintenance_artifact(),
                {
                    "name": "replacement-summary.json",
                    "format": "json",
                    "byte_len": 256,
                    "checksum": 1,
                    "json": {
                        "parse_ready": true,
                        "protocol": "skein-nowledge-replacement-summary",
                        "ready": true,
                        "blocker_codes": [],
                        "blocking_categories": [],
                        "failed_checks": [],
                        "missing_evidence": [],
                        "production_cutover_ready": true,
                        "production_replacement_per_million": 1000000
                    }
                }
            ],
            "redaction": {
                "raw_query_text_copied": false,
                "raw_parameters_copied": false,
                "raw_artifact_payloads_copied": false,
                "artifact_paths_are_relative": true
            }
        })
    }

    fn ready_blackbox_slow_query_artifact() -> serde_json::Value {
        serde_json::json!({
            "name": "slow-query-log.jsonl",
            "format": "jsonl",
            "byte_len": 0,
            "checksum": 0,
            "jsonl": {
                "line_count": 0,
                "nonempty_line_count": 0
            }
        })
    }

    fn ready_blackbox_background_maintenance_artifact() -> serde_json::Value {
        serde_json::json!({
            "name": "background-maintenance.json",
            "format": "json",
            "byte_len": 256,
            "checksum": 2,
            "json": {
                "parse_ready": true,
                "protocol": "skein-background-maintenance-report",
                "ready": true,
                "blocker_codes": []
            },
            "background_qos": {
                "protocol": "skein-background-maintenance-report",
                "ready": true,
                "total_candidates": 1,
                "admitted_count": 1,
                "deferred_count": 0,
                "rejected_count": 0,
                "executable_search_projection_graph_delta_count": 1,
                "admitted_search_projection_graph_delta_count": 1,
                "deferred_search_projection_graph_delta_count": 0,
                "rejected_search_projection_graph_delta_count": 0,
                "executable_search_projection_graph_delta_operations": 2,
                "admitted_search_projection_graph_delta_operations": 2,
                "max_search_projection_graph_delta_complete_through_graph_commit_epoch": 7,
                "memory_pressure_ready": true,
                "memory_budget_bytes": 4096,
                "estimated_memory_bytes": 1024,
                "blocker_codes": []
            }
        })
    }

    fn unique_test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein-{name}-{nanos}"))
    }
}
