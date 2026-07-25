use skein::{
    nowledge_contract::{
        SEARCH_CANDIDATE_SHADOW_EVIDENCE_ROUTE, SEARCH_CANDIDATE_SHADOW_EVIDENCE_SOURCE,
        SEARCH_PROJECTION_EVIDENCE_SOURCE, SEARCH_PROJECTION_SHADOW_EVIDENCE_ROUTE,
        SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
        SKEIN_NOWLEDGE_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL,
        SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL,
    },
    replacement_readiness_family_evidence_health_from_bundle,
    REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};
use std::collections::BTreeSet;

const SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-mem-bounded-read-evidence-v1";
const NMEM_GRAPH_ROUTE_SHADOW_PARITY_EVIDENCE_PROTOCOL: &str =
    "nmem-graph-route-shadow-parity-evidence-v1";
const SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY: &str =
    "search_projection_shadow_pushdown_evidence_not_ready";
const SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING: &str =
    "skein_search_projection_segment_descriptor_missing";
const REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES: &[&str] = &[
    "/graph/overview",
    "/graph/search",
    "/graph/explore",
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
    "/sources/{source_id}",
    "/graph/orphans",
    "/graph/shortest-path",
];

pub fn nowledge_replacement_summary_usage() -> String {
    "nowledge-replacement-summary requires [--require-production-ready] [--compact] [--max-family-items <n>] [--max-blockers <n>] [--search-projection-evidence-json <path>] [--search-projection-shadow-evidence-json <path>] [--search-candidate-shadow-evidence-json <path>] [--bounded-read-evidence-json <path>] [--augmentation-state-parity-evidence-json <path>] [--pagerank-plan-parity-evidence-json <path>] [--overview-parity-evidence-json <path>] [--graph-search-parity-evidence-json <path>] [--explore-parity-evidence-json <path>] [--expand-parity-evidence-json <path>] [--live-preview-parity-evidence-json <path>] [--live-preview-node-parity-evidence-json <path>] [--node-details-parity-evidence-json <path>] [--source-detail-parity-evidence-json <path>] [--orphans-parity-evidence-json <path>] [--shortest-path-parity-evidence-json <path>] [--community-members-parity-evidence-json <path>] [--community-subgraph-parity-evidence-json <path>] [--community-recent-memories-parity-evidence-json <path>] [--related-communities-parity-evidence-json <path>] [--graph-analysis-parity-evidence-json <path>] [--query-family-evidence-json <path>] <migration-gate-json>"
        .to_string()
}

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
    let bounded_read_evidence = bounded_read_evidence_summary(bundle);
    let bounded_read_evidence_ready = bounded_read_evidence.ready;
    let augmentation_state_parity_evidence = augmentation_state_parity_evidence_summary(bundle);
    let augmentation_state_parity_evidence_ready = augmentation_state_parity_evidence.ready;
    let pagerank_plan_parity_evidence = pagerank_plan_parity_evidence_summary(bundle);
    let pagerank_plan_parity_evidence_ready = pagerank_plan_parity_evidence.ready;
    let overview_parity_evidence = overview_parity_evidence_summary(bundle);
    let overview_parity_evidence_ready = overview_parity_evidence.ready;
    let graph_search_parity_evidence = graph_search_parity_evidence_summary(bundle);
    let graph_search_parity_evidence_ready = graph_search_parity_evidence.ready;
    let explore_parity_evidence = explore_parity_evidence_summary(bundle);
    let explore_parity_evidence_ready = explore_parity_evidence.ready;
    let expand_parity_evidence = expand_parity_evidence_summary(bundle);
    let expand_parity_evidence_ready = expand_parity_evidence.ready;
    let live_preview_parity_evidence = live_preview_parity_evidence_summary(bundle);
    let live_preview_parity_evidence_ready = live_preview_parity_evidence.ready;
    let live_preview_node_parity_evidence = live_preview_node_parity_evidence_summary(bundle);
    let live_preview_node_parity_evidence_ready = live_preview_node_parity_evidence.ready;
    let node_details_parity_evidence = node_details_parity_evidence_summary(bundle);
    let node_details_parity_evidence_ready = node_details_parity_evidence.ready;
    let source_detail_parity_evidence = source_detail_parity_evidence_summary(bundle);
    let source_detail_parity_evidence_ready = source_detail_parity_evidence.ready;
    let orphans_parity_evidence = orphans_parity_evidence_summary(bundle);
    let orphans_parity_evidence_ready = orphans_parity_evidence.ready;
    let shortest_path_parity_evidence = shortest_path_parity_evidence_summary(bundle);
    let shortest_path_parity_evidence_ready = shortest_path_parity_evidence.ready;
    let community_members_parity_evidence = community_members_parity_evidence_summary(bundle);
    let community_members_parity_evidence_ready = community_members_parity_evidence.ready;
    let community_subgraph_parity_evidence = community_subgraph_parity_evidence_summary(bundle);
    let community_subgraph_parity_evidence_ready = community_subgraph_parity_evidence.ready;
    let community_recent_memories_parity_evidence =
        community_recent_memories_parity_evidence_summary(bundle);
    let community_recent_memories_parity_evidence_ready =
        community_recent_memories_parity_evidence.ready;
    let related_communities_parity_evidence = related_communities_parity_evidence_summary(bundle);
    let related_communities_parity_evidence_ready = related_communities_parity_evidence.ready;
    let graph_analysis_parity_evidence = graph_analysis_parity_evidence_summary(bundle);
    let graph_analysis_parity_evidence_ready = graph_analysis_parity_evidence.ready;
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
        && bounded_read_evidence_ready
        && augmentation_state_parity_evidence_ready
        && pagerank_plan_parity_evidence_ready
        && overview_parity_evidence_ready
        && graph_search_parity_evidence_ready
        && explore_parity_evidence_ready
        && expand_parity_evidence_ready
        && live_preview_parity_evidence_ready
        && live_preview_node_parity_evidence_ready
        && node_details_parity_evidence_ready
        && source_detail_parity_evidence_ready
        && orphans_parity_evidence_ready
        && shortest_path_parity_evidence_ready
        && community_members_parity_evidence_ready
        && community_subgraph_parity_evidence_ready
        && community_recent_memories_parity_evidence_ready
        && related_communities_parity_evidence_ready
        && graph_analysis_parity_evidence_ready
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
            bounded_read_evidence_ready,
            augmentation_state_parity_evidence_ready,
            pagerank_plan_parity_evidence_ready,
            overview_parity_evidence_ready,
            graph_search_parity_evidence_ready,
            explore_parity_evidence_ready,
            expand_parity_evidence_ready,
            live_preview_parity_evidence_ready,
            live_preview_node_parity_evidence_ready,
            node_details_parity_evidence_ready,
            source_detail_parity_evidence_ready,
            orphans_parity_evidence_ready,
            shortest_path_parity_evidence_ready,
            community_members_parity_evidence_ready,
            community_subgraph_parity_evidence_ready,
            community_recent_memories_parity_evidence_ready,
            related_communities_parity_evidence_ready,
            graph_analysis_parity_evidence_ready,
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
            bounded_read_evidence_ready,
            augmentation_state_parity_evidence_ready,
            pagerank_plan_parity_evidence_ready,
            overview_parity_evidence_ready,
            graph_search_parity_evidence_ready,
            explore_parity_evidence_ready,
            expand_parity_evidence_ready,
            live_preview_parity_evidence_ready,
            live_preview_node_parity_evidence_ready,
            node_details_parity_evidence_ready,
            source_detail_parity_evidence_ready,
            orphans_parity_evidence_ready,
            shortest_path_parity_evidence_ready,
            community_members_parity_evidence_ready,
            community_subgraph_parity_evidence_ready,
            community_recent_memories_parity_evidence_ready,
            related_communities_parity_evidence_ready,
            graph_analysis_parity_evidence_ready,
            background_graph_delta_evidence_missing,
            family_evidence_ready,
            production_cutover_ready,
        },
    );

    serde_json::json!({
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
            "evidence_source": search_projection_evidence.evidence_source,
            "present": search_projection_evidence.present,
            "ready": search_projection_evidence.ready,
            "derived_projection": search_projection_evidence.derived_projection,
            "all_tables_covered": search_projection_evidence.all_tables_covered,
            "covered_table_count": search_projection_evidence.covered_table_count,
            "required_table_count": search_projection_evidence.required_table_count,
            "fts_ready": search_projection_evidence.fts_ready,
            "vector_ready": search_projection_evidence.vector_ready,
            "embedding_identity_ready": search_projection_evidence.embedding_identity_ready,
            "fail_soft_ready": search_projection_evidence.fail_soft_ready,
            "rebuild_marker_ready": search_projection_evidence.rebuild_marker_ready,
            "metadata_repair_marker_ready": search_projection_evidence.metadata_repair_marker_ready,
            "incremental_update_ready": search_projection_evidence.incremental_update_ready,
            "source_chunk_ready": search_projection_evidence.source_chunk_ready,
            "predicate_pushdown_ready": search_projection_evidence.predicate_pushdown_ready,
            "compressed_vector_projection_required": search_projection_evidence.compressed_vector_projection_required,
            "compressed_vector_projection_ready": search_projection_evidence.compressed_vector_projection_ready,
            "blocker_codes": search_projection_evidence.blocker_codes,
        },
        "search_projection_shadow_evidence": {
            "protocol": search_projection_shadow_evidence.protocol,
            "evidence_source": search_projection_shadow_evidence.evidence_source,
            "route": search_projection_shadow_evidence.route,
            "present": search_projection_shadow_evidence.present,
            "ready": search_projection_shadow_evidence.ready,
            "primary_ready": search_projection_shadow_evidence.primary_ready,
            "shadow_ready": search_projection_shadow_evidence.shadow_ready,
            "document_count_parity": search_projection_shadow_evidence.document_count_parity,
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
        "search_candidate_shadow_evidence": {
            "protocol": search_candidate_shadow_evidence.protocol,
            "evidence_source": search_candidate_shadow_evidence.evidence_source,
            "route": search_candidate_shadow_evidence.route,
            "present": search_candidate_shadow_evidence.present,
            "ready": search_candidate_shadow_evidence.ready,
            "reported_ready": search_candidate_shadow_evidence.reported_ready,
            "engine": search_candidate_shadow_evidence.engine,
            "candidate_primary_engine": search_candidate_shadow_evidence.candidate_primary_engine,
            "primary_engine": search_candidate_shadow_evidence.primary_engine,
            "shadow_engine": search_candidate_shadow_evidence.shadow_engine,
            "row_count_parity": search_candidate_shadow_evidence.row_count_parity,
            "vector_top_k_overlap_ready": search_candidate_shadow_evidence.vector_top_k_overlap_ready,
            "fts_top_k_overlap_ready": search_candidate_shadow_evidence.fts_top_k_overlap_ready,
            "shadow_scan_present": search_candidate_shadow_evidence.shadow_scan_present,
            "shadow_scan_filter_pushdown_ready": search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready,
            "shadow_scan_field_pruning_ready": search_candidate_shadow_evidence.shadow_scan_field_pruning_ready,
            "shadow_scan_reduction_ready": search_candidate_shadow_evidence.shadow_scan_reduction_ready,
            "shadow_scan_descriptor_bounded_ready": search_candidate_shadow_evidence.shadow_scan_descriptor_bounded_ready,
            "shadow_scan_descriptor_field_count": search_candidate_shadow_evidence.shadow_scan_descriptor_field_count,
            "shadow_scan_field_summary_count": search_candidate_shadow_evidence.shadow_scan_field_summary_count,
            "shadow_scan_input_predicate_count": search_candidate_shadow_evidence.shadow_scan_input_predicate_count,
            "shadow_scan_pushed_predicate_count": search_candidate_shadow_evidence.shadow_scan_pushed_predicate_count,
            "shadow_scan_residual_predicate_count": search_candidate_shadow_evidence.shadow_scan_residual_predicate_count,
            "shadow_scan_filtered_out_count": search_candidate_shadow_evidence.shadow_scan_filtered_out_count,
            "shadow_scan_pruned_document_count": search_candidate_shadow_evidence.shadow_scan_pruned_document_count,
            "shadow_scan_scanned_document_count": search_candidate_shadow_evidence.shadow_scan_scanned_document_count,
            "shadow_scan_parse_error": search_candidate_shadow_evidence.shadow_scan_parse_error,
            "shadow_scan_unsatisfiable": search_candidate_shadow_evidence.shadow_scan_unsatisfiable,
            "blocker_codes": search_candidate_shadow_evidence.blocker_codes,
        },
        "bounded_read_evidence": {
            "protocol": bounded_read_evidence.protocol,
            "present": bounded_read_evidence.present,
            "ready": bounded_read_evidence.ready,
            "mode": bounded_read_evidence.mode,
            "max_rows": bounded_read_evidence.max_rows,
            "execution_row_cap": bounded_read_evidence.execution_row_cap,
            "row_limit_enforced_before_output": bounded_read_evidence.row_limit_enforced_before_output,
            "operator_row_cap_enabled": bounded_read_evidence.operator_row_cap_enabled,
            "streaming": bounded_read_evidence.streaming,
            "blocking_operator_count": bounded_read_evidence.blocking_operator_count,
            "covered_routes": bounded_read_evidence.covered_routes,
            "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_covered_routes": bounded_read_evidence.missing_covered_routes,
            "route_primary_ready": bounded_read_evidence.route_primary_ready,
            "query_runtime_report_count": bounded_read_evidence.query_runtime_report_count,
            "query_runtime_plan_report_count": bounded_read_evidence.query_runtime_plan_report_count,
            "query_runtime_profile_report_count": bounded_read_evidence.query_runtime_profile_report_count,
            "query_runtime_failed_query_count": bounded_read_evidence.query_runtime_failed_query_count,
            "query_runtime_missing_plan_evidence_count": bounded_read_evidence.query_runtime_missing_plan_evidence_count,
            "query_runtime_missing_profile_evidence_count": bounded_read_evidence.query_runtime_missing_profile_evidence_count,
            "primary_ready_routes": bounded_read_evidence.primary_ready_routes,
            "missing_primary_routes": bounded_read_evidence.missing_primary_routes,
            "blocker_codes": bounded_read_evidence.blocker_codes,
        },
        "graph_route_parity_evidence": {
            "augmentation_state": {
                "protocol": augmentation_state_parity_evidence.protocol,
                "present": augmentation_state_parity_evidence.present,
                "ready": augmentation_state_parity_evidence.ready,
                "reported_ready": augmentation_state_parity_evidence.reported_ready,
                "route": augmentation_state_parity_evidence.route,
                "matches": augmentation_state_parity_evidence.matches,
                "primary_engine": augmentation_state_parity_evidence.primary_engine,
                "shadow_engine": augmentation_state_parity_evidence.shadow_engine,
                "primary_ready": augmentation_state_parity_evidence.primary_ready,
                "shadow_ready": augmentation_state_parity_evidence.shadow_ready,
                "blocker_codes": augmentation_state_parity_evidence.blocker_codes,
            },
            "pagerank_plan": {
                "protocol": pagerank_plan_parity_evidence.protocol,
                "present": pagerank_plan_parity_evidence.present,
                "ready": pagerank_plan_parity_evidence.ready,
                "reported_ready": pagerank_plan_parity_evidence.reported_ready,
                "route": pagerank_plan_parity_evidence.route,
                "matches": pagerank_plan_parity_evidence.matches,
                "primary_engine": pagerank_plan_parity_evidence.primary_engine,
                "shadow_engine": pagerank_plan_parity_evidence.shadow_engine,
                "primary_ready": pagerank_plan_parity_evidence.primary_ready,
                "shadow_ready": pagerank_plan_parity_evidence.shadow_ready,
                "blocker_codes": pagerank_plan_parity_evidence.blocker_codes,
            },
            "overview": {
                "protocol": overview_parity_evidence.protocol,
                "present": overview_parity_evidence.present,
                "ready": overview_parity_evidence.ready,
                "reported_ready": overview_parity_evidence.reported_ready,
                "route": overview_parity_evidence.route,
                "matches": overview_parity_evidence.matches,
                "primary_engine": overview_parity_evidence.primary_engine,
                "shadow_engine": overview_parity_evidence.shadow_engine,
                "primary_ready": overview_parity_evidence.primary_ready,
                "shadow_ready": overview_parity_evidence.shadow_ready,
                "blocker_codes": overview_parity_evidence.blocker_codes,
            },
            "graph_search": {
                "protocol": graph_search_parity_evidence.protocol,
                "present": graph_search_parity_evidence.present,
                "ready": graph_search_parity_evidence.ready,
                "reported_ready": graph_search_parity_evidence.reported_ready,
                "route": graph_search_parity_evidence.route,
                "matches": graph_search_parity_evidence.matches,
                "primary_engine": graph_search_parity_evidence.primary_engine,
                "shadow_engine": graph_search_parity_evidence.shadow_engine,
                "primary_ready": graph_search_parity_evidence.primary_ready,
                "shadow_ready": graph_search_parity_evidence.shadow_ready,
                "blocker_codes": graph_search_parity_evidence.blocker_codes,
            },
            "explore": {
                "protocol": explore_parity_evidence.protocol,
                "present": explore_parity_evidence.present,
                "ready": explore_parity_evidence.ready,
                "reported_ready": explore_parity_evidence.reported_ready,
                "route": explore_parity_evidence.route,
                "matches": explore_parity_evidence.matches,
                "primary_engine": explore_parity_evidence.primary_engine,
                "shadow_engine": explore_parity_evidence.shadow_engine,
                "primary_ready": explore_parity_evidence.primary_ready,
                "shadow_ready": explore_parity_evidence.shadow_ready,
                "blocker_codes": explore_parity_evidence.blocker_codes,
            },
            "expand": {
                "protocol": expand_parity_evidence.protocol,
                "present": expand_parity_evidence.present,
                "ready": expand_parity_evidence.ready,
                "reported_ready": expand_parity_evidence.reported_ready,
                "route": expand_parity_evidence.route,
                "matches": expand_parity_evidence.matches,
                "primary_engine": expand_parity_evidence.primary_engine,
                "shadow_engine": expand_parity_evidence.shadow_engine,
                "primary_ready": expand_parity_evidence.primary_ready,
                "shadow_ready": expand_parity_evidence.shadow_ready,
                "blocker_codes": expand_parity_evidence.blocker_codes,
            },
            "live_preview": {
                "protocol": live_preview_parity_evidence.protocol,
                "present": live_preview_parity_evidence.present,
                "ready": live_preview_parity_evidence.ready,
                "reported_ready": live_preview_parity_evidence.reported_ready,
                "route": live_preview_parity_evidence.route,
                "matches": live_preview_parity_evidence.matches,
                "primary_engine": live_preview_parity_evidence.primary_engine,
                "shadow_engine": live_preview_parity_evidence.shadow_engine,
                "primary_ready": live_preview_parity_evidence.primary_ready,
                "shadow_ready": live_preview_parity_evidence.shadow_ready,
                "blocker_codes": live_preview_parity_evidence.blocker_codes,
            },
            "live_preview_node": {
                "protocol": live_preview_node_parity_evidence.protocol,
                "present": live_preview_node_parity_evidence.present,
                "ready": live_preview_node_parity_evidence.ready,
                "reported_ready": live_preview_node_parity_evidence.reported_ready,
                "route": live_preview_node_parity_evidence.route,
                "matches": live_preview_node_parity_evidence.matches,
                "primary_engine": live_preview_node_parity_evidence.primary_engine,
                "shadow_engine": live_preview_node_parity_evidence.shadow_engine,
                "primary_ready": live_preview_node_parity_evidence.primary_ready,
                "shadow_ready": live_preview_node_parity_evidence.shadow_ready,
                "blocker_codes": live_preview_node_parity_evidence.blocker_codes,
            },
            "node_details": {
                "protocol": node_details_parity_evidence.protocol,
                "present": node_details_parity_evidence.present,
                "ready": node_details_parity_evidence.ready,
                "reported_ready": node_details_parity_evidence.reported_ready,
                "route": node_details_parity_evidence.route,
                "matches": node_details_parity_evidence.matches,
                "primary_engine": node_details_parity_evidence.primary_engine,
                "shadow_engine": node_details_parity_evidence.shadow_engine,
                "primary_ready": node_details_parity_evidence.primary_ready,
                "shadow_ready": node_details_parity_evidence.shadow_ready,
                "blocker_codes": node_details_parity_evidence.blocker_codes,
            },
            "source_detail": {
                "protocol": source_detail_parity_evidence.protocol,
                "present": source_detail_parity_evidence.present,
                "ready": source_detail_parity_evidence.ready,
                "reported_ready": source_detail_parity_evidence.reported_ready,
                "route": source_detail_parity_evidence.route,
                "matches": source_detail_parity_evidence.matches,
                "primary_engine": source_detail_parity_evidence.primary_engine,
                "shadow_engine": source_detail_parity_evidence.shadow_engine,
                "primary_ready": source_detail_parity_evidence.primary_ready,
                "shadow_ready": source_detail_parity_evidence.shadow_ready,
                "blocker_codes": source_detail_parity_evidence.blocker_codes,
            },
            "orphans": {
                "protocol": orphans_parity_evidence.protocol,
                "present": orphans_parity_evidence.present,
                "ready": orphans_parity_evidence.ready,
                "reported_ready": orphans_parity_evidence.reported_ready,
                "route": orphans_parity_evidence.route,
                "matches": orphans_parity_evidence.matches,
                "primary_engine": orphans_parity_evidence.primary_engine,
                "shadow_engine": orphans_parity_evidence.shadow_engine,
                "primary_ready": orphans_parity_evidence.primary_ready,
                "shadow_ready": orphans_parity_evidence.shadow_ready,
                "blocker_codes": orphans_parity_evidence.blocker_codes,
            },
            "shortest_path": {
                "protocol": shortest_path_parity_evidence.protocol,
                "present": shortest_path_parity_evidence.present,
                "ready": shortest_path_parity_evidence.ready,
                "reported_ready": shortest_path_parity_evidence.reported_ready,
                "route": shortest_path_parity_evidence.route,
                "matches": shortest_path_parity_evidence.matches,
                "primary_engine": shortest_path_parity_evidence.primary_engine,
                "shadow_engine": shortest_path_parity_evidence.shadow_engine,
                "primary_ready": shortest_path_parity_evidence.primary_ready,
                "shadow_ready": shortest_path_parity_evidence.shadow_ready,
                "blocker_codes": shortest_path_parity_evidence.blocker_codes,
            },
            "community_members": {
                "protocol": community_members_parity_evidence.protocol,
                "present": community_members_parity_evidence.present,
                "ready": community_members_parity_evidence.ready,
                "reported_ready": community_members_parity_evidence.reported_ready,
                "route": community_members_parity_evidence.route,
                "matches": community_members_parity_evidence.matches,
                "primary_engine": community_members_parity_evidence.primary_engine,
                "shadow_engine": community_members_parity_evidence.shadow_engine,
                "primary_ready": community_members_parity_evidence.primary_ready,
                "shadow_ready": community_members_parity_evidence.shadow_ready,
                "blocker_codes": community_members_parity_evidence.blocker_codes,
            },
            "community_subgraph": {
                "protocol": community_subgraph_parity_evidence.protocol,
                "present": community_subgraph_parity_evidence.present,
                "ready": community_subgraph_parity_evidence.ready,
                "reported_ready": community_subgraph_parity_evidence.reported_ready,
                "route": community_subgraph_parity_evidence.route,
                "matches": community_subgraph_parity_evidence.matches,
                "primary_engine": community_subgraph_parity_evidence.primary_engine,
                "shadow_engine": community_subgraph_parity_evidence.shadow_engine,
                "primary_ready": community_subgraph_parity_evidence.primary_ready,
                "shadow_ready": community_subgraph_parity_evidence.shadow_ready,
                "blocker_codes": community_subgraph_parity_evidence.blocker_codes,
            },
            "community_recent_memories": {
                "protocol": community_recent_memories_parity_evidence.protocol,
                "present": community_recent_memories_parity_evidence.present,
                "ready": community_recent_memories_parity_evidence.ready,
                "reported_ready": community_recent_memories_parity_evidence.reported_ready,
                "route": community_recent_memories_parity_evidence.route,
                "matches": community_recent_memories_parity_evidence.matches,
                "primary_engine": community_recent_memories_parity_evidence.primary_engine,
                "shadow_engine": community_recent_memories_parity_evidence.shadow_engine,
                "primary_ready": community_recent_memories_parity_evidence.primary_ready,
                "shadow_ready": community_recent_memories_parity_evidence.shadow_ready,
                "blocker_codes": community_recent_memories_parity_evidence.blocker_codes,
            },
            "related_communities": {
                "protocol": related_communities_parity_evidence.protocol,
                "present": related_communities_parity_evidence.present,
                "ready": related_communities_parity_evidence.ready,
                "reported_ready": related_communities_parity_evidence.reported_ready,
                "route": related_communities_parity_evidence.route,
                "matches": related_communities_parity_evidence.matches,
                "primary_engine": related_communities_parity_evidence.primary_engine,
                "shadow_engine": related_communities_parity_evidence.shadow_engine,
                "primary_ready": related_communities_parity_evidence.primary_ready,
                "shadow_ready": related_communities_parity_evidence.shadow_ready,
                "blocker_codes": related_communities_parity_evidence.blocker_codes,
            },
            "graph_analysis": {
                "protocol": graph_analysis_parity_evidence.protocol,
                "present": graph_analysis_parity_evidence.present,
                "ready": graph_analysis_parity_evidence.ready,
                "reported_ready": graph_analysis_parity_evidence.reported_ready,
                "route": graph_analysis_parity_evidence.route,
                "matches": graph_analysis_parity_evidence.matches,
                "primary_engine": graph_analysis_parity_evidence.primary_engine,
                "shadow_engine": graph_analysis_parity_evidence.shadow_engine,
                "primary_ready": graph_analysis_parity_evidence.primary_ready,
                "shadow_ready": graph_analysis_parity_evidence.shadow_ready,
                "blocker_codes": graph_analysis_parity_evidence.blocker_codes,
            },
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
    bounded_read_evidence_ready: bool,
    augmentation_state_parity_evidence_ready: bool,
    pagerank_plan_parity_evidence_ready: bool,
    overview_parity_evidence_ready: bool,
    graph_search_parity_evidence_ready: bool,
    explore_parity_evidence_ready: bool,
    expand_parity_evidence_ready: bool,
    live_preview_parity_evidence_ready: bool,
    live_preview_node_parity_evidence_ready: bool,
    node_details_parity_evidence_ready: bool,
    source_detail_parity_evidence_ready: bool,
    orphans_parity_evidence_ready: bool,
    shortest_path_parity_evidence_ready: bool,
    community_members_parity_evidence_ready: bool,
    community_subgraph_parity_evidence_ready: bool,
    community_recent_memories_parity_evidence_ready: bool,
    related_communities_parity_evidence_ready: bool,
    graph_analysis_parity_evidence_ready: bool,
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
    bounded_read_evidence_ready: bool,
    augmentation_state_parity_evidence_ready: bool,
    pagerank_plan_parity_evidence_ready: bool,
    overview_parity_evidence_ready: bool,
    graph_search_parity_evidence_ready: bool,
    explore_parity_evidence_ready: bool,
    expand_parity_evidence_ready: bool,
    live_preview_parity_evidence_ready: bool,
    live_preview_node_parity_evidence_ready: bool,
    node_details_parity_evidence_ready: bool,
    source_detail_parity_evidence_ready: bool,
    orphans_parity_evidence_ready: bool,
    shortest_path_parity_evidence_ready: bool,
    community_members_parity_evidence_ready: bool,
    community_subgraph_parity_evidence_ready: bool,
    community_recent_memories_parity_evidence_ready: bool,
    related_communities_parity_evidence_ready: bool,
    graph_analysis_parity_evidence_ready: bool,
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
    evidence_source: Option<String>,
    present: bool,
    ready: bool,
    derived_projection: Option<bool>,
    all_tables_covered: Option<bool>,
    covered_table_count: Option<u64>,
    required_table_count: Option<u64>,
    fts_ready: Option<bool>,
    vector_ready: Option<bool>,
    embedding_identity_ready: Option<bool>,
    fail_soft_ready: Option<bool>,
    rebuild_marker_ready: Option<bool>,
    metadata_repair_marker_ready: Option<bool>,
    incremental_update_ready: Option<bool>,
    source_chunk_ready: Option<bool>,
    predicate_pushdown_ready: Option<bool>,
    compressed_vector_projection_required: Option<bool>,
    compressed_vector_projection_ready: Option<bool>,
    blocker_codes: serde_json::Value,
}

struct SearchProjectionShadowEvidenceSummary<'a> {
    protocol: Option<String>,
    evidence_source: Option<&'a str>,
    route: Option<&'a str>,
    present: bool,
    ready: bool,
    primary_ready: Option<bool>,
    shadow_ready: Option<bool>,
    document_count_parity: Option<bool>,
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

struct SearchCandidateShadowEvidenceSummary<'a> {
    protocol: Option<String>,
    evidence_source: Option<&'a str>,
    route: Option<&'a str>,
    present: bool,
    ready: bool,
    reported_ready: Option<bool>,
    engine: Option<&'a str>,
    candidate_primary_engine: Option<&'a str>,
    primary_engine: Option<&'a str>,
    shadow_engine: Option<&'a str>,
    row_count_parity: Option<bool>,
    vector_top_k_overlap_ready: Option<bool>,
    fts_top_k_overlap_ready: Option<bool>,
    shadow_scan_present: bool,
    shadow_scan_filter_pushdown_ready: bool,
    shadow_scan_field_pruning_ready: bool,
    shadow_scan_reduction_ready: bool,
    shadow_scan_descriptor_bounded_ready: bool,
    shadow_scan_descriptor_field_count: Option<u64>,
    shadow_scan_field_summary_count: Option<u64>,
    shadow_scan_input_predicate_count: Option<u64>,
    shadow_scan_pushed_predicate_count: Option<u64>,
    shadow_scan_residual_predicate_count: Option<u64>,
    shadow_scan_filtered_out_count: Option<u64>,
    shadow_scan_pruned_document_count: Option<u64>,
    shadow_scan_scanned_document_count: Option<u64>,
    shadow_scan_parse_error: Option<&'a str>,
    shadow_scan_unsatisfiable: Option<bool>,
    blocker_codes: serde_json::Value,
}

struct BoundedReadEvidenceSummary<'a> {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    mode: Option<&'a str>,
    max_rows: Option<u64>,
    execution_row_cap: Option<u64>,
    row_limit_enforced_before_output: Option<bool>,
    operator_row_cap_enabled: Option<bool>,
    streaming: Option<bool>,
    blocking_operator_count: Option<u64>,
    covered_routes: Vec<String>,
    missing_covered_routes: Vec<&'static str>,
    route_primary_ready: Option<bool>,
    query_runtime_report_count: Option<u64>,
    query_runtime_plan_report_count: Option<u64>,
    query_runtime_profile_report_count: Option<u64>,
    query_runtime_failed_query_count: Option<u64>,
    query_runtime_missing_plan_evidence_count: Option<u64>,
    query_runtime_missing_profile_evidence_count: Option<u64>,
    primary_ready_routes: Vec<String>,
    missing_primary_routes: Vec<String>,
    blocker_codes: serde_json::Value,
}

struct RouteParityEvidenceSummary<'a> {
    protocol: Option<String>,
    present: bool,
    ready: bool,
    reported_ready: Option<bool>,
    route: Option<&'a str>,
    matches: Option<bool>,
    primary_engine: Option<&'a str>,
    shadow_engine: Option<&'a str>,
    primary_ready: Option<bool>,
    shadow_ready: Option<bool>,
    blocker_codes: serde_json::Value,
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
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let evidence_source =
        json_get_str_path_from_dynamic(bundle, path, "evidence_source").map(str::to_string);
    let covered_table_count = json_get_u64_path_from_dynamic(bundle, path, "covered_table_count");
    let required_table_count = json_get_u64_path_from_dynamic(bundle, path, "required_table_count");
    let derived_projection = json_get_bool_path_from_dynamic(bundle, path, "derived_projection");
    let all_tables_covered = json_get_bool_path_from_dynamic(bundle, path, "all_tables_covered");
    let fts_ready = json_get_bool_path_from_dynamic(bundle, path, "fts_ready");
    let vector_ready = json_get_bool_path_from_dynamic(bundle, path, "vector_ready");
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
    let compressed_vector_projection_required =
        json_get_bool_path_from_dynamic(bundle, path, "compressed_vector_projection_required");
    let compressed_vector_projection_ready =
        json_get_bool_path_from_dynamic(bundle, path, "compressed_vector_projection_ready");
    let ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL)
        && evidence_source.as_deref() == Some(SEARCH_PROJECTION_EVIDENCE_SOURCE)
        && derived_projection == Some(true)
        && all_tables_covered == Some(true)
        && covered_table_count.is_some_and(|count| count > 0)
        && covered_table_count == required_table_count
        && fts_ready == Some(true)
        && vector_ready == Some(true)
        && embedding_identity_ready == Some(true)
        && fail_soft_ready == Some(true)
        && rebuild_marker_ready == Some(true)
        && metadata_repair_marker_ready == Some(true)
        && incremental_update_ready == Some(true)
        && source_chunk_ready == Some(true)
        && predicate_pushdown_ready == Some(true)
        && compressed_vector_projection_ready.unwrap_or(true);
    SearchProjectionEvidenceSummary {
        protocol,
        evidence_source,
        present,
        ready,
        derived_projection,
        all_tables_covered,
        covered_table_count,
        required_table_count,
        fts_ready,
        vector_ready,
        embedding_identity_ready,
        fail_soft_ready,
        rebuild_marker_ready,
        metadata_repair_marker_ready,
        incremental_update_ready,
        source_chunk_ready,
        predicate_pushdown_ready,
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
    let evidence_source = json_get_str_path_from_dynamic(bundle, path, "evidence_source");
    let route = json_get_str_path_from_dynamic(bundle, path, "route");
    let primary_ready = json_get_bool_path_from_dynamic(bundle, path, "primary_ready");
    let shadow_ready = json_get_bool_path_from_dynamic(bundle, path, "shadow_ready");
    let document_count_parity =
        json_get_bool_path_from_dynamic(bundle, path, "document_count_parity");
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
    let primary_engine = json_get_str_path_from_dynamic(bundle, path, "primary_engine");
    let shadow_engine = json_get_str_path_from_dynamic(bundle, path, "shadow_engine");
    let ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL)
        && evidence_source == Some(SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE)
        && route == Some(SEARCH_PROJECTION_SHADOW_EVIDENCE_ROUTE)
        && primary_ready == Some(true)
        && shadow_ready == Some(true)
        && document_count_parity == Some(true)
        && table_parity_ready == Some(true)
        && embedding_identity_parity == Some(true)
        && lifecycle_parity == Some(true)
        && incremental_watermark_parity == Some(true)
        && predicate_pushdown_parity == Some(true)
        && pushdown_ready == Some(true)
        && primary_engine == Some("lancedb")
        && shadow_engine == Some("skein");
    SearchProjectionShadowEvidenceSummary {
        protocol,
        evidence_source,
        route,
        present,
        ready,
        primary_ready,
        shadow_ready,
        document_count_parity,
        table_parity_ready,
        embedding_identity_parity,
        lifecycle_parity,
        incremental_watermark_parity,
        predicate_pushdown_parity,
        pushdown_evidence,
        primary_engine,
        shadow_engine,
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
        return pushdown.clone();
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
    let primary_scan_filter_fields = {
        let mut nested = path.to_vec();
        nested.extend([
            "primary_evidence",
            "predicate_pushdown",
            "scan_filter_fields",
        ]);
        json_get_array_path(bundle, &nested)
    };
    let shadow_scan_filter_fields = {
        let mut nested = path.to_vec();
        nested.extend([
            "shadow_evidence",
            "predicate_pushdown",
            "scan_filter_fields",
        ]);
        json_get_array_path(bundle, &nested)
    };
    let ready = predicate_pushdown_parity
        && primary_predicate_pushdown_ready
        && shadow_predicate_pushdown_ready
        && shadow_persisted_segment_descriptor_ready;
    serde_json::json!({
        "ready": ready,
        "predicate_pushdown_parity": predicate_pushdown_parity,
        "primary_predicate_pushdown_ready": primary_predicate_pushdown_ready,
        "shadow_predicate_pushdown_ready": shadow_predicate_pushdown_ready,
        "shadow_persisted_segment_descriptor_ready": shadow_persisted_segment_descriptor_ready,
        "primary_scan_filter_fields": primary_scan_filter_fields,
        "shadow_scan_filter_fields": shadow_scan_filter_fields,
    })
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
    serde_json::json!(blockers.into_iter().collect::<Vec<_>>())
}

fn search_candidate_shadow_evidence_summary(
    bundle: &serde_json::Value,
) -> SearchCandidateShadowEvidenceSummary<'_> {
    let path = if json_get_path(bundle, &["search_candidate_shadow_evidence"]).is_some() {
        &["search_candidate_shadow_evidence"][..]
    } else {
        &["cutover_evidence", "search_candidate_shadow_evidence"][..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let evidence_source = json_get_str_path_from_dynamic(bundle, path, "evidence_source");
    let route = json_get_str_path_from_dynamic(bundle, path, "route");
    let reported_ready = json_get_bool_path_from_dynamic(bundle, path, "ready");
    let engine = json_get_str_path_from_dynamic(bundle, path, "engine");
    let candidate_primary_engine =
        json_get_str_path_from_dynamic(bundle, path, "candidate_primary_engine");
    let primary_engine = json_get_str_path_from_dynamic(bundle, path, "primary_engine");
    let shadow_engine = json_get_str_path_from_dynamic(bundle, path, "shadow_engine");
    let row_count_parity =
        json_get_bool_path_from_dynamic_nested(bundle, path, &["row_counts", "matches"]);
    let vector_top_k_overlap_ready =
        json_get_bool_path_from_dynamic_nested(bundle, path, &["vector", "top_k_overlap_ready"]);
    let fts_top_k_overlap_ready =
        json_get_bool_path_from_dynamic_nested(bundle, path, &["fts", "top_k_overlap_ready"]);
    let shadow_scan_path = &["filter_pushdown", "shadow_scan"][..];
    let shadow_scan_present =
        json_get_path_from_dynamic_nested(bundle, path, shadow_scan_path).is_some();
    let predicate_path = &[
        "filter_pushdown",
        "shadow_scan",
        "metadata_predicate_pushdown",
    ][..];
    let shadow_scan_input_predicate_count = json_get_u64_path_from_dynamic_nested(
        bundle,
        path,
        &nested_path(predicate_path, "input_predicate_count"),
    );
    let shadow_scan_pushed_predicate_count = json_get_u64_path_from_dynamic_nested(
        bundle,
        path,
        &nested_path(predicate_path, "pushed_predicate_count"),
    );
    let shadow_scan_residual_predicate_count = json_get_u64_path_from_dynamic_nested(
        bundle,
        path,
        &nested_path(predicate_path, "residual_predicate_count"),
    );
    let shadow_scan_filtered_out_count = json_get_u64_path_from_dynamic_nested(
        bundle,
        path,
        &["filter_pushdown", "shadow_scan", "filtered_out_count"],
    );
    let shadow_scan_document_count = json_get_u64_path_from_dynamic_nested(
        bundle,
        path,
        &["filter_pushdown", "shadow_scan", "document_count"],
    );
    let shadow_scan_filtered_document_count = json_get_u64_path_from_dynamic_nested(
        bundle,
        path,
        &["filter_pushdown", "shadow_scan", "filtered_document_count"],
    );
    let shadow_scan_candidate_set_cardinality = json_get_u64_path_from_dynamic_nested(
        bundle,
        path,
        &[
            "filter_pushdown",
            "shadow_scan",
            "candidate_set_cardinality",
        ],
    );
    let shadow_scan_pruned_document_count = json_get_u64_path_from_dynamic_nested(
        bundle,
        path,
        &nested_path(predicate_path, "pruned_document_count"),
    );
    let shadow_scan_scanned_document_count = json_get_u64_path_from_dynamic_nested(
        bundle,
        path,
        &nested_path(predicate_path, "scanned_document_count"),
    );
    let shadow_scan_parse_error = json_get_str_path_from_dynamic_nested(
        bundle,
        path,
        &nested_path(predicate_path, "parse_error"),
    );
    let shadow_scan_unsatisfiable = json_get_bool_path_from_dynamic_nested(
        bundle,
        path,
        &nested_path(predicate_path, "unsatisfiable"),
    );
    let shadow_scan_descriptor_bounded = json_get_bool_path_from_dynamic_nested(
        bundle,
        path,
        &nested_path(predicate_path, "segment_descriptor_bounded"),
    );
    let shadow_scan_descriptor_field_count = json_get_u64_path_from_dynamic_nested(
        bundle,
        path,
        &nested_path(predicate_path, "segment_descriptor_field_count"),
    );
    let shadow_scan_field_summary_count = json_get_array_len_path_from_dynamic_nested(
        bundle,
        path,
        &nested_path(predicate_path, "field_summaries"),
    );
    let blocker_codes = json_get_array_path_from_dynamic(bundle, path, "blocker_codes");
    let blocker_codes_empty = blocker_codes
        .as_array()
        .is_some_and(|blockers| blockers.is_empty());
    let shadow_scan_filter_pushdown_ready = shadow_scan_present
        && shadow_scan_input_predicate_count.is_some_and(|count| count > 0)
        && shadow_scan_pushed_predicate_count.is_some_and(|count| count > 0)
        && shadow_scan_residual_predicate_count == Some(0)
        && shadow_scan_parse_error.is_none()
        && shadow_scan_unsatisfiable != Some(true);
    let shadow_scan_field_pruning_ready = match shadow_scan_pushed_predicate_count {
        Some(0) => true,
        Some(_) => shadow_scan_field_summary_count.is_some_and(|count| count > 0),
        None => false,
    };
    let shadow_scan_descriptor_bounded_ready = match shadow_scan_pushed_predicate_count {
        Some(0) => true,
        Some(_) => shadow_scan_descriptor_bounded == Some(true),
        None => false,
    };
    let shadow_scan_reduction_ready = match shadow_scan_pushed_predicate_count {
        Some(0) => true,
        Some(_) => {
            shadow_scan_filtered_out_count.is_some_and(|count| count > 0)
                || shadow_scan_pruned_document_count.is_some_and(|count| count > 0)
                || matches!(
                    (
                        shadow_scan_document_count,
                        shadow_scan_filtered_document_count,
                        shadow_scan_candidate_set_cardinality,
                    ),
                    (Some(document_count), Some(filtered_document_count), Some(candidate_count))
                        if filtered_document_count < document_count
                            && candidate_count == filtered_document_count
                )
        }
        None => false,
    };
    let stable_envelope_ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL)
        && evidence_source == Some(SEARCH_CANDIDATE_SHADOW_EVIDENCE_SOURCE)
        && route == Some(SEARCH_CANDIDATE_SHADOW_EVIDENCE_ROUTE)
        && reported_ready == Some(true)
        && blocker_codes_empty;
    let primary_read_ready = stable_envelope_ready
        && engine == Some("skein-primary")
        && candidate_primary_engine == Some("skein")
        && primary_engine == Some("skein");
    let shadow_parity_ready = stable_envelope_ready
        && primary_engine == Some("lancedb")
        && shadow_engine == Some("skein")
        && row_count_parity == Some(true)
        && vector_top_k_overlap_ready == Some(true)
        && fts_top_k_overlap_ready == Some(true)
        && shadow_scan_filter_pushdown_ready
        && shadow_scan_field_pruning_ready
        && shadow_scan_reduction_ready;
    let ready = primary_read_ready || shadow_parity_ready;
    let mut synthesized_blocker_codes = json_string_array(&blocker_codes)
        .into_iter()
        .collect::<BTreeSet<_>>();
    if present && !primary_read_ready && !shadow_scan_reduction_ready {
        synthesized_blocker_codes
            .insert("skein_search_scan_reduction_evidence_missing".to_string());
    }
    let blocker_codes =
        serde_json::json!(synthesized_blocker_codes.into_iter().collect::<Vec<_>>());
    SearchCandidateShadowEvidenceSummary {
        protocol,
        evidence_source,
        route,
        present,
        ready,
        reported_ready,
        engine,
        candidate_primary_engine,
        primary_engine,
        shadow_engine,
        row_count_parity,
        vector_top_k_overlap_ready,
        fts_top_k_overlap_ready,
        shadow_scan_present,
        shadow_scan_filter_pushdown_ready,
        shadow_scan_field_pruning_ready: primary_read_ready || shadow_scan_field_pruning_ready,
        shadow_scan_reduction_ready: primary_read_ready || shadow_scan_reduction_ready,
        shadow_scan_descriptor_bounded_ready: primary_read_ready
            || shadow_scan_descriptor_bounded_ready,
        shadow_scan_descriptor_field_count,
        shadow_scan_field_summary_count,
        shadow_scan_input_predicate_count,
        shadow_scan_pushed_predicate_count,
        shadow_scan_residual_predicate_count,
        shadow_scan_filtered_out_count,
        shadow_scan_pruned_document_count,
        shadow_scan_scanned_document_count,
        shadow_scan_parse_error,
        shadow_scan_unsatisfiable,
        blocker_codes,
    }
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
    let row_limit_enforced_before_output =
        json_get_bool_path_from_dynamic(bundle, path, "row_limit_enforced_before_output");
    let operator_row_cap_enabled =
        json_get_bool_path_from_dynamic(bundle, path, "operator_row_cap_enabled");
    let streaming = json_get_bool_path_from_dynamic(bundle, path, "streaming");
    let blocking_operator_count =
        json_get_u64_path_from_dynamic(bundle, path, "blocking_operator_count");
    let covered_routes = json_get_string_array_path_from_dynamic(bundle, path, "covered_routes");
    let covered_route_set = covered_routes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let primary_ready_routes =
        json_get_string_array_path_from_dynamic(bundle, path, "primary_ready_routes");
    let primary_ready_route_set = primary_ready_routes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let missing_covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !covered_route_set.contains(route))
        .collect::<Vec<_>>();
    let missing_primary_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !primary_ready_route_set.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let reported_missing_primary_routes =
        json_get_string_array_path_from_dynamic(bundle, path, "missing_primary_routes");
    let route_primary_ready = json_get_bool_path_from_dynamic(bundle, path, "route_primary_ready");
    let query_runtime_report_count =
        json_get_u64_path_from_dynamic(bundle, path, "query_runtime_report_count");
    let query_runtime_plan_report_count =
        json_get_u64_path_from_dynamic(bundle, path, "query_runtime_plan_report_count");
    let query_runtime_profile_report_count =
        json_get_u64_path_from_dynamic(bundle, path, "query_runtime_profile_report_count");
    let query_runtime_failed_query_count =
        json_get_u64_path_from_dynamic(bundle, path, "query_runtime_failed_query_count");
    let query_runtime_missing_plan_evidence_count =
        json_get_u64_path_from_dynamic(bundle, path, "query_runtime_missing_plan_evidence_count");
    let query_runtime_missing_profile_evidence_count = json_get_u64_path_from_dynamic(
        bundle,
        path,
        "query_runtime_missing_profile_evidence_count",
    );
    let ready = present
        && protocol.as_deref() == Some(SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL)
        && mode == Some("shadow_read_only")
        && max_rows.is_some_and(|value| value > 0)
        && execution_row_cap == max_rows.and_then(|value| value.checked_add(1))
        && row_limit_enforced_before_output == Some(true)
        && operator_row_cap_enabled == Some(true)
        && missing_covered_routes.is_empty()
        && route_primary_ready == Some(true)
        && missing_primary_routes.is_empty()
        && reported_missing_primary_routes.is_empty();
    BoundedReadEvidenceSummary {
        protocol,
        present,
        ready,
        mode,
        max_rows,
        execution_row_cap,
        row_limit_enforced_before_output,
        operator_row_cap_enabled,
        streaming,
        blocking_operator_count,
        covered_routes,
        missing_covered_routes,
        route_primary_ready,
        query_runtime_report_count,
        query_runtime_plan_report_count,
        query_runtime_profile_report_count,
        query_runtime_failed_query_count,
        query_runtime_missing_plan_evidence_count,
        query_runtime_missing_profile_evidence_count,
        primary_ready_routes,
        missing_primary_routes,
        blocker_codes: json_get_array_path_from_dynamic(bundle, path, "blocker_codes"),
    }
}

fn overview_parity_evidence_summary(bundle: &serde_json::Value) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "overview_parity_evidence",
        "overview",
        "overview_parity_evidence",
        "/graph/overview",
    )
}

fn graph_search_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "graph_search_parity_evidence",
        "graph_search",
        "graph_search_parity_evidence",
        "/graph/search",
    )
}

fn explore_parity_evidence_summary(bundle: &serde_json::Value) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "explore_parity_evidence",
        "explore",
        "explore_parity_evidence",
        "/graph/explore",
    )
}

fn expand_parity_evidence_summary(bundle: &serde_json::Value) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "expand_parity_evidence",
        "expand",
        "expand_parity_evidence",
        "/graph/expand/{node_id}",
    )
}

fn live_preview_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "live_preview_parity_evidence",
        "live_preview",
        "live_preview_parity_evidence",
        "/graph/live-preview",
    )
}

fn live_preview_node_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "live_preview_node_parity_evidence",
        "live_preview_node",
        "live_preview_node_parity_evidence",
        "/graph/live-preview/{node_id}",
    )
}

fn node_details_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "node_details_parity_evidence",
        "node_details",
        "node_details_parity_evidence",
        "/graph/node-details/{node_id}",
    )
}

fn source_detail_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "source_detail_parity_evidence",
        "source_detail",
        "source_detail_parity_evidence",
        "/sources/{source_id}",
    )
}

fn orphans_parity_evidence_summary(bundle: &serde_json::Value) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "orphans_parity_evidence",
        "orphans",
        "orphans_parity_evidence",
        "/graph/orphans",
    )
}

fn shortest_path_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "shortest_path_parity_evidence",
        "shortest_path",
        "shortest_path_parity_evidence",
        "/graph/shortest-path",
    )
}

fn community_members_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "community_members_parity_evidence",
        "community_members",
        "community_members_parity_evidence",
        "/graph/community-members/{community_id}",
    )
}

fn augmentation_state_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "augmentation_state_parity_evidence",
        "augmentation_state",
        "augmentation_state_parity_evidence",
        "/graph/augmentation/state",
    )
}

fn pagerank_plan_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "pagerank_plan_parity_evidence",
        "pagerank_plan",
        "pagerank_plan_parity_evidence",
        "/graph/augmentation/pagerank/plan",
    )
}

fn community_subgraph_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "community_subgraph_parity_evidence",
        "community_subgraph",
        "community_subgraph_parity_evidence",
        "/library/community/{community_id}/subgraph",
    )
}

fn community_recent_memories_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "community_recent_memories_parity_evidence",
        "community_recent_memories",
        "community_recent_memories_parity_evidence",
        "/library/community/{community_id}/recent-memories",
    )
}

fn related_communities_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "related_communities_parity_evidence",
        "related_communities",
        "related_communities_parity_evidence",
        "/library/community/{community_id}/related",
    )
}

fn graph_analysis_parity_evidence_summary(
    bundle: &serde_json::Value,
) -> RouteParityEvidenceSummary<'_> {
    route_parity_evidence_summary(
        bundle,
        "graph_analysis_parity_evidence",
        "graph_analysis",
        "graph_analysis_parity_evidence",
        "/graph/analysis",
    )
}

fn route_parity_evidence_summary<'a>(
    bundle: &'a serde_json::Value,
    top_level_key: &'static str,
    nested_key: &'static str,
    cutover_key: &'static str,
    expected_route: &'static str,
) -> RouteParityEvidenceSummary<'a> {
    let top_level_path = [top_level_key];
    let nested_path = ["graph_route_parity_evidence", nested_key];
    let cutover_path = ["cutover_evidence", cutover_key];
    let path = if json_get_path(bundle, &top_level_path).is_some() {
        &top_level_path[..]
    } else if json_get_path(bundle, &nested_path).is_some() {
        &nested_path[..]
    } else {
        &cutover_path[..]
    };
    let present = json_get_path(bundle, path).is_some();
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let reported_ready = json_get_bool_path_from_dynamic(bundle, path, "ready");
    let route = json_get_str_path_from_dynamic(bundle, path, "route");
    let matches = json_get_bool_path_from_dynamic(bundle, path, "matches");
    let primary_ready = json_get_bool_path_from_dynamic(bundle, path, "primary_ready");
    let shadow_ready = json_get_bool_path_from_dynamic(bundle, path, "shadow_ready");
    let blocker_codes = json_get_array_path_from_dynamic(bundle, path, "blocker_codes");
    let blocker_codes_empty = blocker_codes
        .as_array()
        .is_some_and(|blockers| blockers.is_empty());
    let ready = present
        && protocol.as_deref() == Some(NMEM_GRAPH_ROUTE_SHADOW_PARITY_EVIDENCE_PROTOCOL)
        && reported_ready == Some(true)
        && route == Some(expected_route)
        && matches == Some(true)
        && primary_ready == Some(true)
        && shadow_ready == Some(true)
        && blocker_codes_empty;
    RouteParityEvidenceSummary {
        protocol,
        present,
        ready,
        reported_ready,
        route,
        matches,
        primary_engine: json_get_str_path_from_dynamic(bundle, path, "primary_engine"),
        shadow_engine: json_get_str_path_from_dynamic(bundle, path, "shadow_engine"),
        primary_ready,
        shadow_ready,
        blocker_codes,
    }
}

fn json_get_string_array_path_from_dynamic(
    value: &serde_json::Value,
    base_path: &[&str],
    field: &str,
) -> Vec<String> {
    let mut path = base_path.to_vec();
    path.push(field);
    json_get_string_array_path(value, &path)
}

fn nested_path<'a>(prefix: &[&'a str], field: &'a str) -> Vec<&'a str> {
    let mut path = prefix.to_vec();
    path.push(field);
    path
}

fn json_get_path_from_dynamic_nested<'a>(
    value: &'a serde_json::Value,
    base_path: &[&str],
    fields: &[&str],
) -> Option<&'a serde_json::Value> {
    let mut path = base_path.to_vec();
    path.extend(fields);
    json_get_path(value, &path)
}

fn json_get_bool_path_from_dynamic_nested(
    value: &serde_json::Value,
    base_path: &[&str],
    fields: &[&str],
) -> Option<bool> {
    json_get_path_from_dynamic_nested(value, base_path, fields).and_then(serde_json::Value::as_bool)
}

fn json_get_u64_path_from_dynamic_nested(
    value: &serde_json::Value,
    base_path: &[&str],
    fields: &[&str],
) -> Option<u64> {
    json_get_path_from_dynamic_nested(value, base_path, fields).and_then(serde_json::Value::as_u64)
}

fn json_get_array_len_path_from_dynamic_nested(
    value: &serde_json::Value,
    base_path: &[&str],
    fields: &[&str],
) -> Option<u64> {
    json_get_path_from_dynamic_nested(value, base_path, fields)?
        .as_array()
        .and_then(|items| u64::try_from(items.len()).ok())
}

fn json_get_str_path_from_dynamic_nested<'a>(
    value: &'a serde_json::Value,
    base_path: &[&str],
    fields: &[&str],
) -> Option<&'a str> {
    json_get_path_from_dynamic_nested(value, base_path, fields).and_then(serde_json::Value::as_str)
}

fn json_get_array_path_from_dynamic(
    value: &serde_json::Value,
    base_path: &[&str],
    field: &str,
) -> serde_json::Value {
    let mut path = base_path.to_vec();
    path.push(field);
    json_get_array_path(value, &path)
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
    if !inputs.bounded_read_evidence_ready {
        categories.insert("bounded_read_evidence".to_string());
    }
    if !inputs.augmentation_state_parity_evidence_ready {
        categories.insert("augmentation_state_parity_evidence".to_string());
    }
    if !inputs.pagerank_plan_parity_evidence_ready {
        categories.insert("pagerank_plan_parity_evidence".to_string());
    }
    if !inputs.overview_parity_evidence_ready {
        categories.insert("overview_parity_evidence".to_string());
    }
    if !inputs.graph_search_parity_evidence_ready {
        categories.insert("graph_search_parity_evidence".to_string());
    }
    if !inputs.explore_parity_evidence_ready {
        categories.insert("explore_parity_evidence".to_string());
    }
    if !inputs.expand_parity_evidence_ready {
        categories.insert("expand_parity_evidence".to_string());
    }
    if !inputs.live_preview_parity_evidence_ready {
        categories.insert("live_preview_parity_evidence".to_string());
    }
    if !inputs.live_preview_node_parity_evidence_ready {
        categories.insert("live_preview_node_parity_evidence".to_string());
    }
    if !inputs.node_details_parity_evidence_ready {
        categories.insert("node_details_parity_evidence".to_string());
    }
    if !inputs.source_detail_parity_evidence_ready {
        categories.insert("source_detail_parity_evidence".to_string());
    }
    if !inputs.orphans_parity_evidence_ready {
        categories.insert("orphans_parity_evidence".to_string());
    }
    if !inputs.shortest_path_parity_evidence_ready {
        categories.insert("shortest_path_parity_evidence".to_string());
    }
    if !inputs.community_members_parity_evidence_ready {
        categories.insert("community_members_parity_evidence".to_string());
    }
    if !inputs.community_subgraph_parity_evidence_ready {
        categories.insert("community_subgraph_parity_evidence".to_string());
    }
    if !inputs.community_recent_memories_parity_evidence_ready {
        categories.insert("community_recent_memories_parity_evidence".to_string());
    }
    if !inputs.related_communities_parity_evidence_ready {
        categories.insert("related_communities_parity_evidence".to_string());
    }
    if !inputs.graph_analysis_parity_evidence_ready {
        categories.insert("graph_analysis_parity_evidence".to_string());
    }
    if json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]) == Some(true)
        && json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]) != Some(true)
    {
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
    if !inputs.search_projection_evidence_ready {
        actions.push(next_action(
            "attach_search_projection_replacement_evidence",
            "LanceDB replacement evidence is missing or not ready",
            [
                "search_projection_evidence.protocol",
                "search_projection_evidence.evidence_source",
                "search_projection_evidence.present",
                "search_projection_evidence.ready",
                "search_projection_evidence.derived_projection",
                "search_projection_evidence.all_tables_covered",
                "search_projection_evidence.covered_table_count",
                "search_projection_evidence.required_table_count",
                "search_projection_evidence.fts_ready",
                "search_projection_evidence.vector_ready",
                "search_projection_evidence.embedding_identity_ready",
                "search_projection_evidence.fail_soft_ready",
                "search_projection_evidence.rebuild_marker_ready",
                "search_projection_evidence.metadata_repair_marker_ready",
                "search_projection_evidence.incremental_update_ready",
                "search_projection_evidence.source_chunk_ready",
                "search_projection_evidence.predicate_pushdown_ready",
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
                "search_projection_shadow_evidence.route",
                "search_projection_shadow_evidence.present",
                "search_projection_shadow_evidence.ready",
                "search_projection_shadow_evidence.primary_engine",
                "search_projection_shadow_evidence.shadow_engine",
                "search_projection_shadow_evidence.primary_ready",
                "search_projection_shadow_evidence.shadow_ready",
                "search_projection_shadow_evidence.document_count_parity",
                "search_projection_shadow_evidence.table_parity.ready",
                "search_projection_shadow_evidence.embedding_identity_parity",
                "search_projection_shadow_evidence.lifecycle_parity",
                "search_projection_shadow_evidence.incremental_watermark_parity",
                "search_projection_shadow_evidence.predicate_pushdown_parity",
                "search_projection_shadow_evidence.pushdown_evidence.ready",
                "search_projection_shadow_evidence.pushdown_evidence.primary_predicate_pushdown_ready",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_predicate_pushdown_ready",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_persisted_segment_descriptor_ready",
                "search_projection_shadow_evidence.pushdown_evidence.primary_scan_filter_fields",
                "search_projection_shadow_evidence.pushdown_evidence.shadow_scan_filter_fields",
                "search_projection_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.search_candidate_shadow_evidence_ready {
        actions.push(next_action(
            "run_search_candidate_shadow_compare",
            "LanceDB/Skein candidate shadow evidence is missing or lacks scan-filter pushdown proof",
            [
                "search_candidate_shadow_evidence.protocol",
                "search_candidate_shadow_evidence.evidence_source",
                "search_candidate_shadow_evidence.route",
                "search_candidate_shadow_evidence.present",
                "search_candidate_shadow_evidence.ready",
                "search_candidate_shadow_evidence.reported_ready",
                "search_candidate_shadow_evidence.engine",
                "search_candidate_shadow_evidence.candidate_primary_engine",
                "search_candidate_shadow_evidence.primary_engine",
                "search_candidate_shadow_evidence.shadow_engine",
                "search_candidate_shadow_evidence.row_count_parity",
                "search_candidate_shadow_evidence.vector_top_k_overlap_ready",
                "search_candidate_shadow_evidence.fts_top_k_overlap_ready",
                "search_candidate_shadow_evidence.shadow_scan_present",
                "search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready",
                "search_candidate_shadow_evidence.shadow_scan_field_pruning_ready",
                "search_candidate_shadow_evidence.shadow_scan_reduction_ready",
                "search_candidate_shadow_evidence.shadow_scan_descriptor_bounded_ready",
                "search_candidate_shadow_evidence.shadow_scan_descriptor_field_count",
                "search_candidate_shadow_evidence.shadow_scan_field_summary_count",
                "search_candidate_shadow_evidence.shadow_scan_input_predicate_count",
                "search_candidate_shadow_evidence.shadow_scan_pushed_predicate_count",
                "search_candidate_shadow_evidence.shadow_scan_residual_predicate_count",
                "search_candidate_shadow_evidence.shadow_scan_pruned_document_count",
                "search_candidate_shadow_evidence.shadow_scan_scanned_document_count",
                "search_candidate_shadow_evidence.shadow_scan_parse_error",
                "search_candidate_shadow_evidence.shadow_scan_unsatisfiable",
                "search_candidate_shadow_evidence.blocker_codes",
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
                "bounded_read_evidence.row_limit_enforced_before_output",
                "bounded_read_evidence.operator_row_cap_enabled",
                "bounded_read_evidence.blocking_operator_count",
                "bounded_read_evidence.covered_routes",
                "bounded_read_evidence.route_primary_ready",
                "bounded_read_evidence.query_runtime_report_count",
                "bounded_read_evidence.query_runtime_plan_report_count",
                "bounded_read_evidence.query_runtime_profile_report_count",
                "bounded_read_evidence.query_runtime_failed_query_count",
                "bounded_read_evidence.query_runtime_missing_plan_evidence_count",
                "bounded_read_evidence.query_runtime_missing_profile_evidence_count",
                "bounded_read_evidence.primary_ready_routes",
                "bounded_read_evidence.missing_primary_routes",
                "bounded_read_evidence.blocker_codes",
            ],
        ));
    }
    if !inputs.augmentation_state_parity_evidence_ready {
        actions.push(next_action(
            "run_augmentation_state_route_shadow_compare",
            "augmentation-state graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.augmentation_state.protocol",
                "graph_route_parity_evidence.augmentation_state.present",
                "graph_route_parity_evidence.augmentation_state.ready",
                "graph_route_parity_evidence.augmentation_state.route",
                "graph_route_parity_evidence.augmentation_state.matches",
                "graph_route_parity_evidence.augmentation_state.primary_ready",
                "graph_route_parity_evidence.augmentation_state.shadow_ready",
                "graph_route_parity_evidence.augmentation_state.blocker_codes",
            ],
        ));
    }
    if !inputs.pagerank_plan_parity_evidence_ready {
        actions.push(next_action(
            "run_pagerank_plan_route_shadow_compare",
            "pagerank-plan graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.pagerank_plan.protocol",
                "graph_route_parity_evidence.pagerank_plan.present",
                "graph_route_parity_evidence.pagerank_plan.ready",
                "graph_route_parity_evidence.pagerank_plan.route",
                "graph_route_parity_evidence.pagerank_plan.matches",
                "graph_route_parity_evidence.pagerank_plan.primary_ready",
                "graph_route_parity_evidence.pagerank_plan.shadow_ready",
                "graph_route_parity_evidence.pagerank_plan.blocker_codes",
            ],
        ));
    }
    if !inputs.overview_parity_evidence_ready {
        actions.push(next_action(
            "run_overview_route_shadow_compare",
            "overview graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.overview.protocol",
                "graph_route_parity_evidence.overview.present",
                "graph_route_parity_evidence.overview.ready",
                "graph_route_parity_evidence.overview.route",
                "graph_route_parity_evidence.overview.matches",
                "graph_route_parity_evidence.overview.primary_ready",
                "graph_route_parity_evidence.overview.shadow_ready",
                "graph_route_parity_evidence.overview.blocker_codes",
            ],
        ));
    }
    if !inputs.graph_search_parity_evidence_ready {
        actions.push(next_action(
            "run_graph_search_route_shadow_compare",
            "graph-search route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.graph_search.protocol",
                "graph_route_parity_evidence.graph_search.present",
                "graph_route_parity_evidence.graph_search.ready",
                "graph_route_parity_evidence.graph_search.route",
                "graph_route_parity_evidence.graph_search.matches",
                "graph_route_parity_evidence.graph_search.primary_ready",
                "graph_route_parity_evidence.graph_search.shadow_ready",
                "graph_route_parity_evidence.graph_search.blocker_codes",
            ],
        ));
    }
    if !inputs.explore_parity_evidence_ready {
        actions.push(next_action(
            "run_explore_route_shadow_compare",
            "explore graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.explore.protocol",
                "graph_route_parity_evidence.explore.present",
                "graph_route_parity_evidence.explore.ready",
                "graph_route_parity_evidence.explore.route",
                "graph_route_parity_evidence.explore.matches",
                "graph_route_parity_evidence.explore.primary_ready",
                "graph_route_parity_evidence.explore.shadow_ready",
                "graph_route_parity_evidence.explore.blocker_codes",
            ],
        ));
    }
    if !inputs.expand_parity_evidence_ready {
        actions.push(next_action(
            "run_expand_route_shadow_compare",
            "expand graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.expand.protocol",
                "graph_route_parity_evidence.expand.present",
                "graph_route_parity_evidence.expand.ready",
                "graph_route_parity_evidence.expand.route",
                "graph_route_parity_evidence.expand.matches",
                "graph_route_parity_evidence.expand.primary_ready",
                "graph_route_parity_evidence.expand.shadow_ready",
                "graph_route_parity_evidence.expand.blocker_codes",
            ],
        ));
    }
    if !inputs.live_preview_parity_evidence_ready {
        actions.push(next_action(
            "run_live_preview_route_shadow_compare",
            "live-preview graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.live_preview.protocol",
                "graph_route_parity_evidence.live_preview.present",
                "graph_route_parity_evidence.live_preview.ready",
                "graph_route_parity_evidence.live_preview.route",
                "graph_route_parity_evidence.live_preview.matches",
                "graph_route_parity_evidence.live_preview.primary_ready",
                "graph_route_parity_evidence.live_preview.shadow_ready",
                "graph_route_parity_evidence.live_preview.blocker_codes",
            ],
        ));
    }
    if !inputs.live_preview_node_parity_evidence_ready {
        actions.push(next_action(
            "run_live_preview_node_route_shadow_compare",
            "live-preview node graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.live_preview_node.protocol",
                "graph_route_parity_evidence.live_preview_node.present",
                "graph_route_parity_evidence.live_preview_node.ready",
                "graph_route_parity_evidence.live_preview_node.route",
                "graph_route_parity_evidence.live_preview_node.matches",
                "graph_route_parity_evidence.live_preview_node.primary_ready",
                "graph_route_parity_evidence.live_preview_node.shadow_ready",
                "graph_route_parity_evidence.live_preview_node.blocker_codes",
            ],
        ));
    }
    if !inputs.node_details_parity_evidence_ready {
        actions.push(next_action(
            "run_node_details_route_shadow_compare",
            "node-details graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.node_details.protocol",
                "graph_route_parity_evidence.node_details.present",
                "graph_route_parity_evidence.node_details.ready",
                "graph_route_parity_evidence.node_details.route",
                "graph_route_parity_evidence.node_details.matches",
                "graph_route_parity_evidence.node_details.primary_ready",
                "graph_route_parity_evidence.node_details.shadow_ready",
                "graph_route_parity_evidence.node_details.blocker_codes",
            ],
        ));
    }
    if !inputs.source_detail_parity_evidence_ready {
        actions.push(next_action(
            "run_source_detail_route_shadow_compare",
            "source-detail graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.source_detail.protocol",
                "graph_route_parity_evidence.source_detail.present",
                "graph_route_parity_evidence.source_detail.ready",
                "graph_route_parity_evidence.source_detail.route",
                "graph_route_parity_evidence.source_detail.matches",
                "graph_route_parity_evidence.source_detail.primary_ready",
                "graph_route_parity_evidence.source_detail.shadow_ready",
                "graph_route_parity_evidence.source_detail.blocker_codes",
            ],
        ));
    }
    if !inputs.orphans_parity_evidence_ready {
        actions.push(next_action(
            "run_orphans_route_shadow_compare",
            "orphans graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.orphans.protocol",
                "graph_route_parity_evidence.orphans.present",
                "graph_route_parity_evidence.orphans.ready",
                "graph_route_parity_evidence.orphans.route",
                "graph_route_parity_evidence.orphans.matches",
                "graph_route_parity_evidence.orphans.primary_ready",
                "graph_route_parity_evidence.orphans.shadow_ready",
                "graph_route_parity_evidence.orphans.blocker_codes",
            ],
        ));
    }
    if !inputs.shortest_path_parity_evidence_ready {
        actions.push(next_action(
            "run_shortest_path_route_shadow_compare",
            "shortest-path graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.shortest_path.protocol",
                "graph_route_parity_evidence.shortest_path.present",
                "graph_route_parity_evidence.shortest_path.ready",
                "graph_route_parity_evidence.shortest_path.route",
                "graph_route_parity_evidence.shortest_path.matches",
                "graph_route_parity_evidence.shortest_path.primary_ready",
                "graph_route_parity_evidence.shortest_path.shadow_ready",
                "graph_route_parity_evidence.shortest_path.blocker_codes",
            ],
        ));
    }
    if !inputs.community_members_parity_evidence_ready {
        actions.push(next_action(
            "run_community_members_route_shadow_compare",
            "community-members graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.community_members.protocol",
                "graph_route_parity_evidence.community_members.present",
                "graph_route_parity_evidence.community_members.ready",
                "graph_route_parity_evidence.community_members.route",
                "graph_route_parity_evidence.community_members.matches",
                "graph_route_parity_evidence.community_members.primary_ready",
                "graph_route_parity_evidence.community_members.shadow_ready",
                "graph_route_parity_evidence.community_members.blocker_codes",
            ],
        ));
    }
    if !inputs.community_subgraph_parity_evidence_ready {
        actions.push(next_action(
            "run_community_subgraph_route_shadow_compare",
            "community-subgraph graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.community_subgraph.protocol",
                "graph_route_parity_evidence.community_subgraph.present",
                "graph_route_parity_evidence.community_subgraph.ready",
                "graph_route_parity_evidence.community_subgraph.route",
                "graph_route_parity_evidence.community_subgraph.matches",
                "graph_route_parity_evidence.community_subgraph.primary_ready",
                "graph_route_parity_evidence.community_subgraph.shadow_ready",
                "graph_route_parity_evidence.community_subgraph.blocker_codes",
            ],
        ));
    }
    if !inputs.community_recent_memories_parity_evidence_ready {
        actions.push(next_action(
            "run_community_recent_memories_route_shadow_compare",
            "community recent memories graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.community_recent_memories.protocol",
                "graph_route_parity_evidence.community_recent_memories.present",
                "graph_route_parity_evidence.community_recent_memories.ready",
                "graph_route_parity_evidence.community_recent_memories.route",
                "graph_route_parity_evidence.community_recent_memories.matches",
                "graph_route_parity_evidence.community_recent_memories.primary_ready",
                "graph_route_parity_evidence.community_recent_memories.shadow_ready",
                "graph_route_parity_evidence.community_recent_memories.blocker_codes",
            ],
        ));
    }
    if !inputs.related_communities_parity_evidence_ready {
        actions.push(next_action(
            "run_related_communities_route_shadow_compare",
            "related communities graph route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.related_communities.protocol",
                "graph_route_parity_evidence.related_communities.present",
                "graph_route_parity_evidence.related_communities.ready",
                "graph_route_parity_evidence.related_communities.route",
                "graph_route_parity_evidence.related_communities.matches",
                "graph_route_parity_evidence.related_communities.primary_ready",
                "graph_route_parity_evidence.related_communities.shadow_ready",
                "graph_route_parity_evidence.related_communities.blocker_codes",
            ],
        ));
    }
    if !inputs.graph_analysis_parity_evidence_ready {
        actions.push(next_action(
            "run_graph_analysis_route_shadow_compare",
            "graph-analysis route parity evidence is missing or not ready",
            [
                "graph_route_parity_evidence.graph_analysis.protocol",
                "graph_route_parity_evidence.graph_analysis.present",
                "graph_route_parity_evidence.graph_analysis.ready",
                "graph_route_parity_evidence.graph_analysis.route",
                "graph_route_parity_evidence.graph_analysis.matches",
                "graph_route_parity_evidence.graph_analysis.primary_ready",
                "graph_route_parity_evidence.graph_analysis.shadow_ready",
                "graph_route_parity_evidence.graph_analysis.blocker_codes",
            ],
        ));
    }
    if json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]) == Some(true)
        && json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_ready"]) != Some(true)
    {
        actions.push(next_action(
            "attach_storage_recovery_report",
            "required storage recovery evidence is missing or blocked",
            [
                "cutover_evidence.storage_recovery_present",
                "cutover_evidence.storage_recovery_ready",
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
    if json_get_bool_path(bundle, &["cutover_evidence", "storage_recovery_required"]) == Some(true)
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
    if bundle.get("bounded_read_evidence").is_none()
        && json_get_path(bundle, &["cutover_evidence", "bounded_read_evidence"]).is_none()
    {
        missing.push("bounded_read_evidence".to_string());
    } else if !bounded_read_evidence_summary(bundle).ready {
        missing.push("bounded_read_evidence_ready".to_string());
    }
    if bundle.get("augmentation_state_parity_evidence").is_none()
        && json_get_path(
            bundle,
            &["graph_route_parity_evidence", "augmentation_state"],
        )
        .is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "augmentation_state_parity_evidence"],
        )
        .is_none()
    {
        missing.push("augmentation_state_parity_evidence".to_string());
    } else if !augmentation_state_parity_evidence_summary(bundle).ready {
        missing.push("augmentation_state_parity_evidence_ready".to_string());
    }
    if bundle.get("pagerank_plan_parity_evidence").is_none()
        && json_get_path(bundle, &["graph_route_parity_evidence", "pagerank_plan"]).is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "pagerank_plan_parity_evidence"],
        )
        .is_none()
    {
        missing.push("pagerank_plan_parity_evidence".to_string());
    } else if !pagerank_plan_parity_evidence_summary(bundle).ready {
        missing.push("pagerank_plan_parity_evidence_ready".to_string());
    }
    if bundle.get("overview_parity_evidence").is_none()
        && json_get_path(bundle, &["graph_route_parity_evidence", "overview"]).is_none()
        && json_get_path(bundle, &["cutover_evidence", "overview_parity_evidence"]).is_none()
    {
        missing.push("overview_parity_evidence".to_string());
    } else if !overview_parity_evidence_summary(bundle).ready {
        missing.push("overview_parity_evidence_ready".to_string());
    }
    if bundle.get("graph_search_parity_evidence").is_none()
        && json_get_path(bundle, &["graph_route_parity_evidence", "graph_search"]).is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "graph_search_parity_evidence"],
        )
        .is_none()
    {
        missing.push("graph_search_parity_evidence".to_string());
    } else if !graph_search_parity_evidence_summary(bundle).ready {
        missing.push("graph_search_parity_evidence_ready".to_string());
    }
    if bundle.get("explore_parity_evidence").is_none()
        && json_get_path(bundle, &["graph_route_parity_evidence", "explore"]).is_none()
        && json_get_path(bundle, &["cutover_evidence", "explore_parity_evidence"]).is_none()
    {
        missing.push("explore_parity_evidence".to_string());
    } else if !explore_parity_evidence_summary(bundle).ready {
        missing.push("explore_parity_evidence_ready".to_string());
    }
    if bundle.get("expand_parity_evidence").is_none()
        && json_get_path(bundle, &["graph_route_parity_evidence", "expand"]).is_none()
        && json_get_path(bundle, &["cutover_evidence", "expand_parity_evidence"]).is_none()
    {
        missing.push("expand_parity_evidence".to_string());
    } else if !expand_parity_evidence_summary(bundle).ready {
        missing.push("expand_parity_evidence_ready".to_string());
    }
    if bundle.get("live_preview_parity_evidence").is_none()
        && json_get_path(bundle, &["graph_route_parity_evidence", "live_preview"]).is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "live_preview_parity_evidence"],
        )
        .is_none()
    {
        missing.push("live_preview_parity_evidence".to_string());
    } else if !live_preview_parity_evidence_summary(bundle).ready {
        missing.push("live_preview_parity_evidence_ready".to_string());
    }
    if bundle.get("live_preview_node_parity_evidence").is_none()
        && json_get_path(
            bundle,
            &["graph_route_parity_evidence", "live_preview_node"],
        )
        .is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "live_preview_node_parity_evidence"],
        )
        .is_none()
    {
        missing.push("live_preview_node_parity_evidence".to_string());
    } else if !live_preview_node_parity_evidence_summary(bundle).ready {
        missing.push("live_preview_node_parity_evidence_ready".to_string());
    }
    if bundle.get("node_details_parity_evidence").is_none()
        && json_get_path(bundle, &["graph_route_parity_evidence", "node_details"]).is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "node_details_parity_evidence"],
        )
        .is_none()
    {
        missing.push("node_details_parity_evidence".to_string());
    } else if !node_details_parity_evidence_summary(bundle).ready {
        missing.push("node_details_parity_evidence_ready".to_string());
    }
    if bundle.get("source_detail_parity_evidence").is_none()
        && json_get_path(bundle, &["graph_route_parity_evidence", "source_detail"]).is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "source_detail_parity_evidence"],
        )
        .is_none()
    {
        missing.push("source_detail_parity_evidence".to_string());
    } else if !source_detail_parity_evidence_summary(bundle).ready {
        missing.push("source_detail_parity_evidence_ready".to_string());
    }
    if bundle.get("orphans_parity_evidence").is_none()
        && json_get_path(bundle, &["graph_route_parity_evidence", "orphans"]).is_none()
        && json_get_path(bundle, &["cutover_evidence", "orphans_parity_evidence"]).is_none()
    {
        missing.push("orphans_parity_evidence".to_string());
    } else if !orphans_parity_evidence_summary(bundle).ready {
        missing.push("orphans_parity_evidence_ready".to_string());
    }
    if bundle.get("shortest_path_parity_evidence").is_none()
        && json_get_path(bundle, &["graph_route_parity_evidence", "shortest_path"]).is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "shortest_path_parity_evidence"],
        )
        .is_none()
    {
        missing.push("shortest_path_parity_evidence".to_string());
    } else if !shortest_path_parity_evidence_summary(bundle).ready {
        missing.push("shortest_path_parity_evidence_ready".to_string());
    }
    if bundle.get("community_members_parity_evidence").is_none()
        && json_get_path(
            bundle,
            &["graph_route_parity_evidence", "community_members"],
        )
        .is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "community_members_parity_evidence"],
        )
        .is_none()
    {
        missing.push("community_members_parity_evidence".to_string());
    } else if !community_members_parity_evidence_summary(bundle).ready {
        missing.push("community_members_parity_evidence_ready".to_string());
    }
    if bundle.get("community_subgraph_parity_evidence").is_none()
        && json_get_path(
            bundle,
            &["graph_route_parity_evidence", "community_subgraph"],
        )
        .is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "community_subgraph_parity_evidence"],
        )
        .is_none()
    {
        missing.push("community_subgraph_parity_evidence".to_string());
    } else if !community_subgraph_parity_evidence_summary(bundle).ready {
        missing.push("community_subgraph_parity_evidence_ready".to_string());
    }
    if bundle
        .get("community_recent_memories_parity_evidence")
        .is_none()
        && json_get_path(
            bundle,
            &["graph_route_parity_evidence", "community_recent_memories"],
        )
        .is_none()
        && json_get_path(
            bundle,
            &[
                "cutover_evidence",
                "community_recent_memories_parity_evidence",
            ],
        )
        .is_none()
    {
        missing.push("community_recent_memories_parity_evidence".to_string());
    } else if !community_recent_memories_parity_evidence_summary(bundle).ready {
        missing.push("community_recent_memories_parity_evidence_ready".to_string());
    }
    if bundle.get("related_communities_parity_evidence").is_none()
        && json_get_path(
            bundle,
            &["graph_route_parity_evidence", "related_communities"],
        )
        .is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "related_communities_parity_evidence"],
        )
        .is_none()
    {
        missing.push("related_communities_parity_evidence".to_string());
    } else if !related_communities_parity_evidence_summary(bundle).ready {
        missing.push("related_communities_parity_evidence_ready".to_string());
    }
    if bundle.get("graph_analysis_parity_evidence").is_none()
        && json_get_path(bundle, &["graph_route_parity_evidence", "graph_analysis"]).is_none()
        && json_get_path(
            bundle,
            &["cutover_evidence", "graph_analysis_parity_evidence"],
        )
        .is_none()
    {
        missing.push("graph_analysis_parity_evidence".to_string());
    } else if !graph_analysis_parity_evidence_summary(bundle).ready {
        missing.push("graph_analysis_parity_evidence_ready".to_string());
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

    [
        "background_maintenance_executable_search_projection_graph_delta_count",
        "background_maintenance_admitted_search_projection_graph_delta_count",
        "background_maintenance_deferred_search_projection_graph_delta_count",
        "background_maintenance_rejected_search_projection_graph_delta_count",
        "background_maintenance_executable_search_projection_graph_delta_operations",
        "background_maintenance_admitted_search_projection_graph_delta_operations",
        "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
    ]
    .into_iter()
    .any(|field| json_get_u64_path_from_dynamic(bundle, &["cutover_evidence"], field).is_none())
}

fn nowledge_replacement_blockers(bundle: &serde_json::Value) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    for blocker in
        json_string_array(&search_projection_shadow_evidence_summary(bundle).blocker_codes)
    {
        blockers.insert(blocker);
    }
    for blocker in
        json_string_array(&search_candidate_shadow_evidence_summary(bundle).blocker_codes)
    {
        blockers.insert(blocker);
    }
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
        &["bounded_read_evidence", "blocker_codes"][..],
        &["cutover_evidence", "bounded_read_evidence", "blocker_codes"][..],
        &["augmentation_state_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "augmentation_state",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "augmentation_state_parity_evidence",
            "blocker_codes",
        ][..],
        &["pagerank_plan_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "pagerank_plan",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "pagerank_plan_parity_evidence",
            "blocker_codes",
        ][..],
        &["overview_parity_evidence", "blocker_codes"][..],
        &["graph_route_parity_evidence", "overview", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "overview_parity_evidence",
            "blocker_codes",
        ][..],
        &["graph_search_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "graph_search",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "graph_search_parity_evidence",
            "blocker_codes",
        ][..],
        &["explore_parity_evidence", "blocker_codes"][..],
        &["graph_route_parity_evidence", "explore", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "explore_parity_evidence",
            "blocker_codes",
        ][..],
        &["expand_parity_evidence", "blocker_codes"][..],
        &["graph_route_parity_evidence", "expand", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "expand_parity_evidence",
            "blocker_codes",
        ][..],
        &["live_preview_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "live_preview",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "live_preview_parity_evidence",
            "blocker_codes",
        ][..],
        &["live_preview_node_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "live_preview_node",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "live_preview_node_parity_evidence",
            "blocker_codes",
        ][..],
        &["node_details_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "node_details",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "node_details_parity_evidence",
            "blocker_codes",
        ][..],
        &["source_detail_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "source_detail",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "source_detail_parity_evidence",
            "blocker_codes",
        ][..],
        &["orphans_parity_evidence", "blocker_codes"][..],
        &["graph_route_parity_evidence", "orphans", "blocker_codes"][..],
        &[
            "cutover_evidence",
            "orphans_parity_evidence",
            "blocker_codes",
        ][..],
        &["shortest_path_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "shortest_path",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "shortest_path_parity_evidence",
            "blocker_codes",
        ][..],
        &["community_members_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "community_members",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "community_members_parity_evidence",
            "blocker_codes",
        ][..],
        &["community_subgraph_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "community_subgraph",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "community_subgraph_parity_evidence",
            "blocker_codes",
        ][..],
        &["community_recent_memories_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "community_recent_memories",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "community_recent_memories_parity_evidence",
            "blocker_codes",
        ][..],
        &["related_communities_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "related_communities",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "related_communities_parity_evidence",
            "blocker_codes",
        ][..],
        &["graph_analysis_parity_evidence", "blocker_codes"][..],
        &[
            "graph_route_parity_evidence",
            "graph_analysis",
            "blocker_codes",
        ][..],
        &[
            "cutover_evidence",
            "graph_analysis_parity_evidence",
            "blocker_codes",
        ][..],
    ] {
        for blocker in json_get_string_array_path(bundle, path) {
            blockers.insert(blocker);
        }
    }
    blockers.into_iter().collect()
}

fn json_get_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn json_get_u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    json_get_path(value, path).and_then(serde_json::Value::as_u64)
}

fn json_get_bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    json_get_path(value, path).and_then(serde_json::Value::as_bool)
}

fn json_get_str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    json_get_path(value, path).and_then(serde_json::Value::as_str)
}

fn json_get_bool_path_from_dynamic(
    value: &serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<bool> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_bool)
}

fn json_get_str_path_from_dynamic<'a>(
    value: &'a serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<&'a str> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_str)
}

fn json_get_u64_path_from_dynamic(
    value: &serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<u64> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_u64)
}

fn json_get_path_from_dynamic<'a>(
    value: &'a serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in prefix {
        current = current.get(*key)?;
    }
    current.get(field)
}

fn json_get_array_path(value: &serde_json::Value, path: &[&str]) -> serde_json::Value {
    json_get_path(value, path)
        .filter(|value| value.is_array())
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

fn json_get_string_array_path(value: &serde_json::Value, path: &[&str]) -> Vec<String> {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.as_str().map(str::to_string))
        .collect()
}

fn json_string_array(value: &serde_json::Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| item.as_str().map(str::to_string))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_replacement_summary_json, nowledge_replacement_summary_json_with_options,
        nowledge_replacement_summary_usage, NowledgeReplacementSummaryOptions,
        SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY,
        SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING,
    };
    use skein::nowledge_contract::{
        SEARCH_CANDIDATE_SHADOW_EVIDENCE_ROUTE, SEARCH_CANDIDATE_SHADOW_EVIDENCE_SOURCE,
        SEARCH_PROJECTION_EVIDENCE_SOURCE, SEARCH_PROJECTION_SHADOW_EVIDENCE_ROUTE,
        SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
        SKEIN_NOWLEDGE_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
        SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL,
        SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL,
    };

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
                "augmentation_state_parity_evidence",
                "bounded_read_evidence",
                "community_members_parity_evidence",
                "community_recent_memories_parity_evidence",
                "community_subgraph_parity_evidence",
                "cutover_evidence",
                "dual_engine_evidence",
                "expand_parity_evidence",
                "explore_parity_evidence",
                "graph_analysis_parity_evidence",
                "graph_search_parity_evidence",
                "live_preview_node_parity_evidence",
                "live_preview_parity_evidence",
                "node_details_parity_evidence",
                "orphans_parity_evidence",
                "overview_parity_evidence",
                "pagerank_plan_parity_evidence",
                "previous_wrapper_contract",
                "query_family_readiness",
                "related_communities_parity_evidence",
                "search_candidate_shadow_evidence",
                "search_projection_evidence",
                "search_projection_shadow_evidence",
                "shadow_parity",
                "shortest_path_parity_evidence",
                "source_detail_parity_evidence"
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
        assert_eq!(summary["blocking_categories"], serde_json::json!([]));
        assert_eq!(summary["missing_evidence"], serde_json::json!([]));
        assert_eq!(summary["next_actions"], serde_json::json!([]));
        assert_eq!(summary["bounded_read_evidence"]["present"], true);
        assert_eq!(summary["bounded_read_evidence"]["ready"], true);
        assert_eq!(summary["bounded_read_evidence"]["mode"], "shadow_read_only");
        assert_eq!(summary["bounded_read_evidence"]["execution_row_cap"], 513);
        assert_eq!(
            summary["bounded_read_evidence"]["query_runtime_report_count"],
            17
        );
        assert_eq!(
            summary["bounded_read_evidence"]["query_runtime_plan_report_count"],
            17
        );
        assert_eq!(
            summary["bounded_read_evidence"]["query_runtime_profile_report_count"],
            17
        );
        assert_eq!(
            summary["bounded_read_evidence"]["query_runtime_failed_query_count"],
            0
        );
        assert_eq!(
            summary["graph_route_parity_evidence"]["overview"]["ready"],
            true
        );
        assert_eq!(
            summary["graph_route_parity_evidence"]["overview"]["route"],
            "/graph/overview"
        );
        assert_eq!(
            summary["graph_route_parity_evidence"]["explore"]["ready"],
            true
        );
        assert_eq!(
            summary["graph_route_parity_evidence"]["explore"]["route"],
            "/graph/explore"
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
            summary["cutover_evidence"]
                ["background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"],
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
        assert_eq!(summary["dual_engine_evidence"]["primary_engine"], "skein");
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
            summary["search_projection_shadow_evidence"]["route"],
            SEARCH_PROJECTION_SHADOW_EVIDENCE_ROUTE
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["primary_engine"],
            "lancedb"
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["shadow_engine"],
            "skein"
        );
        assert_eq!(
            summary["search_projection_shadow_evidence"]["table_parity_ready"],
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
        assert_eq!(summary["search_candidate_shadow_evidence"]["present"], true);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], true);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["route"],
            SEARCH_CANDIDATE_SHADOW_EVIDENCE_ROUTE
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["row_count_parity"],
            true
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_filter_pushdown_ready"],
            true
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"],
            true
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_reduction_ready"],
            true
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_field_summary_count"],
            2
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_pushed_predicate_count"],
            2
        );
        assert_eq!(
            summary["replacement_readiness_by_query_family"][0]["query_family"],
            "memory_lookup"
        );
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
    fn replacement_summary_accepts_search_candidate_primary_evidence() {
        let mut bundle = production_ready_bundle();
        bundle["search_candidate_shadow_evidence"] = ready_search_candidate_primary_evidence();

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], true);
        assert_eq!(summary["search_candidate_shadow_evidence"]["present"], true);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], true);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["engine"],
            "skein-primary"
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["candidate_primary_engine"],
            "skein"
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["primary_engine"],
            "skein"
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_filter_pushdown_ready"],
            false
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_reduction_ready"],
            true
        );
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
                        .any(|field| {
                            field == "search_projection_shadow_evidence.table_parity.ready"
                        })
            }));
    }

    #[test]
    fn replacement_summary_recomputes_search_projection_shadow_evidence_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_projection_shadow_evidence"]["table_parity"]["ready"] =
            serde_json::json!(false);
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
                .any(|code| code == SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING)
        );
        assert!(summary["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == SEARCH_PROJECTION_SHADOW_PUSHDOWN_NOT_READY));
        assert!(summary["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == SKEIN_SEARCH_PROJECTION_SEGMENT_DESCRIPTOR_MISSING));
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
                        .any(|field| {
                            field
                                == "search_projection_shadow_evidence.pushdown_evidence.shadow_persisted_segment_descriptor_ready"
                        })
            }));
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
    fn replacement_summary_requires_search_projection_shadow_route_and_engines() {
        let mut bundle = production_ready_bundle();
        bundle["search_projection_shadow_evidence"]["route"] =
            serde_json::json!("/search-index/legacy-shadow/evidence");
        bundle["search_projection_shadow_evidence"]["primary_engine"] = serde_json::json!("sqlite");
        bundle["search_projection_shadow_evidence"]["shadow_engine"] = serde_json::json!("custom");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["search_projection_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_projection_shadow_evidence"]["route"],
            "/search-index/legacy-shadow/evidence"
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
                        .any(|field| field == "search_projection_shadow_evidence.route")
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
            .any(|action| {
                action["action"] == "run_search_candidate_shadow_compare"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| {
                            field
                                == "search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready"
                        })
            }));
    }

    #[test]
    fn replacement_summary_recomputes_search_candidate_shadow_evidence_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["search_candidate_shadow_evidence"]["ready"] = serde_json::json!(true);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["shadow_scan"]
            ["metadata_predicate_pushdown"]["residual_predicate_count"] = serde_json::json!(1);
        bundle["search_candidate_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["candidate_filter_residual"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["production_replacement_per_million"], 0);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_filter_pushdown_ready"],
            false
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_residual_predicate_count"],
            1
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "search_candidate_shadow_evidence_ready"));
        assert!(summary["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "candidate_filter_residual"));
    }

    #[test]
    fn replacement_summary_requires_search_candidate_shadow_field_pruning_evidence() {
        let mut bundle = production_ready_bundle();
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["shadow_scan"]
            ["metadata_predicate_pushdown"]
            .as_object_mut()
            .unwrap()
            .remove("field_summaries");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_filter_pushdown_ready"],
            true
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"],
            false
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_field_summary_count"],
            serde_json::Value::Null
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
                action["action"] == "run_search_candidate_shadow_compare"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| {
                            field
                                == "search_candidate_shadow_evidence.shadow_scan_field_pruning_ready"
                        })
            }));
    }

    #[test]
    fn replacement_summary_requires_search_candidate_shadow_scan_reduction_evidence() {
        let mut bundle = production_ready_bundle();
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["shadow_scan"]
            ["filtered_out_count"] = serde_json::json!(0);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["shadow_scan"]
            ["filtered_document_count"] = serde_json::json!(12);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["shadow_scan"]
            ["candidate_set_cardinality"] = serde_json::json!(12);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["shadow_scan"]
            ["metadata_predicate_pushdown"]["pruned_document_count"] = serde_json::json!(0);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["shadow_scan"]
            ["metadata_predicate_pushdown"]["scanned_document_count"] = serde_json::json!(12);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_filter_pushdown_ready"],
            true
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_field_pruning_ready"],
            true
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_reduction_ready"],
            false
        );
        assert!(summary["search_candidate_shadow_evidence"]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "skein_search_scan_reduction_evidence_missing"));
        assert!(summary["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "skein_search_scan_reduction_evidence_missing"));
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
                action["action"] == "run_search_candidate_shadow_compare"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| {
                            field == "search_candidate_shadow_evidence.shadow_scan_reduction_ready"
                        })
            }));
    }

    #[test]
    fn replacement_summary_accepts_search_candidate_shadow_dynamic_descriptor() {
        let mut bundle = production_ready_bundle();
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["shadow_scan"]
            ["metadata_predicate_pushdown"]["segment_descriptor_bounded"] =
            serde_json::json!(false);
        bundle["search_candidate_shadow_evidence"]["filter_pushdown"]["shadow_scan"]
            ["metadata_predicate_pushdown"]["segment_descriptor_field_count"] =
            serde_json::json!(64);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], true);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], true);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_reduction_ready"],
            true
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_descriptor_bounded_ready"],
            false
        );
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["shadow_scan_descriptor_field_count"],
            64
        );
        assert!(summary["search_candidate_shadow_evidence"]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|code| code != "skein_search_descriptor_bounds_evidence_missing"));
        assert!(summary["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .all(|code| code != "skein_search_descriptor_bounds_evidence_missing"));
    }

    #[test]
    fn replacement_summary_requires_search_candidate_shadow_evidence_protocol() {
        let mut bundle = production_ready_bundle();
        bundle["search_candidate_shadow_evidence"]["protocol"] = serde_json::json!("handwritten");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["protocol"],
            "handwritten"
        );
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "run_search_candidate_shadow_compare"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "search_candidate_shadow_evidence.protocol")
            }));
    }

    #[test]
    fn replacement_summary_requires_search_candidate_shadow_route_and_engines() {
        let mut bundle = production_ready_bundle();
        bundle["search_candidate_shadow_evidence"]["route"] =
            serde_json::json!("/search-index/legacy-shadow/candidate-evidence");
        bundle["search_candidate_shadow_evidence"]["primary_engine"] = serde_json::json!("sqlite");
        bundle["search_candidate_shadow_evidence"]["shadow_engine"] = serde_json::json!("custom");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["search_candidate_shadow_evidence"]["ready"], false);
        assert_eq!(
            summary["search_candidate_shadow_evidence"]["route"],
            "/search-index/legacy-shadow/candidate-evidence"
        );
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| {
                action["action"] == "run_search_candidate_shadow_compare"
                    && action["evidence_fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|field| field == "search_candidate_shadow_evidence.route")
            }));
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
        bundle["bounded_read_evidence"]["blocker_codes"] =
            serde_json::json!(["not_shadow_read_only"]);

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
    fn replacement_summary_requires_bounded_read_route_coverage() {
        let mut bundle = production_ready_bundle();
        bundle["bounded_read_evidence"]["covered_routes"] = serde_json::json!([
            "/graph/overview",
            "/graph/search",
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
            "/sources/{source_id}",
            "/graph/orphans",
            "/graph/shortest-path"
        ]);

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
    fn replacement_summary_requires_bounded_read_primary_route_coverage() {
        let mut bundle = production_ready_bundle();
        bundle["bounded_read_evidence"]["route_primary_ready"] = serde_json::json!(false);
        bundle["bounded_read_evidence"]["primary_ready_routes"] = serde_json::json!([
            "/graph/overview",
            "/graph/search",
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
            "/sources/{source_id}",
            "/graph/orphans",
            "/graph/shortest-path"
        ]);
        bundle["bounded_read_evidence"]["missing_primary_routes"] =
            serde_json::json!(["/graph/explore"]);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(summary["bounded_read_evidence"]["ready"], false);
        assert_eq!(
            summary["bounded_read_evidence"]["missing_primary_routes"],
            serde_json::json!(["/graph/explore"])
        );
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "bounded_read_evidence"));
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
                        .any(|field| field == "bounded_read_evidence.primary_ready_routes")
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
    fn replacement_summary_blocks_production_without_overview_parity_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("overview_parity_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["overview"]["present"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "overview_parity_evidence"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "overview_parity_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_overview_route_shadow_compare"));
    }

    #[test]
    fn replacement_summary_recomputes_overview_parity_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["overview_parity_evidence"]["matches"] = serde_json::json!(false);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["overview"]["ready"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "overview_parity_evidence_ready"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_explore_parity_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("explore_parity_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["explore"]["present"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "explore_parity_evidence"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "explore_parity_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_explore_route_shadow_compare"));
    }

    #[test]
    fn replacement_summary_recomputes_explore_parity_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["explore_parity_evidence"]["matches"] = serde_json::json!(false);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["explore"]["ready"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "explore_parity_evidence_ready"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_expand_parity_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("expand_parity_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["expand"]["present"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "expand_parity_evidence"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "expand_parity_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_expand_route_shadow_compare"));
    }

    #[test]
    fn replacement_summary_recomputes_expand_parity_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["expand_parity_evidence"]["matches"] = serde_json::json!(false);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["expand"]["ready"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "expand_parity_evidence_ready"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_live_preview_parity_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("live_preview_parity_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["live_preview"]["present"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "live_preview_parity_evidence"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "live_preview_parity_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_live_preview_route_shadow_compare"));
    }

    #[test]
    fn replacement_summary_recomputes_live_preview_parity_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["live_preview_parity_evidence"]["matches"] = serde_json::json!(false);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["live_preview"]["ready"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "live_preview_parity_evidence_ready"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_live_preview_node_parity_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("live_preview_node_parity_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["live_preview_node"]["present"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "live_preview_node_parity_evidence"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "live_preview_node_parity_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_live_preview_node_route_shadow_compare"));
    }

    #[test]
    fn replacement_summary_recomputes_live_preview_node_parity_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["live_preview_node_parity_evidence"]["matches"] = serde_json::json!(false);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["live_preview_node"]["ready"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "live_preview_node_parity_evidence_ready"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_node_details_parity_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("node_details_parity_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["node_details"]["present"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "node_details_parity_evidence"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "node_details_parity_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_node_details_route_shadow_compare"));
    }

    #[test]
    fn replacement_summary_recomputes_node_details_parity_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["node_details_parity_evidence"]["matches"] = serde_json::json!(false);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["node_details"]["ready"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "node_details_parity_evidence_ready"));
    }

    #[test]
    fn replacement_summary_blocks_production_without_orphans_parity_evidence() {
        let mut bundle = production_ready_bundle();
        bundle
            .as_object_mut()
            .unwrap()
            .remove("orphans_parity_evidence");

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["orphans"]["present"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "orphans_parity_evidence"));
        assert!(summary["blocking_categories"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "orphans_parity_evidence"));
        assert!(summary["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_orphans_route_shadow_compare"));
    }

    #[test]
    fn replacement_summary_recomputes_orphans_parity_readiness() {
        let mut bundle = production_ready_bundle();
        bundle["orphans_parity_evidence"]["matches"] = serde_json::json!(false);

        let summary = nowledge_replacement_summary_json(&bundle);

        assert_eq!(summary["production_cutover_ready"], false);
        assert_eq!(
            summary["graph_route_parity_evidence"]["orphans"]["ready"],
            false
        );
        assert!(summary["missing_evidence"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "orphans_parity_evidence_ready"));
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
            4
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["ready_count"],
            4
        );
        assert_eq!(
            summary["replacement_readiness_family_summary"]["omitted_count"],
            4
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
        assert_eq!(summary["blocker_summary"]["total_count"], 7);
        assert_eq!(summary["blocker_summary"]["omitted_count"], 7);
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
        assert_eq!(summary["blocker_summary"]["total_count"], 7);
        assert_eq!(summary["blocker_summary"]["omitted_count"], 5);
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
                "augmentation_state_parity_evidence",
                "background_maintenance",
                "bounded_read_evidence",
                "community_members_parity_evidence",
                "community_recent_memories_parity_evidence",
                "community_subgraph_parity_evidence",
                "cutover_evidence",
                "dual_engine_evidence",
                "expand_parity_evidence",
                "explore_parity_evidence",
                "graph_analysis_parity_evidence",
                "graph_search_parity_evidence",
                "live_preview_node_parity_evidence",
                "live_preview_parity_evidence",
                "migration_gate",
                "node_details_parity_evidence",
                "orphans_parity_evidence",
                "overview_parity_evidence",
                "pagerank_plan_parity_evidence",
                "previous_wrapper_contract",
                "query_family_readiness",
                "related_communities_parity_evidence",
                "search_candidate_shadow_evidence",
                "search_projection_evidence",
                "search_projection_shadow_evidence",
                "shadow_parity",
                "shortest_path_parity_evidence",
                "source_detail_parity_evidence",
                "storage_recovery"
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
                "search_projection_evidence",
                "search_projection_shadow_evidence",
                "search_candidate_shadow_evidence",
                "bounded_read_evidence",
                "augmentation_state_parity_evidence",
                "pagerank_plan_parity_evidence",
                "overview_parity_evidence",
                "graph_search_parity_evidence",
                "explore_parity_evidence",
                "expand_parity_evidence",
                "live_preview_parity_evidence",
                "live_preview_node_parity_evidence",
                "node_details_parity_evidence",
                "source_detail_parity_evidence",
                "orphans_parity_evidence",
                "shortest_path_parity_evidence",
                "community_members_parity_evidence",
                "community_subgraph_parity_evidence",
                "community_recent_memories_parity_evidence",
                "related_communities_parity_evidence",
                "graph_analysis_parity_evidence",
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
                    "action": "attach_search_projection_replacement_evidence",
                    "reason": "LanceDB replacement evidence is missing or not ready",
                    "evidence_fields": [
                        "search_projection_evidence.protocol",
                        "search_projection_evidence.evidence_source",
                        "search_projection_evidence.present",
                        "search_projection_evidence.ready",
                        "search_projection_evidence.derived_projection",
                        "search_projection_evidence.all_tables_covered",
                        "search_projection_evidence.covered_table_count",
                        "search_projection_evidence.required_table_count",
                        "search_projection_evidence.fts_ready",
                        "search_projection_evidence.vector_ready",
                        "search_projection_evidence.embedding_identity_ready",
                        "search_projection_evidence.fail_soft_ready",
                        "search_projection_evidence.rebuild_marker_ready",
                        "search_projection_evidence.metadata_repair_marker_ready",
                        "search_projection_evidence.incremental_update_ready",
                        "search_projection_evidence.source_chunk_ready",
                        "search_projection_evidence.predicate_pushdown_ready",
                        "search_projection_evidence.compressed_vector_projection_required",
                        "search_projection_evidence.compressed_vector_projection_ready",
                        "search_projection_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "run_search_projection_shadow_evidence",
                    "reason": "LanceDB/Skein search projection side-by-side evidence is missing or not ready",
                    "evidence_fields": [
                        "search_projection_shadow_evidence.protocol",
                        "search_projection_shadow_evidence.evidence_source",
                        "search_projection_shadow_evidence.route",
                        "search_projection_shadow_evidence.present",
                        "search_projection_shadow_evidence.ready",
                        "search_projection_shadow_evidence.primary_engine",
                        "search_projection_shadow_evidence.shadow_engine",
                        "search_projection_shadow_evidence.primary_ready",
                        "search_projection_shadow_evidence.shadow_ready",
                        "search_projection_shadow_evidence.document_count_parity",
                        "search_projection_shadow_evidence.table_parity.ready",
                        "search_projection_shadow_evidence.embedding_identity_parity",
                        "search_projection_shadow_evidence.lifecycle_parity",
                        "search_projection_shadow_evidence.incremental_watermark_parity",
                        "search_projection_shadow_evidence.predicate_pushdown_parity",
                        "search_projection_shadow_evidence.pushdown_evidence.ready",
                        "search_projection_shadow_evidence.pushdown_evidence.primary_predicate_pushdown_ready",
                        "search_projection_shadow_evidence.pushdown_evidence.shadow_predicate_pushdown_ready",
                        "search_projection_shadow_evidence.pushdown_evidence.shadow_persisted_segment_descriptor_ready",
                        "search_projection_shadow_evidence.pushdown_evidence.primary_scan_filter_fields",
                        "search_projection_shadow_evidence.pushdown_evidence.shadow_scan_filter_fields",
                        "search_projection_shadow_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "run_search_candidate_shadow_compare",
                    "reason": "LanceDB/Skein candidate shadow evidence is missing or lacks scan-filter pushdown proof",
                    "evidence_fields": [
                        "search_candidate_shadow_evidence.protocol",
                        "search_candidate_shadow_evidence.evidence_source",
                        "search_candidate_shadow_evidence.route",
                        "search_candidate_shadow_evidence.present",
                        "search_candidate_shadow_evidence.ready",
                        "search_candidate_shadow_evidence.reported_ready",
                        "search_candidate_shadow_evidence.engine",
                        "search_candidate_shadow_evidence.candidate_primary_engine",
                        "search_candidate_shadow_evidence.primary_engine",
                        "search_candidate_shadow_evidence.shadow_engine",
                        "search_candidate_shadow_evidence.row_count_parity",
                        "search_candidate_shadow_evidence.vector_top_k_overlap_ready",
                        "search_candidate_shadow_evidence.fts_top_k_overlap_ready",
                        "search_candidate_shadow_evidence.shadow_scan_present",
                        "search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready",
                        "search_candidate_shadow_evidence.shadow_scan_field_pruning_ready",
                        "search_candidate_shadow_evidence.shadow_scan_reduction_ready",
                        "search_candidate_shadow_evidence.shadow_scan_descriptor_bounded_ready",
                        "search_candidate_shadow_evidence.shadow_scan_descriptor_field_count",
                        "search_candidate_shadow_evidence.shadow_scan_field_summary_count",
                        "search_candidate_shadow_evidence.shadow_scan_input_predicate_count",
                        "search_candidate_shadow_evidence.shadow_scan_pushed_predicate_count",
                        "search_candidate_shadow_evidence.shadow_scan_residual_predicate_count",
                        "search_candidate_shadow_evidence.shadow_scan_pruned_document_count",
                        "search_candidate_shadow_evidence.shadow_scan_scanned_document_count",
                        "search_candidate_shadow_evidence.shadow_scan_parse_error",
                        "search_candidate_shadow_evidence.shadow_scan_unsatisfiable",
                        "search_candidate_shadow_evidence.blocker_codes"
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
                        "bounded_read_evidence.row_limit_enforced_before_output",
                        "bounded_read_evidence.operator_row_cap_enabled",
                        "bounded_read_evidence.blocking_operator_count",
                        "bounded_read_evidence.covered_routes",
                        "bounded_read_evidence.route_primary_ready",
                        "bounded_read_evidence.query_runtime_report_count",
                        "bounded_read_evidence.query_runtime_plan_report_count",
                        "bounded_read_evidence.query_runtime_profile_report_count",
                        "bounded_read_evidence.query_runtime_failed_query_count",
                        "bounded_read_evidence.query_runtime_missing_plan_evidence_count",
                        "bounded_read_evidence.query_runtime_missing_profile_evidence_count",
                        "bounded_read_evidence.primary_ready_routes",
                        "bounded_read_evidence.missing_primary_routes",
                        "bounded_read_evidence.blocker_codes"
                    ]
                },
                {
                    "action": "run_augmentation_state_route_shadow_compare",
                    "reason": "augmentation-state graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.augmentation_state.protocol",
                        "graph_route_parity_evidence.augmentation_state.present",
                        "graph_route_parity_evidence.augmentation_state.ready",
                        "graph_route_parity_evidence.augmentation_state.route",
                        "graph_route_parity_evidence.augmentation_state.matches",
                        "graph_route_parity_evidence.augmentation_state.primary_ready",
                        "graph_route_parity_evidence.augmentation_state.shadow_ready",
                        "graph_route_parity_evidence.augmentation_state.blocker_codes"
                    ]
                },
                {
                    "action": "run_pagerank_plan_route_shadow_compare",
                    "reason": "pagerank-plan graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.pagerank_plan.protocol",
                        "graph_route_parity_evidence.pagerank_plan.present",
                        "graph_route_parity_evidence.pagerank_plan.ready",
                        "graph_route_parity_evidence.pagerank_plan.route",
                        "graph_route_parity_evidence.pagerank_plan.matches",
                        "graph_route_parity_evidence.pagerank_plan.primary_ready",
                        "graph_route_parity_evidence.pagerank_plan.shadow_ready",
                        "graph_route_parity_evidence.pagerank_plan.blocker_codes"
                    ]
                },
                {
                    "action": "run_overview_route_shadow_compare",
                    "reason": "overview graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.overview.protocol",
                        "graph_route_parity_evidence.overview.present",
                        "graph_route_parity_evidence.overview.ready",
                        "graph_route_parity_evidence.overview.route",
                        "graph_route_parity_evidence.overview.matches",
                        "graph_route_parity_evidence.overview.primary_ready",
                        "graph_route_parity_evidence.overview.shadow_ready",
                        "graph_route_parity_evidence.overview.blocker_codes"
                    ]
                },
                {
                    "action": "run_graph_search_route_shadow_compare",
                    "reason": "graph-search route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.graph_search.protocol",
                        "graph_route_parity_evidence.graph_search.present",
                        "graph_route_parity_evidence.graph_search.ready",
                        "graph_route_parity_evidence.graph_search.route",
                        "graph_route_parity_evidence.graph_search.matches",
                        "graph_route_parity_evidence.graph_search.primary_ready",
                        "graph_route_parity_evidence.graph_search.shadow_ready",
                        "graph_route_parity_evidence.graph_search.blocker_codes"
                    ]
                },
                {
                    "action": "run_explore_route_shadow_compare",
                    "reason": "explore graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.explore.protocol",
                        "graph_route_parity_evidence.explore.present",
                        "graph_route_parity_evidence.explore.ready",
                        "graph_route_parity_evidence.explore.route",
                        "graph_route_parity_evidence.explore.matches",
                        "graph_route_parity_evidence.explore.primary_ready",
                        "graph_route_parity_evidence.explore.shadow_ready",
                        "graph_route_parity_evidence.explore.blocker_codes"
                    ]
                },
                {
                    "action": "run_expand_route_shadow_compare",
                    "reason": "expand graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.expand.protocol",
                        "graph_route_parity_evidence.expand.present",
                        "graph_route_parity_evidence.expand.ready",
                        "graph_route_parity_evidence.expand.route",
                        "graph_route_parity_evidence.expand.matches",
                        "graph_route_parity_evidence.expand.primary_ready",
                        "graph_route_parity_evidence.expand.shadow_ready",
                        "graph_route_parity_evidence.expand.blocker_codes"
                    ]
                },
                {
                    "action": "run_live_preview_route_shadow_compare",
                    "reason": "live-preview graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.live_preview.protocol",
                        "graph_route_parity_evidence.live_preview.present",
                        "graph_route_parity_evidence.live_preview.ready",
                        "graph_route_parity_evidence.live_preview.route",
                        "graph_route_parity_evidence.live_preview.matches",
                        "graph_route_parity_evidence.live_preview.primary_ready",
                        "graph_route_parity_evidence.live_preview.shadow_ready",
                        "graph_route_parity_evidence.live_preview.blocker_codes"
                    ]
                },
                {
                    "action": "run_live_preview_node_route_shadow_compare",
                    "reason": "live-preview node graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.live_preview_node.protocol",
                        "graph_route_parity_evidence.live_preview_node.present",
                        "graph_route_parity_evidence.live_preview_node.ready",
                        "graph_route_parity_evidence.live_preview_node.route",
                        "graph_route_parity_evidence.live_preview_node.matches",
                        "graph_route_parity_evidence.live_preview_node.primary_ready",
                        "graph_route_parity_evidence.live_preview_node.shadow_ready",
                        "graph_route_parity_evidence.live_preview_node.blocker_codes"
                    ]
                },
                {
                    "action": "run_node_details_route_shadow_compare",
                    "reason": "node-details graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.node_details.protocol",
                        "graph_route_parity_evidence.node_details.present",
                        "graph_route_parity_evidence.node_details.ready",
                        "graph_route_parity_evidence.node_details.route",
                        "graph_route_parity_evidence.node_details.matches",
                        "graph_route_parity_evidence.node_details.primary_ready",
                        "graph_route_parity_evidence.node_details.shadow_ready",
                        "graph_route_parity_evidence.node_details.blocker_codes"
                    ]
                },
                {
                    "action": "run_source_detail_route_shadow_compare",
                    "reason": "source-detail graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.source_detail.protocol",
                        "graph_route_parity_evidence.source_detail.present",
                        "graph_route_parity_evidence.source_detail.ready",
                        "graph_route_parity_evidence.source_detail.route",
                        "graph_route_parity_evidence.source_detail.matches",
                        "graph_route_parity_evidence.source_detail.primary_ready",
                        "graph_route_parity_evidence.source_detail.shadow_ready",
                        "graph_route_parity_evidence.source_detail.blocker_codes"
                    ]
                },
                {
                    "action": "run_orphans_route_shadow_compare",
                    "reason": "orphans graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.orphans.protocol",
                        "graph_route_parity_evidence.orphans.present",
                        "graph_route_parity_evidence.orphans.ready",
                        "graph_route_parity_evidence.orphans.route",
                        "graph_route_parity_evidence.orphans.matches",
                        "graph_route_parity_evidence.orphans.primary_ready",
                        "graph_route_parity_evidence.orphans.shadow_ready",
                        "graph_route_parity_evidence.orphans.blocker_codes"
                    ]
                },
                {
                    "action": "run_shortest_path_route_shadow_compare",
                    "reason": "shortest-path graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.shortest_path.protocol",
                        "graph_route_parity_evidence.shortest_path.present",
                        "graph_route_parity_evidence.shortest_path.ready",
                        "graph_route_parity_evidence.shortest_path.route",
                        "graph_route_parity_evidence.shortest_path.matches",
                        "graph_route_parity_evidence.shortest_path.primary_ready",
                        "graph_route_parity_evidence.shortest_path.shadow_ready",
                        "graph_route_parity_evidence.shortest_path.blocker_codes"
                    ]
                },
                {
                    "action": "run_community_members_route_shadow_compare",
                    "reason": "community-members graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.community_members.protocol",
                        "graph_route_parity_evidence.community_members.present",
                        "graph_route_parity_evidence.community_members.ready",
                        "graph_route_parity_evidence.community_members.route",
                        "graph_route_parity_evidence.community_members.matches",
                        "graph_route_parity_evidence.community_members.primary_ready",
                        "graph_route_parity_evidence.community_members.shadow_ready",
                        "graph_route_parity_evidence.community_members.blocker_codes"
                    ]
                },
                {
                    "action": "run_community_subgraph_route_shadow_compare",
                    "reason": "community-subgraph graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.community_subgraph.protocol",
                        "graph_route_parity_evidence.community_subgraph.present",
                        "graph_route_parity_evidence.community_subgraph.ready",
                        "graph_route_parity_evidence.community_subgraph.route",
                        "graph_route_parity_evidence.community_subgraph.matches",
                        "graph_route_parity_evidence.community_subgraph.primary_ready",
                        "graph_route_parity_evidence.community_subgraph.shadow_ready",
                        "graph_route_parity_evidence.community_subgraph.blocker_codes"
                    ]
                },
                {
                    "action": "run_community_recent_memories_route_shadow_compare",
                    "reason": "community recent memories graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.community_recent_memories.protocol",
                        "graph_route_parity_evidence.community_recent_memories.present",
                        "graph_route_parity_evidence.community_recent_memories.ready",
                        "graph_route_parity_evidence.community_recent_memories.route",
                        "graph_route_parity_evidence.community_recent_memories.matches",
                        "graph_route_parity_evidence.community_recent_memories.primary_ready",
                        "graph_route_parity_evidence.community_recent_memories.shadow_ready",
                        "graph_route_parity_evidence.community_recent_memories.blocker_codes"
                    ]
                },
                {
                    "action": "run_related_communities_route_shadow_compare",
                    "reason": "related communities graph route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.related_communities.protocol",
                        "graph_route_parity_evidence.related_communities.present",
                        "graph_route_parity_evidence.related_communities.ready",
                        "graph_route_parity_evidence.related_communities.route",
                        "graph_route_parity_evidence.related_communities.matches",
                        "graph_route_parity_evidence.related_communities.primary_ready",
                        "graph_route_parity_evidence.related_communities.shadow_ready",
                        "graph_route_parity_evidence.related_communities.blocker_codes"
                    ]
                },
                {
                    "action": "run_graph_analysis_route_shadow_compare",
                    "reason": "graph-analysis route parity evidence is missing or not ready",
                    "evidence_fields": [
                        "graph_route_parity_evidence.graph_analysis.protocol",
                        "graph_route_parity_evidence.graph_analysis.present",
                        "graph_route_parity_evidence.graph_analysis.ready",
                        "graph_route_parity_evidence.graph_analysis.route",
                        "graph_route_parity_evidence.graph_analysis.matches",
                        "graph_route_parity_evidence.graph_analysis.primary_ready",
                        "graph_route_parity_evidence.graph_analysis.shadow_ready",
                        "graph_route_parity_evidence.graph_analysis.blocker_codes"
                    ]
                },
                {
                    "action": "attach_storage_recovery_report",
                    "reason": "required storage recovery evidence is missing or blocked",
                    "evidence_fields": [
                        "cutover_evidence.storage_recovery_present",
                        "cutover_evidence.storage_recovery_ready",
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
    fn validates_nowledge_replacement_summary_usage_text() {
        assert!(nowledge_replacement_summary_usage().contains("<migration-gate-json>"));
        assert!(nowledge_replacement_summary_usage().contains("--require-production-ready"));
        assert!(nowledge_replacement_summary_usage().contains("--compact"));
        assert!(nowledge_replacement_summary_usage().contains("--max-family-items"));
        assert!(nowledge_replacement_summary_usage().contains("--max-blockers"));
        assert!(nowledge_replacement_summary_usage().contains("--search-projection-evidence-json"));
        assert!(nowledge_replacement_summary_usage()
            .contains("--search-projection-shadow-evidence-json"));
        assert!(nowledge_replacement_summary_usage()
            .contains("--search-candidate-shadow-evidence-json"));
        assert!(nowledge_replacement_summary_usage().contains("--bounded-read-evidence-json"));
        assert!(nowledge_replacement_summary_usage().contains("--overview-parity-evidence-json"));
        assert!(nowledge_replacement_summary_usage().contains("--explore-parity-evidence-json"));
        assert!(nowledge_replacement_summary_usage().contains("--expand-parity-evidence-json"));
        assert!(
            nowledge_replacement_summary_usage().contains("--live-preview-parity-evidence-json")
        );
        assert!(nowledge_replacement_summary_usage()
            .contains("--live-preview-node-parity-evidence-json"));
        assert!(
            nowledge_replacement_summary_usage().contains("--node-details-parity-evidence-json")
        );
        assert!(nowledge_replacement_summary_usage().contains("--orphans-parity-evidence-json"));
        assert!(
            nowledge_replacement_summary_usage().contains("--shortest-path-parity-evidence-json")
        );
        assert!(nowledge_replacement_summary_usage()
            .contains("--community-subgraph-parity-evidence-json"));
        assert!(nowledge_replacement_summary_usage()
            .contains("--community-recent-memories-parity-evidence-json"));
        assert!(nowledge_replacement_summary_usage().contains("--query-family-evidence-json"));
    }

    fn production_ready_bundle() -> serde_json::Value {
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
                "primary_engine": "skein",
                "shadow_engine": "previous-wrapper",
                "primary_check_count": 1,
                "shadow_check_count": 1,
                "matched_check_count": 1,
                "primary_only_check_count": 0,
                "matched_per_million": 1_000_000
            },
            "search_projection_evidence": {
                "protocol": SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL,
                "evidence_source": SEARCH_PROJECTION_EVIDENCE_SOURCE,
                "ready": true,
                "derived_projection": true,
                "all_tables_covered": true,
                "covered_table_count": 6,
                "required_table_count": 6,
                "fts_ready": true,
                "vector_ready": true,
                "embedding_identity_ready": true,
                "fail_soft_ready": true,
                "rebuild_marker_ready": true,
                "metadata_repair_marker_ready": true,
                "incremental_update_ready": true,
                "source_chunk_ready": true,
                "predicate_pushdown_ready": true,
                "compressed_vector_projection_required": true,
                "compressed_vector_projection_ready": true,
                "blocker_codes": []
            },
            "search_projection_shadow_evidence": {
                "protocol": SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL,
                "evidence_source": SEARCH_PROJECTION_SHADOW_EVIDENCE_SOURCE,
                "route": SEARCH_PROJECTION_SHADOW_EVIDENCE_ROUTE,
                "ready": true,
                "primary_engine": "lancedb",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "document_count_parity": true,
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
                    "primary_scan_filter_fields": ["space_id", "unit_type", "importance", "confidence", "created_at", "event_start", "event_end", "is_latest"],
                    "shadow_scan_filter_fields": ["space_id", "unit_type", "importance", "confidence", "created_at", "event_start", "event_end", "is_latest"]
                },
                "blocker_codes": []
            },
            "bounded_read_evidence": {
                "protocol": "skein-nowledge-mem-bounded-read-evidence-v1",
                "mode": "shadow_read_only",
                "max_rows": 512,
                "execution_row_cap": 513,
                "row_limit_enforced_before_output": true,
                "operator_row_cap_enabled": true,
                "streaming": false,
                "blocking_operator_count": 0,
                "covered_routes": [
                    "/graph/overview",
                    "/graph/search",
                    "/graph/explore",
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
                    "/sources/{source_id}",
                    "/graph/orphans",
                    "/graph/shortest-path"
                ],
                "route_primary_ready": true,
                "query_runtime_report_count": 17,
                "query_runtime_plan_report_count": 17,
                "query_runtime_profile_report_count": 17,
                "query_runtime_failed_query_count": 0,
                "query_runtime_missing_plan_evidence_count": 0,
                "query_runtime_missing_profile_evidence_count": 0,
                "primary_ready_routes": [
                    "/graph/overview",
                    "/graph/search",
                    "/graph/explore",
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
                    "/sources/{source_id}",
                    "/graph/orphans",
                    "/graph/shortest-path"
                ],
                "missing_primary_routes": [],
                "blocker_codes": []
            },
            "augmentation_state_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/augmentation/state",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "pagerank_plan_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/augmentation/pagerank/plan",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "overview_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/overview",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "graph_search_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/search",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "explore_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/explore",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "expand_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/expand/{node_id}",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "live_preview_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/live-preview",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "live_preview_node_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/live-preview/{node_id}",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "node_details_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/node-details/{node_id}",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "source_detail_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/sources/{source_id}",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "orphans_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/orphans",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "shortest_path_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/shortest-path",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "community_members_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/community-members/{community_id}",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "community_subgraph_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/library/community/{community_id}/subgraph",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "community_recent_memories_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/library/community/{community_id}/recent-memories",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "related_communities_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/library/community/{community_id}/related",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "graph_analysis_parity_evidence": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "ready": true,
                "route": "/graph/analysis",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
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
                    "query_family": "search_projection",
                    "replacement_readiness_per_million": 1_000_000
                }
            ]
        });
        bundle["search_candidate_shadow_evidence"] = ready_search_candidate_shadow_evidence();
        bundle
    }

    fn ready_search_candidate_shadow_evidence() -> serde_json::Value {
        serde_json::json!({
            "protocol": SKEIN_NOWLEDGE_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
            "evidence_source": SEARCH_CANDIDATE_SHADOW_EVIDENCE_SOURCE,
            "route": SEARCH_CANDIDATE_SHADOW_EVIDENCE_ROUTE,
            "engine": "skein-shadow",
            "ready": true,
            "primary_engine": "lancedb",
            "shadow_engine": "skein",
            "filter_pushdown": {
                "pushed_metadata_filters": {
                    "unit_type": "memory",
                    "is_latest": "true"
                },
                "pushed_filter_axes": [
                    "unit_type",
                    "is_latest"
                ],
                "not_pushed_filters": [],
                "residual_filter_axes": [],
                "shadow_scan": {
                    "document_count": 12,
                    "filtered_document_count": 8,
                    "filtered_out_count": 4,
                    "candidate_set_cardinality": 8,
                    "metadata_filters": {
                        "unit_type": "memory",
                        "is_latest": "true"
                    },
                    "metadata_predicate_pushdown": {
                        "input_predicate_count": 2,
                        "pushed_predicate_count": 2,
                        "residual_predicate_count": 0,
                        "unsatisfiable": false,
                        "parse_error": null,
                        "segment_count": 3,
                        "pruned_segment_count": 1,
                        "scanned_segment_count": 2,
                        "persisted_segment_descriptor_used": true,
                        "segment_descriptor_bounded": true,
                        "segment_descriptor_field_count": 16,
                        "field_summaries": [
                            {
                                "field": "unit_type",
                                "operation_kinds": ["equals"],
                                "segment_count": 3,
                                "pruned_segment_count": 1,
                                "scanned_segment_count": 2,
                                "numeric_range_summary_used": false,
                                "value_summary_used": true
                            },
                            {
                                "field": "is_latest",
                                "operation_kinds": ["equals"],
                                "segment_count": 3,
                                "pruned_segment_count": 0,
                                "scanned_segment_count": 3,
                                "numeric_range_summary_used": false,
                                "value_summary_used": true
                            }
                        ]
                    }
                }
            },
            "row_counts": {
                "primary": 8,
                "shadow": 8,
                "matches": true
            },
            "vector": {
                "top_k_overlap_ready": true
            },
            "fts": {
                "top_k_overlap_ready": true
            },
            "blocker_codes": []
        })
    }

    fn ready_search_candidate_primary_evidence() -> serde_json::Value {
        serde_json::json!({
            "protocol": SKEIN_NOWLEDGE_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL,
            "evidence_source": SEARCH_CANDIDATE_SHADOW_EVIDENCE_SOURCE,
            "route": SEARCH_CANDIDATE_SHADOW_EVIDENCE_ROUTE,
            "engine": "skein-primary",
            "ready": true,
            "candidate_primary_engine": "skein",
            "primary_engine": "skein",
            "blocker_codes": []
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
}
