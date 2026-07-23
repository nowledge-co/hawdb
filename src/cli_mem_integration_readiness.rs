use skein::{Result, SkeinError, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES};
use std::path::Path;

const NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL: &str =
    "nowledge-mem-skein-integration-bundle";
const SKEIN_NOWLEDGE_REPLACEMENT_SUMMARY_PROTOCOL: &str = "skein-nowledge-replacement-summary";
const SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-evidence";
const SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-projection-shadow-evidence";
const SKEIN_NOWLEDGE_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-search-candidate-shadow-evidence";
const SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL: &str =
    "skein-nowledge-mem-bounded-read-evidence-v1";
const NMEM_GRAPH_ROUTE_SHADOW_PARITY_EVIDENCE_PROTOCOL: &str =
    "nmem-graph-route-shadow-parity-evidence-v1";
const REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES: &[&str] = &[
    "/graph/overview",
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
    "/graph/orphans",
    "/graph/shortest-path",
];
const NMEM_GRAPH_ROUTE_READINESS_PROTOCOL: &str = "nmem-graph-route-readiness-v1";

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
    let checks = vec![
        check(
            "integration_bundle_protocol",
            [str_path(bundle, &["protocol"]) == Some(NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL)],
            ["protocol"],
            Vec::new(),
        ),
        check(
            "replacement_summary_protocol",
            [str_path(bundle, &["replacement_summary", "protocol"])
                == Some(SKEIN_NOWLEDGE_REPLACEMENT_SUMMARY_PROTOCOL)],
            ["replacement_summary.protocol"],
            Vec::new(),
        ),
        check(
            "skein_submodule",
            [
                bool_path(bundle, &["submodule", "present"]) == Some(true),
                non_empty_str_path(bundle, &["submodule", "path"]),
                non_empty_str_path(bundle, &["submodule", "commit"]),
            ],
            [
                "submodule.present",
                "submodule.path",
                "submodule.commit",
            ],
            blocker_codes(bundle, &[&["submodule", "blocker_codes"][..]]),
        ),
        check(
            "legacy_coexistence",
            [
                bool_path(bundle, &["coexistence", "old_database_retained"]) == Some(true),
                coexistence_mode_is_safe(bundle),
                bool_path(bundle, &["coexistence", "old_database_deleted"]) != Some(true),
            ],
            [
                "coexistence.old_database_retained",
                "coexistence.mode",
                "coexistence.old_database_deleted",
            ],
            blocker_codes(bundle, &[&["coexistence", "blocker_codes"][..]]),
        ),
        check(
            "content_store_boundary",
            [
                bool_path(bundle, &["content_store", "present"]) == Some(true),
                str_path(bundle, &["content_store", "engine"]) == Some("sqlite"),
                bool_path(bundle, &["content_store", "messages_available"]) == Some(true),
                bool_path(bundle, &["content_store", "source_chunks_available"]) == Some(true),
            ],
            [
                "content_store.present",
                "content_store.engine",
                "content_store.messages_available",
                "content_store.source_chunks_available",
            ],
            blocker_codes(bundle, &[&["content_store", "blocker_codes"][..]]),
        ),
        check(
            "previous_wrapper_preflight",
            [bool_path(bundle, &["previous_wrapper_preflight", "ready"]) == Some(true)],
            ["previous_wrapper_preflight.ready"],
            blocker_codes(
                bundle,
                &[
                    &["previous_wrapper_preflight", "blocker_codes"][..],
                    &["previous_wrapper_preflight", "failed_checks"][..],
                ],
            ),
        ),
        check(
            "graph_replacement_evidence",
            [
                bool_path(bundle, &["replacement_summary", "production_cutover_ready"])
                    == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "shadow_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "dual_engine_evidence", "present"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "dual_engine_evidence", "ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "dual_engine_evidence", "consistent"],
                ) == Some(true),
            ],
            [
                "replacement_summary.production_cutover_ready",
                "replacement_summary.shadow_evidence.ready",
                "replacement_summary.dual_engine_evidence.present",
                "replacement_summary.dual_engine_evidence.ready",
                "replacement_summary.dual_engine_evidence.consistent",
            ],
            blocker_codes(
                bundle,
                &[
                    &["replacement_summary", "blocking_categories"][..],
                    &["replacement_summary", "missing_evidence"][..],
                    &["replacement_summary", "dual_engine_evidence", "blocker_codes"][..],
                ],
            ),
        ),
        check(
            "query_family_replacement_evidence",
            [
                replacement_summary_required_query_families_present(bundle),
                string_array_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "replacement_readiness_family_summary",
                        "missing_required_query_families",
                    ],
                )
                .is_empty(),
                string_array_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "replacement_readiness_family_summary",
                        "blocked_query_families",
                    ],
                )
                .is_empty(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "replacement_readiness_family_summary",
                        "min_replacement_readiness_per_million",
                    ],
                ) == Some(1_000_000),
            ],
            [
                "replacement_summary.replacement_readiness_family_summary.required_query_families",
                "replacement_summary.replacement_readiness_family_summary.missing_required_query_families",
                "replacement_summary.replacement_readiness_family_summary.blocked_query_families",
                "replacement_summary.replacement_readiness_family_summary.min_replacement_readiness_per_million",
            ],
            blocker_codes(
                bundle,
                &[
                    &["replacement_summary", "blocking_categories"][..],
                    &["replacement_summary", "missing_evidence"][..],
                ],
            ),
        ),
        check(
            "search_projection_replacement_evidence",
            [
                bool_path(
                    bundle,
                    &["replacement_summary", "search_projection_evidence", "ready"],
                ) == Some(true),
                str_path(
                    bundle,
                    &["replacement_summary", "search_projection_evidence", "protocol"],
                ) == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "fts_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "vector_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "incremental_update_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "predicate_pushdown_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "compressed_vector_projection_required",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_evidence",
                        "compressed_vector_projection_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "present",
                    ],
                ) == Some(true),
                str_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "protocol",
                    ],
                ) == Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "document_count_parity",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "table_parity_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "embedding_identity_parity",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_projection_shadow_evidence",
                        "incremental_watermark_parity",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_candidate_shadow_evidence",
                        "present",
                    ],
                ) == Some(true),
                str_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_candidate_shadow_evidence",
                        "protocol",
                    ],
                ) == Some(SKEIN_NOWLEDGE_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_candidate_shadow_evidence",
                        "ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_candidate_shadow_evidence",
                        "row_count_parity",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_candidate_shadow_evidence",
                        "vector_top_k_overlap_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_candidate_shadow_evidence",
                        "fts_top_k_overlap_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "search_candidate_shadow_evidence",
                        "shadow_scan_filter_pushdown_ready",
                    ],
                ) == Some(true),
            ],
            [
                "replacement_summary.search_projection_evidence.ready",
                "replacement_summary.search_projection_evidence.protocol",
                "replacement_summary.search_projection_evidence.fts_ready",
                "replacement_summary.search_projection_evidence.vector_ready",
                "replacement_summary.search_projection_evidence.incremental_update_ready",
                "replacement_summary.search_projection_evidence.predicate_pushdown_ready",
                "replacement_summary.search_projection_evidence.compressed_vector_projection_required",
                "replacement_summary.search_projection_evidence.compressed_vector_projection_ready",
                "replacement_summary.search_projection_shadow_evidence.present",
                "replacement_summary.search_projection_shadow_evidence.protocol",
                "replacement_summary.search_projection_shadow_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.document_count_parity",
                "replacement_summary.search_projection_shadow_evidence.table_parity_ready",
                "replacement_summary.search_projection_shadow_evidence.embedding_identity_parity",
                "replacement_summary.search_projection_shadow_evidence.incremental_watermark_parity",
                "replacement_summary.search_candidate_shadow_evidence.present",
                "replacement_summary.search_candidate_shadow_evidence.protocol",
                "replacement_summary.search_candidate_shadow_evidence.ready",
                "replacement_summary.search_candidate_shadow_evidence.row_count_parity",
                "replacement_summary.search_candidate_shadow_evidence.vector_top_k_overlap_ready",
                "replacement_summary.search_candidate_shadow_evidence.fts_top_k_overlap_ready",
                "replacement_summary.search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready",
            ],
            blocker_codes(
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
                    &[
                        "replacement_summary",
                        "search_candidate_shadow_evidence",
                        "blocker_codes",
                    ][..],
                ],
            ),
        ),
        check(
            "bounded_read_evidence",
            [
                bool_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "present"],
                ) == Some(true),
                str_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "protocol"],
                ) == Some(SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL),
                bool_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "ready"],
                ) == Some(true),
                u64_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "max_rows"],
                )
                .is_some_and(|value| value > 0),
                str_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "mode"],
                ) == Some("shadow_read_only"),
                bounded_read_execution_cap_matches(bundle),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "bounded_read_evidence",
                        "row_limit_enforced_before_output",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "bounded_read_evidence",
                        "operator_row_cap_enabled",
                    ],
                ) == Some(true),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "bounded_read_evidence",
                        "blocking_operator_count",
                    ],
                ) == Some(0),
                bool_path(
                    bundle,
                    &["replacement_summary", "bounded_read_evidence", "streaming"],
                ) == Some(false),
                bounded_read_covered_routes_ready(bundle),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "bounded_read_evidence",
                        "route_primary_ready",
                    ],
                ) == Some(true),
                bounded_read_primary_ready_routes_cover_required(bundle),
                bounded_read_missing_primary_routes_empty(bundle),
            ],
            [
                "replacement_summary.bounded_read_evidence.present",
                "replacement_summary.bounded_read_evidence.protocol",
                "replacement_summary.bounded_read_evidence.ready",
                "replacement_summary.bounded_read_evidence.max_rows",
                "replacement_summary.bounded_read_evidence.mode",
                "replacement_summary.bounded_read_evidence.execution_row_cap",
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output",
                "replacement_summary.bounded_read_evidence.operator_row_cap_enabled",
                "replacement_summary.bounded_read_evidence.blocking_operator_count",
                "replacement_summary.bounded_read_evidence.streaming",
                "replacement_summary.bounded_read_evidence.covered_routes",
                "replacement_summary.bounded_read_evidence.route_primary_ready",
                "replacement_summary.bounded_read_evidence.primary_ready_routes",
                "replacement_summary.bounded_read_evidence.missing_primary_routes",
            ],
            blocker_codes(
                bundle,
                &[&[
                    "replacement_summary",
                    "bounded_read_evidence",
                    "blocker_codes",
                ][..]],
            ),
        ),
        check(
            "bounded_read_evidence_alignment",
            [
                bool_path(bundle, &["bounded_read_evidence", "ready"]) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "evidence_ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "summary_ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "protocol_matches"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "readiness_matches"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "mode_matches"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "max_rows_matches"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary_bounded_read_alignment", "streaming_matches"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary_bounded_read_alignment",
                        "covered_routes_matches",
                    ],
                ) == Some(true),
            ],
            [
                "bounded_read_evidence.ready",
                "replacement_summary_bounded_read_alignment.ready",
                "replacement_summary_bounded_read_alignment.evidence_ready",
                "replacement_summary_bounded_read_alignment.summary_ready",
                "replacement_summary_bounded_read_alignment.protocol_matches",
                "replacement_summary_bounded_read_alignment.readiness_matches",
                "replacement_summary_bounded_read_alignment.mode_matches",
                "replacement_summary_bounded_read_alignment.max_rows_matches",
                "replacement_summary_bounded_read_alignment.streaming_matches",
                "replacement_summary_bounded_read_alignment.covered_routes_matches",
            ],
            blocker_codes(
                bundle,
                &[
                    &["bounded_read_evidence", "blocker_codes"][..],
                    &["replacement_summary_bounded_read_alignment", "blocker_codes"][..],
                ],
            ),
        ),
        check(
            "graph_route_readiness",
            [
                str_path(bundle, &["graph_route_readiness", "protocol"])
                    == Some(NMEM_GRAPH_ROUTE_READINESS_PROTOCOL),
                u64_path(bundle, &["graph_route_readiness", "route_count"])
                    .is_some_and(|value| value > 0),
                bool_path(bundle, &["graph_route_readiness", "route_primary_ready"]) == Some(true),
                graph_route_primary_ready_count_matches(bundle),
                string_array_path(bundle, &["graph_route_readiness", "route_primary_blocker_codes"])
                    .is_empty(),
            ],
            [
                "graph_route_readiness.protocol",
                "graph_route_readiness.route_count",
                "graph_route_readiness.route_primary_ready",
                "graph_route_readiness.primary_ready_route_count",
                "graph_route_readiness.route_primary_blocker_codes",
            ],
            blocker_codes(
                bundle,
                &[
                    &["graph_route_readiness", "blocker_codes"][..],
                    &["graph_route_readiness", "route_primary_blocker_codes"][..],
                ],
            ),
        ),
        check(
            "graph_route_overview_parity_evidence",
            [
                replacement_summary_overview_parity_ready(bundle),
                str_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "graph_route_parity_evidence",
                        "overview",
                        "protocol",
                    ],
                ) == Some(NMEM_GRAPH_ROUTE_SHADOW_PARITY_EVIDENCE_PROTOCOL),
                str_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "graph_route_parity_evidence",
                        "overview",
                        "route",
                    ],
                ) == Some("/graph/overview"),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "graph_route_parity_evidence",
                        "overview",
                        "matches",
                    ],
                ) == Some(true),
            ],
            [
                "replacement_summary.graph_route_parity_evidence.overview.ready",
                "replacement_summary.graph_route_parity_evidence.overview.protocol",
                "replacement_summary.graph_route_parity_evidence.overview.route",
                "replacement_summary.graph_route_parity_evidence.overview.matches",
            ],
            blocker_codes(
                bundle,
                &[&[
                    "replacement_summary",
                    "graph_route_parity_evidence",
                    "overview",
                    "blocker_codes",
                ][..]],
            ),
        ),
        check(
            "graph_route_explore_parity_evidence",
            [
                replacement_summary_explore_parity_ready(bundle),
                str_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "graph_route_parity_evidence",
                        "explore",
                        "protocol",
                    ],
                ) == Some(NMEM_GRAPH_ROUTE_SHADOW_PARITY_EVIDENCE_PROTOCOL),
                str_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "graph_route_parity_evidence",
                        "explore",
                        "route",
                    ],
                ) == Some("/graph/explore"),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "graph_route_parity_evidence",
                        "explore",
                        "matches",
                    ],
                ) == Some(true),
            ],
            [
                "replacement_summary.graph_route_parity_evidence.explore.ready",
                "replacement_summary.graph_route_parity_evidence.explore.protocol",
                "replacement_summary.graph_route_parity_evidence.explore.route",
                "replacement_summary.graph_route_parity_evidence.explore.matches",
            ],
            blocker_codes(
                bundle,
                &[&[
                    "replacement_summary",
                    "graph_route_parity_evidence",
                    "explore",
                    "blocker_codes",
                ][..]],
            ),
        ),
        check(
            "graph_route_live_preview_parity_evidence",
            [
                replacement_summary_live_preview_parity_ready(bundle),
                str_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "graph_route_parity_evidence",
                        "live_preview",
                        "protocol",
                    ],
                ) == Some(NMEM_GRAPH_ROUTE_SHADOW_PARITY_EVIDENCE_PROTOCOL),
                str_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "graph_route_parity_evidence",
                        "live_preview",
                        "route",
                    ],
                ) == Some("/graph/live-preview"),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "graph_route_parity_evidence",
                        "live_preview",
                        "matches",
                    ],
                ) == Some(true),
            ],
            [
                "replacement_summary.graph_route_parity_evidence.live_preview.ready",
                "replacement_summary.graph_route_parity_evidence.live_preview.protocol",
                "replacement_summary.graph_route_parity_evidence.live_preview.route",
                "replacement_summary.graph_route_parity_evidence.live_preview.matches",
            ],
            blocker_codes(
                bundle,
                &[&[
                    "replacement_summary",
                    "graph_route_parity_evidence",
                    "live_preview",
                    "blocker_codes",
                ][..]],
            ),
        ),
        check(
            "background_maintenance_evidence",
            [
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_required",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_ready",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_protocol_matches",
                    ],
                ) == Some(true),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_executable_search_projection_graph_delta_count",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_admitted_search_projection_graph_delta_count",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_deferred_search_projection_graph_delta_count",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_rejected_search_projection_graph_delta_count",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_executable_search_projection_graph_delta_operations",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_admitted_search_projection_graph_delta_operations",
                    ],
                )
                .is_some(),
                u64_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
                    ],
                )
                .is_some(),
            ],
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
            ],
            blocker_codes(
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
        ),
        check(
            "storage_recovery_evidence",
            [
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_required",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "cutover_evidence", "storage_recovery_ready"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_protocol_matches",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &["replacement_summary", "cutover_evidence", "storage_recovery_durable"],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_checkpoint_boundary_present",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_wal_replay_bounded",
                    ],
                ) == Some(true),
                bool_path(
                    bundle,
                    &[
                        "replacement_summary",
                        "cutover_evidence",
                        "storage_recovery_torn_tail_clean",
                    ],
                ) == Some(true),
            ],
            [
                "replacement_summary.cutover_evidence.storage_recovery_required",
                "replacement_summary.cutover_evidence.storage_recovery_ready",
                "replacement_summary.cutover_evidence.storage_recovery_protocol_matches",
                "replacement_summary.cutover_evidence.storage_recovery_durable",
                "replacement_summary.cutover_evidence.storage_recovery_checkpoint_boundary_present",
                "replacement_summary.cutover_evidence.storage_recovery_wal_replay_bounded",
                "replacement_summary.cutover_evidence.storage_recovery_torn_tail_clean",
            ],
            blocker_codes(
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
        ),
    ];
    let ready = checks
        .iter()
        .all(|check| bool_path(check, &["ready"]) == Some(true));
    let failed_checks = checks
        .iter()
        .filter(|check| bool_path(check, &["ready"]) != Some(true))
        .filter_map(|check| str_path(check, &["name"]))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let blocker_codes = checks
        .iter()
        .flat_map(|check| string_array_path(check, &["blocker_codes"]))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    serde_json::json!({
        "protocol": "skein-nowledge-mem-integration-readiness",
        "ready": ready,
        "failed_checks": failed_checks,
        "checks": checks,
        "blocker_codes": blocker_codes,
        "next_actions": next_actions(bundle, ready),
    })
}

fn check(
    name: &'static str,
    conditions: impl IntoIterator<Item = bool>,
    evidence_fields: impl IntoIterator<Item = &'static str>,
    blocker_codes: Vec<String>,
) -> serde_json::Value {
    let conditions = conditions.into_iter().collect::<Vec<_>>();
    let evidence_fields = evidence_fields.into_iter().collect::<Vec<_>>();
    let failed_evidence_fields = conditions
        .iter()
        .zip(evidence_fields.iter())
        .filter_map(|(condition, field)| (!*condition).then_some(*field))
        .collect::<Vec<_>>();
    serde_json::json!({
        "name": name,
        "ready": failed_evidence_fields.is_empty(),
        "evidence_fields": evidence_fields,
        "failed_evidence_fields": failed_evidence_fields,
        "blocker_codes": blocker_codes,
    })
}

fn next_actions(bundle: &serde_json::Value, ready: bool) -> Vec<serde_json::Value> {
    if ready {
        return Vec::new();
    }
    let mut actions = Vec::new();
    if str_path(bundle, &["protocol"]) != Some(NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL) {
        actions.push(next_action(
            "regenerate_skein_integration_bundle",
            "Nowledge Mem integration readiness requires the versioned integration bundle protocol",
            ["protocol"],
        ));
    }
    if bool_path(bundle, &["submodule", "present"]) != Some(true) {
        actions.push(next_action(
            "add_skein_submodule",
            "Nowledge Mem must depend on Skein as a submodule instead of copying sources",
            ["submodule.present", "submodule.path", "submodule.commit"],
        ));
    }
    if bool_path(bundle, &["coexistence", "old_database_retained"]) != Some(true)
        || !coexistence_mode_is_safe(bundle)
        || bool_path(bundle, &["coexistence", "old_database_deleted"]) == Some(true)
    {
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
    if bool_path(bundle, &["content_store", "present"]) != Some(true) {
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
    if bool_path(bundle, &["previous_wrapper_preflight", "ready"]) != Some(true) {
        actions.push(next_action(
            "run_previous_wrapper_preflight",
            "the previous-wrapper release bundle must pass before Mem cutover",
            ["previous_wrapper_preflight.ready"],
        ));
    }
    if bool_path(bundle, &["replacement_summary", "production_cutover_ready"]) != Some(true) {
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
    if str_path(bundle, &["replacement_summary", "protocol"])
        != Some(SKEIN_NOWLEDGE_REPLACEMENT_SUMMARY_PROTOCOL)
    {
        actions.push(next_action(
            "produce_replacement_summary",
            "replacement summary must use the Skein Nowledge replacement-summary protocol",
            ["replacement_summary.protocol"],
        ));
    }
    if !replacement_summary_required_query_families_present(bundle)
        || !string_array_path(
            bundle,
            &[
                "replacement_summary",
                "replacement_readiness_family_summary",
                "missing_required_query_families",
            ],
        )
        .is_empty()
        || !string_array_path(
            bundle,
            &[
                "replacement_summary",
                "replacement_readiness_family_summary",
                "blocked_query_families",
            ],
        )
        .is_empty()
        || u64_path(
            bundle,
            &[
                "replacement_summary",
                "replacement_readiness_family_summary",
                "min_replacement_readiness_per_million",
            ],
        ) != Some(1_000_000)
    {
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
    if !replacement_summary_search_projection_ready(bundle) {
        actions.push(next_action(
            "attach_search_projection_replacement_evidence",
            "LanceDB replacement evidence must prove FTS, vector, incremental, predicate pushdown, compressed projection, shadow parity, and candidate scan-filter pushdown",
            [
                "replacement_summary.search_projection_evidence.ready",
                "replacement_summary.search_projection_evidence.protocol",
                "replacement_summary.search_projection_evidence.fts_ready",
                "replacement_summary.search_projection_evidence.vector_ready",
                "replacement_summary.search_projection_evidence.incremental_update_ready",
                "replacement_summary.search_projection_evidence.predicate_pushdown_ready",
                "replacement_summary.search_projection_evidence.compressed_vector_projection_required",
                "replacement_summary.search_projection_evidence.compressed_vector_projection_ready",
                "replacement_summary.search_projection_shadow_evidence.protocol",
                "replacement_summary.search_projection_shadow_evidence.ready",
                "replacement_summary.search_projection_shadow_evidence.blocker_codes",
                "replacement_summary.search_candidate_shadow_evidence.protocol",
                "replacement_summary.search_candidate_shadow_evidence.ready",
                "replacement_summary.search_candidate_shadow_evidence.row_count_parity",
                "replacement_summary.search_candidate_shadow_evidence.vector_top_k_overlap_ready",
                "replacement_summary.search_candidate_shadow_evidence.fts_top_k_overlap_ready",
                "replacement_summary.search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready",
                "replacement_summary.search_candidate_shadow_evidence.blocker_codes",
            ],
        ));
    }
    if !replacement_summary_bounded_read_ready(bundle) {
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
                "replacement_summary.bounded_read_evidence.row_limit_enforced_before_output",
                "replacement_summary.bounded_read_evidence.operator_row_cap_enabled",
                "replacement_summary.bounded_read_evidence.blocking_operator_count",
                "replacement_summary.bounded_read_evidence.streaming",
                "replacement_summary.bounded_read_evidence.blocker_codes",
            ],
        ));
    }
    if !bounded_read_alignment_ready(bundle) {
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
    if !graph_route_readiness_ready(bundle) {
        actions.push(next_action(
            "attach_graph_route_readiness_evidence",
            "Nowledge Mem graph cutover requires route-level primary-read readiness evidence",
            [
                "graph_route_readiness.protocol",
                "graph_route_readiness.route_count",
                "graph_route_readiness.route_primary_ready",
                "graph_route_readiness.primary_ready_route_count",
                "graph_route_readiness.route_primary_blocker_codes",
            ],
        ));
    }
    if !replacement_summary_overview_parity_ready(bundle) {
        actions.push(next_action(
            "run_overview_route_shadow_compare",
            "overview graph route parity evidence must be ready before Mem graph cutover",
            [
                "replacement_summary.graph_route_parity_evidence.overview.protocol",
                "replacement_summary.graph_route_parity_evidence.overview.ready",
                "replacement_summary.graph_route_parity_evidence.overview.route",
                "replacement_summary.graph_route_parity_evidence.overview.matches",
                "replacement_summary.graph_route_parity_evidence.overview.primary_ready",
                "replacement_summary.graph_route_parity_evidence.overview.shadow_ready",
                "replacement_summary.graph_route_parity_evidence.overview.blocker_codes",
            ],
        ));
    }
    if !replacement_summary_explore_parity_ready(bundle) {
        actions.push(next_action(
            "run_explore_route_shadow_compare",
            "explore graph route parity evidence must be ready before Mem graph cutover",
            [
                "replacement_summary.graph_route_parity_evidence.explore.protocol",
                "replacement_summary.graph_route_parity_evidence.explore.ready",
                "replacement_summary.graph_route_parity_evidence.explore.route",
                "replacement_summary.graph_route_parity_evidence.explore.matches",
                "replacement_summary.graph_route_parity_evidence.explore.primary_ready",
                "replacement_summary.graph_route_parity_evidence.explore.shadow_ready",
                "replacement_summary.graph_route_parity_evidence.explore.blocker_codes",
            ],
        ));
    }
    if !replacement_summary_live_preview_parity_ready(bundle) {
        actions.push(next_action(
            "run_live_preview_route_shadow_compare",
            "live-preview graph route parity evidence must be ready before Mem graph cutover",
            [
                "replacement_summary.graph_route_parity_evidence.live_preview.protocol",
                "replacement_summary.graph_route_parity_evidence.live_preview.ready",
                "replacement_summary.graph_route_parity_evidence.live_preview.route",
                "replacement_summary.graph_route_parity_evidence.live_preview.matches",
                "replacement_summary.graph_route_parity_evidence.live_preview.primary_ready",
                "replacement_summary.graph_route_parity_evidence.live_preview.shadow_ready",
                "replacement_summary.graph_route_parity_evidence.live_preview.blocker_codes",
            ],
        ));
    }
    if !replacement_summary_storage_recovery_ready(bundle) {
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
                "replacement_summary.cutover_evidence.storage_recovery_torn_tail_clean",
                "replacement_summary.cutover_evidence.storage_recovery_blocker_codes",
            ],
        ));
    }
    if !replacement_summary_background_maintenance_ready(bundle) {
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
) -> serde_json::Value {
    serde_json::json!({
        "action": action,
        "reason": reason,
        "evidence_fields": evidence_fields.into_iter().collect::<Vec<_>>(),
    })
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read Nowledge Mem integration bundle: {error}",
        ))
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to parse Nowledge Mem integration bundle: {error}",
        ))
    })
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

fn replacement_summary_search_projection_ready(bundle: &serde_json::Value) -> bool {
    if str_path(
        bundle,
        &[
            "replacement_summary",
            "search_projection_evidence",
            "protocol",
        ],
    ) != Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_EVIDENCE_PROTOCOL)
        || str_path(
            bundle,
            &[
                "replacement_summary",
                "search_projection_shadow_evidence",
                "protocol",
            ],
        ) != Some(SKEIN_NOWLEDGE_SEARCH_PROJECTION_SHADOW_EVIDENCE_PROTOCOL)
        || str_path(
            bundle,
            &[
                "replacement_summary",
                "search_candidate_shadow_evidence",
                "protocol",
            ],
        ) != Some(SKEIN_NOWLEDGE_SEARCH_CANDIDATE_SHADOW_EVIDENCE_PROTOCOL)
    {
        return false;
    }
    [
        &["replacement_summary", "search_projection_evidence", "ready"][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "fts_ready",
        ][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "vector_ready",
        ][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "incremental_update_ready",
        ][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "predicate_pushdown_ready",
        ][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "compressed_vector_projection_required",
        ][..],
        &[
            "replacement_summary",
            "search_projection_evidence",
            "compressed_vector_projection_ready",
        ][..],
        &[
            "replacement_summary",
            "search_projection_shadow_evidence",
            "ready",
        ][..],
        &[
            "replacement_summary",
            "search_candidate_shadow_evidence",
            "present",
        ][..],
        &[
            "replacement_summary",
            "search_candidate_shadow_evidence",
            "ready",
        ][..],
        &[
            "replacement_summary",
            "search_candidate_shadow_evidence",
            "row_count_parity",
        ][..],
        &[
            "replacement_summary",
            "search_candidate_shadow_evidence",
            "vector_top_k_overlap_ready",
        ][..],
        &[
            "replacement_summary",
            "search_candidate_shadow_evidence",
            "fts_top_k_overlap_ready",
        ][..],
        &[
            "replacement_summary",
            "search_candidate_shadow_evidence",
            "shadow_scan_filter_pushdown_ready",
        ][..],
    ]
    .iter()
    .all(|path| bool_path(bundle, path) == Some(true))
}

fn replacement_summary_bounded_read_ready(bundle: &serde_json::Value) -> bool {
    bool_path(
        bundle,
        &["replacement_summary", "bounded_read_evidence", "present"],
    ) == Some(true)
        && str_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "protocol"],
        ) == Some(SKEIN_NOWLEDGE_MEM_BOUNDED_READ_EVIDENCE_PROTOCOL)
        && bool_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "ready"],
        ) == Some(true)
        && u64_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "max_rows"],
        )
        .is_some_and(|value| value > 0)
        && str_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "mode"],
        ) == Some("shadow_read_only")
        && bounded_read_execution_cap_matches(bundle)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "row_limit_enforced_before_output",
            ],
        ) == Some(true)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "operator_row_cap_enabled",
            ],
        ) == Some(true)
        && u64_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "blocking_operator_count",
            ],
        ) == Some(0)
        && bool_path(
            bundle,
            &["replacement_summary", "bounded_read_evidence", "streaming"],
        ) == Some(false)
        && bounded_read_route_coverage_ready(bundle)
}

fn bounded_read_alignment_ready(bundle: &serde_json::Value) -> bool {
    bool_path(bundle, &["bounded_read_evidence", "ready"]) == Some(true)
        && [
            &["replacement_summary_bounded_read_alignment", "ready"][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "evidence_ready",
            ][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "summary_ready",
            ][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "protocol_matches",
            ][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "readiness_matches",
            ][..],
            &["replacement_summary_bounded_read_alignment", "mode_matches"][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "max_rows_matches",
            ][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "streaming_matches",
            ][..],
            &[
                "replacement_summary_bounded_read_alignment",
                "covered_routes_matches",
            ][..],
        ]
        .iter()
        .all(|path| bool_path(bundle, path) == Some(true))
}

fn bounded_read_route_coverage_ready(bundle: &serde_json::Value) -> bool {
    bounded_read_covered_routes_ready(bundle)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "bounded_read_evidence",
                "route_primary_ready",
            ],
        ) == Some(true)
        && bounded_read_primary_ready_routes_cover_required(bundle)
        && bounded_read_missing_primary_routes_empty(bundle)
}

fn bounded_read_covered_routes_ready(bundle: &serde_json::Value) -> bool {
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

fn bounded_read_primary_ready_routes_cover_required(bundle: &serde_json::Value) -> bool {
    let primary_ready_routes = string_array_path(
        bundle,
        &[
            "replacement_summary",
            "bounded_read_evidence",
            "primary_ready_routes",
        ],
    );
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .all(|route| primary_ready_routes.iter().any(|ready| ready == route))
}

fn bounded_read_missing_primary_routes_empty(bundle: &serde_json::Value) -> bool {
    string_array_path(
        bundle,
        &[
            "replacement_summary",
            "bounded_read_evidence",
            "missing_primary_routes",
        ],
    )
    .is_empty()
}

fn graph_route_readiness_ready(bundle: &serde_json::Value) -> bool {
    str_path(bundle, &["graph_route_readiness", "protocol"])
        == Some(NMEM_GRAPH_ROUTE_READINESS_PROTOCOL)
        && u64_path(bundle, &["graph_route_readiness", "route_count"])
            .is_some_and(|value| value > 0)
        && bool_path(bundle, &["graph_route_readiness", "route_primary_ready"]) == Some(true)
        && graph_route_primary_ready_count_matches(bundle)
        && string_array_path(
            bundle,
            &["graph_route_readiness", "route_primary_blocker_codes"],
        )
        .is_empty()
}

fn replacement_summary_overview_parity_ready(bundle: &serde_json::Value) -> bool {
    str_path(
        bundle,
        &[
            "replacement_summary",
            "graph_route_parity_evidence",
            "overview",
            "protocol",
        ],
    ) == Some(NMEM_GRAPH_ROUTE_SHADOW_PARITY_EVIDENCE_PROTOCOL)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                "overview",
                "ready",
            ],
        ) == Some(true)
        && str_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                "overview",
                "route",
            ],
        ) == Some("/graph/overview")
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                "overview",
                "matches",
            ],
        ) == Some(true)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                "overview",
                "primary_ready",
            ],
        ) == Some(true)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                "overview",
                "shadow_ready",
            ],
        ) == Some(true)
        && string_array_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                "overview",
                "blocker_codes",
            ],
        )
        .is_empty()
}

fn replacement_summary_explore_parity_ready(bundle: &serde_json::Value) -> bool {
    graph_route_parity_ready(bundle, "explore", "/graph/explore")
}

fn replacement_summary_live_preview_parity_ready(bundle: &serde_json::Value) -> bool {
    graph_route_parity_ready(bundle, "live_preview", "/graph/live-preview")
}

fn graph_route_parity_ready(bundle: &serde_json::Value, key: &str, route: &str) -> bool {
    str_path(
        bundle,
        &[
            "replacement_summary",
            "graph_route_parity_evidence",
            key,
            "protocol",
        ],
    ) == Some(NMEM_GRAPH_ROUTE_SHADOW_PARITY_EVIDENCE_PROTOCOL)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                key,
                "ready",
            ],
        ) == Some(true)
        && str_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                key,
                "route",
            ],
        ) == Some(route)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                key,
                "matches",
            ],
        ) == Some(true)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                key,
                "primary_ready",
            ],
        ) == Some(true)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                key,
                "shadow_ready",
            ],
        ) == Some(true)
        && string_array_path(
            bundle,
            &[
                "replacement_summary",
                "graph_route_parity_evidence",
                key,
                "blocker_codes",
            ],
        )
        .is_empty()
}

fn graph_route_primary_ready_count_matches(bundle: &serde_json::Value) -> bool {
    let route_count = u64_path(bundle, &["graph_route_readiness", "route_count"]);
    let primary_ready_route_count = u64_path(
        bundle,
        &["graph_route_readiness", "primary_ready_route_count"],
    );
    route_count.is_some_and(|value| value > 0) && route_count == primary_ready_route_count
}

fn replacement_summary_storage_recovery_ready(bundle: &serde_json::Value) -> bool {
    [
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_required",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_ready",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_protocol_matches",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_durable",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_checkpoint_boundary_present",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_wal_replay_bounded",
        ][..],
        &[
            "replacement_summary",
            "cutover_evidence",
            "storage_recovery_torn_tail_clean",
        ][..],
    ]
    .iter()
    .all(|path| bool_path(bundle, path) == Some(true))
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

fn replacement_summary_background_maintenance_ready(bundle: &serde_json::Value) -> bool {
    bool_path(
        bundle,
        &[
            "replacement_summary",
            "cutover_evidence",
            "background_maintenance_required",
        ],
    ) == Some(true)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_ready",
            ],
        ) == Some(true)
        && bool_path(
            bundle,
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_protocol_matches",
            ],
        ) == Some(true)
        && [
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_executable_search_projection_graph_delta_count",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_admitted_search_projection_graph_delta_count",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_deferred_search_projection_graph_delta_count",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_rejected_search_projection_graph_delta_count",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_executable_search_projection_graph_delta_operations",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_admitted_search_projection_graph_delta_operations",
            ][..],
            &[
                "replacement_summary",
                "cutover_evidence",
                "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
            ][..],
        ]
        .iter()
        .all(|path| u64_path(bundle, path).is_some())
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

fn non_empty_str_path(value: &serde_json::Value, path: &[&str]) -> bool {
    str_path(value, path).is_some_and(|s| !s.trim().is_empty())
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
    use super::nowledge_mem_integration_readiness_json;

    #[test]
    fn reports_ready_when_mem_integration_evidence_is_complete() {
        let report = nowledge_mem_integration_readiness_json(&ready_bundle());

        assert_eq!(report["ready"], true);
        assert_eq!(report["failed_checks"], serde_json::json!([]));
        assert_eq!(report["blocker_codes"], serde_json::json!([]));
        assert_eq!(report["next_actions"], serde_json::json!([]));
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
    fn requires_search_candidate_shadow_scan_filter_pushdown() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["search_candidate_shadow_evidence"]["ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["search_candidate_shadow_evidence"]
            ["shadow_scan_filter_pushdown_ready"] = serde_json::json!(false);
        bundle["replacement_summary"]["search_candidate_shadow_evidence"]["blocker_codes"] =
            serde_json::json!(["candidate_filter_residual"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["search_projection_replacement_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["candidate_filter_residual"])
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
                "replacement_summary.search_candidate_shadow_evidence.ready",
                "replacement_summary.search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready"
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
                            field
                                == "replacement_summary.search_candidate_shadow_evidence.shadow_scan_filter_pushdown_ready"
                        })
            }));
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
    fn requires_bounded_read_primary_route_coverage() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["bounded_read_evidence"]["route_primary_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["bounded_read_evidence"]["primary_ready_routes"] =
            serde_json::json!(["/graph/overview"]);
        bundle["replacement_summary"]["bounded_read_evidence"]["missing_primary_routes"] =
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
            serde_json::json!([
                "replacement_summary.bounded_read_evidence.route_primary_ready",
                "replacement_summary.bounded_read_evidence.primary_ready_routes",
                "replacement_summary.bounded_read_evidence.missing_primary_routes"
            ])
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
                "replacement_summary.bounded_read_evidence.blocking_operator_count",
                "replacement_summary.bounded_read_evidence.streaming"
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
                "graph_route_readiness.route_count",
                "graph_route_readiness.route_primary_ready",
                "graph_route_readiness.primary_ready_route_count"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "attach_graph_route_readiness_evidence"));
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
    fn requires_overview_route_parity_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["graph_route_parity_evidence"]
            .as_object_mut()
            .unwrap()
            .remove("overview");

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_overview_parity_evidence"])
        );
        let parity_check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["name"] == "graph_route_overview_parity_evidence")
            .unwrap();
        assert_eq!(
            parity_check["failed_evidence_fields"],
            serde_json::json!([
                "replacement_summary.graph_route_parity_evidence.overview.ready",
                "replacement_summary.graph_route_parity_evidence.overview.protocol",
                "replacement_summary.graph_route_parity_evidence.overview.route",
                "replacement_summary.graph_route_parity_evidence.overview.matches"
            ])
        );
        assert!(report["next_actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|action| action["action"] == "run_overview_route_shadow_compare"));
    }

    #[test]
    fn rejects_overview_route_parity_mismatch() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["graph_route_parity_evidence"]["overview"]["matches"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["graph_route_parity_evidence"]["overview"]["blocker_codes"] =
            serde_json::json!(["overview_route_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_overview_parity_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["overview_route_mismatch"])
        );
    }

    #[test]
    fn rejects_explore_route_parity_mismatch() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["graph_route_parity_evidence"]["explore"]["matches"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["graph_route_parity_evidence"]["explore"]["blocker_codes"] =
            serde_json::json!(["explore_route_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_explore_parity_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["explore_route_mismatch"])
        );
    }

    #[test]
    fn rejects_live_preview_route_parity_mismatch() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["graph_route_parity_evidence"]["live_preview"]["matches"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["graph_route_parity_evidence"]["live_preview"]
            ["blocker_codes"] = serde_json::json!(["live_preview_route_mismatch"]);

        let report = nowledge_mem_integration_readiness_json(&bundle);

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["failed_checks"],
            serde_json::json!(["graph_route_live_preview_parity_evidence"])
        );
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["live_preview_route_mismatch"])
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
        bundle["replacement_summary"]["cutover_evidence"]
            ["background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch"] =
            serde_json::Value::Null;

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
    fn requires_storage_recovery_evidence() {
        let mut bundle = ready_bundle();
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_ready"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_wal_replay_bounded"] =
            serde_json::json!(false);
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_blocker_codes"] =
            serde_json::json!(["wal_replay_unbounded"]);

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
        bundle["replacement_summary"]["cutover_evidence"]["storage_recovery_torn_tail_clean"] =
            serde_json::json!(false);

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
                "protocol": "skein-nowledge-mem-bounded-read-evidence-v1",
                "ready": true,
                "mode": "shadow_read_only",
                "max_rows": 512,
                "execution_row_cap": 513,
                "row_limit_enforced_before_output": true,
                "operator_row_cap_enabled": true,
                "blocking_operator_count": 0,
                "streaming": false,
                "covered_routes": [
                    "/graph/overview",
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
                    "/graph/orphans",
                    "/graph/shortest-path"
                ],
                "route_primary_ready": true,
                "primary_ready_routes": [
                    "/graph/overview",
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
                    "/graph/orphans",
                    "/graph/shortest-path"
                ],
                "missing_primary_routes": [],
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
                "streaming_matches": true,
                "covered_routes_matches": true,
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
                    "total_count": 4,
                    "ready_count": 4,
                    "blocked_count": 0,
                    "omitted_count": 0,
                    "min_replacement_readiness_per_million": 1_000_000,
                    "blocked_query_families": [],
                    "required_query_families": [
                        "memory_lookup",
                        "graph_traversal",
                        "projected_graph",
                        "search_projection"
                    ],
                    "missing_required_query_families": []
                },
                "search_projection_evidence": {
                    "protocol": "skein-nowledge-search-projection-evidence",
                    "ready": true,
                    "fts_ready": true,
                    "vector_ready": true,
                    "incremental_update_ready": true,
                    "predicate_pushdown_ready": true,
                    "compressed_vector_projection_required": true,
                    "compressed_vector_projection_ready": true,
                    "blocker_codes": []
                },
                "search_projection_shadow_evidence": {
                    "protocol": "skein-nowledge-search-projection-shadow-evidence",
                    "present": true,
                    "ready": true,
                    "document_count_parity": true,
                    "table_parity_ready": true,
                    "embedding_identity_parity": true,
                    "incremental_watermark_parity": true,
                    "blocker_codes": []
                },
                "bounded_read_evidence": {
                    "protocol": "skein-nowledge-mem-bounded-read-evidence-v1",
                    "present": true,
                    "ready": true,
                    "mode": "shadow_read_only",
                    "max_rows": 512,
                    "execution_row_cap": 513,
                    "row_limit_enforced_before_output": true,
                    "operator_row_cap_enabled": true,
                    "blocking_operator_count": 0,
                    "streaming": false,
                    "covered_routes": [
                        "/graph/overview",
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
                        "/graph/orphans",
                        "/graph/shortest-path"
                    ],
                    "missing_covered_routes": [],
                    "route_primary_ready": true,
                    "primary_ready_routes": [
                        "/graph/overview",
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
                        "/graph/orphans",
                        "/graph/shortest-path"
                    ],
                    "missing_primary_routes": [],
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
        bundle["graph_route_readiness"] = serde_json::json!({
            "protocol": "nmem-graph-route-readiness-v1",
            "route_count": 15,
            "shadow_compare_route_count": 15,
            "primary_ready_route_count": 15,
            "route_primary_ready": true,
            "route_primary_blocker_codes": [],
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "blocker_codes": []
                }
            ]
        });
        bundle["replacement_summary"]["graph_route_parity_evidence"] = serde_json::json!({
            "overview": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "present": true,
                "ready": true,
                "reported_ready": true,
                "route": "/graph/overview",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "explore": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "present": true,
                "ready": true,
                "reported_ready": true,
                "route": "/graph/explore",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            },
            "live_preview": {
                "protocol": "nmem-graph-route-shadow-parity-evidence-v1",
                "present": true,
                "ready": true,
                "reported_ready": true,
                "route": "/graph/live-preview",
                "matches": true,
                "primary_engine": "kuzu",
                "shadow_engine": "skein",
                "primary_ready": true,
                "shadow_ready": true,
                "blocker_codes": []
            }
        });
        bundle["replacement_summary"]["search_candidate_shadow_evidence"] =
            ready_search_candidate_shadow_evidence_summary();
        bundle
    }

    fn ready_search_candidate_shadow_evidence_summary() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-search-candidate-shadow-evidence",
            "present": true,
            "ready": true,
            "row_count_parity": true,
            "vector_top_k_overlap_ready": true,
            "fts_top_k_overlap_ready": true,
            "shadow_scan_filter_pushdown_ready": true,
            "blocker_codes": []
        })
    }
}
