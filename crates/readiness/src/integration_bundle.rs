//! Pure integration-bundle assembly and cross-report evidence alignment.
//! File adapters, database probes, and final activation stay in the embedded facade.
use crate::evidence_json::{
    json_get_bool_path as bool_path, json_get_path as value_path, json_get_str_path as str_path,
    json_get_u64_path as u64_path,
};
use crate::graph_route::NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL;
use crate::graph_summary::nowledge_graph_route_readiness_summary;
use skein_core::{Result, SkeinError};
use skein_route_ownership::graph::{
    nowledge_mem_graph_read_route_catalog_digest, NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};
use skein_route_ownership::{
    NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL,
    NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL, REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES,
    REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL: &str =
    "nowledge-mem-skein-integration-bundle";
const SKEIN_NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL: &str =
    "skein-nowledge-query-runtime-preflight-v1";
const ROUTE_PARITY_EVIDENCE_SOURCE: &str = "route_parity_evidence";
const ROUTE_PARITY_FULL_MATCH_PER_MILLION: u64 = 1_000_000;

#[derive(Debug, Clone, Default)]
pub struct IntegrationBundleInputs {
    pub require_ready: bool,
    pub submodule_path: Option<String>,
    pub submodule_commit: Option<String>,
    pub legacy_data_retained: bool,
    pub legacy_data_deleted: bool,
    pub coexistence_mode: Option<String>,
    pub content_store_present: bool,
    pub content_store_engine: Option<String>,
    pub content_store_messages_available: bool,
    pub content_store_source_chunks_available: bool,
    pub previous_wrapper_preflight: Option<serde_json::Value>,
    pub replacement_summary: Option<serde_json::Value>,
    pub bounded_read_evidence: Option<serde_json::Value>,
    pub graph_route_readiness: Option<serde_json::Value>,
    pub route_ownership: Option<serde_json::Value>,
    pub search_route_ownership: Option<serde_json::Value>,
    pub active_search_route_ownership: Option<serde_json::Value>,
    pub active_search_route_readiness: Option<serde_json::Value>,
    pub query_runtime_preflight: Option<serde_json::Value>,
    pub search_candidate_shadow_evidence: Option<serde_json::Value>,
    pub library_readiness: Option<serde_json::Value>,
    pub cutover_controls: Option<serde_json::Value>,
    pub operations_readiness: Option<serde_json::Value>,
    pub blackbox_manifest: Option<serde_json::Value>,
}

pub fn nowledge_mem_integration_bundle_json(
    inputs: IntegrationBundleInputs,
) -> Result<serde_json::Value> {
    let submodule_path = require_non_empty(inputs.submodule_path, "--submodule-path")?;
    let submodule_commit = require_non_empty(inputs.submodule_commit, "--submodule-commit")?;
    let coexistence_mode = require_non_empty(inputs.coexistence_mode, "--coexistence-mode")?;
    if !matches!(coexistence_mode.as_str(), "shadow" | "side_by_side") {
        return Err(SkeinError::Semantic(
            "--coexistence-mode must be shadow or side_by_side".to_string(),
        ));
    }
    let content_store_engine =
        require_non_empty(inputs.content_store_engine, "--content-store-engine")?;
    let previous_wrapper_preflight = require_json(
        inputs.previous_wrapper_preflight,
        "--previous-wrapper-preflight-json",
    )?;
    let replacement_summary =
        require_json(inputs.replacement_summary, "--replacement-summary-json")?;
    let bounded_read_evidence =
        require_json(inputs.bounded_read_evidence, "--bounded-read-evidence-json")?;
    let graph_route_readiness =
        require_json(inputs.graph_route_readiness, "--graph-route-readiness-json")?;
    let route_ownership = require_json(inputs.route_ownership, "--route-ownership-json")?;
    let search_route_ownership = require_json(
        inputs.search_route_ownership,
        "--search-route-ownership-json",
    )?;
    let active_search_route_ownership = require_json(
        inputs.active_search_route_ownership,
        "--active-search-route-ownership-json",
    )?;
    let active_search_route_readiness = require_json(
        inputs.active_search_route_readiness,
        "--active-search-route-readiness-json",
    )?;
    let query_runtime_preflight = require_json(
        inputs.query_runtime_preflight,
        "--query-runtime-preflight-json",
    )?;
    let search_candidate_shadow_evidence = require_json(
        inputs.search_candidate_shadow_evidence,
        "--search-candidate-shadow-evidence-json",
    )?;
    let library_readiness = require_json(inputs.library_readiness, "--library-readiness-json")?;
    let cutover_controls = require_json(inputs.cutover_controls, "--cutover-controls-json")?;
    let operations_readiness =
        require_json(inputs.operations_readiness, "--operations-readiness-json")?;
    let blackbox_manifest = require_json(inputs.blackbox_manifest, "--blackbox-manifest-json")?;
    let bounded_alignment =
        bounded_read_alignment_json(&bounded_read_evidence, &replacement_summary);
    let graph_route_alignment =
        graph_route_alignment_json(&graph_route_readiness, &replacement_summary);
    let search_route_ownership_alignment = search_route_ownership_alignment_json(
        &search_route_ownership,
        &replacement_summary,
        "search_route_ownership",
        REQUIRED_NOWLEDGE_MEM_SEARCH_ROUTES,
    );
    let active_search_route_ownership_alignment = search_route_ownership_alignment_json(
        &active_search_route_ownership,
        &replacement_summary,
        "active_search_route_ownership",
        REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES,
    );
    let active_search_route_readiness_alignment = active_search_route_readiness_alignment_json(
        &active_search_route_readiness,
        &replacement_summary,
    );
    let query_runtime_alignment =
        query_runtime_alignment_json(&query_runtime_preflight, &replacement_summary);
    let graph_route_parity_alignment = graph_route_parity_alignment_json(&graph_route_readiness);

    Ok(serde_json::json!({
        "protocol": NOWLEDGE_MEM_SKEIN_INTEGRATION_BUNDLE_PROTOCOL,
        "submodule": {
            "present": true,
            "path": sanitize_path_label(&submodule_path),
            "commit": submodule_commit,
            "blocker_codes": []
        },
        "coexistence": {
            "old_database_retained": inputs.legacy_data_retained,
            "old_database_deleted": inputs.legacy_data_deleted,
            "mode": coexistence_mode,
            "blocker_codes": coexistence_blocker_codes(
                inputs.legacy_data_retained,
                inputs.legacy_data_deleted
            )
        },
        "content_store": {
            "present": inputs.content_store_present,
            "engine": content_store_engine,
            "messages_available": inputs.content_store_messages_available,
            "source_chunks_available": inputs.content_store_source_chunks_available,
            "blocker_codes": content_store_blocker_codes(
                inputs.content_store_present,
                inputs.content_store_messages_available,
                inputs.content_store_source_chunks_available
            )
        },
        "previous_wrapper_preflight": previous_wrapper_preflight,
        "replacement_summary": replacement_summary,
        "bounded_read_evidence": bounded_read_evidence,
        "replacement_summary_bounded_read_alignment": bounded_alignment,
        "graph_route_readiness": graph_route_readiness,
        "replacement_summary_graph_route_alignment": graph_route_alignment,
        "graph_route_parity_alignment": graph_route_parity_alignment,
        "route_ownership": route_ownership,
        "search_route_ownership": search_route_ownership,
        "replacement_summary_search_route_ownership_alignment": search_route_ownership_alignment,
        "active_search_route_ownership": active_search_route_ownership,
        "replacement_summary_active_search_route_ownership_alignment": active_search_route_ownership_alignment,
        "active_search_route_readiness": active_search_route_readiness,
        "replacement_summary_active_search_route_readiness_alignment": active_search_route_readiness_alignment,
        "query_runtime_preflight": query_runtime_preflight,
        "replacement_summary_query_runtime_alignment": query_runtime_alignment,
        "search_candidate_shadow_evidence": search_candidate_shadow_evidence,
        "library_readiness": library_readiness,
        "cutover_controls": cutover_controls,
        "operations_readiness": operations_readiness,
        "blackbox_manifest": blackbox_manifest,
    }))
}

fn graph_route_alignment_json(
    graph_route_readiness: &serde_json::Value,
    replacement_summary: &serde_json::Value,
) -> serde_json::Value {
    let summary = replacement_summary
        .get("graph_route_readiness")
        .unwrap_or(&serde_json::Value::Null);
    let evidence = nowledge_graph_route_readiness_summary(graph_route_readiness);
    let summary = nowledge_graph_route_readiness_summary(summary);
    let evidence_present = evidence.present;
    let summary_present = summary.present;
    let evidence_protocol_matches =
        evidence.evidence_protocol.as_deref() == Some(NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL);
    let evidence_ready = evidence.evidence_ready == Some(true);
    let evidence_route_primary_ready = evidence.route_primary_ready == Some(true);
    let summary_route_primary_ready = summary.route_primary_ready == Some(true);
    let evidence_route_query_plan_evidence_ready =
        evidence.route_query_plan_evidence_ready == Some(true);
    let summary_route_query_plan_evidence_ready =
        summary.route_query_plan_evidence_ready == Some(true);
    let evidence_route_query_profile_evidence_ready =
        evidence.route_query_profile_evidence_ready == Some(true);
    let summary_route_query_profile_evidence_ready =
        summary.route_query_profile_evidence_ready == Some(true);
    let evidence_route_query_api_behavior_evidence_ready =
        evidence.route_query_api_behavior_evidence_ready == Some(true);
    let summary_route_query_api_behavior_evidence_ready =
        summary.route_query_api_behavior_evidence_ready == Some(true);
    let evidence_route_relationship_property_pruning_evidence_ready =
        evidence.route_relationship_property_pruning_evidence_ready == Some(true);
    let summary_route_relationship_property_pruning_evidence_ready =
        summary.route_relationship_property_pruning_evidence_ready == Some(true);
    let evidence_relationship_property_pruning_required_count =
        evidence.relationship_property_pruning_required_count;
    let summary_relationship_property_pruning_required_count =
        summary.relationship_property_pruning_required_count;
    let evidence_relationship_property_pruning_report_count =
        evidence.relationship_property_pruning_report_count;
    let summary_relationship_property_pruning_report_count =
        summary.relationship_property_pruning_report_count;
    let evidence_route_catalog_metadata_ready = evidence.route_catalog_metadata_ready;
    let summary_route_catalog_metadata_ready = summary.route_catalog_metadata_ready;
    let route_catalog_metadata_ready_matches =
        evidence_route_catalog_metadata_ready == summary_route_catalog_metadata_ready;
    let evidence_route_catalog_version_ready =
        str_path(graph_route_readiness, &["route_catalog_version"])
            == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION);
    let summary_route_catalog_version_ready =
        str_path(
            replacement_summary,
            &["graph_route_readiness", "route_catalog_version"],
        ) == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION);
    let evidence_route_catalog_digest_ready =
        str_path(graph_route_readiness, &["route_catalog_digest"])
            == Some(nowledge_mem_graph_read_route_catalog_digest().as_str());
    let summary_route_catalog_digest_ready =
        str_path(
            replacement_summary,
            &["graph_route_readiness", "route_catalog_digest"],
        ) == Some(nowledge_mem_graph_read_route_catalog_digest().as_str());
    let route_catalog_version_matches = str_path(graph_route_readiness, &["route_catalog_version"])
        == str_path(
            replacement_summary,
            &["graph_route_readiness", "route_catalog_version"],
        );
    let route_catalog_digest_matches = str_path(graph_route_readiness, &["route_catalog_digest"])
        == str_path(
            replacement_summary,
            &["graph_route_readiness", "route_catalog_digest"],
        );
    let evidence_primary_ready_routes = graph_route_primary_ready_routes(graph_route_readiness);
    let summary_primary_ready_routes = summary.covered_routes.into_iter().collect::<BTreeSet<_>>();
    let required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .map(|route| (*route).to_string())
        .collect::<BTreeSet<_>>();
    let route_primary_ready_matches = evidence_route_primary_ready == summary_route_primary_ready;
    let route_query_plan_evidence_ready_matches =
        evidence_route_query_plan_evidence_ready == summary_route_query_plan_evidence_ready;
    let route_query_profile_evidence_ready_matches =
        evidence_route_query_profile_evidence_ready == summary_route_query_profile_evidence_ready;
    let route_query_api_behavior_evidence_ready_matches =
        evidence_route_query_api_behavior_evidence_ready
            == summary_route_query_api_behavior_evidence_ready;
    let route_relationship_property_pruning_evidence_ready_matches =
        evidence_route_relationship_property_pruning_evidence_ready
            == summary_route_relationship_property_pruning_evidence_ready;
    let relationship_property_pruning_required_count_matches =
        evidence_relationship_property_pruning_required_count
            == summary_relationship_property_pruning_required_count;
    let relationship_property_pruning_report_count_matches =
        evidence_relationship_property_pruning_report_count
            == summary_relationship_property_pruning_report_count;
    let primary_ready_routes_match = evidence_primary_ready_routes == summary_primary_ready_routes;
    let evidence_required_routes_covered = required_routes
        .iter()
        .all(|route| evidence_primary_ready_routes.contains(route));
    let summary_required_routes_covered = required_routes
        .iter()
        .all(|route| summary_primary_ready_routes.contains(route));
    let alignment = GraphRouteAlignment {
        evidence_present,
        summary_present,
        evidence_protocol_matches,
        evidence_ready,
        evidence_route_primary_ready,
        summary_route_primary_ready,
        route_primary_ready_matches,
        evidence_route_query_plan_evidence_ready,
        summary_route_query_plan_evidence_ready,
        route_query_plan_evidence_ready_matches,
        evidence_route_query_profile_evidence_ready,
        summary_route_query_profile_evidence_ready,
        route_query_profile_evidence_ready_matches,
        evidence_route_query_api_behavior_evidence_ready,
        summary_route_query_api_behavior_evidence_ready,
        route_query_api_behavior_evidence_ready_matches,
        evidence_route_relationship_property_pruning_evidence_ready,
        summary_route_relationship_property_pruning_evidence_ready,
        route_relationship_property_pruning_evidence_ready_matches,
        relationship_property_pruning_required_count_matches,
        relationship_property_pruning_report_count_matches,
        evidence_route_catalog_metadata_ready,
        summary_route_catalog_metadata_ready,
        route_catalog_metadata_ready_matches,
        evidence_route_catalog_version_ready,
        summary_route_catalog_version_ready,
        evidence_route_catalog_digest_ready,
        summary_route_catalog_digest_ready,
        route_catalog_version_matches,
        route_catalog_digest_matches,
        primary_ready_routes_match,
        evidence_required_routes_covered,
        summary_required_routes_covered,
    };

    serde_json::json!({
        "ready": alignment.ready(),
        "evidence_present": evidence_present,
        "summary_present": summary_present,
        "evidence_protocol_matches": evidence_protocol_matches,
        "evidence_ready": evidence_ready,
        "evidence_route_primary_ready": evidence_route_primary_ready,
        "summary_route_primary_ready": summary_route_primary_ready,
        "route_primary_ready_matches": route_primary_ready_matches,
        "evidence_route_query_plan_evidence_ready": evidence_route_query_plan_evidence_ready,
        "summary_route_query_plan_evidence_ready": summary_route_query_plan_evidence_ready,
        "route_query_plan_evidence_ready_matches": route_query_plan_evidence_ready_matches,
        "evidence_route_query_profile_evidence_ready": evidence_route_query_profile_evidence_ready,
        "summary_route_query_profile_evidence_ready": summary_route_query_profile_evidence_ready,
        "route_query_profile_evidence_ready_matches": route_query_profile_evidence_ready_matches,
        "evidence_route_query_api_behavior_evidence_ready": evidence_route_query_api_behavior_evidence_ready,
        "summary_route_query_api_behavior_evidence_ready": summary_route_query_api_behavior_evidence_ready,
        "route_query_api_behavior_evidence_ready_matches": route_query_api_behavior_evidence_ready_matches,
        "evidence_route_relationship_property_pruning_evidence_ready": evidence_route_relationship_property_pruning_evidence_ready,
        "summary_route_relationship_property_pruning_evidence_ready": summary_route_relationship_property_pruning_evidence_ready,
        "route_relationship_property_pruning_evidence_ready_matches": route_relationship_property_pruning_evidence_ready_matches,
        "evidence_relationship_property_pruning_required_count": evidence_relationship_property_pruning_required_count,
        "summary_relationship_property_pruning_required_count": summary_relationship_property_pruning_required_count,
        "relationship_property_pruning_required_count_matches": relationship_property_pruning_required_count_matches,
        "evidence_relationship_property_pruning_report_count": evidence_relationship_property_pruning_report_count,
        "summary_relationship_property_pruning_report_count": summary_relationship_property_pruning_report_count,
        "relationship_property_pruning_report_count_matches": relationship_property_pruning_report_count_matches,
        "evidence_route_catalog_metadata_ready": evidence_route_catalog_metadata_ready,
        "summary_route_catalog_metadata_ready": summary_route_catalog_metadata_ready,
        "route_catalog_metadata_ready_matches": route_catalog_metadata_ready_matches,
        "evidence_route_catalog_version_ready": evidence_route_catalog_version_ready,
        "summary_route_catalog_version_ready": summary_route_catalog_version_ready,
        "evidence_route_catalog_digest_ready": evidence_route_catalog_digest_ready,
        "summary_route_catalog_digest_ready": summary_route_catalog_digest_ready,
        "route_catalog_version_matches": route_catalog_version_matches,
        "route_catalog_digest_matches": route_catalog_digest_matches,
        "primary_ready_routes_match": primary_ready_routes_match,
        "evidence_required_routes_covered": evidence_required_routes_covered,
        "summary_required_routes_covered": summary_required_routes_covered,
        "evidence_primary_ready_routes": evidence_primary_ready_routes,
        "summary_primary_ready_routes": summary_primary_ready_routes,
        "blocker_codes": alignment.blocker_codes()
    })
}

fn graph_route_primary_ready_routes(value: &serde_json::Value) -> BTreeSet<String> {
    value
        .get("routes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|route| bool_path(route, &["primary_ready"]) == Some(true))
        .filter_map(|route| str_path(route, &["route"]))
        .map(str::to_string)
        .collect()
}

fn graph_route_parity_alignment_json(
    graph_route_readiness: &serde_json::Value,
) -> serde_json::Value {
    let required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .map(|route| (*route).to_string())
        .collect::<BTreeSet<_>>();
    let mut observed_routes = BTreeSet::new();
    let mut ready_routes = BTreeSet::new();
    let mut not_ready_routes = BTreeSet::new();
    let mut blocker_routes = BTreeSet::new();

    for route in graph_route_readiness
        .get("routes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(route_name) = str_path(route, &["route"]) else {
            continue;
        };
        observed_routes.insert(route_name.to_string());
        if graph_route_shadow_compare_ready(route) {
            ready_routes.insert(route_name.to_string());
        } else {
            not_ready_routes.insert(route_name.to_string());
        }
        if route
            .get("blocker_codes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|blockers| !blockers.is_empty())
        {
            blocker_routes.insert(route_name.to_string());
        }
    }

    let missing_routes = required_routes
        .difference(&observed_routes)
        .cloned()
        .collect::<BTreeSet<_>>();
    let relevant_not_ready_routes = not_ready_routes
        .intersection(&required_routes)
        .cloned()
        .collect::<BTreeSet<_>>();
    let relevant_blocker_routes = blocker_routes
        .intersection(&required_routes)
        .cloned()
        .collect::<BTreeSet<_>>();
    let ready = !required_routes.is_empty()
        && missing_routes.is_empty()
        && relevant_not_ready_routes.is_empty()
        && relevant_blocker_routes.is_empty()
        && required_routes
            .iter()
            .all(|route| ready_routes.contains(route));

    serde_json::json!({
        "ready": ready,
        "required_route_count": required_routes.len(),
        "ready_route_count": required_routes
            .iter()
            .filter(|route| ready_routes.contains(*route))
            .count(),
        "ready_routes": ready_routes,
        "missing_routes": missing_routes,
        "not_ready_routes": relevant_not_ready_routes,
        "route_mismatch_routes": [],
        "protocol_mismatch_routes": [],
        "blocker_routes": relevant_blocker_routes,
        "observed_blocker_codes": string_set_path(graph_route_readiness, &["blocker_codes"]),
        "blocker_codes": graph_route_parity_blocker_codes(ready)
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
        && string_set_path(route, &["shadow_compare", "blocker_codes"]).is_empty()
        && string_set_path(route, &["shadow_compare", "computed_blocker_codes"]).is_empty()
}

fn is_legacy_graph_engine(engine: &str) -> bool {
    matches!(engine, "kuzu" | "ladybug" | "kuzu/ladybug")
}

fn graph_route_parity_blocker_codes(ready: bool) -> Vec<&'static str> {
    if ready {
        Vec::new()
    } else {
        vec!["graph_route_parity_alignment_not_ready"]
    }
}

#[derive(Debug, Clone, Copy)]
struct GraphRouteAlignment {
    evidence_present: bool,
    summary_present: bool,
    evidence_protocol_matches: bool,
    evidence_ready: bool,
    evidence_route_primary_ready: bool,
    summary_route_primary_ready: bool,
    route_primary_ready_matches: bool,
    evidence_route_query_plan_evidence_ready: bool,
    summary_route_query_plan_evidence_ready: bool,
    route_query_plan_evidence_ready_matches: bool,
    evidence_route_query_profile_evidence_ready: bool,
    summary_route_query_profile_evidence_ready: bool,
    route_query_profile_evidence_ready_matches: bool,
    evidence_route_query_api_behavior_evidence_ready: bool,
    summary_route_query_api_behavior_evidence_ready: bool,
    route_query_api_behavior_evidence_ready_matches: bool,
    evidence_route_relationship_property_pruning_evidence_ready: bool,
    summary_route_relationship_property_pruning_evidence_ready: bool,
    route_relationship_property_pruning_evidence_ready_matches: bool,
    relationship_property_pruning_required_count_matches: bool,
    relationship_property_pruning_report_count_matches: bool,
    evidence_route_catalog_metadata_ready: bool,
    summary_route_catalog_metadata_ready: bool,
    route_catalog_metadata_ready_matches: bool,
    evidence_route_catalog_version_ready: bool,
    summary_route_catalog_version_ready: bool,
    evidence_route_catalog_digest_ready: bool,
    summary_route_catalog_digest_ready: bool,
    route_catalog_version_matches: bool,
    route_catalog_digest_matches: bool,
    primary_ready_routes_match: bool,
    evidence_required_routes_covered: bool,
    summary_required_routes_covered: bool,
}

impl GraphRouteAlignment {
    fn ready(&self) -> bool {
        self.evidence_present
            && self.summary_present
            && self.evidence_protocol_matches
            && self.evidence_ready
            && self.evidence_route_primary_ready
            && self.summary_route_primary_ready
            && self.route_primary_ready_matches
            && self.evidence_route_query_plan_evidence_ready
            && self.summary_route_query_plan_evidence_ready
            && self.route_query_plan_evidence_ready_matches
            && self.evidence_route_query_profile_evidence_ready
            && self.summary_route_query_profile_evidence_ready
            && self.route_query_profile_evidence_ready_matches
            && self.evidence_route_query_api_behavior_evidence_ready
            && self.summary_route_query_api_behavior_evidence_ready
            && self.route_query_api_behavior_evidence_ready_matches
            && self.evidence_route_relationship_property_pruning_evidence_ready
            && self.summary_route_relationship_property_pruning_evidence_ready
            && self.route_relationship_property_pruning_evidence_ready_matches
            && self.relationship_property_pruning_required_count_matches
            && self.relationship_property_pruning_report_count_matches
            && self.evidence_route_catalog_metadata_ready
            && self.summary_route_catalog_metadata_ready
            && self.route_catalog_metadata_ready_matches
            && self.evidence_route_catalog_version_ready
            && self.summary_route_catalog_version_ready
            && self.evidence_route_catalog_digest_ready
            && self.summary_route_catalog_digest_ready
            && self.route_catalog_version_matches
            && self.route_catalog_digest_matches
            && self.primary_ready_routes_match
            && self.evidence_required_routes_covered
            && self.summary_required_routes_covered
    }

    fn blocker_codes(&self) -> Vec<&'static str> {
        let mut blockers = Vec::new();
        if !self.evidence_present {
            blockers.push("graph_route_readiness_missing");
        }
        if !self.summary_present {
            blockers.push("replacement_summary_graph_route_readiness_missing");
        }
        if !self.evidence_protocol_matches {
            blockers.push("graph_route_evidence_protocol_mismatch");
        }
        if !self.evidence_ready {
            blockers.push("graph_route_evidence_not_ready");
        }
        if !self.evidence_route_primary_ready {
            blockers.push("graph_route_readiness_not_primary_ready");
        }
        if !self.summary_route_primary_ready {
            blockers.push("replacement_summary_route_primary_not_ready");
        }
        if !self.route_primary_ready_matches {
            blockers.push("graph_route_primary_ready_mismatch");
        }
        if !self.evidence_route_query_plan_evidence_ready {
            blockers.push("graph_route_query_plan_evidence_not_ready");
        }
        if !self.summary_route_query_plan_evidence_ready {
            blockers.push("replacement_summary_query_plan_evidence_not_ready");
        }
        if !self.route_query_plan_evidence_ready_matches {
            blockers.push("graph_route_query_plan_evidence_mismatch");
        }
        if !self.evidence_route_query_profile_evidence_ready {
            blockers.push("graph_route_query_profile_evidence_not_ready");
        }
        if !self.summary_route_query_profile_evidence_ready {
            blockers.push("replacement_summary_query_profile_evidence_not_ready");
        }
        if !self.route_query_profile_evidence_ready_matches {
            blockers.push("graph_route_query_profile_evidence_mismatch");
        }
        if !self.evidence_route_query_api_behavior_evidence_ready {
            blockers.push("graph_route_query_api_behavior_evidence_not_ready");
        }
        if !self.summary_route_query_api_behavior_evidence_ready {
            blockers.push("replacement_summary_query_api_behavior_evidence_not_ready");
        }
        if !self.route_query_api_behavior_evidence_ready_matches {
            blockers.push("graph_route_query_api_behavior_evidence_mismatch");
        }
        if !self.evidence_route_relationship_property_pruning_evidence_ready {
            blockers.push("graph_route_relationship_property_pruning_evidence_not_ready");
        }
        if !self.summary_route_relationship_property_pruning_evidence_ready {
            blockers.push("replacement_summary_relationship_property_pruning_evidence_not_ready");
        }
        if !self.route_relationship_property_pruning_evidence_ready_matches {
            blockers.push("graph_route_relationship_property_pruning_evidence_mismatch");
        }
        if !self.relationship_property_pruning_required_count_matches {
            blockers.push("graph_route_relationship_property_pruning_required_count_mismatch");
        }
        if !self.relationship_property_pruning_report_count_matches {
            blockers.push("graph_route_relationship_property_pruning_report_count_mismatch");
        }
        if !self.evidence_route_catalog_metadata_ready {
            blockers.push("graph_route_catalog_metadata_not_ready");
        }
        if !self.summary_route_catalog_metadata_ready {
            blockers.push("replacement_summary_route_catalog_metadata_not_ready");
        }
        if !self.route_catalog_metadata_ready_matches {
            blockers.push("graph_route_catalog_metadata_ready_mismatch");
        }
        if !self.evidence_route_catalog_version_ready {
            blockers.push("graph_route_catalog_version_not_ready");
        }
        if !self.summary_route_catalog_version_ready {
            blockers.push("replacement_summary_graph_route_catalog_version_not_ready");
        }
        if !self.evidence_route_catalog_digest_ready {
            blockers.push("graph_route_catalog_digest_not_ready");
        }
        if !self.summary_route_catalog_digest_ready {
            blockers.push("replacement_summary_graph_route_catalog_digest_not_ready");
        }
        if !self.route_catalog_version_matches {
            blockers.push("graph_route_catalog_version_mismatch");
        }
        if !self.route_catalog_digest_matches {
            blockers.push("graph_route_catalog_digest_mismatch");
        }
        if !self.primary_ready_routes_match {
            blockers.push("graph_route_primary_ready_routes_mismatch");
        }
        if !self.evidence_required_routes_covered {
            blockers.push("graph_route_readiness_required_routes_missing");
        }
        if !self.summary_required_routes_covered {
            blockers.push("replacement_summary_primary_ready_routes_missing");
        }
        blockers
    }
}

fn bounded_read_alignment_json(
    bounded_read_evidence: &serde_json::Value,
    replacement_summary: &serde_json::Value,
) -> serde_json::Value {
    let summary = replacement_summary
        .get("bounded_read_evidence")
        .unwrap_or(&serde_json::Value::Null);
    let evidence_present = !bounded_read_evidence.is_null();
    let summary_present = !summary.is_null();
    let evidence_ready = bool_path(bounded_read_evidence, &["ready"]) == Some(true);
    let summary_ready = bool_path(summary, &["ready"]) == Some(true);
    let protocol_matches =
        str_path(bounded_read_evidence, &["protocol"]) == str_path(summary, &["protocol"]);
    let readiness_matches =
        bool_path(bounded_read_evidence, &["ready"]) == bool_path(summary, &["ready"]);
    let mode_matches = str_path(bounded_read_evidence, &["mode"]) == str_path(summary, &["mode"]);
    let max_rows_matches =
        u64_path(bounded_read_evidence, &["max_rows"]) == u64_path(summary, &["max_rows"]);
    let estimated_payload_bytes_matches =
        u64_path(bounded_read_evidence, &["estimated_payload_bytes"])
            == u64_path(summary, &["estimated_payload_bytes"]);
    let max_estimated_payload_bytes_matches =
        u64_path(bounded_read_evidence, &["max_estimated_payload_bytes"])
            == u64_path(summary, &["max_estimated_payload_bytes"]);
    let payload_budget_exceeded_matches =
        bool_path(bounded_read_evidence, &["payload_budget_exceeded"])
            == bool_path(summary, &["payload_budget_exceeded"]);
    let streaming_matches =
        bool_path(bounded_read_evidence, &["streaming"]) == bool_path(summary, &["streaming"]);
    let covered_routes_matches = string_set_path(bounded_read_evidence, &["covered_routes"])
        == string_set_path(summary, &["covered_routes"]);
    let evidence_route_catalog_version_ready =
        str_path(bounded_read_evidence, &["route_catalog_version"])
            == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION);
    let summary_route_catalog_version_ready = str_path(summary, &["route_catalog_version"])
        == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION);
    let evidence_route_catalog_digest_ready =
        str_path(bounded_read_evidence, &["route_catalog_digest"])
            == Some(nowledge_mem_graph_read_route_catalog_digest().as_str());
    let summary_route_catalog_digest_ready = str_path(summary, &["route_catalog_digest"])
        == Some(nowledge_mem_graph_read_route_catalog_digest().as_str());
    let route_catalog_version_matches = str_path(bounded_read_evidence, &["route_catalog_version"])
        == str_path(summary, &["route_catalog_version"]);
    let route_catalog_digest_matches = str_path(bounded_read_evidence, &["route_catalog_digest"])
        == str_path(summary, &["route_catalog_digest"]);
    let alignment = BoundedReadAlignment {
        evidence_present,
        summary_present,
        evidence_ready,
        summary_ready,
        protocol_matches,
        readiness_matches,
        mode_matches,
        max_rows_matches,
        estimated_payload_bytes_matches,
        max_estimated_payload_bytes_matches,
        payload_budget_exceeded_matches,
        streaming_matches,
        covered_routes_matches,
        evidence_route_catalog_version_ready,
        summary_route_catalog_version_ready,
        evidence_route_catalog_digest_ready,
        summary_route_catalog_digest_ready,
        route_catalog_version_matches,
        route_catalog_digest_matches,
    };

    serde_json::json!({
        "ready": alignment.ready(),
        "evidence_present": alignment.evidence_present,
        "summary_present": alignment.summary_present,
        "evidence_ready": alignment.evidence_ready,
        "summary_ready": alignment.summary_ready,
        "protocol_matches": alignment.protocol_matches,
        "readiness_matches": alignment.readiness_matches,
        "mode_matches": alignment.mode_matches,
        "max_rows_matches": alignment.max_rows_matches,
        "estimated_payload_bytes_matches": alignment.estimated_payload_bytes_matches,
        "max_estimated_payload_bytes_matches": alignment.max_estimated_payload_bytes_matches,
        "payload_budget_exceeded_matches": alignment.payload_budget_exceeded_matches,
        "streaming_matches": alignment.streaming_matches,
        "covered_routes_matches": alignment.covered_routes_matches,
        "evidence_route_catalog_version_ready": alignment.evidence_route_catalog_version_ready,
        "summary_route_catalog_version_ready": alignment.summary_route_catalog_version_ready,
        "evidence_route_catalog_digest_ready": alignment.evidence_route_catalog_digest_ready,
        "summary_route_catalog_digest_ready": alignment.summary_route_catalog_digest_ready,
        "route_catalog_version_matches": alignment.route_catalog_version_matches,
        "route_catalog_digest_matches": alignment.route_catalog_digest_matches,
        "blocker_codes": alignment.blocker_codes()
    })
}

#[derive(Debug, Clone, Copy)]
struct BoundedReadAlignment {
    evidence_present: bool,
    summary_present: bool,
    evidence_ready: bool,
    summary_ready: bool,
    protocol_matches: bool,
    readiness_matches: bool,
    mode_matches: bool,
    max_rows_matches: bool,
    estimated_payload_bytes_matches: bool,
    max_estimated_payload_bytes_matches: bool,
    payload_budget_exceeded_matches: bool,
    streaming_matches: bool,
    covered_routes_matches: bool,
    evidence_route_catalog_version_ready: bool,
    summary_route_catalog_version_ready: bool,
    evidence_route_catalog_digest_ready: bool,
    summary_route_catalog_digest_ready: bool,
    route_catalog_version_matches: bool,
    route_catalog_digest_matches: bool,
}

impl BoundedReadAlignment {
    fn ready(&self) -> bool {
        self.evidence_present
            && self.summary_present
            && self.evidence_ready
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

    fn blocker_codes(&self) -> Vec<&'static str> {
        let mut blockers = Vec::new();
        if !self.evidence_present {
            blockers.push("bounded_read_evidence_missing");
        }
        if !self.summary_present {
            blockers.push("replacement_summary_bounded_read_evidence_missing");
        }
        if !self.evidence_ready {
            blockers.push("bounded_read_evidence_not_ready");
        }
        if !self.summary_ready {
            blockers.push("replacement_summary_bounded_read_evidence_not_ready");
        }
        if !self.protocol_matches {
            blockers.push("bounded_read_protocol_mismatch");
        }
        if !self.readiness_matches {
            blockers.push("bounded_read_readiness_mismatch");
        }
        if !self.mode_matches {
            blockers.push("bounded_read_mode_mismatch");
        }
        if !self.max_rows_matches {
            blockers.push("bounded_read_max_rows_mismatch");
        }
        if !self.estimated_payload_bytes_matches {
            blockers.push("bounded_read_estimated_payload_bytes_mismatch");
        }
        if !self.max_estimated_payload_bytes_matches {
            blockers.push("bounded_read_max_estimated_payload_bytes_mismatch");
        }
        if !self.payload_budget_exceeded_matches {
            blockers.push("bounded_read_payload_budget_exceeded_mismatch");
        }
        if !self.streaming_matches {
            blockers.push("bounded_read_streaming_mismatch");
        }
        if !self.covered_routes_matches {
            blockers.push("bounded_read_covered_routes_mismatch");
        }
        if !self.evidence_route_catalog_version_ready {
            blockers.push("bounded_read_route_catalog_version_not_ready");
        }
        if !self.summary_route_catalog_version_ready {
            blockers.push("replacement_summary_route_catalog_version_not_ready");
        }
        if !self.evidence_route_catalog_digest_ready {
            blockers.push("bounded_read_route_catalog_digest_not_ready");
        }
        if !self.summary_route_catalog_digest_ready {
            blockers.push("replacement_summary_route_catalog_digest_not_ready");
        }
        if !self.route_catalog_version_matches {
            blockers.push("bounded_read_route_catalog_version_mismatch");
        }
        if !self.route_catalog_digest_matches {
            blockers.push("bounded_read_route_catalog_digest_mismatch");
        }
        blockers
    }
}

fn search_route_ownership_alignment_json(
    evidence: &serde_json::Value,
    replacement_summary: &serde_json::Value,
    field: &str,
    required_routes: &[&str],
) -> serde_json::Value {
    let summary = replacement_summary
        .get(field)
        .unwrap_or(&serde_json::Value::Null);
    let evidence_present = !evidence.is_null();
    let summary_present = !summary.is_null();
    let protocol_matches = str_path(evidence, &["protocol"]) == str_path(summary, &["protocol"])
        && str_path(evidence, &["protocol"]) == Some(NOWLEDGE_MEM_SEARCH_ROUTE_OWNERSHIP_PROTOCOL);
    let ready_matches = bool_path(evidence, &["ready"]) == bool_path(summary, &["ready"]);
    let production_cutover_ready_matches = bool_path(evidence, &["production_cutover_ready"])
        == bool_path(summary, &["production_cutover_ready"]);
    let require_all_skein_matches =
        bool_path(evidence, &["require_all_skein"]) == bool_path(summary, &["require_all_skein"]);
    let expected_count = required_routes.len() as u64;
    let required_route_count_matches = u64_path(evidence, &["required_route_count"])
        == u64_path(summary, &["required_route_count"])
        && u64_path(evidence, &["required_route_count"]) == Some(expected_count);
    let explicit_route_count_matches = u64_path(evidence, &["explicit_route_count"])
        == u64_path(summary, &["explicit_route_count"])
        && u64_path(evidence, &["explicit_route_count"]) == Some(expected_count);
    let skein_route_count_matches = u64_path(evidence, &["skein_route_count"])
        == u64_path(summary, &["skein_route_count"])
        && u64_path(evidence, &["skein_route_count"]) == Some(expected_count);
    let lancedb_route_count_matches = u64_path(evidence, &["lancedb_route_count"])
        == u64_path(summary, &["lancedb_route_count"])
        && u64_path(evidence, &["lancedb_route_count"]) == Some(0);
    let missing_required_routes_matches = string_set_path(evidence, &["missing_required_routes"])
        == string_set_path(summary, &["missing_required_routes"])
        && string_set_path(evidence, &["missing_required_routes"]).is_empty();
    let lancedb_routes_matches = string_set_path(evidence, &["lancedb_routes"])
        == string_set_path(summary, &["lancedb_routes"])
        && string_set_path(evidence, &["lancedb_routes"]).is_empty();
    let blocker_codes_match = string_set_path(evidence, &["blocker_codes"])
        == string_set_path(summary, &["blocker_codes"])
        && string_set_path(evidence, &["blocker_codes"]).is_empty();
    let ready = evidence_present
        && summary_present
        && protocol_matches
        && ready_matches
        && bool_path(evidence, &["ready"]) == Some(true)
        && production_cutover_ready_matches
        && bool_path(evidence, &["production_cutover_ready"]) == Some(true)
        && require_all_skein_matches
        && bool_path(evidence, &["require_all_skein"]) == Some(true)
        && required_route_count_matches
        && explicit_route_count_matches
        && skein_route_count_matches
        && lancedb_route_count_matches
        && missing_required_routes_matches
        && lancedb_routes_matches
        && blocker_codes_match;
    serde_json::json!({
        "ready": ready,
        "evidence_present": evidence_present,
        "summary_present": summary_present,
        "protocol_matches": protocol_matches,
        "ready_matches": ready_matches,
        "production_cutover_ready_matches": production_cutover_ready_matches,
        "require_all_skein_matches": require_all_skein_matches,
        "required_route_count_matches": required_route_count_matches,
        "explicit_route_count_matches": explicit_route_count_matches,
        "skein_route_count_matches": skein_route_count_matches,
        "lancedb_route_count_matches": lancedb_route_count_matches,
        "missing_required_routes_matches": missing_required_routes_matches,
        "lancedb_routes_matches": lancedb_routes_matches,
        "blocker_codes_match": blocker_codes_match,
        "evidence_lancedb_routes": string_set_path(evidence, &["lancedb_routes"]),
        "summary_lancedb_routes": string_set_path(summary, &["lancedb_routes"]),
        "blocker_codes": search_route_ownership_alignment_blockers(
            ready,
            evidence_present,
            summary_present,
            protocol_matches,
            ready_matches,
            production_cutover_ready_matches,
            require_all_skein_matches,
            required_route_count_matches,
            explicit_route_count_matches,
            skein_route_count_matches,
            lancedb_route_count_matches,
            missing_required_routes_matches,
            lancedb_routes_matches,
            blocker_codes_match,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn search_route_ownership_alignment_blockers(
    ready: bool,
    evidence_present: bool,
    summary_present: bool,
    protocol_matches: bool,
    ready_matches: bool,
    production_cutover_ready_matches: bool,
    require_all_skein_matches: bool,
    required_route_count_matches: bool,
    explicit_route_count_matches: bool,
    skein_route_count_matches: bool,
    lancedb_route_count_matches: bool,
    missing_required_routes_matches: bool,
    lancedb_routes_matches: bool,
    blocker_codes_match: bool,
) -> Vec<&'static str> {
    if ready {
        return Vec::new();
    }
    let mut blockers = Vec::new();
    if !evidence_present {
        blockers.push("search_route_ownership_missing");
    }
    if !summary_present {
        blockers.push("replacement_summary_search_route_ownership_missing");
    }
    if !protocol_matches {
        blockers.push("search_route_ownership_protocol_mismatch");
    }
    if !ready_matches {
        blockers.push("search_route_ownership_ready_mismatch");
    }
    if !production_cutover_ready_matches {
        blockers.push("search_route_ownership_cutover_ready_mismatch");
    }
    if !require_all_skein_matches {
        blockers.push("search_route_ownership_policy_mismatch");
    }
    if !required_route_count_matches {
        blockers.push("search_route_ownership_required_count_mismatch");
    }
    if !explicit_route_count_matches {
        blockers.push("search_route_ownership_explicit_count_mismatch");
    }
    if !skein_route_count_matches {
        blockers.push("search_route_ownership_skein_count_mismatch");
    }
    if !lancedb_route_count_matches {
        blockers.push("search_route_ownership_lancedb_count_mismatch");
    }
    if !missing_required_routes_matches {
        blockers.push("search_route_ownership_missing_routes_mismatch");
    }
    if !lancedb_routes_matches {
        blockers.push("search_route_ownership_lancedb_routes_mismatch");
    }
    if !blocker_codes_match {
        blockers.push("search_route_ownership_blocker_codes_mismatch");
    }
    blockers
}

fn active_search_route_readiness_alignment_json(
    evidence: &serde_json::Value,
    replacement_summary: &serde_json::Value,
) -> serde_json::Value {
    let summary = replacement_summary
        .get("active_search_route_readiness")
        .unwrap_or(&serde_json::Value::Null);
    let evidence_present = !evidence.is_null();
    let summary_present = !summary.is_null();
    let protocol_matches = str_path(evidence, &["protocol"]) == str_path(summary, &["protocol"])
        && str_path(evidence, &["protocol"])
            == Some(NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTE_READINESS_PROTOCOL);
    let ready_matches = bool_path(evidence, &["ready"]) == bool_path(summary, &["ready"]);
    let production_cutover_ready_matches = bool_path(evidence, &["production_cutover_ready"])
        == bool_path(summary, &["production_cutover_ready"]);
    let require_all_skein_matches =
        bool_path(evidence, &["require_all_skein"]) == bool_path(summary, &["require_all_skein"]);
    let expected_count = REQUIRED_NOWLEDGE_MEM_ACTIVE_SEARCH_ROUTES.len() as u64;
    let required_route_count_matches = u64_path(evidence, &["required_route_count"])
        == u64_path(summary, &["required_route_count"])
        && u64_path(evidence, &["required_route_count"]) == Some(expected_count);
    let evidence_route_count_matches = u64_path(evidence, &["evidence_route_count"])
        == u64_path(summary, &["evidence_route_count"])
        && u64_path(evidence, &["evidence_route_count"]) == Some(expected_count);
    let ready_route_count_matches = u64_path(evidence, &["ready_route_count"])
        == u64_path(summary, &["ready_route_count"])
        && u64_path(evidence, &["ready_route_count"]) == Some(expected_count);
    let skein_route_count_matches = u64_path(evidence, &["skein_route_count"])
        == u64_path(summary, &["skein_route_count"])
        && u64_path(evidence, &["skein_route_count"]) == Some(expected_count);
    let lancedb_handle_count_matches = u64_path(evidence, &["lancedb_handle_required_route_count"])
        == u64_path(summary, &["lancedb_handle_required_route_count"])
        && u64_path(evidence, &["lancedb_handle_required_route_count"]) == Some(0);
    let missing_required_routes_matches =
        empty_string_set_matches(evidence, summary, "missing_required_routes");
    let non_skein_routes_matches = empty_string_set_matches(evidence, summary, "non_skein_routes");
    let lancedb_handle_routes_matches =
        empty_string_set_matches(evidence, summary, "lancedb_handle_required_routes");
    let candidate_not_ready_routes_matches =
        empty_string_set_matches(evidence, summary, "candidate_not_ready_routes");
    let candidate_identity_not_ready_routes_matches =
        empty_string_set_matches(evidence, summary, "candidate_identity_not_ready_routes");
    let embedding_identity_not_ready_routes_matches =
        empty_string_set_matches(evidence, summary, "embedding_identity_not_ready_routes");
    let zero_vector_semantics_not_ready_routes_matches =
        empty_string_set_matches(evidence, summary, "zero_vector_semantics_not_ready_routes");
    let cjk_tokenization_not_ready_routes_matches =
        empty_string_set_matches(evidence, summary, "cjk_tokenization_not_ready_routes");
    let metadata_pushdown_not_ready_routes_matches =
        empty_string_set_matches(evidence, summary, "metadata_pushdown_not_ready_routes");
    let ranking_window_not_ready_routes_matches =
        empty_string_set_matches(evidence, summary, "ranking_window_not_ready_routes");
    let ranking_not_ready_routes_matches =
        empty_string_set_matches(evidence, summary, "ranking_not_ready_routes");
    let fail_soft_not_ready_routes_matches =
        empty_string_set_matches(evidence, summary, "fail_soft_not_ready_routes");
    let fail_soft_reason_codes_not_ready_routes_matches =
        empty_string_set_matches(evidence, summary, "fail_soft_reason_codes_not_ready_routes");
    let repair_rebuild_markers_not_ready_routes_matches =
        empty_string_set_matches(evidence, summary, "repair_rebuild_markers_not_ready_routes");
    let blocker_codes_match = string_set_path(evidence, &["blocker_codes"])
        == string_set_path(summary, &["blocker_codes"])
        && string_set_path(evidence, &["blocker_codes"]).is_empty();
    let checks = [
        ("active_search_route_readiness_missing", evidence_present),
        (
            "replacement_summary_active_search_route_readiness_missing",
            summary_present,
        ),
        (
            "active_search_route_readiness_protocol_mismatch",
            protocol_matches,
        ),
        (
            "active_search_route_readiness_ready_mismatch",
            ready_matches,
        ),
        (
            "active_search_route_readiness_cutover_ready_mismatch",
            production_cutover_ready_matches,
        ),
        (
            "active_search_route_readiness_policy_mismatch",
            require_all_skein_matches,
        ),
        (
            "active_search_route_readiness_required_count_mismatch",
            required_route_count_matches,
        ),
        (
            "active_search_route_readiness_evidence_count_mismatch",
            evidence_route_count_matches,
        ),
        (
            "active_search_route_readiness_ready_count_mismatch",
            ready_route_count_matches,
        ),
        (
            "active_search_route_readiness_skein_count_mismatch",
            skein_route_count_matches,
        ),
        (
            "active_search_route_readiness_lancedb_handle_count_mismatch",
            lancedb_handle_count_matches,
        ),
        (
            "active_search_route_readiness_missing_routes_mismatch",
            missing_required_routes_matches,
        ),
        (
            "active_search_route_readiness_non_skein_routes_mismatch",
            non_skein_routes_matches,
        ),
        (
            "active_search_route_readiness_lancedb_handle_routes_mismatch",
            lancedb_handle_routes_matches,
        ),
        (
            "active_search_route_readiness_candidate_routes_mismatch",
            candidate_not_ready_routes_matches,
        ),
        (
            "active_search_route_readiness_candidate_identity_routes_mismatch",
            candidate_identity_not_ready_routes_matches,
        ),
        (
            "active_search_route_readiness_embedding_identity_routes_mismatch",
            embedding_identity_not_ready_routes_matches,
        ),
        (
            "active_search_route_readiness_zero_vector_semantics_routes_mismatch",
            zero_vector_semantics_not_ready_routes_matches,
        ),
        (
            "active_search_route_readiness_cjk_tokenization_routes_mismatch",
            cjk_tokenization_not_ready_routes_matches,
        ),
        (
            "active_search_route_readiness_metadata_pushdown_routes_mismatch",
            metadata_pushdown_not_ready_routes_matches,
        ),
        (
            "active_search_route_readiness_ranking_window_routes_mismatch",
            ranking_window_not_ready_routes_matches,
        ),
        (
            "active_search_route_readiness_ranking_routes_mismatch",
            ranking_not_ready_routes_matches,
        ),
        (
            "active_search_route_readiness_fail_soft_routes_mismatch",
            fail_soft_not_ready_routes_matches,
        ),
        (
            "active_search_route_readiness_fail_soft_reason_codes_routes_mismatch",
            fail_soft_reason_codes_not_ready_routes_matches,
        ),
        (
            "active_search_route_readiness_repair_rebuild_markers_routes_mismatch",
            repair_rebuild_markers_not_ready_routes_matches,
        ),
        (
            "active_search_route_readiness_blocker_codes_mismatch",
            blocker_codes_match,
        ),
    ];
    let ready = checks.iter().all(|(_, value)| *value)
        && bool_path(evidence, &["ready"]) == Some(true)
        && bool_path(evidence, &["production_cutover_ready"]) == Some(true)
        && bool_path(evidence, &["require_all_skein"]) == Some(true);
    serde_json::json!({
        "ready": ready,
        "evidence_present": evidence_present,
        "summary_present": summary_present,
        "protocol_matches": protocol_matches,
        "ready_matches": ready_matches,
        "production_cutover_ready_matches": production_cutover_ready_matches,
        "require_all_skein_matches": require_all_skein_matches,
        "required_route_count_matches": required_route_count_matches,
        "evidence_route_count_matches": evidence_route_count_matches,
        "ready_route_count_matches": ready_route_count_matches,
        "skein_route_count_matches": skein_route_count_matches,
        "lancedb_handle_count_matches": lancedb_handle_count_matches,
        "missing_required_routes_matches": missing_required_routes_matches,
        "non_skein_routes_matches": non_skein_routes_matches,
        "lancedb_handle_routes_matches": lancedb_handle_routes_matches,
        "candidate_not_ready_routes_matches": candidate_not_ready_routes_matches,
        "candidate_identity_not_ready_routes_matches": candidate_identity_not_ready_routes_matches,
        "embedding_identity_not_ready_routes_matches": embedding_identity_not_ready_routes_matches,
        "zero_vector_semantics_not_ready_routes_matches": zero_vector_semantics_not_ready_routes_matches,
        "cjk_tokenization_not_ready_routes_matches": cjk_tokenization_not_ready_routes_matches,
        "metadata_pushdown_not_ready_routes_matches": metadata_pushdown_not_ready_routes_matches,
        "ranking_window_not_ready_routes_matches": ranking_window_not_ready_routes_matches,
        "ranking_not_ready_routes_matches": ranking_not_ready_routes_matches,
        "fail_soft_not_ready_routes_matches": fail_soft_not_ready_routes_matches,
        "fail_soft_reason_codes_not_ready_routes_matches": fail_soft_reason_codes_not_ready_routes_matches,
        "repair_rebuild_markers_not_ready_routes_matches": repair_rebuild_markers_not_ready_routes_matches,
        "blocker_codes_match": blocker_codes_match,
        "evidence_lancedb_handle_required_routes": string_set_path(evidence, &["lancedb_handle_required_routes"]),
        "summary_lancedb_handle_required_routes": string_set_path(summary, &["lancedb_handle_required_routes"]),
        "blocker_codes": alignment_blockers(ready, &checks),
    })
}

fn empty_string_set_matches(
    evidence: &serde_json::Value,
    summary: &serde_json::Value,
    field: &str,
) -> bool {
    string_set_path(evidence, &[field]) == string_set_path(summary, &[field])
        && string_set_path(evidence, &[field]).is_empty()
}

fn alignment_blockers(ready: bool, checks: &[(&'static str, bool)]) -> Vec<&'static str> {
    if ready {
        Vec::new()
    } else {
        checks
            .iter()
            .filter_map(|(code, passed)| (!*passed).then_some(*code))
            .collect()
    }
}

fn query_runtime_alignment_json(
    query_runtime_preflight: &serde_json::Value,
    replacement_summary: &serde_json::Value,
) -> serde_json::Value {
    let summary = replacement_summary
        .get("query_runtime_preflight")
        .unwrap_or(&serde_json::Value::Null);
    let evidence_present = !query_runtime_preflight.is_null();
    let summary_present = !summary.is_null();
    let evidence_ready = bool_path(query_runtime_preflight, &["ready"]) == Some(true);
    let summary_ready = bool_path(summary, &["ready"]) == Some(true);
    let protocol_matches =
        str_path(query_runtime_preflight, &["protocol"]) == str_path(summary, &["protocol"]);
    let readiness_matches =
        bool_path(query_runtime_preflight, &["ready"]) == bool_path(summary, &["ready"]);
    let database_opened_matches = bool_path(query_runtime_preflight, &["database_opened"])
        == bool_path(summary, &["database_opened"]);
    let probe_count_matches =
        u64_path(query_runtime_preflight, &["probe_count"]) == u64_path(summary, &["probe_count"]);
    let passed_probe_count_matches = u64_path(query_runtime_preflight, &["passed_probe_count"])
        == u64_path(summary, &["passed_probe_count"]);
    let failed_probe_count_matches = u64_path(query_runtime_preflight, &["failed_probe_count"])
        == u64_path(summary, &["failed_probe_count"]);
    let required_route_count_matches = u64_path(query_runtime_preflight, &["required_route_count"])
        == u64_path(summary, &["required_route_count"]);
    let covered_route_count_matches = u64_path(query_runtime_preflight, &["covered_route_count"])
        == u64_path(summary, &["covered_route_count"]);
    let evidence_covered_routes = query_runtime_probe_route_set(query_runtime_preflight);
    let summary_covered_routes = string_set_path(summary, &["covered_routes"]);
    let covered_routes_matches = evidence_covered_routes == summary_covered_routes;
    let required_routes_covered_matches =
        bool_path(query_runtime_preflight, &["required_routes_covered"])
            == bool_path(summary, &["required_routes_covered"]);
    let route_coverage_ready_matches = query_runtime_route_coverage_ready(query_runtime_preflight)
        == bool_path(summary, &["route_coverage_ready"]);
    let evidence_route_catalog_version_ready =
        str_path(query_runtime_preflight, &["route_catalog_version"])
            == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION);
    let summary_route_catalog_version_ready = str_path(summary, &["route_catalog_version"])
        == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION);
    let evidence_route_catalog_digest_ready =
        str_path(query_runtime_preflight, &["route_catalog_digest"])
            == Some(nowledge_mem_graph_read_route_catalog_digest().as_str());
    let summary_route_catalog_digest_ready = str_path(summary, &["route_catalog_digest"])
        == Some(nowledge_mem_graph_read_route_catalog_digest().as_str());
    let route_catalog_version_matches =
        str_path(query_runtime_preflight, &["route_catalog_version"])
            == str_path(summary, &["route_catalog_version"]);
    let route_catalog_digest_matches = str_path(query_runtime_preflight, &["route_catalog_digest"])
        == str_path(summary, &["route_catalog_digest"]);
    let alignment = QueryRuntimeAlignment {
        evidence_present,
        summary_present,
        evidence_ready,
        summary_ready,
        protocol_matches,
        readiness_matches,
        database_opened_matches,
        probe_count_matches,
        passed_probe_count_matches,
        failed_probe_count_matches,
        required_route_count_matches,
        covered_route_count_matches,
        covered_routes_matches,
        required_routes_covered_matches,
        route_coverage_ready_matches,
        evidence_route_catalog_version_ready,
        summary_route_catalog_version_ready,
        evidence_route_catalog_digest_ready,
        summary_route_catalog_digest_ready,
        route_catalog_version_matches,
        route_catalog_digest_matches,
    };

    serde_json::json!({
        "ready": alignment.ready(),
        "evidence_present": alignment.evidence_present,
        "summary_present": alignment.summary_present,
        "evidence_ready": alignment.evidence_ready,
        "summary_ready": alignment.summary_ready,
        "protocol_matches": alignment.protocol_matches,
        "readiness_matches": alignment.readiness_matches,
        "database_opened_matches": alignment.database_opened_matches,
        "probe_count_matches": alignment.probe_count_matches,
        "passed_probe_count_matches": alignment.passed_probe_count_matches,
        "failed_probe_count_matches": alignment.failed_probe_count_matches,
        "required_route_count_matches": alignment.required_route_count_matches,
        "covered_route_count_matches": alignment.covered_route_count_matches,
        "covered_routes_matches": alignment.covered_routes_matches,
        "required_routes_covered_matches": alignment.required_routes_covered_matches,
        "route_coverage_ready_matches": alignment.route_coverage_ready_matches,
        "evidence_route_catalog_version_ready": alignment.evidence_route_catalog_version_ready,
        "summary_route_catalog_version_ready": alignment.summary_route_catalog_version_ready,
        "evidence_route_catalog_digest_ready": alignment.evidence_route_catalog_digest_ready,
        "summary_route_catalog_digest_ready": alignment.summary_route_catalog_digest_ready,
        "route_catalog_version_matches": alignment.route_catalog_version_matches,
        "route_catalog_digest_matches": alignment.route_catalog_digest_matches,
        "evidence_covered_routes": evidence_covered_routes,
        "summary_covered_routes": summary_covered_routes,
        "blocker_codes": alignment.blocker_codes()
    })
}

fn query_runtime_probe_route_set(value: &serde_json::Value) -> BTreeSet<String> {
    query_runtime_probe_routes(value).into_iter().collect()
}

fn query_runtime_probe_routes(value: &serde_json::Value) -> Vec<String> {
    value
        .get("probes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|probe| str_path(probe, &["route"]))
        .map(str::to_string)
        .collect()
}

fn query_runtime_route_coverage_ready(value: &serde_json::Value) -> Option<bool> {
    let observed_routes = query_runtime_probe_routes(value);
    let observed_route_set = observed_routes.iter().cloned().collect::<BTreeSet<_>>();
    let unknown_route_count = observed_route_set
        .iter()
        .filter(|route| !REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(&route.as_str()))
        .count();
    let duplicate_routes = duplicate_routes(&observed_routes);
    Some(
        str_path(value, &["protocol"]) == Some(SKEIN_NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL)
            && u64_path(value, &["required_route_count"])
                == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
            && u64_path(value, &["covered_route_count"])
                == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
            && bool_path(value, &["required_routes_covered"]) == Some(true)
            && string_set_path(value, &["missing_required_routes"]).is_empty()
            && string_set_path(value, &["unknown_routes"]).is_empty()
            && string_set_path(value, &["duplicate_routes"]).is_empty()
            && str_path(value, &["route_catalog_version"])
                == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION)
            && str_path(value, &["route_catalog_digest"])
                == Some(nowledge_mem_graph_read_route_catalog_digest().as_str())
            && bool_path(value, &["route_coverage_ready"]) == Some(true)
            && string_set_path(value, &["route_coverage_blocker_codes"]).is_empty()
            && unknown_route_count == 0
            && duplicate_routes.is_empty()
            && REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
                .iter()
                .all(|route| observed_route_set.contains(*route)),
    )
}

fn duplicate_routes(routes: &[String]) -> Vec<String> {
    let mut counts = BTreeMap::<&str, usize>::new();
    for route in routes {
        *counts.entry(route).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(route, _)| route.to_string())
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct QueryRuntimeAlignment {
    evidence_present: bool,
    summary_present: bool,
    evidence_ready: bool,
    summary_ready: bool,
    protocol_matches: bool,
    readiness_matches: bool,
    database_opened_matches: bool,
    probe_count_matches: bool,
    passed_probe_count_matches: bool,
    failed_probe_count_matches: bool,
    required_route_count_matches: bool,
    covered_route_count_matches: bool,
    covered_routes_matches: bool,
    required_routes_covered_matches: bool,
    route_coverage_ready_matches: bool,
    evidence_route_catalog_version_ready: bool,
    summary_route_catalog_version_ready: bool,
    evidence_route_catalog_digest_ready: bool,
    summary_route_catalog_digest_ready: bool,
    route_catalog_version_matches: bool,
    route_catalog_digest_matches: bool,
}

impl QueryRuntimeAlignment {
    fn ready(&self) -> bool {
        self.evidence_present
            && self.summary_present
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

    fn blocker_codes(&self) -> Vec<&'static str> {
        let mut blockers = Vec::new();
        if !self.evidence_present {
            blockers.push("query_runtime_preflight_missing");
        }
        if !self.summary_present {
            blockers.push("replacement_summary_query_runtime_preflight_missing");
        }
        if !self.evidence_ready {
            blockers.push("query_runtime_preflight_not_ready");
        }
        if !self.summary_ready {
            blockers.push("replacement_summary_query_runtime_preflight_not_ready");
        }
        if !self.protocol_matches {
            blockers.push("query_runtime_preflight_protocol_mismatch");
        }
        if !self.readiness_matches {
            blockers.push("query_runtime_preflight_readiness_mismatch");
        }
        if !self.database_opened_matches {
            blockers.push("query_runtime_preflight_database_opened_mismatch");
        }
        if !self.probe_count_matches {
            blockers.push("query_runtime_preflight_probe_count_mismatch");
        }
        if !self.passed_probe_count_matches {
            blockers.push("query_runtime_preflight_passed_probe_count_mismatch");
        }
        if !self.failed_probe_count_matches {
            blockers.push("query_runtime_preflight_failed_probe_count_mismatch");
        }
        if !self.required_route_count_matches {
            blockers.push("query_runtime_preflight_required_route_count_mismatch");
        }
        if !self.covered_route_count_matches {
            blockers.push("query_runtime_preflight_covered_route_count_mismatch");
        }
        if !self.covered_routes_matches {
            blockers.push("query_runtime_preflight_covered_routes_mismatch");
        }
        if !self.required_routes_covered_matches {
            blockers.push("query_runtime_preflight_required_routes_covered_mismatch");
        }
        if !self.route_coverage_ready_matches {
            blockers.push("query_runtime_preflight_route_coverage_ready_mismatch");
        }
        if !self.evidence_route_catalog_version_ready {
            blockers.push("query_runtime_preflight_route_catalog_version_not_ready");
        }
        if !self.summary_route_catalog_version_ready {
            blockers.push("replacement_summary_query_runtime_route_catalog_version_not_ready");
        }
        if !self.evidence_route_catalog_digest_ready {
            blockers.push("query_runtime_preflight_route_catalog_digest_not_ready");
        }
        if !self.summary_route_catalog_digest_ready {
            blockers.push("replacement_summary_query_runtime_route_catalog_digest_not_ready");
        }
        if !self.route_catalog_version_matches {
            blockers.push("query_runtime_preflight_route_catalog_version_mismatch");
        }
        if !self.route_catalog_digest_matches {
            blockers.push("query_runtime_preflight_route_catalog_digest_mismatch");
        }
        blockers
    }
}

fn coexistence_blocker_codes(
    legacy_data_retained: bool,
    legacy_data_deleted: bool,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if !legacy_data_retained {
        blockers.push("legacy_data_not_retained");
    }
    if legacy_data_deleted {
        blockers.push("legacy_data_deleted");
    }
    blockers
}

fn content_store_blocker_codes(
    present: bool,
    messages_available: bool,
    source_chunks_available: bool,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if !present {
        blockers.push("content_store_missing");
    }
    if !messages_available {
        blockers.push("content_store_messages_missing");
    }
    if !source_chunks_available {
        blockers.push("content_store_source_chunks_missing");
    }
    blockers
}

fn sanitize_path_label(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("<redacted>")
        .to_string()
}

fn require_non_empty(value: Option<String>, flag: &str) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| SkeinError::Semantic(format!("{flag} is required")))
}

fn require_json(value: Option<serde_json::Value>, flag: &str) -> Result<serde_json::Value> {
    value.ok_or_else(|| SkeinError::Semantic(format!("{flag} is required")))
}

fn string_set_path(value: &serde_json::Value, path: &[&str]) -> BTreeSet<String> {
    value_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests;
