//! Shared graph-route readiness summaries; no database or host activation dependencies.
use crate::evidence_json::{
    json_get_array_path_from_dynamic, json_get_bool_path, json_get_bool_path_from_dynamic,
    json_get_path, json_get_path_from_dynamic, json_get_str_path, json_get_str_path_from_dynamic,
    json_get_string_array_path_from_dynamic, json_get_u64_path_from_dynamic,
};
use crate::graph_route::{NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL, NMEM_GRAPH_ROUTE_READINESS_PROTOCOL};
use skein_route_ownership::graph::{
    nowledge_mem_graph_read_route_spec, nowledge_mem_graph_read_route_specs_json,
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRouteReadinessSummary {
    pub protocol: Option<String>,
    pub present: bool,
    pub ready: bool,
    pub evidence_protocol: Option<String>,
    pub evidence_ready: Option<bool>,
    pub required_route_count: Option<u64>,
    pub covered_route_count: Option<u64>,
    pub covered_routes: Vec<String>,
    pub missing_required_routes: Vec<&'static str>,
    pub unknown_routes: Vec<String>,
    pub duplicate_routes: Vec<String>,
    pub route_coverage_ready: Option<bool>,
    pub evidence_route_coverage_present: Option<bool>,
    pub evidence_route_coverage_matches: Option<bool>,
    pub route_query_runtime_ready: Option<bool>,
    pub route_query_plan_evidence_ready: Option<bool>,
    pub route_query_profile_evidence_ready: Option<bool>,
    pub route_query_api_behavior_evidence_ready: Option<bool>,
    pub relationship_property_pruning_required_count: Option<u64>,
    pub relationship_property_pruning_report_count: Option<u64>,
    pub route_relationship_property_pruning_evidence_ready: Option<bool>,
    pub route_primary_ready: Option<bool>,
    pub primary_ready_route_count: Option<u64>,
    pub route_catalog_metadata_ready: bool,
    pub missing_route_catalog_metadata_routes: Vec<&'static str>,
    pub route_catalog_metadata_mismatch_routes: Vec<String>,
    pub blocker_codes: serde_json::Value,
}

impl GraphRouteReadinessSummary {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "present": self.present,
            "ready": self.ready,
            "evidence_protocol": self.evidence_protocol,
            "evidence_ready": self.evidence_ready,
            "required_route_count": self.required_route_count,
            "covered_route_count": self.covered_route_count,
            "covered_routes": self.covered_routes,
            "required_covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "missing_required_routes": self.missing_required_routes,
            "unknown_routes": self.unknown_routes,
            "duplicate_routes": self.duplicate_routes,
            "route_coverage_ready": self.route_coverage_ready,
            "evidence_route_coverage_present": self.evidence_route_coverage_present,
            "evidence_route_coverage_matches": self.evidence_route_coverage_matches,
            "route_query_runtime_ready": self.route_query_runtime_ready,
            "route_query_plan_evidence_ready": self.route_query_plan_evidence_ready,
            "route_query_profile_evidence_ready": self.route_query_profile_evidence_ready,
            "route_query_api_behavior_evidence_ready": self.route_query_api_behavior_evidence_ready,
            "relationship_property_pruning_required_count": self.relationship_property_pruning_required_count,
            "relationship_property_pruning_report_count": self.relationship_property_pruning_report_count,
            "route_relationship_property_pruning_evidence_ready": self.route_relationship_property_pruning_evidence_ready,
            "route_primary_ready": self.route_primary_ready,
            "primary_ready_route_count": self.primary_ready_route_count,
            "route_catalog": nowledge_mem_graph_read_route_specs_json(),
            "route_catalog_metadata_ready": self.route_catalog_metadata_ready,
            "missing_route_catalog_metadata_routes": self.missing_route_catalog_metadata_routes,
            "route_catalog_metadata_mismatch_routes": self.route_catalog_metadata_mismatch_routes,
            "blocker_codes": self.blocker_codes,
        })
    }
}
pub fn nowledge_graph_route_readiness_summary_from_bundle(
    bundle: &serde_json::Value,
) -> GraphRouteReadinessSummary {
    let path = if json_get_path(bundle, &["graph_route_readiness"]).is_some() {
        &["graph_route_readiness"][..]
    } else {
        &["cutover_evidence", "graph_route_readiness"][..]
    };
    graph_route_readiness_summary_at(bundle, path)
}

pub fn nowledge_graph_route_readiness_summary(
    value: &serde_json::Value,
) -> GraphRouteReadinessSummary {
    graph_route_readiness_summary_at(value, &[])
}

fn graph_route_readiness_summary_at(
    bundle: &serde_json::Value,
    path: &[&str],
) -> GraphRouteReadinessSummary {
    let present = json_get_path(bundle, path).is_some_and(|value| !value.is_null());
    let protocol = json_get_str_path_from_dynamic(bundle, path, "protocol").map(str::to_string);
    let evidence_protocol =
        json_get_str_path_from_dynamic(bundle, path, "evidence_protocol").map(str::to_string);
    let evidence_ready = json_get_bool_path_from_dynamic(bundle, path, "evidence_ready");
    let required_route_count = json_get_u64_path_from_dynamic(bundle, path, "required_route_count");
    let covered_route_count = json_get_u64_path_from_dynamic(bundle, path, "covered_route_count");
    let covered_routes = json_get_string_array_path_from_dynamic(bundle, path, "covered_routes");
    let covered_route_set = covered_routes
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !covered_route_set.contains(route))
        .collect::<Vec<_>>();
    let unknown_routes = covered_routes
        .iter()
        .map(String::as_str)
        .filter(|route| !REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let duplicate_routes = duplicate_strings(&covered_routes);
    let route_coverage_ready =
        json_get_bool_path_from_dynamic(bundle, path, "route_coverage_ready");
    let evidence_route_coverage_present =
        json_get_bool_path_from_dynamic(bundle, path, "evidence_route_coverage_present");
    let evidence_route_coverage_matches =
        json_get_bool_path_from_dynamic(bundle, path, "evidence_route_coverage_matches");
    let route_query_runtime_ready =
        json_get_bool_path_from_dynamic(bundle, path, "route_query_runtime_ready");
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
    let route_primary_ready = json_get_bool_path_from_dynamic(bundle, path, "route_primary_ready");
    let primary_ready_route_count =
        json_get_u64_path_from_dynamic(bundle, path, "primary_ready_route_count");
    let required_route_len = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64;
    let relationship_property_pruning_counts_match =
        relationship_property_pruning_required_count == relationship_property_pruning_report_count;
    let route_catalog_metadata = graph_route_catalog_metadata_summary(bundle, path);
    let ready = present
        && protocol.as_deref() == Some(NMEM_GRAPH_ROUTE_READINESS_PROTOCOL)
        && evidence_protocol.as_deref() == Some(NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL)
        && evidence_ready == Some(true)
        && required_route_count == Some(required_route_len)
        && covered_route_count == Some(required_route_len)
        && missing_required_routes.is_empty()
        && unknown_routes.is_empty()
        && duplicate_routes.is_empty()
        && route_coverage_ready == Some(true)
        && evidence_route_coverage_present == Some(true)
        && evidence_route_coverage_matches == Some(true)
        && route_query_runtime_ready == Some(true)
        && route_query_plan_evidence_ready == Some(true)
        && route_query_profile_evidence_ready == Some(true)
        && route_query_api_behavior_evidence_ready == Some(true)
        && route_relationship_property_pruning_evidence_ready == Some(true)
        && relationship_property_pruning_required_count.is_some()
        && relationship_property_pruning_counts_match
        && route_primary_ready == Some(true)
        && primary_ready_route_count == Some(required_route_len)
        && route_catalog_metadata.ready;
    GraphRouteReadinessSummary {
        protocol,
        present,
        ready,
        evidence_protocol,
        evidence_ready,
        required_route_count,
        covered_route_count,
        covered_routes,
        missing_required_routes,
        unknown_routes,
        duplicate_routes,
        route_coverage_ready,
        evidence_route_coverage_present,
        evidence_route_coverage_matches,
        route_query_runtime_ready,
        route_query_plan_evidence_ready,
        route_query_profile_evidence_ready,
        route_query_api_behavior_evidence_ready,
        relationship_property_pruning_required_count,
        relationship_property_pruning_report_count,
        route_relationship_property_pruning_evidence_ready,
        route_primary_ready,
        primary_ready_route_count,
        route_catalog_metadata_ready: route_catalog_metadata.ready,
        missing_route_catalog_metadata_routes: route_catalog_metadata.missing_routes,
        route_catalog_metadata_mismatch_routes: route_catalog_metadata.mismatch_routes,
        blocker_codes: json_get_array_path_from_dynamic(
            bundle,
            path,
            "route_primary_blocker_codes",
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GraphRouteCatalogMetadataSummary {
    ready: bool,
    missing_routes: Vec<&'static str>,
    mismatch_routes: Vec<String>,
}

fn graph_route_catalog_metadata_summary(
    bundle: &serde_json::Value,
    path: &[&str],
) -> GraphRouteCatalogMetadataSummary {
    let catalog_matches = json_get_path_from_dynamic(bundle, path, "route_catalog")
        == Some(&nowledge_mem_graph_read_route_specs_json());
    let routes = json_get_path_from_dynamic(bundle, path, "routes")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|route| json_get_str_path(route, &["route"]).map(|name| (name, route)))
        .collect::<BTreeMap<_, _>>();
    let mut missing_routes = Vec::new();
    let mut mismatch_routes = Vec::new();

    for route in REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES {
        let Some(entry) = routes.get(route) else {
            missing_routes.push(*route);
            continue;
        };
        match graph_route_catalog_metadata_entry_state(route, entry) {
            GraphRouteCatalogMetadataEntryState::Ready => {}
            GraphRouteCatalogMetadataEntryState::Missing => missing_routes.push(*route),
            GraphRouteCatalogMetadataEntryState::Mismatch => {
                mismatch_routes.push((*route).to_string())
            }
        }
    }
    if !catalog_matches {
        mismatch_routes.push("__route_catalog__".to_string());
    }

    GraphRouteCatalogMetadataSummary {
        ready: catalog_matches && missing_routes.is_empty() && mismatch_routes.is_empty(),
        missing_routes,
        mismatch_routes,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphRouteCatalogMetadataEntryState {
    Ready,
    Missing,
    Mismatch,
}

fn graph_route_catalog_metadata_entry_state(
    route: &str,
    entry: &serde_json::Value,
) -> GraphRouteCatalogMetadataEntryState {
    let Some(spec) = nowledge_mem_graph_read_route_spec(route) else {
        return GraphRouteCatalogMetadataEntryState::Mismatch;
    };
    let owner = json_get_str_path(entry, &["owner"]);
    let required_evidence_kind = json_get_str_path(entry, &["required_evidence_kind"]);
    let stale_on_catalog_change = json_get_bool_path(entry, &["stale_on_catalog_change"]);
    if owner.is_none() || required_evidence_kind.is_none() || stale_on_catalog_change.is_none() {
        return GraphRouteCatalogMetadataEntryState::Missing;
    }
    if owner == Some(spec.owner.as_str())
        && required_evidence_kind == Some(spec.required_evidence_kind.as_str())
        && stale_on_catalog_change == Some(spec.stale_on_catalog_change)
    {
        GraphRouteCatalogMetadataEntryState::Ready
    } else {
        GraphRouteCatalogMetadataEntryState::Mismatch
    }
}
fn duplicate_strings(values: &[String]) -> Vec<String> {
    let mut counts = BTreeMap::<&str, usize>::new();
    for value in values {
        *counts.entry(value.as_str()).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(value, _)| value.to_string())
        .collect()
}

#[cfg(test)]
mod tests;
