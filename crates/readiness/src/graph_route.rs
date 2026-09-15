//! Storage-independent graph route evidence validation.
//!
//! The embedded facade retains file/CLI adapters, database probes and activation.
use skein_core::{Result, SkeinError};
use skein_evidence::inventory::{
    NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL, REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};
use skein_route_ownership::graph::{
    nowledge_mem_graph_read_route_catalog_digest, nowledge_mem_graph_read_route_spec,
    nowledge_mem_graph_read_route_specs_json, nowledge_mem_required_query_families_for_route,
    NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};
use std::collections::{BTreeMap, BTreeSet};

pub const NMEM_GRAPH_ROUTE_READINESS_PROTOCOL: &str = "nmem-graph-route-readiness-v1";
pub use super::graph_route_catalog::NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL;
const ROUTE_PARITY_EVIDENCE_SOURCE: &str = "route_parity_evidence";
const ROUTE_PARITY_FULL_MATCH_PER_MILLION: u64 = 1_000_000;

pub fn nowledge_graph_route_readiness_json(
    evidence: &serde_json::Value,
) -> Result<serde_json::Value> {
    let parsed_evidence = parse_route_evidence(evidence)?;
    let evidence_protocol = parsed_evidence.protocol.clone();
    let evidence_ready = parsed_evidence.ready;
    let evidence_route_coverage = parsed_evidence.route_coverage;
    let routes = parsed_evidence.routes;
    let route_coverage = route_coverage(&routes);
    let evidence_route_coverage_present = evidence_route_coverage.is_some();
    let evidence_route_coverage_matches = evidence_route_coverage
        .as_ref()
        .is_some_and(|evidence| evidence.matches(&route_coverage));
    let evidence_route_coverage_blocker_codes = evidence_route_coverage_blocker_codes(
        evidence_route_coverage_present,
        evidence_route_coverage_matches,
    );
    let route_primary_blocker_codes = route_primary_blocker_codes(
        evidence_protocol.as_deref(),
        evidence_ready,
        &routes,
        &route_coverage,
        &evidence_route_coverage_blocker_codes,
    );
    let shadow_compare_route_count = routes
        .iter()
        .filter(|route| route.shadow_compare_ready)
        .count();
    let primary_ready_route_count = routes.iter().filter(|route| route.primary_ready).count();
    let query_runtime_ready_route_names = routes
        .iter()
        .filter(|route| route.query_runtime_ready())
        .map(|route| route.route.as_str())
        .collect::<BTreeSet<_>>();
    let missing_query_runtime_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .filter(|route| !query_runtime_ready_route_names.contains(**route))
        .copied()
        .collect::<Vec<_>>();
    let query_runtime_route_count =
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() - missing_query_runtime_routes.len();
    let query_runtime_report_count = routes
        .iter()
        .map(RouteEvidence::query_runtime_report_count)
        .sum::<usize>();
    let query_runtime_plan_report_count = routes
        .iter()
        .map(RouteEvidence::query_runtime_plan_report_count)
        .sum::<usize>();
    let query_runtime_profile_report_count = routes
        .iter()
        .map(RouteEvidence::query_runtime_profile_report_count)
        .sum::<usize>();
    let query_runtime_api_behavior_report_count = routes
        .iter()
        .map(RouteEvidence::query_runtime_api_behavior_report_count)
        .sum::<usize>();
    let query_runtime_failed_query_count = routes
        .iter()
        .map(RouteEvidence::query_runtime_failed_query_count)
        .sum::<usize>();
    let query_runtime_missing_plan_evidence_count = routes
        .iter()
        .map(RouteEvidence::query_runtime_missing_plan_evidence_count)
        .sum::<usize>();
    let query_runtime_missing_profile_evidence_count = routes
        .iter()
        .map(RouteEvidence::query_runtime_missing_profile_evidence_count)
        .sum::<usize>();
    let relationship_property_pruning_required_count = routes
        .iter()
        .map(RouteEvidence::relationship_property_pruning_required_count)
        .sum::<usize>();
    let relationship_property_pruning_report_count = routes
        .iter()
        .map(RouteEvidence::relationship_property_pruning_report_count)
        .sum::<usize>();
    let route_query_plan_evidence_ready =
        route_coverage.ready && routes.iter().all(RouteEvidence::query_plan_evidence_ready);
    let route_query_profile_evidence_ready = route_coverage.ready
        && routes
            .iter()
            .all(RouteEvidence::query_profile_evidence_ready);
    let route_query_api_behavior_evidence_ready = route_coverage.ready
        && routes
            .iter()
            .all(RouteEvidence::query_api_behavior_evidence_ready);
    let route_relationship_property_pruning_evidence_ready = route_coverage.ready
        && routes
            .iter()
            .all(RouteEvidence::relationship_property_pruning_evidence_ready);
    let route_primary_ready = route_coverage.ready && route_primary_blocker_codes.is_empty();
    let route_count = routes.len();

    Ok(serde_json::json!({
        "protocol": NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
        "evidence_protocol": evidence_protocol,
        "evidence_ready": evidence_ready,
        "route_count": route_count,
        "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "covered_route_count": route_coverage.covered_routes.len(),
        "covered_routes": route_coverage.covered_routes,
        "missing_required_routes": route_coverage.missing_required_routes,
        "required_routes_covered": route_coverage.required_routes_covered,
        "unknown_routes": route_coverage.unknown_routes,
        "duplicate_routes": route_coverage.duplicate_routes,
        "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
        "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
        "route_coverage_ready": route_coverage.ready,
        "route_coverage_blocker_codes": route_coverage.blocker_codes,
        "evidence_route_coverage_present": evidence_route_coverage_present,
        "evidence_route_coverage_matches": evidence_route_coverage_matches,
        "evidence_route_coverage_blocker_codes": evidence_route_coverage_blocker_codes,
        "shadow_compare_route_count": shadow_compare_route_count,
        "primary_ready_route_count": primary_ready_route_count,
        "query_runtime_route_count": query_runtime_route_count,
        "query_runtime_report_count": query_runtime_report_count,
        "query_runtime_plan_report_count": query_runtime_plan_report_count,
        "query_runtime_profile_report_count": query_runtime_profile_report_count,
        "query_runtime_api_behavior_report_count": query_runtime_api_behavior_report_count,
        "query_runtime_failed_query_count": query_runtime_failed_query_count,
        "query_runtime_missing_plan_evidence_count": query_runtime_missing_plan_evidence_count,
        "query_runtime_missing_profile_evidence_count": query_runtime_missing_profile_evidence_count,
        "relationship_property_pruning_required_count": relationship_property_pruning_required_count,
        "relationship_property_pruning_report_count": relationship_property_pruning_report_count,
        "missing_query_runtime_routes": missing_query_runtime_routes,
        "route_query_runtime_ready": missing_query_runtime_routes.is_empty(),
        "route_query_plan_evidence_ready": route_query_plan_evidence_ready,
        "route_query_profile_evidence_ready": route_query_profile_evidence_ready,
        "route_query_api_behavior_evidence_ready": route_query_api_behavior_evidence_ready,
        "route_relationship_property_pruning_evidence_ready": route_relationship_property_pruning_evidence_ready,
        "route_primary_ready": route_primary_ready,
        "route_primary_blocker_codes": route_primary_blocker_codes,
        "route_catalog": nowledge_mem_graph_read_route_specs_json(),
        "routes": routes.into_iter().map(RouteEvidence::json).collect::<Vec<_>>(),
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EvidenceRouteCoverage {
    required_route_count: Option<u64>,
    covered_route_count: Option<u64>,
    covered_routes: Vec<String>,
    missing_required_routes: Vec<String>,
    required_routes_covered: Option<bool>,
    unknown_routes: Vec<String>,
    duplicate_routes: Vec<String>,
    route_catalog_version: Option<String>,
    route_catalog_digest: Option<String>,
    ready: Option<bool>,
    blocker_codes: Vec<String>,
}

impl EvidenceRouteCoverage {
    fn parse(value: &serde_json::Value) -> Option<Self> {
        if value.is_array() {
            return None;
        }
        let has_route_coverage_field = [
            "required_route_count",
            "covered_route_count",
            "covered_routes",
            "missing_required_routes",
            "required_routes_covered",
            "unknown_routes",
            "duplicate_routes",
            "route_coverage_ready",
            "route_coverage_blocker_codes",
        ]
        .iter()
        .any(|field| value.get(*field).is_some());
        if !has_route_coverage_field {
            return None;
        }
        Some(Self {
            required_route_count: u64_path(value, &["required_route_count"]),
            covered_route_count: u64_path(value, &["covered_route_count"]),
            covered_routes: string_array_path(value, &["covered_routes"]),
            missing_required_routes: string_array_path(value, &["missing_required_routes"]),
            required_routes_covered: bool_path(value, &["required_routes_covered"]),
            unknown_routes: string_array_path(value, &["unknown_routes"]),
            duplicate_routes: string_array_path(value, &["duplicate_routes"]),
            route_catalog_version: str_path(value, &["route_catalog_version"]).map(str::to_string),
            route_catalog_digest: str_path(value, &["route_catalog_digest"]).map(str::to_string),
            ready: bool_path(value, &["route_coverage_ready"]),
            blocker_codes: string_array_path(value, &["route_coverage_blocker_codes"]),
        })
    }

    fn matches(&self, recomputed: &RouteCoverage) -> bool {
        self.required_route_count == Some(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() as u64)
            && self.covered_route_count == Some(recomputed.covered_routes.len() as u64)
            && self.covered_routes == recomputed.covered_routes
            && self.missing_required_routes == recomputed.missing_required_route_strings()
            && self.required_routes_covered == Some(recomputed.required_routes_covered)
            && self.unknown_routes == recomputed.unknown_routes
            && self.duplicate_routes == recomputed.duplicate_routes
            && self.route_catalog_version.as_deref()
                == Some(NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION)
            && self.route_catalog_digest.as_deref()
                == Some(nowledge_mem_graph_read_route_catalog_digest().as_str())
            && self.ready == Some(recomputed.ready)
            && self.blocker_codes == recomputed.blocker_code_strings()
    }
}

fn evidence_route_coverage_blocker_codes(
    present: bool,
    matches_recomputed: bool,
) -> Vec<&'static str> {
    if !present {
        return vec!["route_coverage_evidence_missing"];
    }
    if !matches_recomputed {
        return vec!["route_coverage_evidence_mismatch"];
    }
    Vec::new()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteCoverage {
    covered_routes: Vec<String>,
    missing_required_routes: Vec<&'static str>,
    required_routes_covered: bool,
    unknown_routes: Vec<String>,
    duplicate_routes: Vec<String>,
    ready: bool,
    blocker_codes: Vec<&'static str>,
}

impl RouteCoverage {
    fn missing_required_route_strings(&self) -> Vec<String> {
        self.missing_required_routes
            .iter()
            .map(|route| (*route).to_string())
            .collect()
    }

    fn blocker_code_strings(&self) -> Vec<String> {
        self.blocker_codes
            .iter()
            .map(|code| (*code).to_string())
            .collect()
    }
}

fn route_coverage(routes: &[RouteEvidence]) -> RouteCoverage {
    let required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut route_counts = BTreeMap::<&str, usize>::new();
    for route in routes {
        *route_counts.entry(route.route.as_str()).or_default() += 1;
    }
    let route_names = route_counts.keys().copied().collect::<BTreeSet<_>>();
    let covered_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .filter(|route| route_names.contains(**route))
        .map(|route| (*route).to_string())
        .collect::<Vec<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .filter(|route| !route_names.contains(**route))
        .copied()
        .collect::<Vec<_>>();
    let unknown_routes = route_names
        .iter()
        .filter(|route| !required_routes.contains(**route))
        .map(|route| (*route).to_string())
        .collect::<Vec<_>>();
    let duplicate_routes = route_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(route, _)| (*route).to_string())
        .collect::<Vec<_>>();
    let required_routes_covered = missing_required_routes.is_empty();
    let mut blocker_codes = Vec::new();
    if !required_routes_covered {
        blocker_codes.push("missing_required_routes");
    }
    if !unknown_routes.is_empty() {
        blocker_codes.push("unknown_routes");
    }
    if !duplicate_routes.is_empty() {
        blocker_codes.push("duplicate_routes");
    }
    let ready = blocker_codes.is_empty();
    RouteCoverage {
        covered_routes,
        missing_required_routes,
        required_routes_covered,
        unknown_routes,
        duplicate_routes,
        ready,
        blocker_codes,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteEvidence {
    route: String,
    shadow_compare_ready: bool,
    shadow_compare_evidence_source: Option<String>,
    shadow_compare: RouteShadowCompareEvidence,
    primary_ready: bool,
    required_query_families: Vec<String>,
    computed_required_query_families: Vec<String>,
    query_family_blocker_codes: Vec<String>,
    reported_relationship_property_pruning_required_count: Option<u64>,
    reported_relationship_property_pruning_report_count: Option<u64>,
    reported_relationship_property_pruning_evidence_ready: Option<bool>,
    blocker_codes: Vec<String>,
    query_reports: Vec<QueryRuntimeReport>,
}

impl RouteEvidence {
    fn query_runtime_ready(&self) -> bool {
        self.query_family_blocker_codes.is_empty()
            && !self.query_reports.is_empty()
            && self.query_reports.iter().all(QueryRuntimeReport::ready)
    }

    fn query_runtime_report_count(&self) -> usize {
        self.query_reports.len()
    }

    fn query_runtime_plan_report_count(&self) -> usize {
        self.query_reports
            .iter()
            .filter(|report| report.plan_evidence_ready())
            .count()
    }

    fn query_runtime_profile_report_count(&self) -> usize {
        self.query_reports
            .iter()
            .filter(|report| report.profile_evidence_ready())
            .count()
    }

    fn query_runtime_api_behavior_report_count(&self) -> usize {
        self.query_reports
            .iter()
            .filter(|report| report.api_behavior_evidence_ready())
            .count()
    }

    fn query_runtime_failed_query_count(&self) -> usize {
        self.query_reports
            .iter()
            .filter(|report| !report.ready())
            .count()
    }

    fn query_runtime_missing_plan_evidence_count(&self) -> usize {
        self.query_runtime_report_count() - self.query_runtime_plan_report_count()
    }

    fn query_runtime_missing_profile_evidence_count(&self) -> usize {
        self.query_runtime_report_count() - self.query_runtime_profile_report_count()
    }

    fn query_plan_evidence_ready(&self) -> bool {
        !self.query_reports.is_empty() && self.query_runtime_missing_plan_evidence_count() == 0
    }

    fn query_profile_evidence_ready(&self) -> bool {
        !self.query_reports.is_empty() && self.query_runtime_missing_profile_evidence_count() == 0
    }

    fn query_api_behavior_evidence_ready(&self) -> bool {
        !self.query_reports.is_empty()
            && self.query_runtime_api_behavior_report_count() == self.query_runtime_report_count()
    }

    fn relationship_property_pruning_required_count(&self) -> usize {
        self.reported_relationship_property_pruning_required_count
            .unwrap_or(0) as usize
    }

    fn relationship_property_pruning_report_count(&self) -> usize {
        self.query_reports
            .iter()
            .filter(|report| report.relationship_property_pruning_evidence_ready())
            .count()
    }

    fn relationship_property_pruning_evidence_ready(&self) -> bool {
        let Some(required_count) = self.reported_relationship_property_pruning_required_count
        else {
            return false;
        };
        let Some(reported_count) = self.reported_relationship_property_pruning_report_count else {
            return false;
        };
        self.reported_relationship_property_pruning_evidence_ready == Some(true)
            && reported_count == required_count
            && self.relationship_property_pruning_report_count() as u64 == reported_count
    }

    fn json(self) -> serde_json::Value {
        let query_runtime_ready = self.query_runtime_ready();
        let query_report_count = self.query_runtime_report_count();
        let query_runtime_plan_report_count = self.query_runtime_plan_report_count();
        let query_runtime_profile_report_count = self.query_runtime_profile_report_count();
        let query_runtime_api_behavior_report_count =
            self.query_runtime_api_behavior_report_count();
        let query_runtime_failed_query_count = self.query_runtime_failed_query_count();
        let query_runtime_missing_plan_evidence_count =
            self.query_runtime_missing_plan_evidence_count();
        let query_runtime_missing_profile_evidence_count =
            self.query_runtime_missing_profile_evidence_count();
        let query_plan_evidence_ready = self.query_plan_evidence_ready();
        let query_profile_evidence_ready = self.query_profile_evidence_ready();
        let query_api_behavior_evidence_ready = self.query_api_behavior_evidence_ready();
        let relationship_property_pruning_required_count =
            self.reported_relationship_property_pruning_required_count;
        let relationship_property_pruning_report_count =
            self.relationship_property_pruning_report_count();
        let reported_relationship_property_pruning_report_count =
            self.reported_relationship_property_pruning_report_count;
        let relationship_property_pruning_evidence_ready =
            self.relationship_property_pruning_evidence_ready();
        let query_reports = self
            .query_reports
            .into_iter()
            .map(QueryRuntimeReport::json)
            .collect::<Vec<_>>();
        let route_catalog_metadata = nowledge_mem_graph_read_route_spec(&self.route);
        let mut json = serde_json::json!({
            "route": self.route,
            "shadow_compare_ready": self.shadow_compare_ready,
            "shadow_compare_evidence_source": self.shadow_compare_evidence_source,
            "shadow_compare": self.shadow_compare.json(),
            "primary_ready": self.primary_ready,
            "required_query_families": self.required_query_families,
            "computed_required_query_families": self.computed_required_query_families,
            "query_family_blocker_codes": self.query_family_blocker_codes,
            "query_runtime_ready": query_runtime_ready,
            "query_report_count": query_report_count,
            "query_runtime_report_count": query_report_count,
            "query_runtime_plan_report_count": query_runtime_plan_report_count,
            "query_runtime_profile_report_count": query_runtime_profile_report_count,
            "query_runtime_api_behavior_report_count": query_runtime_api_behavior_report_count,
            "query_runtime_failed_query_count": query_runtime_failed_query_count,
            "query_runtime_missing_plan_evidence_count": query_runtime_missing_plan_evidence_count,
            "query_runtime_missing_profile_evidence_count": query_runtime_missing_profile_evidence_count,
            "relationship_property_pruning_required_count": relationship_property_pruning_required_count,
            "relationship_property_pruning_report_count": relationship_property_pruning_report_count,
            "reported_relationship_property_pruning_report_count": reported_relationship_property_pruning_report_count,
            "relationship_property_pruning_evidence_ready": relationship_property_pruning_evidence_ready,
            "query_plan_evidence_ready": query_plan_evidence_ready,
            "query_profile_evidence_ready": query_profile_evidence_ready,
            "query_api_behavior_evidence_ready": query_api_behavior_evidence_ready,
            "query_reports": query_reports,
            "blocker_codes": self.blocker_codes,
        });
        if let Some(spec) = route_catalog_metadata {
            let object = json.as_object_mut().expect("route JSON is an object");
            object.insert("owner".to_string(), serde_json::json!(spec.owner.as_str()));
            object.insert(
                "required_evidence_kind".to_string(),
                serde_json::json!(spec.required_evidence_kind.as_str()),
            );
            object.insert(
                "stale_on_catalog_change".to_string(),
                serde_json::json!(spec.stale_on_catalog_change),
            );
        }
        json
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteShadowCompareEvidence {
    present: bool,
    source: Option<String>,
    ready: Option<bool>,
    matched_per_million: Option<u64>,
    primary_engine: Option<String>,
    shadow_engine: Option<String>,
    blocker_codes: Vec<String>,
    computed_blocker_codes: Vec<String>,
}

impl RouteShadowCompareEvidence {
    fn parse(value: &serde_json::Value) -> Self {
        let Some(shadow_compare) = value_path(value, &["shadow_compare"]) else {
            return Self {
                present: false,
                source: None,
                ready: None,
                matched_per_million: None,
                primary_engine: None,
                shadow_engine: None,
                blocker_codes: Vec::new(),
                computed_blocker_codes: vec!["route_shadow_compare_detail_missing".to_string()],
            };
        };
        let mut parsed = Self {
            present: true,
            source: str_path(shadow_compare, &["source"]).map(str::to_string),
            ready: bool_path(shadow_compare, &["ready"]),
            matched_per_million: u64_path(shadow_compare, &["matched_per_million"]),
            primary_engine: str_path(shadow_compare, &["primary_engine"]).map(str::to_string),
            shadow_engine: str_path(shadow_compare, &["shadow_engine"]).map(str::to_string),
            blocker_codes: string_array_path(shadow_compare, &["blocker_codes"]),
            computed_blocker_codes: Vec::new(),
        };
        parsed.computed_blocker_codes = parsed.compute_blocker_codes();
        parsed
    }

    fn ready(&self) -> bool {
        self.computed_blocker_codes.is_empty()
    }

    fn compute_blocker_codes(&self) -> Vec<String> {
        let mut blockers = BTreeSet::new();
        if self.source.as_deref() != Some(ROUTE_PARITY_EVIDENCE_SOURCE) {
            blockers.insert("route_shadow_compare_detail_source_mismatch".to_string());
        }
        if self.ready != Some(true) {
            blockers.insert("route_shadow_compare_detail_not_ready".to_string());
        }
        if self.matched_per_million != Some(ROUTE_PARITY_FULL_MATCH_PER_MILLION) {
            blockers.insert("route_parity_matched_per_million_not_full".to_string());
        }
        if !self
            .primary_engine
            .as_deref()
            .is_some_and(is_legacy_graph_engine)
        {
            blockers.insert("route_parity_primary_engine_mismatch".to_string());
        }
        if self.shadow_engine.as_deref() != Some("skein") {
            blockers.insert("route_parity_shadow_engine_mismatch".to_string());
        }
        blockers.extend(self.blocker_codes.iter().cloned());
        blockers.into_iter().collect()
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "present": self.present,
            "source": self.source,
            "ready": self.ready,
            "matched_per_million": self.matched_per_million,
            "primary_engine": self.primary_engine,
            "shadow_engine": self.shadow_engine,
            "blocker_codes": self.blocker_codes,
            "computed_blocker_codes": self.computed_blocker_codes,
        })
    }
}

fn is_legacy_graph_engine(engine: &str) -> bool {
    matches!(engine, "kuzu" | "ladybug" | "kuzu/ladybug")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryRuntimeReport {
    query_name: Option<String>,
    query_index: Option<u64>,
    query_family: Option<String>,
    protocol: Option<String>,
    statement_kind: Option<String>,
    execution_path: Option<String>,
    fast_path_selected: Option<bool>,
    slow_log_candidate: Option<bool>,
    physical_plan_captured: Option<bool>,
    elapsed_micros: Option<u64>,
    physical_operator_counts_present: bool,
    optimizer_decision_count: Option<u64>,
    optimizer_rule_event_count: Option<u64>,
    scan_pruning_report_count: Option<u64>,
    scan_pruning_reports: Vec<serde_json::Value>,
    output_row_shape: QueryOutputRowShapeEvidence,
    plan_cache_lookup: Option<String>,
    plan_cache_cacheable: Option<bool>,
    plan_cache_hit: Option<bool>,
    plan_cache_miss: Option<bool>,
    plan_cache_bypassed: Option<bool>,
    api_behavior: QueryApiBehaviorEvidence,
    blocker_codes: Vec<String>,
}

impl QueryRuntimeReport {
    fn parse(value: &serde_json::Value) -> Self {
        let plan_cache_lookup = str_path(value, &["plan_cache", "lookup"])
            .or_else(|| str_path(value, &["plan_cache_lookup"]))
            .map(str::to_string);
        let plan_cache_cacheable = bool_path(value, &["plan_cache", "cacheable"])
            .or_else(|| bool_path(value, &["plan_cache_cacheable"]));
        let plan_cache_hit = bool_path(value, &["plan_cache", "hit"])
            .or_else(|| bool_path(value, &["plan_cache_hit"]));
        let plan_cache_miss = bool_path(value, &["plan_cache", "miss"])
            .or_else(|| bool_path(value, &["plan_cache_miss"]));
        let plan_cache_bypassed = bool_path(value, &["plan_cache", "bypassed"])
            .or_else(|| bool_path(value, &["plan_cache_bypassed"]));
        let mut report = Self {
            query_name: str_path(value, &["query_name"]).map(str::to_string),
            query_index: u64_path(value, &["query_index"]),
            query_family: str_path(value, &["query_family"]).map(str::to_string),
            protocol: str_path(value, &["protocol"]).map(str::to_string),
            statement_kind: str_path(value, &["statement_kind"]).map(str::to_string),
            execution_path: str_path(value, &["execution_path"]).map(str::to_string),
            fast_path_selected: bool_path(value, &["fast_path_selected"]),
            slow_log_candidate: bool_path(value, &["slow_log_candidate"]),
            physical_plan_captured: bool_path(value, &["physical_plan_captured"]),
            elapsed_micros: u64_path(value, &["elapsed_micros"]),
            physical_operator_counts_present: value_path(value, &["physical_operator_counts"])
                .is_some_and(serde_json::Value::is_object),
            optimizer_decision_count: u64_path(value, &["optimizer_decision_count"]),
            optimizer_rule_event_count: u64_path(value, &["optimizer_rule_event_count"]),
            scan_pruning_report_count: u64_path(value, &["scan_pruning_report_count"]),
            scan_pruning_reports: value_path(value, &["scan_pruning_reports"])
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default(),
            output_row_shape: QueryOutputRowShapeEvidence::parse(value),
            plan_cache_lookup,
            plan_cache_cacheable,
            plan_cache_hit,
            plan_cache_miss,
            plan_cache_bypassed,
            api_behavior: QueryApiBehaviorEvidence::parse(value),
            blocker_codes: Vec::new(),
        };
        report.blocker_codes = report.computed_blocker_codes();
        report
    }

    fn ready(&self) -> bool {
        self.blocker_codes.is_empty()
    }

    fn plan_evidence_ready(&self) -> bool {
        self.physical_operator_counts_present
            && self.optimizer_decision_count.is_some()
            && self.optimizer_rule_event_count.is_some()
            && self.plan_cache_state_ready()
    }

    fn profile_evidence_ready(&self) -> bool {
        self.elapsed_micros.is_some()
            && self.physical_plan_captured.is_some()
            && self.scan_pruning_reports_ready()
    }

    fn api_behavior_evidence_ready(&self) -> bool {
        self.api_behavior.ready()
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "query_name": self.query_name,
            "query_index": self.query_index,
            "query_family": self.query_family,
            "protocol": self.protocol,
            "statement_kind": self.statement_kind,
            "execution_path": self.execution_path,
            "fast_path_selected": self.fast_path_selected,
            "slow_log_candidate": self.slow_log_candidate,
            "physical_plan_captured": self.physical_plan_captured,
            "elapsed_micros": self.elapsed_micros,
            "physical_operator_counts_present": self.physical_operator_counts_present,
            "optimizer_decision_count": self.optimizer_decision_count,
            "optimizer_rule_event_count": self.optimizer_rule_event_count,
            "scan_pruning_report_count": self.scan_pruning_report_count,
            "scan_pruning_reports_present": !self.scan_pruning_reports.is_empty(),
            "scan_pruning_reports": self.scan_pruning_reports,
            "output_row_shape": self.output_row_shape.json(),
            "plan_cache_lookup": self.plan_cache_lookup.clone(),
            "plan_cache": {
                "lookup": self.plan_cache_lookup,
                "cacheable": self.plan_cache_cacheable,
                "hit": self.plan_cache_hit,
                "miss": self.plan_cache_miss,
                "bypassed": self.plan_cache_bypassed,
            },
            "api_behavior": self.api_behavior.json(),
            "ready": self.ready(),
            "blocker_codes": self.blocker_codes,
        })
    }

    fn computed_blocker_codes(&self) -> Vec<String> {
        let mut blockers = BTreeSet::new();
        if self
            .query_name
            .as_deref()
            .is_none_or(|name| name.trim().is_empty())
            || self.query_index.is_none()
        {
            blockers.insert("query_report_identity_missing".to_string());
        }
        match self.query_family.as_deref() {
            Some(family) if REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.contains(&family) => {}
            Some(_) => {
                blockers.insert("query_report_unknown_query_family".to_string());
            }
            None => {
                blockers.insert("query_report_query_family_missing".to_string());
            }
        }
        if self.protocol.as_deref() != Some(NOWLEDGE_MEM_QUERY_REPORT_PROTOCOL) {
            blockers.insert("query_report_protocol_mismatch".to_string());
        }
        if !self
            .statement_kind
            .as_deref()
            .is_some_and(is_nowledge_graph_read_statement_kind)
        {
            blockers.insert("query_report_not_graph_read".to_string());
        }
        if !matches!(
            self.execution_path.as_deref(),
            Some("fast_path" | "optimized_path")
        ) {
            blockers.insert("query_report_execution_path_missing".to_string());
        }
        if self.fast_path_selected.is_none() {
            blockers.insert("query_report_fast_path_selected_missing".to_string());
        }
        if self.slow_log_candidate.is_none() {
            blockers.insert("query_report_slow_log_candidate_missing".to_string());
        }
        if self.physical_plan_captured.is_none() {
            blockers.insert("query_report_physical_plan_flag_missing".to_string());
        }
        if self.elapsed_micros.is_none() {
            blockers.insert("query_report_elapsed_micros_missing".to_string());
        }
        if !self.physical_operator_counts_present {
            blockers.insert("query_report_physical_operator_counts_missing".to_string());
        }
        if self.optimizer_decision_count.is_none() {
            blockers.insert("query_report_optimizer_decision_count_missing".to_string());
        }
        if self.optimizer_rule_event_count.is_none() {
            blockers.insert("query_report_optimizer_rule_event_count_missing".to_string());
        }
        if !self.scan_pruning_reports_ready() {
            blockers.insert("query_report_scan_pruning_profile_missing".to_string());
        }
        if !self.output_row_shape.ready() {
            blockers.insert("query_report_output_row_shape_missing".to_string());
        }
        if self.plan_cache_cacheable.is_none()
            || self.plan_cache_hit.is_none()
            || self.plan_cache_miss.is_none()
            || self.plan_cache_bypassed.is_none()
        {
            blockers.insert("query_report_plan_cache_state_missing".to_string());
        }
        if matches!(self.plan_cache_lookup.as_deref(), Some("bypass"))
            || self.plan_cache_bypassed == Some(true)
        {
            blockers.insert("query_report_plan_cache_bypassed".to_string());
        }
        if self.api_behavior.include_metadata_false_strips_metadata != Some(true) {
            blockers.insert("query_report_api_behavior_metadata_stripping_missing".to_string());
        }
        if self.api_behavior.ordering_contract_recorded != Some(true) {
            blockers.insert("query_report_api_behavior_ordering_missing".to_string());
        }
        if self.api_behavior.pagination_contract_recorded != Some(true) {
            blockers.insert("query_report_api_behavior_pagination_missing".to_string());
        }
        if self.api_behavior.error_class_stable != Some(true) {
            blockers.insert("query_report_api_behavior_error_class_missing".to_string());
        }
        blockers.into_iter().collect()
    }

    fn plan_cache_state_ready(&self) -> bool {
        self.plan_cache_lookup
            .as_deref()
            .is_some_and(|lookup| !lookup.is_empty() && lookup != "bypass")
            && self.plan_cache_cacheable.is_some()
            && self.plan_cache_hit.is_some()
            && self.plan_cache_miss.is_some()
            && self.plan_cache_bypassed == Some(false)
    }

    fn relationship_property_pruning_evidence_ready(&self) -> bool {
        self.scan_pruning_reports
            .iter()
            .any(relationship_property_pruning_report_ready)
    }

    fn scan_pruning_reports_ready(&self) -> bool {
        self.scan_pruning_report_count == Some(self.scan_pruning_reports.len() as u64)
            && !self.scan_pruning_reports.is_empty()
            && self
                .scan_pruning_reports
                .iter()
                .all(scan_pruning_report_ready)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryApiBehaviorEvidence {
    include_metadata_false_strips_metadata: Option<bool>,
    ordering_contract_recorded: Option<bool>,
    pagination_contract_recorded: Option<bool>,
    error_class_stable: Option<bool>,
    statement_has_ordering: Option<bool>,
    statement_has_pagination: Option<bool>,
}

impl QueryApiBehaviorEvidence {
    fn parse(value: &serde_json::Value) -> Self {
        Self {
            include_metadata_false_strips_metadata: bool_path(
                value,
                &["api_behavior", "include_metadata_false_strips_metadata"],
            )
            .or_else(|| bool_path(value, &["include_metadata_false_strips_metadata"])),
            ordering_contract_recorded: bool_path(
                value,
                &["api_behavior", "ordering_contract_recorded"],
            ),
            pagination_contract_recorded: bool_path(
                value,
                &["api_behavior", "pagination_contract_recorded"],
            ),
            error_class_stable: bool_path(value, &["api_behavior", "error_class_stable"]),
            statement_has_ordering: bool_path(value, &["api_behavior", "statement_has_ordering"]),
            statement_has_pagination: bool_path(
                value,
                &["api_behavior", "statement_has_pagination"],
            ),
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "include_metadata_false_strips_metadata": self.include_metadata_false_strips_metadata,
            "ordering_contract_recorded": self.ordering_contract_recorded,
            "pagination_contract_recorded": self.pagination_contract_recorded,
            "error_class_stable": self.error_class_stable,
            "statement_has_ordering": self.statement_has_ordering,
            "statement_has_pagination": self.statement_has_pagination,
            "ready": self.ready(),
        })
    }

    fn ready(&self) -> bool {
        self.include_metadata_false_strips_metadata == Some(true)
            && self.ordering_contract_recorded == Some(true)
            && self.pagination_contract_recorded == Some(true)
            && self.error_class_stable == Some(true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryOutputRowShapeEvidence {
    row_count: Option<u64>,
    column_count: Option<u64>,
    columns: Vec<String>,
}

impl QueryOutputRowShapeEvidence {
    fn parse(value: &serde_json::Value) -> Self {
        Self {
            row_count: u64_path(value, &["output_row_shape", "row_count"]),
            column_count: u64_path(value, &["output_row_shape", "column_count"]),
            columns: string_array_path(value, &["output_row_shape", "columns"]),
        }
    }

    fn ready(&self) -> bool {
        self.row_count.is_some()
            && self.column_count == Some(self.columns.len() as u64)
            && !self.columns.is_empty()
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": self.columns,
            "ready": self.ready(),
        })
    }
}

fn scan_pruning_report_ready(report: &serde_json::Value) -> bool {
    scan_pruning_target_kind_ready(report)
        && value_path(report, &["strategy"])
            .filter(|strategy| {
                strategy.is_object()
                    && str_path(strategy, &["kind"]).is_some_and(|kind| !kind.is_empty())
            })
            .is_some()
        && bool_path(report, &["pruned"]).is_some()
        && bool_path(report, &["exact_empty"]).is_some()
        && u64_path(report, &["candidate_count_before_pruning"]).is_some()
        && u64_path(report, &["pruned_candidate_count"]).is_some()
        && u64_path(report, &["candidate_count_before_filter"]).is_some()
        && u64_path(report, &["output_count"]).is_some()
        && u64_path(report, &["filtered_out_count"]).is_some()
}

fn relationship_property_pruning_report_ready(report: &serde_json::Value) -> bool {
    scan_pruning_report_ready(report)
        && str_path(report, &["target_kind"]) == Some("relationship")
        && str_path(report, &["strategy", "kind"]) == Some("relationship_property")
}

fn scan_pruning_target_kind_ready(report: &serde_json::Value) -> bool {
    matches!(
        str_path(report, &["target_kind"]),
        Some("node" | "relationship")
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedRouteEvidence {
    protocol: Option<String>,
    ready: Option<bool>,
    route_coverage: Option<EvidenceRouteCoverage>,
    routes: Vec<RouteEvidence>,
}

fn parse_route_evidence(evidence: &serde_json::Value) -> Result<ParsedRouteEvidence> {
    let protocol = if evidence.is_array() {
        None
    } else {
        str_path(evidence, &["protocol"]).map(str::to_string)
    };
    let ready = if evidence.is_array() {
        None
    } else {
        bool_path(evidence, &["ready"])
    };
    let routes = if evidence.is_array() {
        evidence.as_array()
    } else {
        evidence.get("routes").and_then(serde_json::Value::as_array)
    }
    .ok_or_else(|| {
        SkeinError::Semantic("graph route evidence JSON must contain a routes array".to_string())
    })?;
    Ok(ParsedRouteEvidence {
        protocol,
        ready,
        route_coverage: EvidenceRouteCoverage::parse(evidence),
        routes: routes.iter().map(parse_route).collect::<Result<Vec<_>>>()?,
    })
}

fn parse_route(value: &serde_json::Value) -> Result<RouteEvidence> {
    let route = str_path(value, &["route"])
        .filter(|route| !route.trim().is_empty())
        .ok_or_else(|| SkeinError::Semantic("graph route evidence route is required".to_string()))?
        .to_string();
    let required_query_families = string_array_path(value, &["required_query_families"]);
    let computed_required_query_families = nowledge_mem_required_query_families_for_route(&route)
        .iter()
        .map(|family| (*family).to_string())
        .collect::<Vec<_>>();
    let query_reports = value_path(value, &["query_reports"])
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .map(QueryRuntimeReport::parse)
        .collect::<Vec<_>>();
    let query_family_blocker_codes = route_query_family_blocker_codes(
        &required_query_families,
        &computed_required_query_families,
        &query_reports,
    );
    Ok(RouteEvidence {
        route,
        shadow_compare_ready: bool_path(value, &["shadow_compare_ready"]) == Some(true),
        shadow_compare_evidence_source: str_path(value, &["shadow_compare_evidence_source"])
            .map(str::to_string),
        shadow_compare: RouteShadowCompareEvidence::parse(value),
        primary_ready: bool_path(value, &["primary_ready"]) == Some(true),
        required_query_families,
        computed_required_query_families,
        query_family_blocker_codes,
        reported_relationship_property_pruning_required_count: u64_path(
            value,
            &["relationship_property_pruning_required_count"],
        ),
        reported_relationship_property_pruning_report_count: u64_path(
            value,
            &["relationship_property_pruning_report_count"],
        ),
        reported_relationship_property_pruning_evidence_ready: bool_path(
            value,
            &["relationship_property_pruning_evidence_ready"],
        ),
        blocker_codes: string_array_path(value, &["blocker_codes"]),
        query_reports,
    })
}

fn route_query_family_blocker_codes(
    required_query_families: &[String],
    computed_required_query_families: &[String],
    query_reports: &[QueryRuntimeReport],
) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    if required_query_families != computed_required_query_families {
        blockers.insert("route_required_query_families_mismatch".to_string());
    }
    if !computed_required_query_families.is_empty() {
        let observed_query_families = query_reports
            .iter()
            .filter_map(|report| report.query_family.as_deref())
            .collect::<BTreeSet<_>>();
        if !computed_required_query_families
            .iter()
            .any(|family| observed_query_families.contains(family.as_str()))
        {
            blockers.insert("route_required_query_family_missing".to_string());
        }
    }
    blockers.into_iter().collect()
}

fn route_primary_blocker_codes(
    evidence_protocol: Option<&str>,
    evidence_ready: Option<bool>,
    routes: &[RouteEvidence],
    route_coverage: &RouteCoverage,
    evidence_route_coverage_blocker_codes: &[&str],
) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    if evidence_protocol != Some(NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL) {
        blockers.insert("graph_route_evidence_protocol_mismatch".to_string());
    }
    if evidence_ready != Some(true) {
        blockers.insert("graph_route_evidence_not_ready".to_string());
    }
    if !route_coverage.missing_required_routes.is_empty() {
        blockers.insert("missing_required_routes".to_string());
    }
    for blocker in &route_coverage.blocker_codes {
        blockers.insert((*blocker).to_string());
    }
    for blocker in evidence_route_coverage_blocker_codes {
        blockers.insert((*blocker).to_string());
    }
    for route in routes {
        if !route.shadow_compare_ready {
            blockers.insert("route_shadow_compare_not_ready".to_string());
        }
        if route.shadow_compare_evidence_source.as_deref() != Some(ROUTE_PARITY_EVIDENCE_SOURCE) {
            blockers.insert("route_shadow_compare_evidence_missing".to_string());
        }
        if !route.shadow_compare.ready() {
            blockers.insert("route_shadow_compare_detail_not_ready".to_string());
        }
        blockers.extend(route.shadow_compare.computed_blocker_codes.iter().cloned());
        if !route.primary_ready {
            blockers.insert("route_primary_not_ready".to_string());
        }
        if route.query_reports.is_empty() {
            blockers.insert("missing_query_runtime_reports".to_string());
        }
        if !route.query_runtime_ready() {
            blockers.insert("route_query_runtime_not_ready".to_string());
        }
        if route.query_runtime_failed_query_count() > 0 {
            blockers.insert("route_query_runtime_query_failed".to_string());
        }
        if !route.query_plan_evidence_ready() {
            blockers.insert("route_query_plan_evidence_not_ready".to_string());
        }
        if !route.query_profile_evidence_ready() {
            blockers.insert("route_query_profile_evidence_not_ready".to_string());
        }
        if !route.query_api_behavior_evidence_ready() {
            blockers.insert("route_query_api_behavior_evidence_not_ready".to_string());
        }
        if !route.relationship_property_pruning_evidence_ready() {
            blockers.insert("route_relationship_property_pruning_evidence_not_ready".to_string());
        }
        blockers.extend(route.query_family_blocker_codes.iter().cloned());
        for report in &route.query_reports {
            blockers.extend(report.blocker_codes.iter().cloned());
        }
        blockers.extend(route.blocker_codes.iter().cloned());
    }
    blockers.into_iter().collect()
}

fn is_nowledge_graph_read_statement_kind(kind: &str) -> bool {
    matches!(
        kind,
        "match_return"
            | "match_nodes_return"
            | "shortest_path_return"
            | "match_optional_relationship_count_sum"
            | "match_thread_repair_stats"
            | "graph_algorithm"
            | "project_graph"
    )
}

fn bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    value_path(value, path).and_then(serde_json::Value::as_bool)
}

fn str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    value_path(value, path).and_then(serde_json::Value::as_str)
}

fn u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    value_path(value, path).and_then(serde_json::Value::as_u64)
}

fn string_array_path(value: &serde_json::Value, path: &[&str]) -> Vec<String> {
    value_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect()
}

fn value_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod differential;
