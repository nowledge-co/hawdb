use skein::{Result, SkeinError};
use std::collections::BTreeSet;
use std::path::Path;

const NMEM_GRAPH_ROUTE_READINESS_PROTOCOL: &str = "nmem-graph-route-readiness-v1";
const NOWLEDGE_MEM_READ_REPORT_PROTOCOL: &str = "skein-nowledge-mem-read-report";
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

pub fn nowledge_graph_route_readiness_usage() -> String {
    "nowledge-graph-route-readiness requires [--require-ready] <route-evidence-json>".to_string()
}

pub fn run_nowledge_graph_route_readiness(
    args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut evidence_path = None;
    for arg in args {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(nowledge_graph_route_readiness_usage()));
            }
            path => {
                if evidence_path.replace(path.to_string()).is_some() {
                    return Err(SkeinError::Semantic(nowledge_graph_route_readiness_usage()));
                }
            }
        }
    }
    let Some(evidence_path) = evidence_path else {
        return Err(SkeinError::Semantic(nowledge_graph_route_readiness_usage()));
    };
    Ok((
        nowledge_graph_route_readiness_json(&read_json_file(Path::new(&evidence_path))?)?,
        require_ready,
    ))
}

fn nowledge_graph_route_readiness_json(evidence: &serde_json::Value) -> Result<serde_json::Value> {
    let routes = parse_route_evidence(evidence)?;
    let route_names = routes
        .iter()
        .map(|route| route.route.clone())
        .collect::<BTreeSet<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .filter(|route| !route_names.contains(**route))
        .copied()
        .collect::<Vec<_>>();
    let route_primary_blocker_codes =
        route_primary_blocker_codes(&routes, &missing_required_routes);
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
        .map(|route| route.query_reports.len())
        .sum::<usize>();
    let route_primary_ready =
        missing_required_routes.is_empty() && route_primary_blocker_codes.is_empty();
    let route_count = routes.len();

    Ok(serde_json::json!({
        "protocol": NMEM_GRAPH_ROUTE_READINESS_PROTOCOL,
        "evidence_source": "graph_route_execution_evidence",
        "route_count": route_count,
        "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "missing_required_routes": missing_required_routes,
        "shadow_compare_route_count": shadow_compare_route_count,
        "primary_ready_route_count": primary_ready_route_count,
        "query_runtime_route_count": query_runtime_route_count,
        "query_runtime_report_count": query_runtime_report_count,
        "missing_query_runtime_routes": missing_query_runtime_routes,
        "route_query_runtime_ready": missing_query_runtime_routes.is_empty(),
        "route_primary_ready": route_primary_ready,
        "route_primary_blocker_codes": route_primary_blocker_codes,
        "routes": routes.into_iter().map(RouteEvidence::json).collect::<Vec<_>>(),
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RouteEvidence {
    route: String,
    shadow_compare_ready: bool,
    primary_ready: bool,
    blocker_codes: Vec<String>,
    query_reports: Vec<QueryRuntimeReport>,
}

impl RouteEvidence {
    fn query_runtime_ready(&self) -> bool {
        !self.query_reports.is_empty() && self.query_reports.iter().all(QueryRuntimeReport::ready)
    }

    fn json(self) -> serde_json::Value {
        let query_runtime_ready = self.query_runtime_ready();
        let query_report_count = self.query_reports.len();
        let query_reports = self
            .query_reports
            .into_iter()
            .map(QueryRuntimeReport::json)
            .collect::<Vec<_>>();
        serde_json::json!({
            "route": self.route,
            "shadow_compare_ready": self.shadow_compare_ready,
            "primary_ready": self.primary_ready,
            "query_runtime_ready": query_runtime_ready,
            "query_report_count": query_report_count,
            "query_reports": query_reports,
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryRuntimeReport {
    protocol: Option<String>,
    mode: Option<String>,
    row_count: Option<u64>,
    max_rows: Option<u64>,
    execution_row_cap: Option<u64>,
    estimated_payload_bytes: Option<u64>,
    max_estimated_payload_bytes: Option<u64>,
    row_budget_exceeded: Option<bool>,
    payload_budget_exceeded: Option<bool>,
    row_limit_enforced_before_output: Option<bool>,
    operator_row_cap_enabled: Option<bool>,
    blocking_operator_count: Option<u64>,
    streaming: Option<bool>,
    blocker_codes: Vec<String>,
}

impl QueryRuntimeReport {
    fn parse(value: &serde_json::Value) -> Self {
        let mut report = Self {
            protocol: str_path(value, &["protocol"]).map(str::to_string),
            mode: str_path(value, &["mode"]).map(str::to_string),
            row_count: u64_path(value, &["row_count"]),
            max_rows: u64_path(value, &["max_rows"]),
            execution_row_cap: u64_path(value, &["execution_row_cap"]),
            estimated_payload_bytes: u64_path(value, &["estimated_payload_bytes"]),
            max_estimated_payload_bytes: u64_path(value, &["max_estimated_payload_bytes"]),
            row_budget_exceeded: bool_path(value, &["row_budget_exceeded"]),
            payload_budget_exceeded: bool_path(value, &["payload_budget_exceeded"]),
            row_limit_enforced_before_output: bool_path(
                value,
                &["row_limit_enforced_before_output"],
            ),
            operator_row_cap_enabled: bool_path(value, &["operator_row_cap_enabled"]),
            blocking_operator_count: u64_path(value, &["blocking_operator_count"]),
            streaming: bool_path(value, &["streaming"]),
            blocker_codes: Vec::new(),
        };
        report.blocker_codes = report.computed_blocker_codes();
        report
    }

    fn ready(&self) -> bool {
        self.blocker_codes.is_empty()
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "mode": self.mode,
            "row_count": self.row_count,
            "max_rows": self.max_rows,
            "execution_row_cap": self.execution_row_cap,
            "estimated_payload_bytes": self.estimated_payload_bytes,
            "max_estimated_payload_bytes": self.max_estimated_payload_bytes,
            "row_budget_exceeded": self.row_budget_exceeded,
            "payload_budget_exceeded": self.payload_budget_exceeded,
            "row_limit_enforced_before_output": self.row_limit_enforced_before_output,
            "operator_row_cap_enabled": self.operator_row_cap_enabled,
            "blocking_operator_count": self.blocking_operator_count,
            "streaming": self.streaming,
            "ready": self.ready(),
            "blocker_codes": self.blocker_codes,
        })
    }

    fn computed_blocker_codes(&self) -> Vec<String> {
        let mut blockers = BTreeSet::new();
        if self.protocol.as_deref() != Some(NOWLEDGE_MEM_READ_REPORT_PROTOCOL) {
            blockers.insert("query_report_protocol_mismatch".to_string());
        }
        if self.mode.as_deref() != Some("shadow_read_only") {
            blockers.insert("query_report_not_shadow_read_only".to_string());
        }
        if self.max_rows.is_none() {
            blockers.insert("query_report_max_rows_missing".to_string());
        }
        match (self.execution_row_cap, self.max_rows) {
            (Some(execution_row_cap), Some(max_rows)) if execution_row_cap == max_rows + 1 => {}
            (Some(_), Some(_)) => {
                blockers.insert("query_report_execution_row_cap_mismatch".to_string());
            }
            _ => {
                blockers.insert("query_report_execution_row_cap_missing".to_string());
            }
        }
        if self.row_count.is_none() {
            blockers.insert("query_report_row_count_missing".to_string());
        }
        if self.estimated_payload_bytes.is_none() || self.max_estimated_payload_bytes.is_none() {
            blockers.insert("query_report_payload_budget_missing".to_string());
        }
        if self.row_budget_exceeded != Some(false) {
            blockers.insert("query_report_row_budget_exceeded".to_string());
        }
        if self.payload_budget_exceeded != Some(false) {
            blockers.insert("query_report_payload_budget_exceeded".to_string());
        }
        if self.row_limit_enforced_before_output != Some(true) {
            blockers.insert("query_report_row_limit_not_enforced_before_output".to_string());
        }
        if self.operator_row_cap_enabled != Some(true) {
            blockers.insert("query_report_operator_row_cap_disabled".to_string());
        }
        if self.blocking_operator_count.is_none() {
            blockers.insert("query_report_blocking_operator_count_missing".to_string());
        }
        if self.streaming != Some(false) {
            blockers.insert("query_report_streaming_enabled".to_string());
        }
        blockers.into_iter().collect()
    }
}

fn parse_route_evidence(evidence: &serde_json::Value) -> Result<Vec<RouteEvidence>> {
    let routes = if evidence.is_array() {
        evidence.as_array()
    } else {
        evidence.get("routes").and_then(serde_json::Value::as_array)
    }
    .ok_or_else(|| {
        SkeinError::Semantic("graph route evidence JSON must contain a routes array".to_string())
    })?;
    routes.iter().map(parse_route).collect()
}

fn parse_route(value: &serde_json::Value) -> Result<RouteEvidence> {
    let route = str_path(value, &["route"])
        .filter(|route| !route.trim().is_empty())
        .ok_or_else(|| SkeinError::Semantic("graph route evidence route is required".to_string()))?
        .to_string();
    Ok(RouteEvidence {
        route,
        shadow_compare_ready: bool_path(value, &["shadow_compare_ready"]) == Some(true),
        primary_ready: bool_path(value, &["primary_ready"]) == Some(true),
        blocker_codes: string_array_path(value, &["blocker_codes"]),
        query_reports: value_path(value, &["query_reports"])
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .map(QueryRuntimeReport::parse)
            .collect(),
    })
}

fn route_primary_blocker_codes(
    routes: &[RouteEvidence],
    missing_required_routes: &[&str],
) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    if !missing_required_routes.is_empty() {
        blockers.insert("missing_required_routes".to_string());
    }
    for route in routes {
        if !route.shadow_compare_ready {
            blockers.insert("route_shadow_compare_not_ready".to_string());
        }
        if !route.primary_ready {
            blockers.insert("route_primary_not_ready".to_string());
        }
        if route.query_reports.is_empty() {
            blockers.insert("missing_query_runtime_reports".to_string());
        }
        if !route.query_runtime_ready() {
            blockers.insert("route_query_runtime_not_ready".to_string());
        }
        for report in &route.query_reports {
            blockers.extend(report.blocker_codes.iter().cloned());
        }
        blockers.extend(route.blocker_codes.iter().cloned());
    }
    blockers.into_iter().collect()
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read graph route readiness evidence: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse graph route readiness evidence: {error}"
        ))
    })
}

fn bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    value_path(value, path).and_then(serde_json::Value::as_bool)
}

fn str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    value_path(value, path).and_then(serde_json::Value::as_str)
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

fn u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    value_path(value, path).and_then(serde_json::Value::as_u64)
}

fn value_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::{nowledge_graph_route_readiness_json, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES};

    #[test]
    fn route_readiness_reports_ready_for_all_required_routes() {
        let readiness = nowledge_graph_route_readiness_json(&serde_json::json!({
            "routes": ready_routes()
        }))
        .unwrap();

        assert_eq!(readiness["protocol"], "nmem-graph-route-readiness-v1");
        assert_eq!(
            readiness["route_count"],
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
        );
        assert_eq!(readiness["route_primary_ready"], true);
        assert_eq!(readiness["route_query_runtime_ready"], true);
        assert_eq!(
            readiness["query_runtime_route_count"],
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
        );
        assert_eq!(
            readiness["route_primary_blocker_codes"],
            serde_json::json!([])
        );
        assert_eq!(readiness["missing_required_routes"], serde_json::json!([]));
    }

    #[test]
    fn route_readiness_fails_closed_for_missing_required_route() {
        let mut routes = ready_routes();
        routes.pop();

        let readiness = nowledge_graph_route_readiness_json(&serde_json::json!({
            "routes": routes
        }))
        .unwrap();

        assert_eq!(readiness["route_primary_ready"], false);
        assert_eq!(
            readiness["route_primary_blocker_codes"],
            serde_json::json!(["missing_required_routes"])
        );
        assert_eq!(
            readiness["missing_required_routes"],
            serde_json::json!(["/graph/shortest-path"])
        );
    }

    #[test]
    fn route_readiness_fails_closed_for_unready_route() {
        let mut routes = ready_routes();
        routes[0]["primary_ready"] = serde_json::json!(false);
        routes[0]["blocker_codes"] = serde_json::json!(["primary_route_disabled"]);

        let readiness = nowledge_graph_route_readiness_json(&serde_json::json!({
            "routes": routes
        }))
        .unwrap();

        assert_eq!(readiness["route_primary_ready"], false);
        assert_eq!(
            readiness["route_primary_blocker_codes"],
            serde_json::json!(["primary_route_disabled", "route_primary_not_ready"])
        );
    }

    #[test]
    fn route_readiness_fails_closed_without_query_runtime_reports() {
        let mut routes = ready_routes();
        routes[0]["query_reports"] = serde_json::json!([]);

        let readiness = nowledge_graph_route_readiness_json(&serde_json::json!({
            "routes": routes
        }))
        .unwrap();

        assert_eq!(readiness["route_primary_ready"], false);
        assert_eq!(readiness["route_query_runtime_ready"], false);
        assert!(readiness["route_primary_blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "missing_query_runtime_reports"));
    }

    #[test]
    fn route_readiness_fails_closed_for_wrong_query_report_protocol() {
        let mut routes = ready_routes();
        routes[0]["query_reports"][0]["protocol"] = serde_json::json!("legacy-report");

        let readiness = nowledge_graph_route_readiness_json(&serde_json::json!({
            "routes": routes
        }))
        .unwrap();

        assert_eq!(readiness["route_primary_ready"], false);
        assert_eq!(readiness["route_query_runtime_ready"], false);
        assert!(readiness["route_primary_blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_report_protocol_mismatch"));
    }

    fn ready_routes() -> Vec<serde_json::Value> {
        REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                serde_json::json!({
                    "route": route,
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "query_reports": [ready_query_report()],
                    "blocker_codes": []
                })
            })
            .collect()
    }

    fn ready_query_report() -> serde_json::Value {
        serde_json::json!({
            "protocol": "skein-nowledge-mem-read-report",
            "mode": "shadow_read_only",
            "row_count": 1,
            "max_rows": 512,
            "execution_row_cap": 513,
            "estimated_payload_bytes": 128,
            "max_estimated_payload_bytes": 4194304,
            "row_budget_exceeded": false,
            "payload_budget_exceeded": false,
            "row_limit_enforced_before_output": true,
            "operator_row_cap_enabled": true,
            "blocking_operator_count": 0,
            "blocking_operator_kinds": [],
            "streaming": false
        })
    }
}
