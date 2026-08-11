use crate::{
    nowledge_mem_required_query_families_for_route, NowledgeMemGraph, NowledgeMemGraphMode,
    NowledgeMemQueryReportOptions, Result, SkeinError, Value,
    NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_QUERY, NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_MEMORY_QUERY, NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_QUERY,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_EDGE_QUERY,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ENTITY_QUERY,
    NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE, NOWLEDGE_MEM_GRAPH_NODE_DETAILS_MEMORY_QUERY,
    NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE, NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE,
    NOWLEDGE_MEM_GRAPH_ORPHAN_ENTITIES_QUERY, NOWLEDGE_MEM_GRAPH_OVERVIEW_MEMORY_RANKING_QUERY,
    NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ACTIVE_MEMORY_RELATION_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_RELATION_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_RELATION_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MENTION_EDGE_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_RELATION_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_GRAPH_META_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MEMORY_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MENTION_EDGE_COUNT_QUERY,
    NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE, NOWLEDGE_MEM_GRAPH_SAMPLE_MEMORY_QUERY,
    NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
    REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub const NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL: &str = "nmem-graph-route-evidence-v1";
pub const NMEM_GRAPH_ROUTE_PARITY_EVIDENCE_PROTOCOL: &str = "nmem-graph-route-parity-evidence-v1";
const ROUTE_PARITY_EVIDENCE_SOURCE: &str = "route_parity_evidence";
const ROUTE_QUERY_INVENTORY_EVIDENCE_SOURCE: &str = "route_query_inventory";
const ROUTE_PARITY_FULL_MATCH_PER_MILLION: u64 = 1_000_000;

pub fn nowledge_graph_route_evidence_usage() -> String {
    "nowledge-graph-route-evidence requires [--require-ready] [--mode shadow_read_only|writable_cutover] [--capture-physical-plan] [--slow-log-threshold-micros <n>] [--route-parity-json <path>] <graph-db> <route-query-json>".to_string()
}

pub fn run_nowledge_graph_route_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut mode = NowledgeMemGraphMode::ShadowReadOnly;
    let mut options = NowledgeMemQueryReportOptions::default();
    let mut route_parity = None;
    let mut graph_path = None;
    let mut route_query_path = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--mode" => {
                mode =
                    parse_mode(&args.next().ok_or_else(|| {
                        SkeinError::Semantic(nowledge_graph_route_evidence_usage())
                    })?)?;
            }
            "--capture-physical-plan" => {
                options.capture_physical_plan = true;
            }
            "--slow-log-threshold-micros" => {
                let raw = args
                    .next()
                    .ok_or_else(|| SkeinError::Semantic(nowledge_graph_route_evidence_usage()))?;
                options.slow_log_threshold_micros = Some(raw.parse::<u128>().map_err(|_| {
                    SkeinError::Semantic(
                        "--slow-log-threshold-micros requires a non-negative integer".to_string(),
                    )
                })?);
            }
            "--route-parity-json" => {
                route_parity = Some(parse_route_parity_evidence(&read_json_file(Path::new(
                    &args.next().ok_or_else(|| {
                        SkeinError::Semantic(nowledge_graph_route_evidence_usage())
                    })?,
                ))?)?);
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(nowledge_graph_route_evidence_usage()));
            }
            path => {
                if graph_path.is_none() {
                    graph_path = Some(path.to_string());
                } else if route_query_path.replace(path.to_string()).is_some() {
                    return Err(SkeinError::Semantic(nowledge_graph_route_evidence_usage()));
                }
            }
        }
    }

    let Some(graph_path) = graph_path else {
        return Err(SkeinError::Semantic(nowledge_graph_route_evidence_usage()));
    };
    let Some(route_query_path) = route_query_path else {
        return Err(SkeinError::Semantic(nowledge_graph_route_evidence_usage()));
    };

    let mut graph = NowledgeMemGraph::open(graph_path, mode)?;
    let route_queries =
        parse_route_query_inventory(&read_json_file(Path::new(&route_query_path))?)?;
    Ok((
        nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            options,
            route_parity.as_ref(),
        ),
        require_ready,
    ))
}

pub fn nowledge_graph_route_evidence_json(
    graph: &mut NowledgeMemGraph,
    route_queries: &[RouteQuery],
    options: NowledgeMemQueryReportOptions,
    route_parity: Option<&RouteParityEvidence>,
) -> serde_json::Value {
    let routes = route_queries
        .iter()
        .map(|route| route.query_runtime_evidence(graph, options, route_parity))
        .collect::<Vec<_>>();
    let route_coverage = route_coverage(&routes);
    let ready = route_coverage.ready && routes.iter().all(route_evidence_ready);
    serde_json::json!({
        "protocol": NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL,
        "mode": graph.mode().as_str(),
        "ready": !routes.is_empty() && ready,
        "route_count": routes.len(),
        "required_route_count": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        "covered_route_count": route_coverage.covered_routes.len(),
        "covered_routes": route_coverage.covered_routes,
        "missing_required_routes": route_coverage.missing_required_routes,
        "required_routes_covered": route_coverage.required_routes_covered,
        "unknown_routes": route_coverage.unknown_routes,
        "duplicate_routes": route_coverage.duplicate_routes,
        "route_coverage_ready": route_coverage.ready,
        "route_coverage_blocker_codes": route_coverage.blocker_codes,
        "routes": routes,
    })
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

fn route_coverage(routes: &[serde_json::Value]) -> RouteCoverage {
    let required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut route_counts = BTreeMap::<&str, usize>::new();
    for route in routes
        .iter()
        .filter_map(|route| route.get("route").and_then(serde_json::Value::as_str))
    {
        *route_counts.entry(route).or_default() += 1;
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
        required_routes_covered,
        missing_required_routes,
        unknown_routes,
        duplicate_routes,
        ready,
        blocker_codes,
    }
}

fn route_evidence_ready(route: &serde_json::Value) -> bool {
    let Some(query_reports) = route
        .get("query_reports")
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    route
        .get("primary_ready")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        && !query_reports.is_empty()
        && route
            .get("query_errors")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|errors| errors.is_empty())
        && route
            .get("blocker_codes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|blockers| blockers.is_empty())
}

#[derive(Debug, Clone, PartialEq)]
pub struct RouteQuery {
    pub route: String,
    pub shadow_compare_ready: bool,
    pub primary_read_routing_enabled: bool,
    pub primary_ready: bool,
    pub blocker_codes: Vec<String>,
    pub queries: Vec<RouteCypherQuery>,
}

impl RouteQuery {
    fn query_runtime_evidence(
        &self,
        graph: &mut NowledgeMemGraph,
        options: NowledgeMemQueryReportOptions,
        route_parity: Option<&RouteParityEvidence>,
    ) -> serde_json::Value {
        let mut query_reports = Vec::with_capacity(self.queries.len());
        let mut query_errors = Vec::new();
        let mut blocker_codes = self.blocker_codes.clone();
        let shadow_compare = self.shadow_compare_evidence(route_parity);
        if self.queries.is_empty() {
            blocker_codes.push("missing_route_queries".to_string());
        }
        let required_query_families = nowledge_mem_required_query_families_for_route(&self.route);
        if !required_query_families.is_empty() {
            let observed_query_families = self
                .queries
                .iter()
                .filter_map(|query| query.query_family.as_deref())
                .collect::<BTreeSet<_>>();
            if !required_query_families
                .iter()
                .any(|family| observed_query_families.contains(family))
            {
                blocker_codes.push("route_required_query_family_missing".to_string());
            }
        }
        if !shadow_compare.ready {
            blocker_codes.push("shadow_compare_evidence_not_ready".to_string());
        }
        if shadow_compare.source != ROUTE_PARITY_EVIDENCE_SOURCE {
            blocker_codes.push("shadow_compare_evidence_missing".to_string());
        }
        blocker_codes.extend(
            shadow_compare
                .blocker_codes
                .iter()
                .map(|code| (*code).to_string()),
        );
        for (query_index, query) in self.queries.iter().enumerate() {
            match graph.query_with_params_with_report_options(
                &query.cypher,
                &query.parameters,
                options,
            ) {
                Ok(output) => {
                    let report =
                        query_report_with_route_context(query, query_index, output.report.json());
                    blocker_codes.extend(query_requirement_blockers(query, &report));
                    query_reports.push(report);
                }
                Err(error) => {
                    blocker_codes.push("query_runtime_execution_failed".to_string());
                    query_errors.push(serde_json::json!({
                        "query_name": query.name,
                        "query_index": query_index as u64,
                        "error_class": error_class(&error),
                    }));
                }
            }
        }
        let query_runtime_succeeded = !query_reports.is_empty() && query_errors.is_empty();
        if query_runtime_succeeded {
            blocker_codes.retain(|code| code != "graph_route_execution_evidence_missing");
        }
        blocker_codes.sort();
        blocker_codes.dedup();
        serde_json::json!({
            "route": self.route,
            "shadow_compare_ready": shadow_compare.ready,
            "shadow_compare_evidence_source": shadow_compare.source,
            "shadow_compare": shadow_compare.json(),
            "primary_read_routing_enabled": self.primary_read_routing_enabled,
            "primary_ready": (self.primary_ready || self.primary_read_routing_enabled)
                && query_runtime_succeeded
                && blocker_codes.is_empty(),
            "required_query_families": required_query_families,
            "query_reports": query_reports,
            "query_errors": query_errors,
            "blocker_codes": blocker_codes,
        })
    }

    fn shadow_compare_evidence<'a>(
        &self,
        route_parity: Option<&'a RouteParityEvidence>,
    ) -> RouteShadowCompareEvidence<'a> {
        let Some(route_parity) = route_parity else {
            return RouteShadowCompareEvidence::from_inventory(self.shadow_compare_ready);
        };
        let Some(route) = route_parity.route(&self.route) else {
            return RouteShadowCompareEvidence::missing_parity();
        };
        RouteShadowCompareEvidence::from_parity_route(route)
    }
}

#[derive(Debug, Clone)]
struct RouteShadowCompareEvidence<'a> {
    source: &'static str,
    ready: bool,
    matched_per_million: Option<u64>,
    primary_engine: Option<&'a str>,
    shadow_engine: Option<&'a str>,
    blocker_codes: Vec<&'static str>,
}

impl<'a> RouteShadowCompareEvidence<'a> {
    fn from_inventory(ready: bool) -> Self {
        Self {
            source: ROUTE_QUERY_INVENTORY_EVIDENCE_SOURCE,
            ready,
            matched_per_million: None,
            primary_engine: None,
            shadow_engine: None,
            blocker_codes: Vec::new(),
        }
    }

    fn missing_parity() -> Self {
        Self {
            source: ROUTE_PARITY_EVIDENCE_SOURCE,
            ready: false,
            matched_per_million: None,
            primary_engine: None,
            shadow_engine: None,
            blocker_codes: vec!["route_parity_evidence_missing"],
        }
    }

    fn from_parity_route(route: &'a RouteParityEvidenceRoute) -> Self {
        let matched_per_million_ready =
            route.matched_per_million == Some(ROUTE_PARITY_FULL_MATCH_PER_MILLION);
        let primary_engine_ready = route
            .primary_engine
            .as_deref()
            .is_some_and(is_legacy_graph_engine);
        let shadow_engine_ready = route.shadow_engine.as_deref() == Some("skein");
        let ready =
            route.ready && matched_per_million_ready && primary_engine_ready && shadow_engine_ready;
        let mut blocker_codes = Vec::new();
        if !route.ready {
            blocker_codes.push("route_parity_not_ready");
        }
        if !matched_per_million_ready {
            blocker_codes.push("route_parity_matched_per_million_not_full");
        }
        if !primary_engine_ready {
            blocker_codes.push("route_parity_primary_engine_mismatch");
        }
        if !shadow_engine_ready {
            blocker_codes.push("route_parity_shadow_engine_mismatch");
        }
        Self {
            source: ROUTE_PARITY_EVIDENCE_SOURCE,
            ready,
            matched_per_million: route.matched_per_million,
            primary_engine: route.primary_engine.as_deref(),
            shadow_engine: route.shadow_engine.as_deref(),
            blocker_codes,
        }
    }

    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "source": self.source,
            "ready": self.ready,
            "matched_per_million": self.matched_per_million,
            "primary_engine": self.primary_engine,
            "shadow_engine": self.shadow_engine,
            "blocker_codes": self.blocker_codes,
        })
    }
}

fn is_legacy_graph_engine(engine: &str) -> bool {
    matches!(engine, "kuzu" | "ladybug" | "kuzu/ladybug")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteParityEvidence {
    pub routes: BTreeMap<String, RouteParityEvidenceRoute>,
}

impl RouteParityEvidence {
    fn route(&self, route: &str) -> Option<&RouteParityEvidenceRoute> {
        self.routes.get(route)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteParityEvidenceRoute {
    pub ready: bool,
    pub matched_per_million: Option<u64>,
    pub primary_engine: Option<String>,
    pub shadow_engine: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RouteCypherQuery {
    pub name: String,
    pub query_family: Option<String>,
    pub require_scan_pruning: bool,
    pub require_pruned: bool,
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
}

pub fn nowledge_mem_graph_overview_route_query(limit: usize) -> Result<RouteQuery> {
    let limit = i64::try_from(limit).map_err(|_| {
        SkeinError::Semantic(
            "graph overview route evidence limit exceeds supported range".to_string(),
        )
    })?;
    if limit <= 0 {
        return Err(SkeinError::Semantic(
            "graph overview route evidence limit must be greater than zero".to_string(),
        ));
    }
    Ok(RouteQuery {
        route: NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "overview-memory-ranking".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: NOWLEDGE_MEM_GRAPH_OVERVIEW_MEMORY_RANKING_QUERY.to_string(),
            parameters: BTreeMap::from([("limit".to_string(), Value::Int(limit))]),
        }],
    })
}

pub fn nowledge_mem_graph_sample_route_query(limit: usize) -> Result<RouteQuery> {
    let limit = i64::try_from(limit).map_err(|_| {
        SkeinError::Semantic(
            "graph sample route evidence limit exceeds supported range".to_string(),
        )
    })?;
    if limit <= 0 {
        return Err(SkeinError::Semantic(
            "graph sample route evidence limit must be greater than zero".to_string(),
        ));
    }
    Ok(RouteQuery {
        route: NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "sample-memory-list".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: false,
            require_pruned: false,
            cypher: NOWLEDGE_MEM_GRAPH_SAMPLE_MEMORY_QUERY.to_string(),
            parameters: BTreeMap::from([("limit".to_string(), Value::Int(limit))]),
        }],
    })
}

pub fn nowledge_mem_graph_node_details_route_query(node_id: u64) -> Result<RouteQuery> {
    let node_id = i64::try_from(node_id).map_err(|_| {
        SkeinError::Semantic(
            "graph node details route evidence node_id exceeds supported range".to_string(),
        )
    })?;
    Ok(RouteQuery {
        route: NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "node-details-memory-lookup".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: NOWLEDGE_MEM_GRAPH_NODE_DETAILS_MEMORY_QUERY.to_string(),
            parameters: BTreeMap::from([("node_id".to_string(), Value::Int(node_id))]),
        }],
    })
}

pub fn nowledge_mem_graph_community_members_route_query(
    community_id: i64,
    limit: usize,
) -> Result<RouteQuery> {
    let limit = i64::try_from(limit).map_err(|_| {
        SkeinError::Semantic(
            "graph community members route evidence limit exceeds supported range".to_string(),
        )
    })?;
    if limit <= 0 {
        return Err(SkeinError::Semantic(
            "graph community members route evidence limit must be greater than zero".to_string(),
        ));
    }
    Ok(RouteQuery {
        route: NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "community-members-memory-lookup".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_MEMORY_QUERY.to_string(),
            parameters: BTreeMap::from([
                ("community_id".to_string(), Value::Int(community_id)),
                ("limit".to_string(), Value::Int(limit)),
            ]),
        }],
    })
}

pub fn nowledge_mem_graph_community_recent_memories_route_query(
    community_id: i64,
    limit: usize,
) -> Result<RouteQuery> {
    let limit = i64::try_from(limit).map_err(|_| {
        SkeinError::Semantic(
            "graph community recent memories route evidence limit exceeds supported range"
                .to_string(),
        )
    })?;
    if limit <= 0 {
        return Err(SkeinError::Semantic(
            "graph community recent memories route evidence limit must be greater than zero"
                .to_string(),
        ));
    }
    Ok(RouteQuery {
        route: NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "community-recent-memories-lookup".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_QUERY.to_string(),
            parameters: BTreeMap::from([
                ("community_id".to_string(), Value::Int(community_id)),
                ("limit".to_string(), Value::Int(limit)),
            ]),
        }],
    })
}

pub fn nowledge_mem_graph_community_subgraph_route_query<I, S>(
    community_id: i64,
    max_entities: usize,
    edge_entity_ids: I,
    max_edges: usize,
) -> Result<RouteQuery>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let max_entities = i64::try_from(max_entities).map_err(|_| {
        SkeinError::Semantic(
            "graph community subgraph route evidence max_entities exceeds supported range"
                .to_string(),
        )
    })?;
    let max_edges = i64::try_from(max_edges).map_err(|_| {
        SkeinError::Semantic(
            "graph community subgraph route evidence max_edges exceeds supported range".to_string(),
        )
    })?;
    if max_entities <= 0 {
        return Err(SkeinError::Semantic(
            "graph community subgraph route evidence max_entities must be greater than zero"
                .to_string(),
        ));
    }
    if max_edges < 0 {
        return Err(SkeinError::Semantic(
            "graph community subgraph route evidence max_edges must not be negative".to_string(),
        ));
    }
    let entity_ids = edge_entity_ids
        .into_iter()
        .map(|entity_id| Value::String(entity_id.into()))
        .collect::<Vec<_>>();
    Ok(RouteQuery {
        route: NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![
            RouteCypherQuery {
                name: "community-subgraph-entity-ranking".to_string(),
                query_family: Some("graph_traversal".to_string()),
                require_scan_pruning: true,
                require_pruned: false,
                cypher: NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ENTITY_QUERY.to_string(),
                parameters: BTreeMap::from([
                    ("community_id".to_string(), Value::Int(community_id)),
                    ("max_entities".to_string(), Value::Int(max_entities)),
                ]),
            },
            RouteCypherQuery {
                name: "community-subgraph-relation-edges".to_string(),
                query_family: Some("graph_traversal".to_string()),
                require_scan_pruning: false,
                require_pruned: false,
                cypher: NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_EDGE_QUERY.to_string(),
                parameters: BTreeMap::from([
                    ("entity_ids".to_string(), Value::List(entity_ids)),
                    ("max_edges".to_string(), Value::Int(max_edges)),
                ]),
            },
        ],
    })
}

pub fn nowledge_mem_graph_augmentation_state_route_query() -> RouteQuery {
    RouteQuery {
        route: NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "augmentation-state-graph-meta".to_string(),
            query_family: Some("projected_graph".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_QUERY.to_string(),
            parameters: BTreeMap::new(),
        }],
    }
}

pub fn nowledge_mem_graph_pagerank_plan_route_query(
    changed_since_epoch_nanos: Option<i64>,
) -> RouteQuery {
    let mut queries = vec![
        pagerank_plan_route_query(
            "pagerank-plan-graph-meta",
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_GRAPH_META_QUERY,
            BTreeMap::new(),
            true,
        ),
        pagerank_plan_route_query(
            "pagerank-plan-memory-count",
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MEMORY_COUNT_QUERY,
            BTreeMap::new(),
            false,
        ),
        pagerank_plan_route_query(
            "pagerank-plan-entity-count",
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_COUNT_QUERY,
            BTreeMap::new(),
            false,
        ),
        pagerank_plan_route_query(
            "pagerank-plan-entity-relation-count",
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_RELATION_COUNT_QUERY,
            BTreeMap::new(),
            false,
        ),
        pagerank_plan_route_query(
            "pagerank-plan-mention-edge-count",
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MENTION_EDGE_COUNT_QUERY,
            BTreeMap::new(),
            false,
        ),
        pagerank_plan_route_query(
            "pagerank-plan-active-memory-relation-count",
            NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ACTIVE_MEMORY_RELATION_COUNT_QUERY,
            BTreeMap::new(),
            false,
        ),
    ];
    if let Some(cutoff) = changed_since_epoch_nanos {
        let parameters = BTreeMap::from([("cutoff".to_string(), Value::Int(cutoff))]);
        queries.extend([
            pagerank_plan_route_query(
                "pagerank-plan-changed-memory-count",
                NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_COUNT_QUERY,
                parameters.clone(),
                true,
            ),
            pagerank_plan_route_query(
                "pagerank-plan-changed-entity-count",
                NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_COUNT_QUERY,
                parameters.clone(),
                true,
            ),
            pagerank_plan_route_query(
                "pagerank-plan-changed-mention-edge-count",
                NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MENTION_EDGE_COUNT_QUERY,
                parameters.clone(),
                false,
            ),
            pagerank_plan_route_query(
                "pagerank-plan-changed-entity-relation-count",
                NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_RELATION_COUNT_QUERY,
                parameters.clone(),
                false,
            ),
            pagerank_plan_route_query(
                "pagerank-plan-changed-memory-relation-count",
                NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_RELATION_COUNT_QUERY,
                parameters,
                false,
            ),
        ]);
    }
    RouteQuery {
        route: NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries,
    }
}

fn pagerank_plan_route_query(
    name: impl Into<String>,
    cypher: impl Into<String>,
    parameters: BTreeMap<String, Value>,
    require_scan_pruning: bool,
) -> RouteCypherQuery {
    RouteCypherQuery {
        name: name.into(),
        query_family: Some("projected_graph".to_string()),
        require_scan_pruning,
        require_pruned: false,
        cypher: cypher.into(),
        parameters,
    }
}

pub fn nowledge_mem_graph_orphans_route_query(limit: usize) -> Result<RouteQuery> {
    let limit = i64::try_from(limit).map_err(|_| {
        SkeinError::Semantic(
            "graph orphans route evidence limit exceeds supported range".to_string(),
        )
    })?;
    if limit <= 0 {
        return Err(SkeinError::Semantic(
            "graph orphans route evidence limit must be greater than zero".to_string(),
        ));
    }
    Ok(RouteQuery {
        route: NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "orphan-entity-relationship-exclusion".to_string(),
            query_family: Some("graph_traversal".to_string()),
            require_scan_pruning: false,
            require_pruned: false,
            cypher: NOWLEDGE_MEM_GRAPH_ORPHAN_ENTITIES_QUERY.to_string(),
            parameters: BTreeMap::from([("limit".to_string(), Value::Int(limit))]),
        }],
    })
}

pub fn parse_route_query_inventory(value: &serde_json::Value) -> Result<Vec<RouteQuery>> {
    let routes = if value.is_array() {
        value.as_array()
    } else {
        value.get("routes").and_then(serde_json::Value::as_array)
    }
    .ok_or_else(|| {
        SkeinError::Semantic("graph route query JSON must contain a routes array".to_string())
    })?;
    routes.iter().map(parse_route_query).collect()
}

pub fn parse_route_parity_evidence(value: &serde_json::Value) -> Result<RouteParityEvidence> {
    if value.get("protocol").and_then(serde_json::Value::as_str)
        != Some(NMEM_GRAPH_ROUTE_PARITY_EVIDENCE_PROTOCOL)
    {
        return Err(SkeinError::Semantic(
            "graph route parity evidence protocol mismatch".to_string(),
        ));
    }
    let routes = value
        .get("routes")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Semantic(
                "graph route parity evidence must contain a routes array".to_string(),
            )
        })?
        .iter()
        .map(parse_route_parity_evidence_route)
        .collect::<Result<Vec<_>>>()?;
    let mut route_map = BTreeMap::new();
    for (route, evidence) in routes {
        if route_map.insert(route.clone(), evidence).is_some() {
            return Err(SkeinError::Semantic(format!(
                "duplicate graph route parity evidence route: {route}"
            )));
        }
    }
    Ok(RouteParityEvidence { routes: route_map })
}

fn parse_route_parity_evidence_route(
    value: &serde_json::Value,
) -> Result<(String, RouteParityEvidenceRoute)> {
    let route = required_string(value, "route")?.to_string();
    if route.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "graph route parity evidence route is required".to_string(),
        ));
    }
    Ok((
        route,
        RouteParityEvidenceRoute {
            ready: bool_field(value, "ready"),
            matched_per_million: optional_u64_field(value, "matched_per_million")?,
            primary_engine: optional_string_field(value, "primary_engine")?,
            shadow_engine: optional_string_field(value, "shadow_engine")?,
        },
    ))
}

fn parse_route_query(value: &serde_json::Value) -> Result<RouteQuery> {
    let route = required_string(value, "route")?.to_string();
    if route.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "graph route query route is required".to_string(),
        ));
    }
    let queries = value
        .get("queries")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Semantic("graph route query field 'queries' must be an array".to_string())
        })?
        .iter()
        .enumerate()
        .map(|(query_index, query)| parse_route_cypher_query(query, query_index))
        .collect::<Result<Vec<_>>>()?;
    Ok(RouteQuery {
        route,
        shadow_compare_ready: bool_field(value, "shadow_compare_ready"),
        primary_read_routing_enabled: bool_field(value, "primary_read_routing_enabled"),
        primary_ready: bool_field(value, "primary_ready"),
        blocker_codes: string_array_field(value, "blocker_codes")?,
        queries,
    })
}

fn parse_route_cypher_query(
    value: &serde_json::Value,
    query_index: usize,
) -> Result<RouteCypherQuery> {
    let cypher = required_string(value, "cypher")?.to_string();
    if cypher.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "graph route query field 'cypher' must be non-empty".to_string(),
        ));
    }
    let name = optional_query_name(value).unwrap_or_else(|| format!("query-{}", query_index + 1));
    if name.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "graph route query name must be non-empty when provided".to_string(),
        ));
    }
    let parameters = value
        .get("parameters")
        .map(parse_parameters_json)
        .transpose()?
        .unwrap_or_default();
    Ok(RouteCypherQuery {
        name,
        query_family: optional_string_field(value, "query_family")?,
        require_scan_pruning: bool_field(value, "require_scan_pruning"),
        require_pruned: bool_field(value, "require_pruned"),
        cypher,
        parameters,
    })
}

fn optional_query_name(value: &serde_json::Value) -> Option<String> {
    ["name", "query_id", "id"]
        .iter()
        .find_map(|field| value.get(*field).and_then(serde_json::Value::as_str))
        .map(str::to_string)
}

fn query_report_with_route_context(
    query: &RouteCypherQuery,
    query_index: usize,
    mut report: serde_json::Value,
) -> serde_json::Value {
    if let Some(object) = report.as_object_mut() {
        object.insert(
            "query_name".to_string(),
            serde_json::Value::String(query.name.clone()),
        );
        object.insert(
            "query_index".to_string(),
            serde_json::json!(query_index as u64),
        );
        object.insert(
            "query_family".to_string(),
            serde_json::json!(query.query_family.clone()),
        );
        object.insert(
            "require_scan_pruning".to_string(),
            serde_json::json!(query.require_scan_pruning),
        );
        object.insert(
            "require_pruned".to_string(),
            serde_json::json!(query.require_pruned),
        );
    }
    report
}

fn query_requirement_blockers(query: &RouteCypherQuery, report: &serde_json::Value) -> Vec<String> {
    let mut blockers = Vec::new();
    match query.query_family.as_deref() {
        Some(family) if REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES.contains(&family) => {}
        Some(_) => blockers.push("query_unknown_query_family".to_string()),
        None => blockers.push("query_family_missing".to_string()),
    }
    let scan_pruning_reports = report
        .get("scan_pruning_reports")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let scan_pruning_report_count = report
        .get("scan_pruning_report_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    if query.require_scan_pruning && scan_pruning_report_count != scan_pruning_reports.len() as u64
    {
        blockers.push("query_scan_pruning_report_count_mismatch".to_string());
    }
    if query.require_scan_pruning
        && (scan_pruning_report_count == 0 || scan_pruning_reports.is_empty())
    {
        blockers.push("query_scan_pruning_required_but_missing".to_string());
    }
    if query.require_scan_pruning
        && scan_pruning_reports
            .iter()
            .any(|report| scan_pruning_strategy_kind(report).is_none())
    {
        blockers.push("query_scan_pruning_strategy_missing".to_string());
    }
    if query.require_pruned && !scan_pruning_reports.iter().any(scan_report_pruned_rows) {
        blockers.push("query_pruned_scan_required_but_missing".to_string());
    }
    blockers
}

fn scan_pruning_strategy_kind(report: &serde_json::Value) -> Option<&str> {
    report
        .get("strategy")
        .and_then(|strategy| strategy.get("kind"))
        .and_then(serde_json::Value::as_str)
        .filter(|kind| !kind.trim().is_empty())
}

fn scan_report_pruned_rows(report: &serde_json::Value) -> bool {
    report
        .get("pruned")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
        && report
            .get("pruned_candidate_count")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|count| count > 0)
}

fn parse_parameters_json(value: &serde_json::Value) -> Result<BTreeMap<String, Value>> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("graph route query field 'parameters' must be an object".to_string())
    })?;
    object
        .iter()
        .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
        .collect()
}

fn value_from_json(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(SkeinError::Semantic(format!(
                    "unsupported JSON number in graph route query parameters: {value}"
                )))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(value_from_json)
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(Value::Map),
    }
}

fn parse_mode(raw: &str) -> Result<NowledgeMemGraphMode> {
    match raw {
        "shadow_read_only" => Ok(NowledgeMemGraphMode::ShadowReadOnly),
        "writable_cutover" => Ok(NowledgeMemGraphMode::WritableCutover),
        _ => Err(SkeinError::Semantic(format!(
            "invalid nowledge graph route evidence mode: {raw}"
        ))),
    }
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read graph route query evidence input: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&raw).map_err(|_| {
        SkeinError::Semantic("failed to parse graph route query JSON: invalid_json".to_string())
    })
}

fn required_string<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SkeinError::Semantic(format!(
                "graph route query field '{field}' must be a string"
            ))
        })
}

fn bool_field(value: &serde_json::Value, field: &str) -> bool {
    value
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

fn optional_u64_field(value: &serde_json::Value, field: &str) -> Result<Option<u64>> {
    value
        .get(field)
        .map(|value| {
            value.as_u64().ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "graph route query field '{field}' must be a non-negative integer"
                ))
            })
        })
        .transpose()
}

fn optional_string_field(value: &serde_json::Value, field: &str) -> Result<Option<String>> {
    value
        .get(field)
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "graph route query field '{field}' must be a string"
                ))
            })
        })
        .transpose()
}

fn string_array_field(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let Some(items) = value.get(field) else {
        return Ok(Vec::new());
    };
    let items = items.as_array().ok_or_else(|| {
        SkeinError::Semantic(format!(
            "graph route query field '{field}' must be a string array"
        ))
    })?;
    items
        .iter()
        .map(|item| {
            item.as_str().map(str::to_string).ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "graph route query field '{field}' must be a string array"
                ))
            })
        })
        .collect()
}

fn error_class(error: &SkeinError) -> &'static str {
    match error {
        SkeinError::Parse(_) => "parse",
        SkeinError::Semantic(_) => "semantic",
        SkeinError::Storage(_) | SkeinError::StorageIntegrity(_) => "storage",
        SkeinError::Execution(_) => "execution",
        SkeinError::CapabilityUnavailable { .. } => "capability_unavailable",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_graph_route_evidence_json, nowledge_mem_graph_augmentation_state_route_query,
        nowledge_mem_graph_community_members_route_query,
        nowledge_mem_graph_community_recent_memories_route_query,
        nowledge_mem_graph_community_subgraph_route_query,
        nowledge_mem_graph_node_details_route_query, nowledge_mem_graph_orphans_route_query,
        nowledge_mem_graph_overview_route_query, nowledge_mem_graph_pagerank_plan_route_query,
        nowledge_mem_graph_sample_route_query, parse_route_parity_evidence,
        parse_route_query_inventory, query_requirement_blockers, RouteCypherQuery,
    };
    use crate::{
        nowledge_graph_route_readiness_json, Database, NowledgeMemGraph, NowledgeMemGraphMode,
        NowledgeMemQueryReportOptions, Value, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn route_evidence_runs_queries_through_nowledge_runtime() {
        let mut db = Database::new();
        db.query("CREATE INDEX ON :Memory(id)").unwrap();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-other', title: 'Other Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": "overview-memory-lookup",
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                            "parameters": {
                                "id": "mem-route"
                            },
                            "require_scan_pruning": true,
                            "require_pruned": true
                        }
                    ],
                    "blocker_codes": []
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity();
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["protocol"], "nmem-graph-route-evidence-v1");
        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["required_routes_covered"], false);
        assert_eq!(
            evidence["covered_routes"],
            serde_json::json!(["/graph/overview"])
        );
        assert!(evidence["missing_required_routes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|route| route == "/graph/explore"));
        assert_eq!(evidence["routes"][0]["route"], "/graph/overview");
        assert_eq!(evidence["routes"][0]["shadow_compare_ready"], true);
        assert_eq!(
            evidence["routes"][0]["shadow_compare_evidence_source"],
            "route_parity_evidence"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["protocol"],
            "skein-nowledge-mem-query-report-v1"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "overview-memory-lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert_eq!(evidence["routes"][0]["query_reports"][0]["query_index"], 0);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["require_scan_pruning"],
            true
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["require_pruned"],
            true
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["statement_kind"],
            "match_return"
        );
        assert_eq!(
            evidence["routes"][0]["required_query_families"],
            serde_json::json!(["memory_lookup"])
        );
    }

    #[test]
    fn graph_route_query_json_parse_errors_are_redacted_by_default() {
        let root = unique_test_dir("graph_route_secret_path_do_not_emit");
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("secret-route-inventory-path-do-not-emit.json");
        std::fs::write(
            &path,
            "{ \"cypher\": \"MATCH (m {id: 'secret-route-query-do-not-emit'})\", \"unterminated\": ",
        )
        .unwrap();

        let error = super::read_json_file(&path).unwrap_err().to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse graph route query JSON: invalid_json"
        );
        assert!(!error.contains("secret-route-inventory-path-do-not-emit"));
        assert!(!error.contains("secret-route-query-do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn overview_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'overview-memory-1', title: 'Overview One', content: 'body one', pagerank_score: 3.0, importance: 0.1})")
            .unwrap();
        db.query(
            "CREATE (:Memory {id: 'overview-memory-2', content: 'Fallback body', importance: 2.0})",
        )
        .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_overview_route_query(2).unwrap()];
        let route_parity = ready_route_parity_for(&["/graph/overview"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["route"], "/graph/overview");
        assert_eq!(evidence["routes"][0]["primary_read_routing_enabled"], true);
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "overview-memory-ranking"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["physical_plan_captured"],
            true
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
        assert_eq!(
            readiness["routes"][0]["query_reports"][0]["scan_pruning_reports_present"],
            true
        );
    }

    #[test]
    fn sample_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query(
            "CREATE (:Memory {id: 'sample-route-a', title: 'Sample Route A', importance: 1.0})",
        )
        .unwrap();
        db.query(
            "CREATE (:Memory {id: 'sample-route-b', title: 'Sample Route B', importance: 2.0})",
        )
        .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_sample_route_query(2).unwrap()];
        let route_parity = ready_route_parity_for(&["/graph/sample"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["route"], "/graph/sample");
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "sample-memory-list"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["require_scan_pruning"],
            false
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
    }

    #[test]
    fn node_details_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'detail-memory-route', title: 'Detail Route', content: 'detail body'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let node_id = memory_node_id(&mut graph, "detail-memory-route");
        let route_queries = vec![nowledge_mem_graph_node_details_route_query(node_id).unwrap()];
        let route_parity = ready_route_parity_for(&["/graph/node-details/{node_id}"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(
            evidence["routes"][0]["route"],
            "/graph/node-details/{node_id}"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "node-details-memory-lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["require_pruned"],
            false
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
        assert_eq!(
            readiness["routes"][0]["query_reports"][0]["scan_pruning_reports_present"],
            true
        );
    }

    #[test]
    fn community_members_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'community-route-high', title: 'Community Route High', pagerank_score: 2.0, community_id: 42})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'community-route-other', title: 'Community Route Other', pagerank_score: 9.0, community_id: 7})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_community_members_route_query(42, 5).unwrap()];
        let route_parity = ready_route_parity_for(&["/graph/community-members/{community_id}"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(
            evidence["routes"][0]["route"],
            "/graph/community-members/{community_id}"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "community-members-memory-lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
        assert_eq!(
            readiness["routes"][0]["query_reports"][0]["scan_pruning_reports_present"],
            true
        );
    }

    #[test]
    fn community_recent_memories_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'community-recent-route-memory', title: 'Community Recent Route', content: 'recent route body', importance: 0.9, created_at: 1700000001, updated_at: 1700000002, is_crystal: false})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'community-recent-route-entity', community_id: 3676})")
            .unwrap();
        db.query("MATCH (m:Memory {id: 'community-recent-route-memory'}), (e:Entity {id: 'community-recent-route-entity'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries =
            vec![nowledge_mem_graph_community_recent_memories_route_query(3676, 5).unwrap()];
        let route_parity =
            ready_route_parity_for(&["/library/community/{community_id}/recent-memories"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(
            evidence["routes"][0]["route"],
            "/library/community/{community_id}/recent-memories"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "community-recent-memories-lookup"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
        assert_eq!(
            readiness["routes"][0]["query_reports"][0]["scan_pruning_reports_present"],
            true
        );
    }

    #[test]
    fn community_subgraph_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Entity {id: 'community-subgraph-route-alpha', name: 'Alpha Route', entity_type: 'concept', community_id: 3505, confidence: 0.9})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'community-subgraph-route-beta', name: 'Beta Route', entity_type: 'concept', community_id: 3505, confidence: 0.7})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'community-subgraph-route-memory'})")
            .unwrap();
        db.query("MATCH (m:Memory {id: 'community-subgraph-route-memory'}), (e:Entity {id: 'community-subgraph-route-alpha'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        db.query("MATCH (a:Entity {id: 'community-subgraph-route-alpha'}), (b:Entity {id: 'community-subgraph-route-beta'}) CREATE (a)-[:RELATES_TO {confidence: 0.77, relation_type: 'related'}]->(b)")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_community_subgraph_route_query(
            3505,
            5,
            [
                "community-subgraph-route-alpha",
                "community-subgraph-route-beta",
            ],
            10,
        )
        .unwrap()];
        let route_parity = ready_route_parity_for(&["/library/community/{community_id}/subgraph"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(
            evidence["routes"][0]["route"],
            "/library/community/{community_id}/subgraph"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["required_query_families"],
            serde_json::json!(["graph_traversal"])
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "community-subgraph-entity-ranking"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][1]["query_name"],
            "community-subgraph-relation-edges"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "graph_traversal"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][1]["query_family"],
            "graph_traversal"
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
    }

    #[test]
    fn augmentation_state_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:GraphMeta {meta_id: 'main', community_detection_applied: true, pagerank_applied: true, community_algorithm: 'louvain', community_resolution: 1.0, community_count: 12, pagerank_algorithm: 'pagerank', pagerank_damping: 0.85, pagerank_iterations: 20, last_augmentation_at: 1000, schema_version: 2, community_detection_computed_at: 900, pagerank_computed_at: 950})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_augmentation_state_route_query()];
        let route_parity = ready_route_parity_for(&["/graph/augmentation/state"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["route"], "/graph/augmentation/state");
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["required_query_families"],
            serde_json::json!(["projected_graph"])
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "augmentation-state-graph-meta"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "projected_graph"
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
    }

    #[test]
    fn pagerank_plan_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:GraphMeta {meta_id: 'main', pagerank_applied: true, pagerank_computed_at: 404})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'pagerank-route-m1', created_at: 10, updated_at: 20})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'pagerank-route-m2', created_at: 120, updated_at: 130})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'pagerank-route-e1', created_at: 15, updated_at: 25})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'pagerank-route-e2', created_at: 140, updated_at: 150})")
            .unwrap();
        db.query("MATCH (m:Memory {id: 'pagerank-route-m1'}), (e:Entity {id: 'pagerank-route-e1'}) CREATE (m)-[:MENTIONS {created_at: 30}]->(e)")
            .unwrap();
        db.query("MATCH (a:Entity {id: 'pagerank-route-e1'}), (b:Entity {id: 'pagerank-route-e2'}) CREATE (a)-[:RELATES_TO {created_at: 170}]->(b)")
            .unwrap();
        db.query("MATCH (a:Memory {id: 'pagerank-route-m1'}), (b:Memory {id: 'pagerank-route-m2'}) CREATE (a)-[:MEMORY_RELATES_TO {status: 'active', created_at: 180}]->(b)")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_pagerank_plan_route_query(Some(100))];
        let route_parity = ready_route_parity_for(&["/graph/augmentation/pagerank/plan"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(
            evidence["routes"][0]["route"],
            "/graph/augmentation/pagerank/plan"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["required_query_families"],
            serde_json::json!(["projected_graph"])
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "pagerank-plan-graph-meta"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "projected_graph"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"]
                .as_array()
                .unwrap()
                .len(),
            11
        );
        assert!(
            evidence["routes"][0]["query_reports"][0]["scan_pruning_report_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
    }

    #[test]
    fn orphans_route_query_helper_feeds_route_execution_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Entity {id: 'orphan-route-entity', name: 'Orphan Route Entity', entity_type: 'concept'})")
            .unwrap();
        db.query("CREATE (:Entity {id: 'mentioned-route-entity', name: 'Mentioned Route Entity', entity_type: 'concept'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'orphan-route-memory', title: 'Blocking Memory'})")
            .unwrap();
        db.query("MATCH (m:Memory {id: 'orphan-route-memory'}), (e:Entity {id: 'mentioned-route-entity'}) CREATE (m)-[:MENTIONS]->(e)")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = vec![nowledge_mem_graph_orphans_route_query(10).unwrap()];
        let route_parity = ready_route_parity_for(&["/graph/orphans"]);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: true,
                slow_log_threshold_micros: None,
            },
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["route"], "/graph/orphans");
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "orphan-entity-relationship-exclusion"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "graph_traversal"
        );
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["require_scan_pruning"],
            false
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!([])
        );
        let readiness = nowledge_graph_route_readiness_json(&evidence).unwrap();
        assert_eq!(readiness["routes"][0]["query_runtime_ready"], true);
        assert_eq!(readiness["routes"][0]["query_plan_evidence_ready"], true);
        assert_eq!(readiness["routes"][0]["query_profile_evidence_ready"], true);
    }

    #[test]
    fn route_evidence_requires_complete_required_route_coverage() {
        let mut db = Database::new();
        db.query("CREATE INDEX ON :Memory(id)").unwrap();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-other', title: 'Other Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
                .iter()
                .map(|route| ready_route_query(route))
                .collect::<Vec<_>>()
        }))
        .unwrap();

        let route_parity = ready_route_parity_for(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES);
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], true);
        assert_eq!(
            evidence["required_route_count"],
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
        );
        assert_eq!(
            evidence["covered_route_count"],
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len())
        );
        assert_eq!(
            evidence["covered_routes"],
            serde_json::json!(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES)
        );
        assert_eq!(evidence["missing_required_routes"], serde_json::json!([]));
        assert_eq!(evidence["required_routes_covered"], true);
        assert_eq!(evidence["unknown_routes"], serde_json::json!([]));
        assert_eq!(evidence["duplicate_routes"], serde_json::json!([]));
        assert_eq!(evidence["route_coverage_ready"], true);
        assert_eq!(
            evidence["route_coverage_blocker_codes"],
            serde_json::json!([])
        );
    }

    #[test]
    fn route_evidence_fails_closed_on_unknown_routes() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-other', title: 'Other Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_route_query(route))
            .collect::<Vec<_>>();
        routes.push(ready_route_query("/graph/manual-extra-route"));
        let route_queries =
            parse_route_query_inventory(&serde_json::json!({ "routes": routes })).unwrap();
        let mut parity_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.to_vec();
        parity_routes.push("/graph/manual-extra-route");
        let route_parity = ready_route_parity_for(&parity_routes);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["required_routes_covered"], true);
        assert_eq!(evidence["route_coverage_ready"], false);
        assert_eq!(
            evidence["unknown_routes"],
            serde_json::json!(["/graph/manual-extra-route"])
        );
        assert_eq!(
            evidence["route_coverage_blocker_codes"],
            serde_json::json!(["unknown_routes"])
        );
    }

    #[test]
    fn route_evidence_fails_closed_on_duplicate_routes() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-other', title: 'Other Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let mut routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_route_query(route))
            .collect::<Vec<_>>();
        routes.push(ready_route_query("/graph/overview"));
        let route_queries =
            parse_route_query_inventory(&serde_json::json!({ "routes": routes })).unwrap();
        let route_parity = ready_route_parity_for(REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES);

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["required_routes_covered"], true);
        assert_eq!(evidence["route_coverage_ready"], false);
        assert_eq!(
            evidence["duplicate_routes"],
            serde_json::json!(["/graph/overview"])
        );
        assert_eq!(
            evidence["route_coverage_blocker_codes"],
            serde_json::json!(["duplicate_routes"])
        );
    }

    #[test]
    fn route_evidence_promotes_primary_routing_after_query_runtime_success() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_read_routing_enabled": true,
                    "primary_ready": false,
                    "queries": [
                        {
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                            "parameters": {
                                "id": "mem-route"
                            }
                        }
                    ],
                    "blocker_codes": ["graph_route_execution_evidence_missing"]
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity();
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["primary_read_routing_enabled"], true);
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert!(!evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "graph_route_execution_evidence_missing"));
    }

    #[test]
    fn route_evidence_fails_closed_when_query_family_is_missing() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": "overview-unclassified-smoke",
                            "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                            "parameters": {
                                "id": "mem-route"
                            }
                        }
                    ],
                    "blocker_codes": []
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity();
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            serde_json::Value::Null
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_family_missing"));
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "route_required_query_family_missing"));
    }

    #[test]
    fn route_evidence_fails_closed_when_route_family_does_not_match() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/shortest-path",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": "shortest-path-smoke",
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                            "parameters": {
                                "id": "mem-route"
                            }
                        }
                    ],
                    "blocker_codes": []
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity_for(&["/graph/shortest-path"]);
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["required_query_families"],
            serde_json::json!(["graph_traversal"])
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "route_required_query_family_missing"));
    }

    #[test]
    fn route_evidence_fails_closed_on_query_execution_error() {
        let db = Database::new();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": "broken-overview-query",
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory) RETURN unknown.property AS value"
                        }
                    ]
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity();
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["query_reports"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_runtime_execution_failed"));
        assert_eq!(
            evidence["routes"][0]["query_errors"][0]["error_class"],
            "semantic"
        );
        assert_eq!(
            evidence["routes"][0]["query_errors"][0]["query_name"],
            "broken-overview-query"
        );
        assert_eq!(evidence["routes"][0]["query_errors"][0]["query_index"], 0);
    }

    #[test]
    fn route_evidence_fails_closed_when_required_pruning_is_not_reduced() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": "overview-full-scan",
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory) RETURN m.title AS title",
                            "require_pruned": true
                        }
                    ],
                    "blocker_codes": []
                }
            ]
        }))
        .unwrap();

        let route_parity = ready_route_parity();
        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_name"],
            "overview-full-scan"
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_pruned_scan_required_but_missing"));
    }

    #[test]
    fn route_query_requirements_reject_malformed_scan_pruning_evidence() {
        let query = RouteCypherQuery {
            name: "malformed-pruning".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: "MATCH (m:Memory {id: $id}) RETURN m.title AS title".to_string(),
            parameters: BTreeMap::new(),
        };
        let report = serde_json::json!({
            "scan_pruning_report_count": 2,
            "scan_pruning_reports": [
                {
                    "strategy": { "kind": "id_eq" },
                    "pruned": true,
                    "pruned_candidate_count": 1
                },
                {
                    "strategy": {},
                    "pruned": false,
                    "pruned_candidate_count": 0
                },
                {
                    "pruned": false,
                    "pruned_candidate_count": 0
                }
            ]
        });

        assert_eq!(
            query_requirement_blockers(&query, &report),
            vec![
                "query_scan_pruning_report_count_mismatch".to_string(),
                "query_scan_pruning_strategy_missing".to_string(),
            ]
        );
    }

    #[test]
    fn route_query_requirements_reject_unknown_query_family() {
        let query = RouteCypherQuery {
            name: "unknown-family".to_string(),
            query_family: Some("manual_smoke".to_string()),
            require_scan_pruning: false,
            require_pruned: false,
            cypher: "MATCH (m:Memory {id: $id}) RETURN m.title AS title".to_string(),
            parameters: BTreeMap::new(),
        };

        assert_eq!(
            query_requirement_blockers(&query, &serde_json::json!({})),
            vec!["query_unknown_query_family".to_string()]
        );
    }

    #[test]
    fn route_evidence_fails_closed_without_route_parity_evidence() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [
                {
                    "route": "/graph/overview",
                    "shadow_compare_ready": true,
                    "primary_ready": true,
                    "queries": [
                        {
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                            "parameters": {
                                "id": "mem-route"
                            }
                        }
                    ],
                    "blocker_codes": []
                }
            ]
        }))
        .unwrap();

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            None,
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["shadow_compare_evidence_source"],
            "route_query_inventory"
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "shadow_compare_evidence_missing"));
    }

    #[test]
    fn route_evidence_recomputes_route_parity_identity() {
        let mut db = Database::new();
        db.query("CREATE (:Memory {id: 'mem-route', title: 'Route Evidence'})")
            .unwrap();
        let mut graph = NowledgeMemGraph::from_database(db, NowledgeMemGraphMode::WritableCutover);
        let route_queries = parse_route_query_inventory(&serde_json::json!({
            "routes": [ready_route_query("/graph/overview")]
        }))
        .unwrap();
        let route_parity = parse_route_parity_evidence(&serde_json::json!({
            "protocol": "nmem-graph-route-parity-evidence-v1",
            "routes": [
                {
                    "route": "/graph/overview",
                    "ready": true,
                    "matched_per_million": 999999,
                    "primary_engine": "skein",
                    "shadow_engine": "kuzu"
                }
            ]
        }))
        .unwrap();

        let evidence = nowledge_graph_route_evidence_json(
            &mut graph,
            &route_queries,
            Default::default(),
            Some(&route_parity),
        );

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["routes"][0]["shadow_compare_ready"], false);
        assert_eq!(evidence["routes"][0]["primary_ready"], false);
        assert_eq!(
            evidence["routes"][0]["shadow_compare"]["blocker_codes"],
            serde_json::json!([
                "route_parity_matched_per_million_not_full",
                "route_parity_primary_engine_mismatch",
                "route_parity_shadow_engine_mismatch"
            ])
        );
        assert!(evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "route_parity_primary_engine_mismatch"));
    }

    fn ready_route_parity() -> super::RouteParityEvidence {
        ready_route_parity_for(&["/graph/overview"])
    }

    fn ready_route_query(route: &str) -> serde_json::Value {
        let query_family = crate::nowledge_mem_required_query_families_for_route(route)
            .first()
            .copied()
            .unwrap_or("memory_lookup");
        serde_json::json!({
            "route": route,
            "shadow_compare_ready": true,
            "primary_ready": true,
            "queries": [
                {
                    "name": format!("{}:memory-lookup", route),
                    "query_family": query_family,
                    "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                    "parameters": {
                        "id": "mem-route"
                    },
                    "require_scan_pruning": true,
                    "require_pruned": true
                }
            ],
            "blocker_codes": []
        })
    }

    fn memory_node_id(graph: &mut NowledgeMemGraph, memory_id: &str) -> u64 {
        let mut parameters = BTreeMap::new();
        parameters.insert("id".to_string(), Value::String(memory_id.to_string()));
        let output = graph
            .query_with_params(
                "MATCH (m:Memory {id: $id}) RETURN id(m) AS node_id",
                &parameters,
            )
            .unwrap();
        match output.rows[0].get("node_id") {
            Some(Value::Int(value)) if *value >= 0 => *value as u64,
            other => panic!("expected non-negative node_id, got {other:?}"),
        }
    }

    fn ready_route_parity_for(routes: &[&str]) -> super::RouteParityEvidence {
        parse_route_parity_evidence(&serde_json::json!({
            "protocol": "nmem-graph-route-parity-evidence-v1",
            "routes": routes
                .iter()
                .map(|route| {
                    serde_json::json!({
                        "route": route,
                        "ready": true,
                        "matched_per_million": 1000000,
                        "primary_engine": "kuzu",
                        "shadow_engine": "skein"
                    })
                })
                .collect::<Vec<_>>()
        }))
        .unwrap()
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}", std::process::id()))
    }
}
