//! Route-query catalog and evidence reduction independent of the host database.
//!
//! The embedded facade owns opening a database and executing a query. This module
//! owns the stable route protocol, its input validation, and the fail-closed
//! reduction of query reports into route evidence.
use crate::json_parse::{optional_query_name, parse_parameters_json};
use skein_core::{Result, SkeinError, Value};
use skein_evidence::inventory::REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES;
use skein_route_ownership::graph::{
    nowledge_mem_required_query_families_for_route, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};
use std::collections::{BTreeMap, BTreeSet};

pub const NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL: &str = "nmem-graph-route-evidence-v1";
pub const NMEM_GRAPH_ROUTE_PARITY_EVIDENCE_PROTOCOL: &str = "nmem-graph-route-parity-evidence-v1";
const ROUTE_PARITY_EVIDENCE_SOURCE: &str = "route_parity_evidence";
const ROUTE_QUERY_INVENTORY_EVIDENCE_SOURCE: &str = "route_query_inventory";
const ROUTE_PARITY_FULL_MATCH_PER_MILLION: u64 = 1_000_000;

const NOWLEDGE_MEM_GRAPH_OVERVIEW_ROUTE: &str = "/graph/overview";
const NOWLEDGE_MEM_GRAPH_OVERVIEW_MEMORY_RANKING_QUERY: &str = "\
MATCH (m:Memory) \
RETURN m.id AS memory_id, \
id(m) AS node_id, \
COALESCE(m.title, LEFT(m.content, 60)) AS label, \
m.title AS title, \
LEFT(COALESCE(m.content, ''), 200) AS content_preview, \
COALESCE(m.pagerank_score, m.importance, 0.5) AS score, \
m.community_id AS community_id, \
m.space_id AS raw_space_id, \
m.created_at AS created_at, \
m.updated_at AS updated_at, \
m.source AS source, \
m.event_start AS event_start, \
m.event_end AS event_end, \
m.importance AS importance \
ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC, m.id ASC \
LIMIT $limit";
const NOWLEDGE_MEM_GRAPH_SAMPLE_ROUTE: &str = "/graph/sample";
const NOWLEDGE_MEM_GRAPH_SAMPLE_MEMORY_QUERY: &str = "\
MATCH (m:Memory) \
RETURN m.id AS memory_id, \
id(m) AS node_id, \
COALESCE(m.title, LEFT(m.content, 60)) AS label, \
m.title AS title, \
LEFT(COALESCE(m.content, ''), 200) AS content_preview, \
COALESCE(m.pagerank_score, m.importance, 0.5) AS score, \
m.community_id AS community_id, \
m.space_id AS raw_space_id, \
m.created_at AS created_at, \
m.updated_at AS updated_at, \
m.source AS source, \
m.event_start AS event_start, \
m.event_end AS event_end, \
m.importance AS importance \
ORDER BY m.id ASC \
LIMIT $limit";
const NOWLEDGE_MEM_GRAPH_NODE_DETAILS_ROUTE: &str = "/graph/node-details/{node_id}";
const NOWLEDGE_MEM_GRAPH_NODE_DETAILS_MEMORY_QUERY: &str = "\
MATCH (m:Memory) \
WHERE id(m) = $node_id \
RETURN id(m) AS node_id, \
m.id AS memory_id, \
'Memory' AS node_kind, \
COALESCE(m.title, LEFT(m.content, 60), m.id, 'Memory') AS label, \
m.title AS title, \
m.content AS content, \
LEFT(COALESCE(m.content, ''), 500) AS content_preview, \
m.summary AS summary, \
m.source AS source, \
m.space_id AS raw_space_id, \
m.community_id AS community_id, \
m.created_at AS created_at, \
m.updated_at AS updated_at, \
m.event_start AS event_start, \
m.event_end AS event_end, \
m.importance AS importance, \
m.confidence AS confidence, \
m.is_latest AS is_latest, \
m.is_deleted AS is_deleted \
LIMIT 1";
const NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_ROUTE: &str = "/graph/community-members/{community_id}";
const NOWLEDGE_MEM_GRAPH_COMMUNITY_MEMBERS_MEMORY_QUERY: &str = "\
MATCH (m:Memory) \
WHERE m.community_id = $community_id \
RETURN m.id AS memory_id, \
id(m) AS node_id, \
COALESCE(m.title, LEFT(m.content, 60)) AS label, \
m.title AS title, \
LEFT(COALESCE(m.content, ''), 200) AS content_preview, \
COALESCE(m.pagerank_score, m.importance, 0.5) AS score, \
m.community_id AS community_id, \
m.space_id AS raw_space_id, \
m.created_at AS created_at, \
m.updated_at AS updated_at, \
m.source AS source, \
m.event_start AS event_start, \
m.event_end AS event_end, \
m.importance AS importance \
ORDER BY COALESCE(m.pagerank_score, m.importance, 0.5) DESC, m.id ASC \
LIMIT $limit";
const NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE: &str =
    "/library/community/{community_id}/recent-memories";
const NOWLEDGE_MEM_GRAPH_COMMUNITY_RECENT_MEMORIES_QUERY: &str = "\
MATCH (m:Memory)-[:MENTIONS]->(e:Entity) \
WHERE e.community_id = $community_id \
WITH m.id AS memory_id, \
id(m) AS node_id, \
COALESCE(m.title, LEFT(m.content, 60)) AS label, \
m.title AS title, \
m.content AS content, \
LEFT(COALESCE(m.content, ''), 200) AS content_preview, \
m.importance AS importance, \
m.created_at AS created_at, \
m.updated_at AS updated_at, \
m.is_crystal AS is_crystal, \
COUNT(DISTINCT e) AS mention_breadth \
ORDER BY created_at DESC \
LIMIT $limit \
RETURN memory_id AS memory_id, \
node_id AS node_id, \
label AS label, \
title AS title, \
content AS content, \
content_preview AS content_preview, \
importance AS importance, \
created_at AS created_at, \
updated_at AS updated_at, \
is_crystal AS is_crystal, \
mention_breadth AS mention_breadth";
const NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ROUTE: &str =
    "/library/community/{community_id}/subgraph";
const NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_ENTITY_QUERY: &str = "\
MATCH (e:Entity) \
WHERE e.community_id = $community_id \
OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) \
RETURN e.id AS entity_id, \
id(e) AS node_id, \
COALESCE(e.name, e.id) AS label, \
e.name AS name, \
e.entity_type AS entity_type, \
e.confidence AS confidence, \
COUNT(r) AS mention_count \
ORDER BY mention_count DESC, e.name ASC \
LIMIT $max_entities";
const NOWLEDGE_MEM_GRAPH_COMMUNITY_SUBGRAPH_EDGE_QUERY: &str = "\
MATCH (e1:Entity)-[r:RELATES_TO]-(e2:Entity) \
WHERE e1.id IN $entity_ids \
AND e2.id IN $entity_ids \
AND e1.id < e2.id \
RETURN e1.id AS source_entity_id, \
e2.id AS target_entity_id, \
id(r) AS relationship_id, \
r.confidence AS confidence, \
r.relation_type AS relation_type \
LIMIT $max_edges";
const NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_ROUTE: &str = "/graph/augmentation/state";
const NOWLEDGE_MEM_GRAPH_AUGMENTATION_STATE_QUERY: &str = "\
MATCH (m:GraphMeta {meta_id: 'main'}) \
RETURN m.community_detection_applied AS community_detection_applied, \
m.pagerank_applied AS pagerank_applied, \
m.community_algorithm AS community_algorithm, \
m.community_resolution AS community_resolution, \
m.community_count AS community_count, \
m.pagerank_algorithm AS pagerank_algorithm, \
m.pagerank_damping AS pagerank_damping, \
m.pagerank_iterations AS pagerank_iterations, \
m.last_augmentation_at AS last_augmentation_at, \
m.schema_version AS schema_version, \
m.community_detection_computed_at AS community_detection_computed_at, \
m.pagerank_computed_at AS pagerank_computed_at \
LIMIT 1";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ROUTE: &str = "/graph/augmentation/pagerank/plan";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_GRAPH_META_QUERY: &str = "\
MATCH (m:GraphMeta {meta_id: 'main'}) \
RETURN m.pagerank_applied AS pagerank_applied, \
m.pagerank_computed_at AS pagerank_computed_at \
LIMIT 1";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MEMORY_COUNT_QUERY: &str =
    "MATCH (m:Memory) RETURN count(m) AS total";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_COUNT_QUERY: &str =
    "MATCH (e:Entity) RETURN count(e) AS total";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ENTITY_RELATION_COUNT_QUERY: &str =
    "MATCH (:Entity)-[r:RELATES_TO]->(:Entity) RETURN count(r) AS total";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_MENTION_EDGE_COUNT_QUERY: &str =
    "MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(r) AS total";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_ACTIVE_MEMORY_RELATION_COUNT_QUERY: &str = "MATCH (:Memory)-[r:MEMORY_RELATES_TO]->(:Memory) WHERE r.status = 'active' RETURN count(r) AS total";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_COUNT_QUERY: &str = "MATCH (m:Memory) WHERE m.created_at > $cutoff OR m.updated_at > $cutoff RETURN count(m) AS total";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_COUNT_QUERY: &str = "MATCH (e:Entity) WHERE e.created_at > $cutoff OR e.updated_at > $cutoff RETURN count(e) AS total";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MENTION_EDGE_COUNT_QUERY: &str = "MATCH (:Memory)-[r:MENTIONS]->(:Entity) WHERE r.created_at > $cutoff OR r.updated_at > $cutoff RETURN count(r) AS total";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_RELATION_COUNT_QUERY: &str = "MATCH (:Entity)-[r:RELATES_TO]->(:Entity) WHERE r.created_at > $cutoff OR r.updated_at > $cutoff RETURN count(r) AS total";
const NOWLEDGE_MEM_GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_RELATION_COUNT_QUERY: &str = "MATCH (:Memory)-[r:MEMORY_RELATES_TO]->(:Memory) WHERE r.status = 'active' AND (r.created_at > $cutoff OR r.updated_at > $cutoff) RETURN count(r) AS total";
const NOWLEDGE_MEM_GRAPH_ORPHANS_ROUTE: &str = "/graph/orphans";
const NOWLEDGE_MEM_GRAPH_ORPHAN_ENTITIES_QUERY: &str = "\
MATCH (e:Entity) \
WHERE NOT (e)<-[:MENTIONS]-(:Memory) \
AND NOT (e)-[:RELATES_TO]-() \
AND NOT (e)-[:HAS_LABEL]-() \
RETURN e.id AS entity_id, \
id(e) AS node_id, \
COALESCE(e.name, e.id) AS label, \
e.name AS name, \
e.entity_type AS entity_type, \
e.description AS description, \
e.community_id AS community_id, \
e.confidence AS confidence, \
e.pagerank_score AS pagerank_score \
ORDER BY e.id ASC \
LIMIT $limit";
pub fn graph_route_evidence_json(
    mode: &str,
    route_queries: &[RouteQuery],
    route_parity: Option<&RouteParityEvidence>,
    mut execute_query: impl FnMut(&RouteCypherQuery) -> Result<serde_json::Value>,
) -> serde_json::Value {
    let routes = route_queries
        .iter()
        .map(|route| route.query_runtime_evidence(route_parity, &mut execute_query))
        .collect::<Vec<_>>();
    let route_coverage = route_coverage(&routes);
    let ready = route_coverage.ready && routes.iter().all(route_evidence_ready);
    serde_json::json!({
        "protocol": NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL,
        "mode": mode,
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
        route_parity: Option<&RouteParityEvidence>,
        execute_query: &mut impl FnMut(&RouteCypherQuery) -> Result<serde_json::Value>,
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
            match execute_query(query) {
                Ok(report) => {
                    let report = query_report_with_route_context(query, query_index, report);
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
        .map(|value| parse_parameters_json(value, "graph route query"))
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

#[doc(hidden)]
pub fn query_requirement_blockers(
    query: &RouteCypherQuery,
    report: &serde_json::Value,
) -> Vec<String> {
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
        SkeinError::Storage(_)
        | SkeinError::StorageIntegrity(_)
        | SkeinError::AppendSequenceExhausted { .. } => "storage",
        SkeinError::Execution(_) => "execution",
        SkeinError::CapabilityUnavailable { .. } => "capability_unavailable",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        graph_route_evidence_json, nowledge_mem_graph_overview_route_query, RouteParityEvidence,
        RouteParityEvidenceRoute,
    };
    use skein_core::SkeinError;
    use std::collections::BTreeMap;

    fn complete_overview_parity() -> RouteParityEvidence {
        RouteParityEvidence {
            routes: BTreeMap::from([(
                "/graph/overview".to_string(),
                RouteParityEvidenceRoute {
                    ready: true,
                    matched_per_million: Some(1_000_000),
                    primary_engine: Some("kuzu".to_string()),
                    shadow_engine: Some("skein".to_string()),
                },
            )]),
        }
    }

    #[test]
    fn route_evidence_reducer_receives_host_query_reports() {
        let route = nowledge_mem_graph_overview_route_query(1).unwrap();
        let mut calls = 0;

        let evidence = graph_route_evidence_json(
            "writable_cutover",
            &[route],
            Some(&complete_overview_parity()),
            |query| {
                calls += 1;
                assert_eq!(query.name, "overview-memory-ranking");
                Ok(serde_json::json!({
                    "scan_pruning_report_count": 1,
                    "scan_pruning_reports": [{
                        "strategy": { "kind": "index_lookup" },
                        "pruned": true,
                        "pruned_candidate_count": 1,
                    }],
                }))
            },
        );

        assert_eq!(calls, 1);
        assert_eq!(evidence["mode"], "writable_cutover");
        assert_eq!(evidence["routes"][0]["query_errors"], serde_json::json!([]));
        assert_eq!(evidence["routes"][0]["query_reports"][0]["query_index"], 0);
        assert_eq!(
            evidence["routes"][0]["query_reports"][0]["query_family"],
            "memory_lookup"
        );
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert_eq!(evidence["ready"], false);
    }

    #[test]
    fn route_evidence_reducer_classifies_host_query_failures() {
        let route = nowledge_mem_graph_overview_route_query(1).unwrap();

        let evidence = graph_route_evidence_json(
            "shadow_read_only",
            &[route],
            Some(&complete_overview_parity()),
            |_| Err(SkeinError::Execution("host execution failed".to_string())),
        );

        assert_eq!(
            evidence["routes"][0]["query_errors"],
            serde_json::json!([{
                "query_name": "overview-memory-ranking",
                "query_index": 0,
                "error_class": "execution",
            }])
        );
        assert_eq!(
            evidence["routes"][0]["blocker_codes"],
            serde_json::json!(["query_runtime_execution_failed"])
        );
    }
}
