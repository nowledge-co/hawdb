// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Private parameterized statements used by route qualification fixtures.
//! Hosts own their application queries; these are not embedded database APIs.

use super::{RouteCypherQuery, RouteQuery};
use crate::{HawDBError, Result, Value};
use std::collections::BTreeMap;

// Qualification runs each statement with these independent ceilings. Query LIMIT
// still bounds graph work; exceeding an output ceiling fails the evidence run.
pub(super) const ROUTE_STATEMENT_OUTPUT_LIMITS: crate::QueryStreamOptions =
    crate::QueryStreamOptions {
        max_rows: Some(16_384),
        max_payload_bytes: Some(16 * 1024 * 1024),
    };

const GRAPH_OVERVIEW_ROUTE: &str = "/graph/overview";
const GRAPH_OVERVIEW_MEMORY_RANKING_QUERY: &str = "\
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
const GRAPH_SAMPLE_ROUTE: &str = "/graph/sample";
const GRAPH_SAMPLE_MEMORY_QUERY: &str = "\
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
const GRAPH_NODE_DETAILS_ROUTE: &str = "/graph/node-details/{node_id}";
const GRAPH_NODE_DETAILS_MEMORY_QUERY: &str = "\
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
const GRAPH_COMMUNITY_MEMBERS_ROUTE: &str = "/graph/community-members/{community_id}";
const GRAPH_COMMUNITY_MEMBERS_MEMORY_QUERY: &str = "\
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
const GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE: &str =
    "/library/community/{community_id}/recent-memories";
const GRAPH_COMMUNITY_RECENT_MEMORIES_QUERY: &str = "\
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
const GRAPH_COMMUNITY_SUBGRAPH_ROUTE: &str = "/library/community/{community_id}/subgraph";
const GRAPH_COMMUNITY_SUBGRAPH_ENTITY_QUERY: &str = "\
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
const GRAPH_COMMUNITY_SUBGRAPH_EDGE_QUERY: &str = "\
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
const GRAPH_AUGMENTATION_STATE_ROUTE: &str = "/graph/augmentation/state";
const GRAPH_AUGMENTATION_STATE_QUERY: &str = "\
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
const GRAPH_PAGERANK_PLAN_ROUTE: &str = "/graph/augmentation/pagerank/plan";
const GRAPH_PAGERANK_PLAN_GRAPH_META_QUERY: &str = "\
MATCH (m:GraphMeta {meta_id: 'main'}) \
RETURN m.pagerank_applied AS pagerank_applied, \
m.pagerank_computed_at AS pagerank_computed_at \
LIMIT 1";
const GRAPH_PAGERANK_PLAN_MEMORY_COUNT_QUERY: &str = "MATCH (m:Memory) RETURN count(m) AS total";
const GRAPH_PAGERANK_PLAN_ENTITY_COUNT_QUERY: &str = "MATCH (e:Entity) RETURN count(e) AS total";
const GRAPH_PAGERANK_PLAN_ENTITY_RELATION_COUNT_QUERY: &str =
    "MATCH (:Entity)-[r:RELATES_TO]->(:Entity) RETURN count(r) AS total";
const GRAPH_PAGERANK_PLAN_MENTION_EDGE_COUNT_QUERY: &str =
    "MATCH (:Memory)-[r:MENTIONS]->(:Entity) RETURN count(r) AS total";
const GRAPH_PAGERANK_PLAN_ACTIVE_MEMORY_RELATION_COUNT_QUERY: &str = "MATCH (:Memory)-[r:MEMORY_RELATES_TO]->(:Memory) WHERE r.status = 'active' RETURN count(r) AS total";
const GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_COUNT_QUERY: &str = "MATCH (m:Memory) WHERE m.created_at > $cutoff OR m.updated_at > $cutoff RETURN count(m) AS total";
const GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_COUNT_QUERY: &str = "MATCH (e:Entity) WHERE e.created_at > $cutoff OR e.updated_at > $cutoff RETURN count(e) AS total";
const GRAPH_PAGERANK_PLAN_CHANGED_MENTION_EDGE_COUNT_QUERY: &str = "MATCH (:Memory)-[r:MENTIONS]->(:Entity) WHERE r.created_at > $cutoff OR r.updated_at > $cutoff RETURN count(r) AS total";
const GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_RELATION_COUNT_QUERY: &str = "MATCH (:Entity)-[r:RELATES_TO]->(:Entity) WHERE r.created_at > $cutoff OR r.updated_at > $cutoff RETURN count(r) AS total";
const GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_RELATION_COUNT_QUERY: &str = "MATCH (:Memory)-[r:MEMORY_RELATES_TO]->(:Memory) WHERE r.status = 'active' AND (r.created_at > $cutoff OR r.updated_at > $cutoff) RETURN count(r) AS total";
const GRAPH_ORPHANS_ROUTE: &str = "/graph/orphans";
const GRAPH_ORPHAN_ENTITIES_QUERY: &str = "\
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
pub(crate) fn graph_overview_route_query(limit: usize) -> Result<RouteQuery> {
    let limit = i64::try_from(limit).map_err(|_| {
        HawDBError::Semantic(
            "graph overview route evidence limit exceeds supported range".to_string(),
        )
    })?;
    if limit <= 0 {
        return Err(HawDBError::Semantic(
            "graph overview route evidence limit must be greater than zero".to_string(),
        ));
    }
    Ok(RouteQuery {
        route: GRAPH_OVERVIEW_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "overview-memory-ranking".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: GRAPH_OVERVIEW_MEMORY_RANKING_QUERY.to_string(),
            parameters: BTreeMap::from([("limit".to_string(), Value::Int(limit))]),
        }],
    })
}

pub(crate) fn graph_sample_route_query(limit: usize) -> Result<RouteQuery> {
    let limit = i64::try_from(limit).map_err(|_| {
        HawDBError::Semantic(
            "graph sample route evidence limit exceeds supported range".to_string(),
        )
    })?;
    if limit <= 0 {
        return Err(HawDBError::Semantic(
            "graph sample route evidence limit must be greater than zero".to_string(),
        ));
    }
    Ok(RouteQuery {
        route: GRAPH_SAMPLE_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "sample-memory-list".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: false,
            require_pruned: false,
            cypher: GRAPH_SAMPLE_MEMORY_QUERY.to_string(),
            parameters: BTreeMap::from([("limit".to_string(), Value::Int(limit))]),
        }],
    })
}

pub(crate) fn graph_node_details_route_query(node_id: u64) -> Result<RouteQuery> {
    let node_id = i64::try_from(node_id).map_err(|_| {
        HawDBError::Semantic(
            "graph node details route evidence node_id exceeds supported range".to_string(),
        )
    })?;
    Ok(RouteQuery {
        route: GRAPH_NODE_DETAILS_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "node-details-memory-lookup".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: GRAPH_NODE_DETAILS_MEMORY_QUERY.to_string(),
            parameters: BTreeMap::from([("node_id".to_string(), Value::Int(node_id))]),
        }],
    })
}

pub(crate) fn graph_community_members_route_query(
    community_id: i64,
    limit: usize,
) -> Result<RouteQuery> {
    let limit = i64::try_from(limit).map_err(|_| {
        HawDBError::Semantic(
            "graph community members route evidence limit exceeds supported range".to_string(),
        )
    })?;
    if limit <= 0 {
        return Err(HawDBError::Semantic(
            "graph community members route evidence limit must be greater than zero".to_string(),
        ));
    }
    Ok(RouteQuery {
        route: GRAPH_COMMUNITY_MEMBERS_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "community-members-memory-lookup".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: GRAPH_COMMUNITY_MEMBERS_MEMORY_QUERY.to_string(),
            parameters: BTreeMap::from([
                ("community_id".to_string(), Value::Int(community_id)),
                ("limit".to_string(), Value::Int(limit)),
            ]),
        }],
    })
}

pub(crate) fn graph_community_recent_memories_route_query(
    community_id: i64,
    limit: usize,
) -> Result<RouteQuery> {
    let limit = i64::try_from(limit).map_err(|_| {
        HawDBError::Semantic(
            "graph community recent memories route evidence limit exceeds supported range"
                .to_string(),
        )
    })?;
    if limit <= 0 {
        return Err(HawDBError::Semantic(
            "graph community recent memories route evidence limit must be greater than zero"
                .to_string(),
        ));
    }
    Ok(RouteQuery {
        route: GRAPH_COMMUNITY_RECENT_MEMORIES_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "community-recent-memories-lookup".to_string(),
            query_family: Some("memory_lookup".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: GRAPH_COMMUNITY_RECENT_MEMORIES_QUERY.to_string(),
            parameters: BTreeMap::from([
                ("community_id".to_string(), Value::Int(community_id)),
                ("limit".to_string(), Value::Int(limit)),
            ]),
        }],
    })
}

pub(crate) fn graph_community_subgraph_route_query<I, S>(
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
        HawDBError::Semantic(
            "graph community subgraph route evidence max_entities exceeds supported range"
                .to_string(),
        )
    })?;
    let max_edges = i64::try_from(max_edges).map_err(|_| {
        HawDBError::Semantic(
            "graph community subgraph route evidence max_edges exceeds supported range".to_string(),
        )
    })?;
    if max_entities <= 0 {
        return Err(HawDBError::Semantic(
            "graph community subgraph route evidence max_entities must be greater than zero"
                .to_string(),
        ));
    }
    if max_edges < 0 {
        return Err(HawDBError::Semantic(
            "graph community subgraph route evidence max_edges must not be negative".to_string(),
        ));
    }
    let entity_ids = edge_entity_ids
        .into_iter()
        .map(|entity_id| Value::String(entity_id.into()))
        .collect::<Vec<_>>();
    Ok(RouteQuery {
        route: GRAPH_COMMUNITY_SUBGRAPH_ROUTE.to_string(),
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
                cypher: GRAPH_COMMUNITY_SUBGRAPH_ENTITY_QUERY.to_string(),
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
                cypher: GRAPH_COMMUNITY_SUBGRAPH_EDGE_QUERY.to_string(),
                parameters: BTreeMap::from([
                    ("entity_ids".to_string(), Value::List(entity_ids)),
                    ("max_edges".to_string(), Value::Int(max_edges)),
                ]),
            },
        ],
    })
}

pub(crate) fn graph_augmentation_state_route_query() -> RouteQuery {
    RouteQuery {
        route: GRAPH_AUGMENTATION_STATE_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "augmentation-state-graph-meta".to_string(),
            query_family: Some("projected_graph".to_string()),
            require_scan_pruning: true,
            require_pruned: false,
            cypher: GRAPH_AUGMENTATION_STATE_QUERY.to_string(),
            parameters: BTreeMap::new(),
        }],
    }
}

pub(crate) fn graph_pagerank_plan_route_query(
    changed_since_epoch_nanos: Option<i64>,
) -> RouteQuery {
    let mut queries = vec![
        pagerank_plan_route_query(
            "pagerank-plan-graph-meta",
            GRAPH_PAGERANK_PLAN_GRAPH_META_QUERY,
            BTreeMap::new(),
            true,
        ),
        pagerank_plan_route_query(
            "pagerank-plan-memory-count",
            GRAPH_PAGERANK_PLAN_MEMORY_COUNT_QUERY,
            BTreeMap::new(),
            false,
        ),
        pagerank_plan_route_query(
            "pagerank-plan-entity-count",
            GRAPH_PAGERANK_PLAN_ENTITY_COUNT_QUERY,
            BTreeMap::new(),
            false,
        ),
        pagerank_plan_route_query(
            "pagerank-plan-entity-relation-count",
            GRAPH_PAGERANK_PLAN_ENTITY_RELATION_COUNT_QUERY,
            BTreeMap::new(),
            false,
        ),
        pagerank_plan_route_query(
            "pagerank-plan-mention-edge-count",
            GRAPH_PAGERANK_PLAN_MENTION_EDGE_COUNT_QUERY,
            BTreeMap::new(),
            false,
        ),
        pagerank_plan_route_query(
            "pagerank-plan-active-memory-relation-count",
            GRAPH_PAGERANK_PLAN_ACTIVE_MEMORY_RELATION_COUNT_QUERY,
            BTreeMap::new(),
            false,
        ),
    ];
    if let Some(cutoff) = changed_since_epoch_nanos {
        let parameters = BTreeMap::from([("cutoff".to_string(), Value::Int(cutoff))]);
        queries.extend([
            pagerank_plan_route_query(
                "pagerank-plan-changed-memory-count",
                GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_COUNT_QUERY,
                parameters.clone(),
                true,
            ),
            pagerank_plan_route_query(
                "pagerank-plan-changed-entity-count",
                GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_COUNT_QUERY,
                parameters.clone(),
                true,
            ),
            pagerank_plan_route_query(
                "pagerank-plan-changed-mention-edge-count",
                GRAPH_PAGERANK_PLAN_CHANGED_MENTION_EDGE_COUNT_QUERY,
                parameters.clone(),
                false,
            ),
            pagerank_plan_route_query(
                "pagerank-plan-changed-entity-relation-count",
                GRAPH_PAGERANK_PLAN_CHANGED_ENTITY_RELATION_COUNT_QUERY,
                parameters.clone(),
                false,
            ),
            pagerank_plan_route_query(
                "pagerank-plan-changed-memory-relation-count",
                GRAPH_PAGERANK_PLAN_CHANGED_MEMORY_RELATION_COUNT_QUERY,
                parameters,
                false,
            ),
        ]);
    }
    RouteQuery {
        route: GRAPH_PAGERANK_PLAN_ROUTE.to_string(),
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

pub(crate) fn graph_orphans_route_query(limit: usize) -> Result<RouteQuery> {
    let limit = i64::try_from(limit).map_err(|_| {
        HawDBError::Semantic(
            "graph orphans route evidence limit exceeds supported range".to_string(),
        )
    })?;
    if limit <= 0 {
        return Err(HawDBError::Semantic(
            "graph orphans route evidence limit must be greater than zero".to_string(),
        ));
    }
    Ok(RouteQuery {
        route: GRAPH_ORPHANS_ROUTE.to_string(),
        shadow_compare_ready: false,
        primary_read_routing_enabled: true,
        primary_ready: false,
        blocker_codes: Vec::new(),
        queries: vec![RouteCypherQuery {
            name: "orphan-entity-relationship-exclusion".to_string(),
            query_family: Some("graph_traversal".to_string()),
            require_scan_pruning: false,
            require_pruned: false,
            cypher: GRAPH_ORPHAN_ENTITIES_QUERY.to_string(),
            parameters: BTreeMap::from([("limit".to_string(), Value::Int(limit))]),
        }],
    })
}
