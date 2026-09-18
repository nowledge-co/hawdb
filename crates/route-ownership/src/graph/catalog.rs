// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub const NOWLEDGE_MEM_SEARCH_ROUTE: &str = "/graph/search";
pub const REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES: &[&str] = &[
    "/communities",
    "/communities/{community_id}",
    "/graph/overview",
    "/graph/sample",
    NOWLEDGE_MEM_SEARCH_ROUTE,
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
    "/sources/{source_id}",
    "/stats/entity-relations",
    "/stats/sources",
    "/stats/top-communities",
    "/entities",
    "/entities/{entity_id}/relationships",
    "/agent/evolves",
];
pub const NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION: &str =
    "nowledge-mem-graph-read-route-catalog-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemGraphReadRouteOwner {
    GraphRuntime,
    SearchRuntime,
    ReadBatchRuntime,
}

impl NowledgeMemGraphReadRouteOwner {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GraphRuntime => "graph_runtime",
            Self::SearchRuntime => "search_runtime",
            Self::ReadBatchRuntime => "read_batch_runtime",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemGraphReadRouteEvidenceKind {
    GraphRouteExecution,
    SearchCandidateShadow,
    ReadBatchRuntime,
}

impl NowledgeMemGraphReadRouteEvidenceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GraphRouteExecution => "graph_route_execution",
            Self::SearchCandidateShadow => "search_candidate_shadow",
            Self::ReadBatchRuntime => "read_batch_runtime",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemGraphReadRouteSpec {
    pub route: &'static str,
    pub owner: NowledgeMemGraphReadRouteOwner,
    pub required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind,
    pub stale_on_catalog_change: bool,
}

const fn graph_route(route: &'static str) -> NowledgeMemGraphReadRouteSpec {
    NowledgeMemGraphReadRouteSpec {
        route,
        owner: NowledgeMemGraphReadRouteOwner::GraphRuntime,
        required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind::GraphRouteExecution,
        stale_on_catalog_change: true,
    }
}

const fn search_route(route: &'static str) -> NowledgeMemGraphReadRouteSpec {
    NowledgeMemGraphReadRouteSpec {
        route,
        owner: NowledgeMemGraphReadRouteOwner::SearchRuntime,
        required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind::SearchCandidateShadow,
        stale_on_catalog_change: true,
    }
}

const fn read_batch_route(route: &'static str) -> NowledgeMemGraphReadRouteSpec {
    NowledgeMemGraphReadRouteSpec {
        route,
        owner: NowledgeMemGraphReadRouteOwner::ReadBatchRuntime,
        required_evidence_kind: NowledgeMemGraphReadRouteEvidenceKind::ReadBatchRuntime,
        stale_on_catalog_change: true,
    }
}

pub const NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS: &[NowledgeMemGraphReadRouteSpec] = &[
    read_batch_route("/communities"),
    read_batch_route("/communities/{community_id}"),
    graph_route("/graph/overview"),
    graph_route("/graph/sample"),
    search_route(NOWLEDGE_MEM_SEARCH_ROUTE),
    graph_route("/graph/explore"),
    graph_route("/graph/expand/{node_id}"),
    graph_route("/graph/live-preview"),
    graph_route("/graph/live-preview/{node_id}"),
    graph_route("/graph/community-members/{community_id}"),
    graph_route("/library/community/{community_id}/subgraph"),
    graph_route("/library/community/{community_id}/recent-memories"),
    graph_route("/library/community/{community_id}/related"),
    graph_route("/graph/analysis"),
    graph_route("/graph/augmentation/state"),
    graph_route("/graph/augmentation/pagerank/plan"),
    graph_route("/graph/node-details/{node_id}"),
    graph_route("/graph/orphans"),
    graph_route("/graph/shortest-path"),
    read_batch_route("/sources/{source_id}"),
    read_batch_route("/stats/entity-relations"),
    read_batch_route("/stats/sources"),
    read_batch_route("/stats/top-communities"),
    read_batch_route("/entities"),
    read_batch_route("/entities/{entity_id}/relationships"),
    read_batch_route("/agent/evolves"),
];

pub fn nowledge_mem_graph_read_route_spec(
    route: &str,
) -> Option<&'static NowledgeMemGraphReadRouteSpec> {
    NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS
        .iter()
        .find(|spec| spec.route == route)
}

pub fn nowledge_mem_graph_read_route_spec_json(
    spec: &NowledgeMemGraphReadRouteSpec,
) -> serde_json::Value {
    serde_json::json!({
        "route": spec.route,
        "owner": spec.owner.as_str(),
        "required_evidence_kind": spec.required_evidence_kind.as_str(),
        "stale_on_catalog_change": spec.stale_on_catalog_change,
    })
}

pub fn nowledge_mem_graph_read_route_specs_json() -> serde_json::Value {
    serde_json::json!(NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS
        .iter()
        .map(nowledge_mem_graph_read_route_spec_json)
        .collect::<Vec<_>>())
}

pub fn nowledge_mem_graph_read_route_catalog_digest() -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for spec in NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS {
        fnv1a_update(&mut hash, spec.route.as_bytes());
        fnv1a_update(&mut hash, spec.owner.as_str().as_bytes());
        fnv1a_update(&mut hash, spec.required_evidence_kind.as_str().as_bytes());
        fnv1a_update(
            &mut hash,
            if spec.stale_on_catalog_change {
                b"true"
            } else {
                b"false"
            },
        );
    }
    format!("fnv1a64:{hash:016x}")
}

fn fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    *hash ^= 0xff;
    *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
}

pub fn nowledge_mem_required_query_families_for_route(route: &str) -> &'static [&'static str] {
    match route {
        "/communities"
        | "/communities/{community_id}"
        | "/sources/{source_id}"
        | "/stats/entity-relations"
        | "/stats/sources"
        | "/stats/top-communities"
        | "/entities"
        | "/entities/{entity_id}/relationships"
        | "/agent/evolves" => &["label_stats_read"],
        NOWLEDGE_MEM_SEARCH_ROUTE => &["search_projection"],
        "/graph/overview"
        | "/graph/sample"
        | "/graph/live-preview"
        | "/graph/live-preview/{node_id}"
        | "/graph/community-members/{community_id}"
        | "/library/community/{community_id}/recent-memories"
        | "/graph/node-details/{node_id}" => &["memory_lookup"],
        "/graph/explore"
        | "/graph/expand/{node_id}"
        | "/library/community/{community_id}/subgraph"
        | "/library/community/{community_id}/related"
        | "/graph/orphans"
        | "/graph/shortest-path" => &["graph_traversal"],
        "/graph/analysis" | "/graph/augmentation/state" | "/graph/augmentation/pagerank/plan" => {
            &["projected_graph"]
        }
        _ => &[],
    }
}
