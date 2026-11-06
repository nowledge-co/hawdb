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

use crate::{EmbeddedGraph, EmbeddedQueryReportOptions, Result};
pub use hawdb_readiness::graph_route_evidence_cli::nowledge_graph_route_evidence_usage;
use hawdb_readiness::graph_route_evidence_cli::parse_graph_route_evidence_cli_inputs;

mod queries;
pub(crate) use queries::{
    graph_augmentation_state_route_query, graph_community_members_route_query,
    graph_community_recent_memories_route_query, graph_community_subgraph_route_query,
    graph_node_details_route_query, graph_orphans_route_query, graph_overview_route_query,
    graph_pagerank_plan_route_query, graph_sample_route_query,
};

pub use hawdb_readiness::graph_route_catalog::{
    parse_route_parity_evidence, parse_route_query_inventory, RouteCypherQuery,
    RouteParityEvidence, RouteParityEvidenceRoute, RouteQuery,
    NMEM_GRAPH_ROUTE_PARITY_EVIDENCE_PROTOCOL,
};

pub fn run_nowledge_graph_route_evidence(
    args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let inputs = parse_graph_route_evidence_cli_inputs(args)?;
    let mut graph = EmbeddedGraph::open(inputs.graph_path, inputs.mode)?;
    Ok((
        nowledge_graph_route_evidence_json(
            &mut graph,
            &inputs.route_queries,
            EmbeddedQueryReportOptions {
                capture_physical_plan: inputs.capture_physical_plan,
                slow_log_threshold_micros: inputs.slow_log_threshold_micros,
            },
            inputs.route_parity.as_ref(),
        ),
        inputs.require_ready,
    ))
}

pub fn nowledge_graph_route_evidence_json(
    graph: &mut EmbeddedGraph,
    route_queries: &[RouteQuery],
    options: EmbeddedQueryReportOptions,
    route_parity: Option<&RouteParityEvidence>,
) -> serde_json::Value {
    hawdb_readiness::graph_route_catalog::graph_route_evidence_json(
        graph.mode().as_str(),
        route_queries,
        route_parity,
        |query| {
            graph
                .query_request_with_report(
                    crate::QueryRequest::new(&query.cypher)
                        .with_params(&query.parameters)
                        .with_output_limits(queries::ROUTE_STATEMENT_OUTPUT_LIMITS),
                    options,
                )
                .map(|output| output.report.json())
        },
    )
}

#[cfg(test)]
fn query_requirement_blockers(query: &RouteCypherQuery, report: &serde_json::Value) -> Vec<String> {
    hawdb_readiness::graph_route_catalog::query_requirement_blockers(query, report)
}

#[cfg(test)]
include!("graph_route_evidence_tests.rs");
