use crate::{NowledgeMemGraph, NowledgeMemQueryReportOptions, Result};
pub use skein_readiness::graph_route_evidence_cli::nowledge_graph_route_evidence_usage;
use skein_readiness::graph_route_evidence_cli::parse_graph_route_evidence_cli_inputs;

pub use skein_readiness::graph_route_catalog::{
    nowledge_mem_graph_augmentation_state_route_query,
    nowledge_mem_graph_community_members_route_query,
    nowledge_mem_graph_community_recent_memories_route_query,
    nowledge_mem_graph_community_subgraph_route_query, nowledge_mem_graph_node_details_route_query,
    nowledge_mem_graph_orphans_route_query, nowledge_mem_graph_overview_route_query,
    nowledge_mem_graph_pagerank_plan_route_query, nowledge_mem_graph_sample_route_query,
    parse_route_parity_evidence, parse_route_query_inventory, RouteCypherQuery,
    RouteParityEvidence, RouteParityEvidenceRoute, RouteQuery,
    NMEM_GRAPH_ROUTE_PARITY_EVIDENCE_PROTOCOL,
};

pub fn run_nowledge_graph_route_evidence(
    args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let inputs = parse_graph_route_evidence_cli_inputs(args)?;
    let mut graph = NowledgeMemGraph::open(inputs.graph_path, inputs.mode)?;
    Ok((
        nowledge_graph_route_evidence_json(
            &mut graph,
            &inputs.route_queries,
            NowledgeMemQueryReportOptions {
                capture_physical_plan: inputs.capture_physical_plan,
                slow_log_threshold_micros: inputs.slow_log_threshold_micros,
            },
            inputs.route_parity.as_ref(),
        ),
        inputs.require_ready,
    ))
}

pub fn nowledge_graph_route_evidence_json(
    graph: &mut NowledgeMemGraph,
    route_queries: &[RouteQuery],
    options: NowledgeMemQueryReportOptions,
    route_parity: Option<&RouteParityEvidence>,
) -> serde_json::Value {
    skein_readiness::graph_route_catalog::graph_route_evidence_json(
        graph.mode().as_str(),
        route_queries,
        route_parity,
        |query| {
            graph
                .query_with_params_with_report_options(&query.cypher, &query.parameters, options)
                .map(|output| output.report.json())
        },
    )
}

#[cfg(test)]
fn query_requirement_blockers(query: &RouteCypherQuery, report: &serde_json::Value) -> Vec<String> {
    skein_readiness::graph_route_catalog::query_requirement_blockers(query, report)
}

#[cfg(test)]
include!("graph_route_evidence_tests.rs");
