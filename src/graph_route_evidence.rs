use crate::{
    NowledgeMemGraph, NowledgeMemGraphMode, NowledgeMemQueryReportOptions, Result, SkeinError,
};
use std::path::Path;

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
            "--require-ready" => require_ready = true,
            "--mode" => {
                mode =
                    parse_mode(&args.next().ok_or_else(|| {
                        SkeinError::Semantic(nowledge_graph_route_evidence_usage())
                    })?)?;
            }
            "--capture-physical-plan" => options.capture_physical_plan = true,
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

    let graph_path =
        graph_path.ok_or_else(|| SkeinError::Semantic(nowledge_graph_route_evidence_usage()))?;
    let route_query_path = route_query_path
        .ok_or_else(|| SkeinError::Semantic(nowledge_graph_route_evidence_usage()))?;
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

#[cfg(test)]
fn query_requirement_blockers(query: &RouteCypherQuery, report: &serde_json::Value) -> Vec<String> {
    skein_readiness::graph_route_catalog::query_requirement_blockers(query, report)
}

#[cfg(test)]
include!("graph_route_evidence_tests.rs");
