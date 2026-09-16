//! Developer-facing input parsing for graph route execution evidence.

use crate::bounded_read_evidence::NowledgeMemGraphMode;
use crate::graph_route_catalog::{
    parse_route_parity_evidence, parse_route_query_inventory, RouteParityEvidence, RouteQuery,
};
use skein_core::{Result, SkeinError};
use std::path::Path;

#[derive(Debug, Clone, PartialEq)]
pub struct GraphRouteEvidenceCliInputs {
    pub require_ready: bool,
    pub mode: NowledgeMemGraphMode,
    pub capture_physical_plan: bool,
    pub slow_log_threshold_micros: Option<u128>,
    pub route_parity: Option<RouteParityEvidence>,
    pub graph_path: String,
    pub route_queries: Vec<RouteQuery>,
}

pub fn nowledge_graph_route_evidence_usage() -> String {
    "nowledge-graph-route-evidence requires [--require-ready] [--mode shadow_read_only|writable_cutover] [--capture-physical-plan] [--slow-log-threshold-micros <n>] [--route-parity-json <path>] <graph-db> <route-query-json>".to_string()
}

pub fn parse_graph_route_evidence_cli_inputs(
    mut args: impl Iterator<Item = String>,
) -> Result<GraphRouteEvidenceCliInputs> {
    let mut require_ready = false;
    let mut mode = NowledgeMemGraphMode::ShadowReadOnly;
    let mut capture_physical_plan = false;
    let mut slow_log_threshold_micros = None;
    let mut route_parity = None;
    let mut graph_path = None;
    let mut route_query_path = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => require_ready = true,
            "--mode" => mode = parse_mode(&next_arg(&mut args)?)?,
            "--capture-physical-plan" => capture_physical_plan = true,
            "--slow-log-threshold-micros" => {
                slow_log_threshold_micros =
                    Some(next_arg(&mut args)?.parse::<u128>().map_err(|_| {
                        SkeinError::Semantic(
                            "--slow-log-threshold-micros requires a non-negative integer"
                                .to_string(),
                        )
                    })?);
            }
            "--route-parity-json" => {
                route_parity = Some(parse_route_parity_evidence(&read_json_arg(&mut args)?)?);
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(nowledge_graph_route_evidence_usage()));
            }
            value if graph_path.is_none() => graph_path = Some(value.to_string()),
            value if route_query_path.replace(value.to_string()).is_none() => {}
            _ => return Err(SkeinError::Semantic(nowledge_graph_route_evidence_usage())),
        }
    }

    let graph_path =
        graph_path.ok_or_else(|| SkeinError::Semantic(nowledge_graph_route_evidence_usage()))?;
    let route_query_path = route_query_path
        .ok_or_else(|| SkeinError::Semantic(nowledge_graph_route_evidence_usage()))?;
    Ok(GraphRouteEvidenceCliInputs {
        require_ready,
        mode,
        capture_physical_plan,
        slow_log_threshold_micros,
        route_parity,
        graph_path,
        route_queries: parse_route_query_inventory(&read_json_file(Path::new(&route_query_path))?)?,
    })
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

fn next_arg(args: &mut impl Iterator<Item = String>) -> Result<String> {
    args.next()
        .ok_or_else(|| SkeinError::Semantic(nowledge_graph_route_evidence_usage()))
}

fn read_json_arg(args: &mut impl Iterator<Item = String>) -> Result<serde_json::Value> {
    let input_path = next_arg(args)?;
    read_json_file(Path::new(&input_path))
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
mod tests {
    use super::*;

    #[test]
    fn parser_preserves_options_and_route_inventory() {
        let root = std::env::temp_dir().join(format!(
            "skein_graph_route_cli_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&root).unwrap();
        let routes = root.join("routes.json");
        std::fs::write(&routes, serde_json::json!({"routes": []}).to_string()).unwrap();
        let inputs = parse_graph_route_evidence_cli_inputs(
            [
                "--require-ready",
                "--mode",
                "writable_cutover",
                "--capture-physical-plan",
                "--slow-log-threshold-micros",
                "10",
                "graph.db",
                routes.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        assert!(inputs.require_ready);
        assert_eq!(inputs.mode, NowledgeMemGraphMode::WritableCutover);
        assert!(inputs.capture_physical_plan);
        assert_eq!(inputs.slow_log_threshold_micros, Some(10));
        assert!(inputs.route_queries.is_empty());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn parser_rejects_invalid_or_incomplete_arguments() {
        for args in [
            vec![],
            vec!["--mode", "invalid", "graph", "routes"],
            vec!["graph", "routes", "extra"],
        ] {
            assert!(
                parse_graph_route_evidence_cli_inputs(args.into_iter().map(str::to_string))
                    .is_err()
            );
        }
    }
}
