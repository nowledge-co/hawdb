use skein::{
    NowledgeMemGraph, NowledgeMemGraphMode, NowledgeMemReadOptions, Result, SkeinError, Value,
};
use std::collections::BTreeMap;
use std::path::Path;

const NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL: &str = "nmem-graph-route-evidence-v1";

pub fn nowledge_graph_route_evidence_usage() -> String {
    "nowledge-graph-route-evidence requires [--mode shadow_read_only|writable_cutover] [--capture-physical-plan] [--slow-log-threshold-micros <n>] <graph-db> <route-query-json>".to_string()
}

pub fn run_nowledge_graph_route_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<serde_json::Value> {
    let mut mode = NowledgeMemGraphMode::ShadowReadOnly;
    let options = NowledgeMemReadOptions::default();
    let mut graph_path = None;
    let mut route_query_path = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                mode =
                    parse_mode(&args.next().ok_or_else(|| {
                        SkeinError::Semantic(nowledge_graph_route_evidence_usage())
                    })?)?;
            }
            "--capture-physical-plan" => {
                // Current submodule line exposes bounded read reports, not physical plan capture.
            }
            "--slow-log-threshold-micros" => {
                let raw = args
                    .next()
                    .ok_or_else(|| SkeinError::Semantic(nowledge_graph_route_evidence_usage()))?;
                let _ = raw.parse::<u128>().map_err(|_| {
                    SkeinError::Semantic(
                        "--slow-log-threshold-micros requires a non-negative integer".to_string(),
                    )
                })?;
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
    Ok(nowledge_graph_route_evidence_json(
        &mut graph,
        &route_queries,
        options,
    ))
}

fn nowledge_graph_route_evidence_json(
    graph: &mut NowledgeMemGraph,
    route_queries: &[RouteQuery],
    options: NowledgeMemReadOptions,
) -> serde_json::Value {
    let routes = route_queries
        .iter()
        .map(|route| route.query_runtime_evidence(graph, &options))
        .collect::<Vec<_>>();
    serde_json::json!({
        "protocol": NMEM_GRAPH_ROUTE_EVIDENCE_PROTOCOL,
        "mode": graph.mode().as_str(),
        "route_count": routes.len(),
        "routes": routes,
    })
}

#[derive(Debug, Clone, PartialEq)]
struct RouteQuery {
    route: String,
    shadow_compare_ready: bool,
    primary_read_routing_enabled: bool,
    primary_ready: bool,
    blocker_codes: Vec<String>,
    queries: Vec<RouteCypherQuery>,
}

impl RouteQuery {
    fn query_runtime_evidence(
        &self,
        graph: &mut NowledgeMemGraph,
        options: &NowledgeMemReadOptions,
    ) -> serde_json::Value {
        let mut query_reports = Vec::with_capacity(self.queries.len());
        let mut query_errors = Vec::new();
        let mut blocker_codes = self.blocker_codes.clone();
        if self.queries.is_empty() {
            blocker_codes.push("missing_route_queries".to_string());
        }
        for query in &self.queries {
            match graph.read_query_with_params(&query.cypher, &query.parameters, &options) {
                Ok(output) => query_reports.push(output.report.json()),
                Err(error) => {
                    blocker_codes.push("query_runtime_execution_failed".to_string());
                    query_errors.push(serde_json::json!({
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
            "shadow_compare_ready": self.shadow_compare_ready,
            "primary_read_routing_enabled": self.primary_read_routing_enabled,
            "primary_ready": (self.primary_ready || self.primary_read_routing_enabled)
                && query_runtime_succeeded,
            "query_reports": query_reports,
            "query_errors": query_errors,
            "blocker_codes": blocker_codes,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RouteCypherQuery {
    cypher: String,
    parameters: BTreeMap<String, Value>,
}

fn parse_route_query_inventory(value: &serde_json::Value) -> Result<Vec<RouteQuery>> {
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
        .map(parse_route_cypher_query)
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

fn parse_route_cypher_query(value: &serde_json::Value) -> Result<RouteCypherQuery> {
    let cypher = required_string(value, "cypher")?.to_string();
    if cypher.trim().is_empty() {
        return Err(SkeinError::Semantic(
            "graph route query field 'cypher' must be non-empty".to_string(),
        ));
    }
    let parameters = value
        .get("parameters")
        .map(parse_parameters_json)
        .transpose()?
        .unwrap_or_default();
    Ok(RouteCypherQuery { cypher, parameters })
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
    serde_json::from_str(&raw).map_err(|error| {
        SkeinError::Semantic(format!("failed to parse graph route query JSON: {error}"))
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
        SkeinError::Storage(_) => "storage",
        SkeinError::Execution(_) => "execution",
    }
}

#[cfg(test)]
mod tests {
    use super::{nowledge_graph_route_evidence_json, parse_route_query_inventory};
    use skein::{Database, NowledgeMemGraph, NowledgeMemGraphMode};

    #[test]
    fn route_evidence_runs_queries_through_nowledge_runtime() {
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

        let evidence =
            nowledge_graph_route_evidence_json(&mut graph, &route_queries, Default::default());

        assert_eq!(evidence["protocol"], "nmem-graph-route-evidence-v1");
        assert_eq!(evidence["routes"][0]["route"], "/graph/overview");
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
            "skein-nowledge-mem-read-report"
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

        let evidence =
            nowledge_graph_route_evidence_json(&mut graph, &route_queries, Default::default());

        assert_eq!(evidence["routes"][0]["primary_read_routing_enabled"], true);
        assert_eq!(evidence["routes"][0]["primary_ready"], true);
        assert!(!evidence["routes"][0]["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "graph_route_execution_evidence_missing"));
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
                            "cypher": "MATCH (m:Memory) RETURN unknown.property AS value"
                        }
                    ]
                }
            ]
        }))
        .unwrap();

        let evidence =
            nowledge_graph_route_evidence_json(&mut graph, &route_queries, Default::default());

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
    }
}
