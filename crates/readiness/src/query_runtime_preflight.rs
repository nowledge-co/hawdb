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

//! Storage-neutral query-runtime preflight protocol models.

use crate::json_parse::{optional_query_name, parse_parameters_json};
use hawdb_core::{HawDBError, Result, Value};
use std::collections::BTreeMap;

/// One bounded query probe supplied to the embedded query-runtime preflight.
///
/// The host retains database opening and probe execution. This model only
/// describes the requested query and the evidence requirements.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryRuntimePreflightProbe {
    pub name: String,
    pub route: Option<String>,
    pub query_family: Option<String>,
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
    pub require_scan_pruning: bool,
    pub require_pruned: bool,
    pub min_scan_pruning_reports: usize,
    pub max_output_rows: Option<usize>,
}

impl NowledgeQueryRuntimePreflightProbe {
    pub fn new(name: impl Into<String>, cypher: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            route: None,
            query_family: None,
            cypher: cypher.into(),
            parameters: BTreeMap::new(),
            require_scan_pruning: false,
            require_pruned: false,
            min_scan_pruning_reports: 1,
            max_output_rows: None,
        }
    }

    pub fn with_route(mut self, route: impl Into<String>) -> Self {
        self.route = Some(route.into());
        self
    }

    pub fn with_query_family(mut self, query_family: impl Into<String>) -> Self {
        self.query_family = Some(query_family.into());
        self
    }

    pub fn with_parameters(mut self, parameters: BTreeMap<String, Value>) -> Self {
        self.parameters = parameters;
        self
    }

    pub fn require_scan_pruning(mut self, min_scan_pruning_reports: usize) -> Self {
        self.require_scan_pruning = true;
        self.min_scan_pruning_reports = min_scan_pruning_reports;
        self
    }

    pub fn require_pruned(mut self) -> Self {
        self.require_pruned = true;
        self
    }

    pub fn with_max_output_rows(mut self, max_output_rows: usize) -> Self {
        self.max_output_rows = Some(max_output_rows);
        self
    }
}

/// Parses standalone probes, probe bundles, or graph-route inventories.
#[doc(hidden)]
pub fn parse_query_runtime_preflight_probes(
    value: &serde_json::Value,
) -> Result<Vec<NowledgeQueryRuntimePreflightProbe>> {
    if let Some(array) = value.as_array() {
        return array.iter().map(parse_probe).collect();
    }
    if let Some(array) = value.get("probes").and_then(serde_json::Value::as_array) {
        return array.iter().map(parse_probe).collect();
    }
    if let Some(array) = value.get("routes").and_then(serde_json::Value::as_array) {
        return parse_route_query_inventory_probes(array);
    }
    Ok(vec![parse_probe(value)?])
}

fn parse_route_query_inventory_probes(
    routes: &[serde_json::Value],
) -> Result<Vec<NowledgeQueryRuntimePreflightProbe>> {
    routes
        .iter()
        .flat_map(|route| match parse_route_query_probes(route) {
            Ok(probes) => probes.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(error) => vec![Err(error)],
        })
        .collect()
}

fn parse_route_query_probes(
    value: &serde_json::Value,
) -> Result<Vec<NowledgeQueryRuntimePreflightProbe>> {
    let object = value.as_object().ok_or_else(|| {
        HawDBError::Semantic("graph route query inventory route must be a JSON object".to_string())
    })?;
    let route = object
        .get("route")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            HawDBError::Semantic(
                "graph route query inventory field 'route' must be a string".to_string(),
            )
        })?
        .to_string();
    if route.trim().is_empty() {
        return Err(HawDBError::Semantic(
            "graph route query inventory route must be non-empty".to_string(),
        ));
    }
    let queries = object
        .get("queries")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            HawDBError::Semantic(
                "graph route query inventory field 'queries' must be an array".to_string(),
            )
        })?;
    queries
        .iter()
        .enumerate()
        .map(|(query_index, query)| parse_route_query_probe(&route, query, query_index))
        .collect()
}

fn parse_route_query_probe(
    route: &str,
    value: &serde_json::Value,
    query_index: usize,
) -> Result<NowledgeQueryRuntimePreflightProbe> {
    let object = value.as_object().ok_or_else(|| {
        HawDBError::Semantic("graph route query inventory query must be a JSON object".to_string())
    })?;
    let name =
        optional_query_name(value).unwrap_or_else(|| format!("{route}:query-{}", query_index + 1));
    if name.trim().is_empty() {
        return Err(HawDBError::Semantic(
            "graph route query inventory query name must be non-empty when provided".to_string(),
        ));
    }
    let cypher = object
        .get("cypher")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            HawDBError::Semantic(
                "graph route query inventory field 'cypher' must be a string".to_string(),
            )
        })?
        .to_string();
    if cypher.trim().is_empty() {
        return Err(HawDBError::Semantic(
            "graph route query inventory field 'cypher' must be non-empty".to_string(),
        ));
    }
    let parameters = object
        .get("parameters")
        .map(|value| parse_parameters_json(value, "query runtime probe"))
        .transpose()?
        .unwrap_or_default();
    let min_scan_pruning_reports = optional_usize(object, "min_scan_pruning_reports")?.unwrap_or(1);
    Ok(NowledgeQueryRuntimePreflightProbe {
        name,
        route: Some(route.to_string()),
        query_family: optional_string(object, "query_family")?,
        cypher,
        parameters,
        require_scan_pruning: optional_bool(object, "require_scan_pruning")?.unwrap_or(false),
        require_pruned: optional_bool(object, "require_pruned")?.unwrap_or(false),
        min_scan_pruning_reports,
        max_output_rows: optional_usize(object, "max_output_rows")?,
    })
}

fn parse_probe(value: &serde_json::Value) -> Result<NowledgeQueryRuntimePreflightProbe> {
    let object = value.as_object().ok_or_else(|| {
        HawDBError::Semantic("query runtime probe must be a JSON object".to_string())
    })?;
    let name = object
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unnamed")
        .to_string();
    let cypher = object
        .get("cypher")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            HawDBError::Semantic("query runtime probe field 'cypher' must be a string".to_string())
        })?
        .to_string();
    let parameters = object
        .get("parameters")
        .map(|value| parse_parameters_json(value, "query runtime probe"))
        .transpose()?
        .unwrap_or_default();
    let min_scan_pruning_reports = optional_usize(object, "min_scan_pruning_reports")?.unwrap_or(1);
    Ok(NowledgeQueryRuntimePreflightProbe {
        name,
        route: optional_string(object, "route")?,
        query_family: optional_string(object, "query_family")?,
        cypher,
        parameters,
        require_scan_pruning: optional_bool(object, "require_scan_pruning")?.unwrap_or(false),
        require_pruned: optional_bool(object, "require_pruned")?.unwrap_or(false),
        min_scan_pruning_reports,
        max_output_rows: optional_usize(object, "max_output_rows")?,
    })
}

fn optional_string(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<String>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|value| Some(value.to_string()))
        .ok_or_else(|| {
            HawDBError::Semantic(format!(
                "query runtime probe field '{field}' must be a string"
            ))
        })
}

fn optional_bool(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<bool>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value.as_bool().map(Some).ok_or_else(|| {
        HawDBError::Semantic(format!(
            "query runtime probe field '{field}' must be a boolean"
        ))
    })
}

fn optional_usize(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<usize>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let raw = value.as_u64().ok_or_else(|| {
        HawDBError::Semantic(format!(
            "query runtime probe field '{field}' must be an integer"
        ))
    })?;
    usize::try_from(raw).map(Some).map_err(|_| {
        HawDBError::Semantic(format!(
            "query runtime probe field '{field}' exceeds usize range"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_builder_preserves_protocol_defaults_and_optional_evidence() {
        let probe = NowledgeQueryRuntimePreflightProbe::new("bounded-read", "MATCH (m) RETURN m")
            .with_route("/graph/overview")
            .with_query_family("graph_overview")
            .with_parameters(BTreeMap::from([("limit".to_string(), Value::Int(1))]))
            .require_scan_pruning(2)
            .require_pruned()
            .with_max_output_rows(1);

        assert_eq!(probe.name, "bounded-read");
        assert_eq!(probe.route.as_deref(), Some("/graph/overview"));
        assert_eq!(probe.query_family.as_deref(), Some("graph_overview"));
        assert_eq!(probe.parameters["limit"], Value::Int(1));
        assert!(probe.require_scan_pruning);
        assert!(probe.require_pruned);
        assert_eq!(probe.min_scan_pruning_reports, 2);
        assert_eq!(probe.max_output_rows, Some(1));
    }

    #[test]
    fn parser_preserves_route_inventory_probe_metadata() {
        let probes = parse_query_runtime_preflight_probes(&serde_json::json!({
            "routes": [{
                "route": "/graph/overview",
                "queries": [{
                    "query_id": "overview-by-kind",
                    "query_family": "graph_overview",
                    "cypher": "MATCH (m:Memory) WHERE m.kind = $kind RETURN m",
                    "parameters": {
                        "kind": "note",
                        "limits": [1, 2],
                        "flags": {"strict": true}
                    },
                    "require_scan_pruning": true,
                    "require_pruned": true,
                    "min_scan_pruning_reports": 2,
                    "max_output_rows": 3
                }]
            }]
        }))
        .unwrap();

        assert_eq!(probes.len(), 1);
        let probe = &probes[0];
        assert_eq!(probe.name, "overview-by-kind");
        assert_eq!(probe.route.as_deref(), Some("/graph/overview"));
        assert_eq!(probe.query_family.as_deref(), Some("graph_overview"));
        assert_eq!(probe.parameters["kind"], Value::String("note".to_string()));
        assert_eq!(
            probe.parameters["limits"],
            Value::List(vec![Value::Int(1), Value::Int(2)])
        );
        assert_eq!(
            probe.parameters["flags"],
            Value::Map(BTreeMap::from([("strict".to_string(), Value::Bool(true))]))
        );
        assert!(probe.require_scan_pruning);
        assert!(probe.require_pruned);
        assert_eq!(probe.min_scan_pruning_reports, 2);
        assert_eq!(probe.max_output_rows, Some(3));
    }

    #[test]
    fn parser_rejects_invalid_route_inventory_shape() {
        let error = parse_query_runtime_preflight_probes(&serde_json::json!({
            "routes": [{
                "route": " ",
                "queries": []
            }]
        }))
        .unwrap_err();

        assert!(error.to_string().contains("route must be non-empty"));
    }
}
