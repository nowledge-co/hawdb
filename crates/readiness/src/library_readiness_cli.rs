//! Developer-facing input parsing for the library-readiness preflight.
//!
//! Opening the embedded database and executing the bounded probe remain with
//! the root Skein facade. This module only decodes bounded, redacted inputs.

use crate::bounded_read_evidence::{NowledgeMemGraphMode, NowledgeMemRouteReadinessSummary};
use skein_core::{Result, SkeinError, Value};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemLibraryReadinessProbe {
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemLibraryReadinessCliInputs {
    pub require_ready: bool,
    pub mode: NowledgeMemGraphMode,
    pub graph_path: String,
    pub search_projection_path: Option<String>,
    pub bounded_read_probe: Option<NowledgeMemLibraryReadinessProbe>,
    pub bounded_read_evidence: Option<serde_json::Value>,
    pub covered_routes: Vec<String>,
    pub graph_route_readiness: Option<NowledgeMemRouteReadinessSummary>,
    pub replacement_readiness_by_query_family: Option<serde_json::Value>,
    pub search_projection_evidence: Option<serde_json::Value>,
    pub primary_search_projection_probe: Option<serde_json::Value>,
    pub search_projection_shadow_evidence: Option<serde_json::Value>,
    pub search_candidate_shadow_evidence: Option<serde_json::Value>,
    pub active_embedding_model: Option<String>,
    pub active_embedding_dimension: Option<usize>,
}

pub fn nowledge_mem_library_readiness_usage() -> String {
    "nowledge-mem-library-readiness requires [--require-ready] [--mode shadow_read_only|writable_cutover] [--search-projection <path>] [--bounded-probe-json <path>] [--bounded-read-evidence-json <path>] [--covered-routes-json <path>] [--graph-route-readiness-json <path>] [--query-family-evidence-json <path>] [--search-projection-evidence-json <path>] [--primary-search-projection-probe-json <path>] [--search-projection-shadow-evidence-json <path>] [--search-candidate-shadow-evidence-json <path>] [--active-model <model>] [--active-dimension <n>] <graph-db>"
        .to_string()
}

pub fn parse_nowledge_mem_library_readiness_inputs(
    mut args: impl Iterator<Item = String>,
) -> Result<NowledgeMemLibraryReadinessCliInputs> {
    let mut require_ready = false;
    let mut mode = NowledgeMemGraphMode::ShadowReadOnly;
    let mut search_projection_path = None;
    let mut bounded_read_probe = None;
    let mut bounded_read_evidence = None;
    let mut covered_routes = Vec::new();
    let mut graph_route_readiness = None;
    let mut replacement_readiness_by_query_family = None;
    let mut search_projection_evidence = None;
    let mut primary_search_projection_probe = None;
    let mut search_projection_shadow_evidence = None;
    let mut search_candidate_shadow_evidence = None;
    let mut active_embedding_model = None;
    let mut active_embedding_dimension = None;
    let mut graph_path = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => require_ready = true,
            "--mode" => mode = parse_mem_library_readiness_mode(&next_arg(&mut args)?)?,
            "--search-projection" => search_projection_path = Some(next_arg(&mut args)?),
            "--bounded-probe-json" => {
                bounded_read_probe = Some(parse_bounded_probe_json(&read_json_arg(&mut args)?)?);
            }
            "--bounded-read-evidence-json" => {
                bounded_read_evidence = Some(read_json_arg(&mut args)?);
            }
            "--covered-routes-json" => {
                covered_routes.extend(parse_mem_library_covered_routes_json(&read_json_arg(
                    &mut args,
                )?)?);
            }
            "--graph-route-readiness-json" => {
                graph_route_readiness = Some(parse_mem_library_graph_route_readiness_json(
                    &read_json_arg(&mut args)?,
                )?);
            }
            "--query-family-evidence-json" => {
                replacement_readiness_by_query_family = Some(read_json_arg(&mut args)?);
            }
            "--search-projection-evidence-json" => {
                search_projection_evidence = Some(read_json_arg(&mut args)?);
            }
            "--primary-search-projection-probe-json" => {
                primary_search_projection_probe = Some(read_json_arg(&mut args)?);
            }
            "--search-projection-shadow-evidence-json" => {
                search_projection_shadow_evidence = Some(read_json_arg(&mut args)?);
            }
            "--search-candidate-shadow-evidence-json" => {
                search_candidate_shadow_evidence = Some(read_json_arg(&mut args)?);
            }
            "--active-model" => active_embedding_model = Some(next_arg(&mut args)?),
            "--active-dimension" => {
                active_embedding_dimension = Some(parse_positive_usize(
                    "--active-dimension",
                    &next_arg(&mut args)?,
                )?);
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(nowledge_mem_library_readiness_usage()));
            }
            value => {
                if graph_path.replace(value.to_string()).is_some() {
                    return Err(SkeinError::Semantic(nowledge_mem_library_readiness_usage()));
                }
            }
        }
    }

    let graph_path =
        graph_path.ok_or_else(|| SkeinError::Semantic(nowledge_mem_library_readiness_usage()))?;
    Ok(NowledgeMemLibraryReadinessCliInputs {
        require_ready,
        mode,
        graph_path,
        search_projection_path,
        bounded_read_probe,
        bounded_read_evidence,
        covered_routes,
        graph_route_readiness,
        replacement_readiness_by_query_family,
        search_projection_evidence,
        primary_search_projection_probe,
        search_projection_shadow_evidence,
        search_candidate_shadow_evidence,
        active_embedding_model,
        active_embedding_dimension,
    })
}

pub fn parse_mem_library_readiness_mode(raw: &str) -> Result<NowledgeMemGraphMode> {
    match raw {
        "shadow_read_only" => Ok(NowledgeMemGraphMode::ShadowReadOnly),
        "writable_cutover" => Ok(NowledgeMemGraphMode::WritableCutover),
        _ => Err(SkeinError::Semantic(format!(
            "invalid nowledge mem library readiness mode: {raw}"
        ))),
    }
}

pub fn parse_bounded_probe_json(
    value: &serde_json::Value,
) -> Result<NowledgeMemLibraryReadinessProbe> {
    let object = value
        .as_object()
        .ok_or_else(|| SkeinError::Semantic("bounded probe JSON must be an object".to_string()))?;
    let cypher = object
        .get("cypher")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SkeinError::Semantic("bounded probe JSON field 'cypher' must be a string".to_string())
        })?
        .to_string();
    let parameters = object
        .get("parameters")
        .map(parse_parameters_json)
        .transpose()?
        .unwrap_or_default();
    Ok(NowledgeMemLibraryReadinessProbe { cypher, parameters })
}

pub fn parse_parameters_json(value: &serde_json::Value) -> Result<BTreeMap<String, Value>> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("bounded probe JSON field 'parameters' must be an object".to_string())
    })?;
    object
        .iter()
        .map(|(key, value)| Ok((key.clone(), value_from_json(value)?)))
        .collect()
}

pub fn value_from_json(value: &serde_json::Value) -> Result<Value> {
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
                    "unsupported JSON number in bounded probe parameters: {value}"
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

pub fn parse_mem_library_covered_routes_json(value: &serde_json::Value) -> Result<Vec<String>> {
    if value.is_array() {
        return required_string_array_value(value, "covered routes JSON");
    }
    required_string_array(value, "covered_routes")
}

pub fn parse_mem_library_graph_route_readiness_json(
    value: &serde_json::Value,
) -> Result<NowledgeMemRouteReadinessSummary> {
    Ok(NowledgeMemRouteReadinessSummary {
        route_primary_ready: required_bool(value, "route_primary_ready")?,
        primary_ready_routes: required_string_array(value, "primary_ready_routes")?,
        route_query_plan_evidence_ready: required_bool(value, "route_query_plan_evidence_ready")?,
        route_query_profile_evidence_ready: required_bool(
            value,
            "route_query_profile_evidence_ready",
        )?,
        route_query_api_behavior_evidence_ready: required_bool(
            value,
            "route_query_api_behavior_evidence_ready",
        )?,
        relationship_property_pruning_required_count: required_u64(
            value,
            "relationship_property_pruning_required_count",
        )?,
        relationship_property_pruning_report_count: required_u64(
            value,
            "relationship_property_pruning_report_count",
        )?,
        route_relationship_property_pruning_evidence_ready: required_bool(
            value,
            "route_relationship_property_pruning_evidence_ready",
        )?,
    })
}

fn next_arg(args: &mut impl Iterator<Item = String>) -> Result<String> {
    args.next()
        .ok_or_else(|| SkeinError::Semantic(nowledge_mem_library_readiness_usage()))
}

fn read_json_arg(args: &mut impl Iterator<Item = String>) -> Result<serde_json::Value> {
    let input_path = next_arg(args)?;
    read_json_file(Path::new(&input_path))
}

fn required_string_array(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let items = value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Semantic(format!(
                "readiness JSON field '{field}' must be a string array"
            ))
        })?;
    required_string_array_items(items, field)
}

fn required_string_array_value(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let items = value.as_array().ok_or_else(|| {
        SkeinError::Semantic(format!(
            "readiness JSON field '{field}' must be a string array"
        ))
    })?;
    required_string_array_items(items, field)
}

fn required_string_array_items(items: &[serde_json::Value], field: &str) -> Result<Vec<String>> {
    items
        .iter()
        .map(|item| {
            item.as_str().map(str::to_string).ok_or_else(|| {
                SkeinError::Semantic(format!(
                    "readiness JSON field '{field}' must be a string array"
                ))
            })
        })
        .collect()
}

fn required_bool(value: &serde_json::Value, field: &str) -> Result<bool> {
    value
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| {
            SkeinError::Semantic(format!("readiness JSON field '{field}' must be a boolean"))
        })
}

fn required_u64(value: &serde_json::Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            SkeinError::Semantic(format!("readiness JSON field '{field}' must be an integer"))
        })
}

fn parse_positive_usize(flag: &str, value: &str) -> Result<usize> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| SkeinError::Semantic(format!("{flag} must be a positive integer")))?;
    if parsed == 0 {
        return Err(SkeinError::Semantic(format!(
            "{flag} must be a positive integer"
        )));
    }
    Ok(parsed)
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read nowledge mem library readiness JSON: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&content).map_err(|_| {
        SkeinError::Semantic(
            "failed to parse nowledge mem library readiness JSON: invalid_json".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_preserves_typed_inputs_and_file_boundaries() {
        let root = std::env::temp_dir().join(format!(
            "skein_library_readiness_cli_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&root).unwrap();
        let probe_path = root.join("probe.json");
        std::fs::write(
            &probe_path,
            serde_json::json!({"cypher": "RETURN $id", "parameters": {"id": "memory"}}).to_string(),
        )
        .unwrap();

        let inputs = parse_nowledge_mem_library_readiness_inputs(
            [
                "--require-ready",
                "--mode",
                "writable_cutover",
                "--bounded-probe-json",
                probe_path.to_str().unwrap(),
                "--active-model",
                "test-model",
                "--active-dimension",
                "384",
                "graph.db",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert!(inputs.require_ready);
        assert_eq!(inputs.mode, NowledgeMemGraphMode::WritableCutover);
        assert_eq!(inputs.graph_path, "graph.db");
        assert_eq!(inputs.active_embedding_model.as_deref(), Some("test-model"));
        assert_eq!(inputs.active_embedding_dimension, Some(384));
        assert_eq!(
            inputs.bounded_read_probe,
            Some(NowledgeMemLibraryReadinessProbe {
                cypher: "RETURN $id".to_string(),
                parameters: BTreeMap::from([(
                    "id".to_string(),
                    Value::String("memory".to_string())
                )]),
            }),
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn parser_rejects_incomplete_or_invalid_control_inputs() {
        for args in [
            vec!["--mode", "invalid", "graph.db"],
            vec!["--active-dimension", "0", "graph.db"],
            vec!["--unknown", "graph.db"],
            vec![],
        ] {
            assert!(parse_nowledge_mem_library_readiness_inputs(
                args.into_iter().map(str::to_string)
            )
            .is_err());
        }
    }
}
