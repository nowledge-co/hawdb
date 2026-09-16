//! Developer-facing input parsing for query runtime preflight.

use crate::query_runtime_preflight::{
    parse_query_runtime_preflight_probes, NowledgeQueryRuntimePreflightProbe,
};
use skein_core::{Result, SkeinError};
use std::path::Path;

#[derive(Debug, Clone, PartialEq)]
pub struct QueryRuntimePreflightCliInputs {
    pub require_ready: bool,
    pub database_path: String,
    pub probes: Vec<NowledgeQueryRuntimePreflightProbe>,
}

pub fn nowledge_query_runtime_preflight_usage() -> String {
    "nowledge-query-runtime-preflight requires [--require-ready] --probe-json <path> <database-path>; probe JSON may be a probes array or graph route query inventory"
        .to_string()
}

pub fn parse_query_runtime_preflight_cli_inputs(
    mut args: impl Iterator<Item = String>,
) -> Result<QueryRuntimePreflightCliInputs> {
    let mut require_ready = false;
    let mut probe_path = None;
    let mut database_path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => require_ready = true,
            "--probe-json" => probe_path = Some(next_arg(&mut args)?),
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(
                    nowledge_query_runtime_preflight_usage(),
                ));
            }
            value if database_path.replace(value.to_string()).is_none() => {}
            _ => {
                return Err(SkeinError::Semantic(
                    nowledge_query_runtime_preflight_usage(),
                ))
            }
        }
    }
    let probe_path =
        probe_path.ok_or_else(|| SkeinError::Semantic(nowledge_query_runtime_preflight_usage()))?;
    let database_path = database_path
        .ok_or_else(|| SkeinError::Semantic(nowledge_query_runtime_preflight_usage()))?;
    Ok(QueryRuntimePreflightCliInputs {
        require_ready,
        database_path,
        probes: parse_query_runtime_preflight_probes(&read_json_file(Path::new(&probe_path))?)?,
    })
}

fn next_arg(args: &mut impl Iterator<Item = String>) -> Result<String> {
    args.next()
        .ok_or_else(|| SkeinError::Semantic(nowledge_query_runtime_preflight_usage()))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read query runtime preflight JSON: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&content).map_err(|_| {
        SkeinError::Semantic(
            "failed to parse query runtime preflight JSON: invalid_json".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_preserves_probe_and_require_ready() {
        let root = std::env::temp_dir().join(format!(
            "skein_preflight_cli_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let probes = root.join("probes.json");
        std::fs::write(
            &probes,
            serde_json::json!({"probes": [{"name": "probe", "cypher": "RETURN 1"}]}).to_string(),
        )
        .unwrap();
        let inputs = parse_query_runtime_preflight_cli_inputs(
            [
                "--require-ready",
                "--probe-json",
                probes.to_str().unwrap(),
                "graph.db",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        assert!(inputs.require_ready);
        assert_eq!(inputs.database_path, "graph.db");
        assert_eq!(inputs.probes.len(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }
}
