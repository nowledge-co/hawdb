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

use hawdb_qualification::{run_mixed_soak, MixedSoakConfig, MixedSoakError, MIXED_SOAK_PROTOCOL};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

fn main() -> ExitCode {
    match run() {
        Ok(ready) if ready => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(error) => {
            println!(
                "{}",
                serde_json::json!({
                    "protocol": MIXED_SOAK_PROTOCOL,
                    "evidence_kind": "synthetic_scheduled_soak",
                    "production_eligible": false,
                    "ready": false,
                    "blocker_codes": ["run_failed"],
                    "errors": [error.to_string()],
                })
            );
            eprintln!("hawdb-soak: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, MixedSoakError> {
    let Some(mut config) = parse_config(std::env::args().skip(1))? else {
        println!("{}", usage());
        return Ok(true);
    };
    if config.dataset_id.is_empty() {
        config.dataset_id = "scheduled-synthetic-v1".to_string();
    }
    let report = run_mixed_soak(&config)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report.json())
            .map_err(|error| MixedSoakError::new(format!("failed to encode report: {error}")))?
    );
    Ok(report.ready)
}

fn parse_config(
    args: impl IntoIterator<Item = String>,
) -> Result<Option<MixedSoakConfig>, MixedSoakError> {
    let mut path = None;
    let mut revision = None;
    let mut overrides = Vec::new();
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if matches!(argument.as_str(), "--help" | "-h") {
            return Ok(None);
        }
        let value = args
            .next()
            .ok_or_else(|| MixedSoakError::new(format!("missing value for {argument}")))?;
        match argument.as_str() {
            "--path" => path = Some(PathBuf::from(value)),
            "--revision" => revision = Some(value),
            _ => overrides.push((argument, value)),
        }
    }
    let path = path.ok_or_else(|| MixedSoakError::new("--path is required"))?;
    let revision = revision.ok_or_else(|| MixedSoakError::new("--revision is required"))?;
    let mut config = MixedSoakConfig::scheduled(path, revision);
    for (argument, value) in overrides {
        match argument.as_str() {
            "--dataset-id" => config.dataset_id = value,
            "--node-count" => config.node_count = parse_usize(&argument, &value)?,
            "--payload-bytes" => config.payload_bytes = parse_usize(&argument, &value)?,
            "--foreground-workers" => {
                config.foreground_workers = parse_usize(&argument, &value)?;
            }
            "--foreground-rounds" => {
                config.foreground_rounds_per_worker = parse_usize(&argument, &value)?;
            }
            "--background-rounds" => {
                config.background_rounds = parse_usize(&argument, &value)?;
            }
            "--segment-cache-bytes" => {
                config.segment_cache_bytes = parse_u64(&argument, &value)?;
            }
            "--runtime-memory-bytes" => {
                config.runtime_memory_budget_bytes = parse_u64(&argument, &value)?;
            }
            "--result-budget-bytes" => {
                config.result_budget_bytes = parse_u64(&argument, &value)?;
            }
            "--blocking-operator-bytes" => {
                config.blocking_operator_bytes = parse_usize(&argument, &value)?;
            }
            "--task-timeout-seconds" => {
                config.task_timeout = Duration::from_secs(parse_u64(&argument, &value)?);
            }
            _ => {
                return Err(MixedSoakError::new(format!(
                    "unknown argument '{argument}'\n{}",
                    usage()
                )));
            }
        }
    }
    Ok(Some(config))
}

fn parse_usize(name: &str, value: &str) -> Result<usize, MixedSoakError> {
    value
        .parse()
        .map_err(|_| MixedSoakError::new(format!("{name} must be an unsigned integer")))
}

fn parse_u64(name: &str, value: &str) -> Result<u64, MixedSoakError> {
    value
        .parse()
        .map_err(|_| MixedSoakError::new(format!("{name} must be an unsigned 64-bit integer")))
}

fn usage() -> &'static str {
    "usage: hawdb-soak --path <path> --revision <sha> [--dataset-id <id>] \
     [--node-count <usize>] [--payload-bytes <usize>] \
     [--foreground-workers <usize>] [--foreground-rounds <usize>] \
     [--background-rounds <usize>] [--segment-cache-bytes <u64>] \
     [--runtime-memory-bytes <u64>] [--result-budget-bytes <u64>] \
     [--blocking-operator-bytes <usize>] [--task-timeout-seconds <u64>]"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_required_identity_and_resource_overrides() {
        let config = parse_config([
            "--path".to_string(),
            "soak.db".to_string(),
            "--revision".to_string(),
            "abc123".to_string(),
            "--node-count".to_string(),
            "64".to_string(),
            "--runtime-memory-bytes".to_string(),
            "1048576".to_string(),
        ])
        .unwrap()
        .unwrap();

        assert_eq!(config.database_path, PathBuf::from("soak.db"));
        assert_eq!(config.source_revision, "abc123");
        assert_eq!(config.node_count, 64);
        assert_eq!(config.runtime_memory_budget_bytes, 1_048_576);
    }
}
