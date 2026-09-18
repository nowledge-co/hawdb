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

#[path = "shared/qualification_input.rs"]
mod qualification_input;

#[path = "content_store_memory_qualification/plan.rs"]
mod plan;

use hawdb_qualification::{
    run_production_content_store_memory_qualification,
    PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION_PROTOCOL,
};
use plan::read_plan;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run(std::env::args().skip(1)) {
        Ok(Some(report)) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report.json())
                    .expect("Content Store memory qualification report must serialize")
            );
            if report.ready {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Ok(None) => {
            println!("{}", usage());
            ExitCode::SUCCESS
        }
        Err(error) => {
            println!(
                "{}",
                serde_json::json!({
                    "protocol": PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION_PROTOCOL,
                    "evidence_kind": "production_content_store_memory_profiles",
                    "production_eligible": true,
                    "ready": false,
                    "blocker_codes": ["qualification_input_invalid"],
                    "errors": ["qualification_failed"],
                })
            );
            eprintln!("hawdb-content-store-memory-qualification: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(
    args: impl IntoIterator<Item = String>,
) -> Result<Option<hawdb_qualification::ProductionContentStoreMemoryQualificationReport>, String> {
    let Some((storage_path, plan_path)) = parse_args(args)? else {
        return Ok(None);
    };
    let config = read_plan(&plan_path)?.into_config(storage_path)?;
    run_production_content_store_memory_qualification(config)
        .map(Some)
        .map_err(|error| error.to_string())
}

fn parse_args(
    args: impl IntoIterator<Item = String>,
) -> Result<Option<(PathBuf, PathBuf)>, String> {
    let mut storage_path = None;
    let mut plan_path = None;
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        if matches!(argument.as_str(), "--help" | "-h") {
            return Ok(None);
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {argument}"))?;
        match argument.as_str() {
            "--storage-path" if storage_path.is_none() => {
                storage_path = Some(PathBuf::from(value));
            }
            "--plan-json" if plan_path.is_none() => {
                plan_path = Some(PathBuf::from(value));
            }
            "--storage-path" | "--plan-json" => {
                return Err(format!("duplicate argument '{argument}'"));
            }
            _ => return Err(format!("unknown argument '{argument}'\n{}", usage())),
        }
    }
    Ok(Some((
        storage_path.ok_or_else(|| "--storage-path is required".to_string())?,
        plan_path.ok_or_else(|| "--plan-json is required".to_string())?,
    )))
}

fn usage() -> &'static str {
    "usage: hawdb-content-store-memory-qualification \
     --storage-path <existing-production-storage-path> --plan-json <path>"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argument_parser_requires_both_paths() {
        assert_eq!(parse_args(["--help".to_string()]).unwrap(), None);
        assert!(
            parse_args(["--storage-path".to_string(), "storage".to_string(),])
                .unwrap_err()
                .contains("--plan-json")
        );
        assert!(parse_args([
            "--storage-path".to_string(),
            "storage".to_string(),
            "--plan-json".to_string(),
            "plan.json".to_string(),
            "--storage-path".to_string(),
            "again".to_string(),
        ])
        .unwrap_err()
        .contains("duplicate"));
    }

    #[test]
    fn plan_protocol_is_stable() {
        assert_eq!(
            plan::CONTENT_STORE_MEMORY_QUALIFICATION_PLAN_PROTOCOL,
            "hawdb-production-content-store-memory-plan-v1"
        );
    }
}
