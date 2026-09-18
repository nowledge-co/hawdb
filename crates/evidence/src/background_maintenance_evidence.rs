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

use crate::inventory::background_maintenance_evidence_health;
use hawdb_core::{HawDBError, Result};
use std::path::Path;

pub fn nowledge_background_maintenance_evidence_usage() -> String {
    "nowledge-background-maintenance-evidence requires [--require-ready] [--optional] <background-maintenance-json>".to_string()
}

pub fn run_nowledge_background_maintenance_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut required = true;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--optional" => {
                required = false;
            }
            path => {
                if args.next().is_some() {
                    return Err(HawDBError::Semantic(
                        nowledge_background_maintenance_evidence_usage(),
                    ));
                }
                let summary = read_json_file(Path::new(path))?;
                return Ok((
                    nowledge_background_maintenance_evidence_json(&summary, required),
                    require_ready,
                ));
            }
        }
    }
    Err(HawDBError::Semantic(
        nowledge_background_maintenance_evidence_usage(),
    ))
}

pub fn nowledge_background_maintenance_evidence_json(
    summary: &serde_json::Value,
    required: bool,
) -> serde_json::Value {
    let health = background_maintenance_evidence_health(Some(summary), required);
    let mut object = serde_json::Map::new();
    insert_json(
        &mut object,
        "protocol",
        "hawdb-nowledge-background-maintenance-evidence-v1",
    );
    insert_json(&mut object, "required", health.required);
    insert_json(&mut object, "present", health.present);
    insert_json(&mut object, "ready", health.ready);
    insert_json(&mut object, "protocol_matches", health.protocol_matches);
    insert_json(&mut object, "total_candidates", health.total_candidates);
    insert_json(&mut object, "ranked_count", health.ranked_count);
    insert_json(
        &mut object,
        "executable_search_projection_graph_delta_count",
        health.executable_search_projection_graph_delta_count,
    );
    insert_json(
        &mut object,
        "admitted_search_projection_graph_delta_count",
        health.admitted_search_projection_graph_delta_count,
    );
    insert_json(
        &mut object,
        "deferred_search_projection_graph_delta_count",
        health.deferred_search_projection_graph_delta_count,
    );
    insert_json(
        &mut object,
        "rejected_search_projection_graph_delta_count",
        health.rejected_search_projection_graph_delta_count,
    );
    insert_json(
        &mut object,
        "executable_search_projection_graph_delta_operations",
        health.executable_search_projection_graph_delta_operations,
    );
    insert_json(
        &mut object,
        "admitted_search_projection_graph_delta_operations",
        health.admitted_search_projection_graph_delta_operations,
    );
    insert_json(
        &mut object,
        "max_search_projection_graph_delta_complete_through_graph_commit_epoch",
        health.max_search_projection_graph_delta_complete_through_graph_commit_epoch,
    );
    insert_json(
        &mut object,
        "foreground_admission_probe_ready",
        health.foreground_admission_probe_ready,
    );
    insert_json(
        &mut object,
        "foreground_admission_probe_admission",
        health.foreground_admission_probe_admission_name.as_deref(),
    );
    insert_json(
        &mut object,
        "memory_pressure_ready",
        health.memory_pressure_ready,
    );
    insert_json(
        &mut object,
        "memory_budget_bytes",
        health.memory_budget_bytes,
    );
    insert_json(
        &mut object,
        "estimated_memory_bytes",
        health.estimated_memory_bytes,
    );
    insert_json(&mut object, "qos_snapshot_ready", health.qos_snapshot_ready);
    insert_json(
        &mut object,
        "qos_snapshot_foreground_admitted",
        health.qos_snapshot_foreground_admitted,
    );
    insert_json(
        &mut object,
        "qos_snapshot_background_bounded",
        health.qos_snapshot_background_bounded,
    );
    insert_json(
        &mut object,
        "qos_snapshot_total_background_over_budget",
        health.qos_snapshot_total_background_over_budget,
    );
    insert_json(
        &mut object,
        "qos_snapshot_blocker_codes",
        &health.qos_snapshot_blocker_codes,
    );
    insert_json(&mut object, "slow_query_ready", health.slow_query_ready);
    insert_json(
        &mut object,
        "slow_query_record_count",
        health.slow_query_record_count,
    );
    insert_json(
        &mut object,
        "slow_query_capacity",
        health.slow_query_capacity,
    );
    insert_json(
        &mut object,
        "slow_query_redaction_ready",
        health.slow_query_redaction_ready,
    );
    insert_json(
        &mut object,
        "foreground_ranked_count",
        health.foreground_ranked_count,
    );
    insert_json(
        &mut object,
        "unknown_admission_count",
        health.unknown_admission_count,
    );
    insert_json(&mut object, "blocker_codes", &health.blocker_codes);
    insert_json(&mut object, "blockers", &health.blockers);
    insert_json(
        &mut object,
        "background_maintenance_required",
        health.required,
    );
    insert_json(
        &mut object,
        "background_maintenance_present",
        health.present,
    );
    insert_json(&mut object, "background_maintenance_ready", health.ready);
    insert_json(
        &mut object,
        "background_maintenance_protocol_matches",
        health.protocol_matches,
    );
    insert_json(
        &mut object,
        "background_maintenance_executable_search_projection_graph_delta_count",
        health.executable_search_projection_graph_delta_count,
    );
    insert_json(
        &mut object,
        "background_maintenance_admitted_search_projection_graph_delta_count",
        health.admitted_search_projection_graph_delta_count,
    );
    insert_json(
        &mut object,
        "background_maintenance_deferred_search_projection_graph_delta_count",
        health.deferred_search_projection_graph_delta_count,
    );
    insert_json(
        &mut object,
        "background_maintenance_rejected_search_projection_graph_delta_count",
        health.rejected_search_projection_graph_delta_count,
    );
    insert_json(
        &mut object,
        "background_maintenance_executable_search_projection_graph_delta_operations",
        health.executable_search_projection_graph_delta_operations,
    );
    insert_json(
        &mut object,
        "background_maintenance_admitted_search_projection_graph_delta_operations",
        health.admitted_search_projection_graph_delta_operations,
    );
    insert_json(
        &mut object,
        "background_maintenance_max_search_projection_graph_delta_complete_through_graph_commit_epoch",
        health.max_search_projection_graph_delta_complete_through_graph_commit_epoch,
    );
    insert_json(
        &mut object,
        "background_maintenance_foreground_admission_probe_ready",
        health.foreground_admission_probe_ready,
    );
    insert_json(
        &mut object,
        "background_maintenance_foreground_admission_probe_admission",
        health.foreground_admission_probe_admission_name.as_deref(),
    );
    insert_json(
        &mut object,
        "background_maintenance_memory_pressure_ready",
        health.memory_pressure_ready,
    );
    insert_json(
        &mut object,
        "background_maintenance_memory_budget_bytes",
        health.memory_budget_bytes,
    );
    insert_json(
        &mut object,
        "background_maintenance_estimated_memory_bytes",
        health.estimated_memory_bytes,
    );
    insert_json(
        &mut object,
        "background_maintenance_qos_snapshot_ready",
        health.qos_snapshot_ready,
    );
    insert_json(
        &mut object,
        "background_maintenance_qos_snapshot_foreground_admitted",
        health.qos_snapshot_foreground_admitted,
    );
    insert_json(
        &mut object,
        "background_maintenance_qos_snapshot_background_bounded",
        health.qos_snapshot_background_bounded,
    );
    insert_json(
        &mut object,
        "background_maintenance_qos_snapshot_total_background_over_budget",
        health.qos_snapshot_total_background_over_budget,
    );
    insert_json(
        &mut object,
        "background_maintenance_qos_snapshot_blocker_codes",
        &health.qos_snapshot_blocker_codes,
    );
    insert_json(
        &mut object,
        "background_maintenance_slow_query_ready",
        health.slow_query_ready,
    );
    insert_json(
        &mut object,
        "background_maintenance_slow_query_record_count",
        health.slow_query_record_count,
    );
    insert_json(
        &mut object,
        "background_maintenance_slow_query_capacity",
        health.slow_query_capacity,
    );
    insert_json(
        &mut object,
        "background_maintenance_slow_query_redaction_ready",
        health.slow_query_redaction_ready,
    );
    insert_json(
        &mut object,
        "background_maintenance_blocker_codes",
        &health.blocker_codes,
    );
    insert_json(
        &mut object,
        "background_maintenance_blockers",
        &health.blockers,
    );
    serde_json::Value::Object(object)
}

fn insert_json<T: serde::Serialize>(
    object: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: T,
) {
    object.insert(
        key.to_string(),
        serde_json::to_value(value).expect("background maintenance evidence must serialize"),
    );
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        HawDBError::Execution(format!(
            "failed to read background maintenance JSON: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&content).map_err(|_| {
        HawDBError::Semantic(
            "failed to parse background maintenance JSON: invalid_json".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::run_nowledge_background_maintenance_evidence;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn background_maintenance_evidence_command_accepts_ready_summary() {
        let path = unique_test_file("background_maintenance_ready");
        std::fs::write(&path, ready_summary().to_string()).unwrap();

        let (evidence, require_ready) = run_nowledge_background_maintenance_evidence(
            ["--require-ready", path.to_str().unwrap()]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap();

        assert!(require_ready);
        assert_eq!(
            evidence["protocol"],
            "hawdb-nowledge-background-maintenance-evidence-v1"
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["background_maintenance_required"], true);
        assert_eq!(evidence["background_maintenance_ready"], true);
        assert_eq!(
            evidence["background_maintenance_executable_search_projection_graph_delta_count"],
            1
        );
        assert_eq!(
            evidence["background_maintenance_admitted_search_projection_graph_delta_operations"],
            3
        );
        assert_eq!(evidence["foreground_admission_probe_ready"], true);
        assert_eq!(evidence["foreground_admission_probe_admission"], "admit");
        assert_eq!(evidence["qos_snapshot_ready"], true);
        assert_eq!(evidence["qos_snapshot_background_bounded"], true);
        assert_eq!(evidence["slow_query_ready"], true);
        assert_eq!(evidence["slow_query_redaction_ready"], true);
        assert_eq!(
            evidence["background_maintenance_qos_snapshot_blocker_codes"],
            serde_json::json!([])
        );
        assert_eq!(
            evidence["background_maintenance_blocker_codes"],
            serde_json::json!([])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn background_maintenance_parse_errors_are_redacted_by_default() {
        let path = unique_test_file("background_maintenance_secret_path_do_not_emit");
        std::fs::write(
            &path,
            "{ \"artifact_path\": \"secret-background-path-do-not-emit\", \"unterminated\": ",
        )
        .unwrap();

        let error = super::read_json_file(&path).unwrap_err().to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse background maintenance JSON: invalid_json"
        );
        assert!(!error.contains("background_maintenance_secret_path_do_not_emit"));
        assert!(!error.contains("secret-background-path-do-not-emit"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn background_maintenance_evidence_command_fails_closed_for_protocol_mismatch() {
        let path = unique_test_file("background_maintenance_protocol_mismatch");
        let mut summary = ready_summary();
        summary["protocol"] = serde_json::json!("unexpected-background-maintenance-report");
        std::fs::write(&path, summary.to_string()).unwrap();

        let (evidence, _) = run_nowledge_background_maintenance_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["background_maintenance_blocker_codes"],
            serde_json::json!(["protocol_mismatch"])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn background_maintenance_evidence_command_fails_closed_for_memory_pressure() {
        let path = unique_test_file("background_maintenance_memory_pressure");
        let mut summary = ready_summary();
        summary["memory_pressure"] = serde_json::json!({
            "ready": false,
            "budget_bytes": 4096,
            "estimated_bytes": 8192
        });
        std::fs::write(&path, summary.to_string()).unwrap();

        let (evidence, _) = run_nowledge_background_maintenance_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["memory_pressure_ready"], false);
        assert_eq!(evidence["memory_budget_bytes"], 4096);
        assert_eq!(evidence["estimated_memory_bytes"], 8192);
        assert_eq!(
            evidence["background_maintenance_blocker_codes"],
            serde_json::json!(["memory_budget_exceeded"])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn background_maintenance_evidence_command_fails_closed_for_slow_query_redaction() {
        let path = unique_test_file("background_maintenance_slow_query_redaction");
        let mut summary = ready_summary();
        summary["slow_query"]["redaction"]["query_text_copied"] = serde_json::json!(true);
        std::fs::write(&path, summary.to_string()).unwrap();

        let (evidence, _) = run_nowledge_background_maintenance_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["slow_query_redaction_ready"], false);
        assert_eq!(
            evidence["background_maintenance_blocker_codes"],
            serde_json::json!(["slow_query_redaction_not_ready"])
        );
        std::fs::remove_file(path).unwrap();
    }

    fn ready_summary() -> serde_json::Value {
        serde_json::json!({
            "protocol": "hawdb-background-maintenance-report",
            "total_candidates": 1,
            "foreground_admission_probe_ready": true,
            "foreground_admission_probe_admission": "admit",
            "qos_snapshot": {
                "ready": true,
                "foreground_admitted": true,
                "background_enabled": true,
                "background_bounded": true,
                "running_background_operations": 0,
                "max_total_background_operations": 4096,
                "remaining_total_background_operations": 4096,
                "total_background_over_budget": false,
                "blocker_codes": []
            },
            "slow_query": {
                "ready": true,
                "capacity": 8,
                "record_count": 1,
                "redaction": {
                    "query_text_copied": false,
                    "parameters_copied": false,
                    "local_paths_copied": false
                }
            },
            "memory_pressure": {
                "ready": true,
                "budget_bytes": 4096,
                "estimated_bytes": 1024
            },
            "ranked": [
                {
                    "kind": "search_projection_graph_delta",
                    "name": "search_projection_graph_delta",
                    "work_class": "projection",
                    "priority": "background",
                    "admission": "admit",
                    "has_executable_search_projection_graph_delta": true,
                    "search_projection_graph_delta_operation_count": 3,
                    "search_projection_graph_delta_complete_through_graph_commit_epoch": 42
                }
            ]
        })
    }

    fn unique_test_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("hawdb_{name}_{}_{nanos}.json", std::process::id()))
    }
}
