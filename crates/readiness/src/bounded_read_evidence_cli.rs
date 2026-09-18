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

use crate::bounded_read_evidence::{
    nowledge_mem_bounded_read_evidence_json_with_route_readiness, NowledgeMemGraphMode,
    NowledgeMemReadReport, NowledgeMemRouteReadinessSummary,
};
use hawdb_core::{HawDBError, Result};
use std::path::Path;

pub fn nowledge_bounded_read_evidence_usage() -> String {
    "nowledge-bounded-read-evidence requires [--require-ready] [--covered-route <route> ...] [--covered-routes-json <path>] [--graph-route-readiness-json <path>] <read-report-json>".to_string()
}

pub fn run_nowledge_bounded_read_evidence(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut covered_routes = Vec::new();
    let mut graph_route_readiness = None;
    let mut report_path = None;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--covered-route" => {
                covered_routes.push(
                    args.next().ok_or_else(|| {
                        HawDBError::Semantic(nowledge_bounded_read_evidence_usage())
                    })?,
                );
            }
            "--covered-routes-json" => {
                let path = args
                    .next()
                    .ok_or_else(|| HawDBError::Semantic(nowledge_bounded_read_evidence_usage()))?;
                covered_routes.extend(parse_covered_routes_json(&read_json_file(Path::new(
                    &path,
                ))?)?);
            }
            "--graph-route-readiness-json" => {
                let path = args
                    .next()
                    .ok_or_else(|| HawDBError::Semantic(nowledge_bounded_read_evidence_usage()))?;
                graph_route_readiness = Some(parse_graph_route_readiness_json(&read_json_file(
                    Path::new(&path),
                )?)?);
            }
            value if value.starts_with("--") => {
                return Err(HawDBError::Semantic(nowledge_bounded_read_evidence_usage()));
            }
            path => {
                if report_path.replace(path.to_string()).is_some() {
                    return Err(HawDBError::Semantic(nowledge_bounded_read_evidence_usage()));
                }
            }
        }
    }
    let Some(report_path) = report_path else {
        return Err(HawDBError::Semantic(nowledge_bounded_read_evidence_usage()));
    };
    let report = parse_read_report_json(&read_json_file(Path::new(&report_path))?)?;
    if covered_routes.is_empty()
        && let Some(readiness) = graph_route_readiness.as_ref()
    {
        covered_routes = readiness.primary_ready_routes.clone();
    }
    Ok((
        nowledge_mem_bounded_read_evidence_json_with_route_readiness(
            &report,
            &covered_routes,
            graph_route_readiness.as_ref(),
        ),
        require_ready,
    ))
}

pub fn parse_covered_routes_json(value: &serde_json::Value) -> Result<Vec<String>> {
    if value.is_array() {
        return required_string_array_value(value, "covered routes JSON");
    }
    required_string_array(value, "covered_routes")
}

pub fn parse_read_report_json(value: &serde_json::Value) -> Result<NowledgeMemReadReport> {
    Ok(NowledgeMemReadReport {
        protocol: required_string(value, "protocol")?.to_string(),
        mode: parse_mode(required_string(value, "mode")?)?,
        row_count: required_usize(value, "row_count")?,
        max_rows: optional_usize(value, "max_rows")?,
        execution_row_cap: optional_usize(value, "execution_row_cap")?,
        estimated_payload_bytes: required_usize(value, "estimated_payload_bytes")?,
        max_estimated_payload_bytes: optional_usize(value, "max_estimated_payload_bytes")?,
        row_budget_exceeded: required_bool(value, "row_budget_exceeded")?,
        payload_budget_exceeded: required_bool(value, "payload_budget_exceeded")?,
        row_limit_enforced_before_output: required_bool(value, "row_limit_enforced_before_output")?,
        operator_row_cap_enabled: required_bool(value, "operator_row_cap_enabled")?,
        blocking_operator_count: required_usize(value, "blocking_operator_count")?,
        blocking_operator_kinds: required_string_array(value, "blocking_operator_kinds")?,
        blocking_operator_memory_reports: required_blocking_operator_memory_reports(value)?,
        intermediate_rows: optional_usize(value, "intermediate_rows")?.unwrap_or_default(),
        intermediate_payload_bytes: optional_usize(value, "intermediate_payload_bytes")?
            .unwrap_or_default(),
        output_payload_bytes: optional_usize(value, "output_payload_bytes")?.unwrap_or_default(),
        steady_resident_bytes: optional_u64(value, "steady_resident_bytes")?,
        peak_resident_bytes: optional_u64(value, "peak_resident_bytes")?,
        total_page_faults: optional_u64(value, "total_page_faults")?,
        minor_page_faults: optional_u64(value, "minor_page_faults")?,
        major_page_faults: optional_u64(value, "major_page_faults")?,
        streaming: required_bool(value, "streaming")?,
    })
}

fn required_blocking_operator_memory_reports(
    value: &serde_json::Value,
) -> Result<Vec<hawdb_executor::BlockingOperatorMemoryReport>> {
    value
        .get("blocking_operator_memory_reports")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid_field("blocking_operator_memory_reports", "array"))?
        .iter()
        .map(|report| {
            Ok(hawdb_executor::BlockingOperatorMemoryReport {
                operator: required_string(report, "operator")?.to_string(),
                budget_bytes: required_usize(report, "budget_bytes")?,
                peak_tracked_bytes: required_usize(report, "peak_tracked_bytes")?,
                input_rows: required_usize(report, "input_rows")?,
                max_spill_bytes: required_u64(report, "max_spill_bytes")?,
                max_spill_runs: required_usize(report, "max_spill_runs")?,
                spilled_bytes: required_u64(report, "spilled_bytes")?,
                spill_run_count: required_usize(report, "spill_run_count")?,
                spilled_rows: required_usize(report, "spilled_rows")?,
            })
        })
        .collect()
}

pub fn parse_graph_route_readiness_json(
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

fn parse_mode(value: &str) -> Result<NowledgeMemGraphMode> {
    match value {
        "shadow_read_only" => Ok(NowledgeMemGraphMode::ShadowReadOnly),
        "writable_cutover" => Ok(NowledgeMemGraphMode::WritableCutover),
        _ => Err(HawDBError::Semantic(format!(
            "invalid read report mode: {value}"
        ))),
    }
}

fn required_string<'a>(value: &'a serde_json::Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid_field(field, "string"))
}

fn required_bool(value: &serde_json::Value, field: &str) -> Result<bool> {
    value
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| invalid_field(field, "boolean"))
}

fn required_usize(value: &serde_json::Value, field: &str) -> Result<usize> {
    optional_usize(value, field)?.ok_or_else(|| invalid_field(field, "integer"))
}

fn required_u64(value: &serde_json::Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| invalid_field(field, "integer"))
}

fn optional_usize(value: &serde_json::Value, field: &str) -> Result<Option<usize>> {
    let Some(value) = value.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let raw = value
        .as_u64()
        .ok_or_else(|| invalid_field(field, "integer"))?;
    usize::try_from(raw).map(Some).map_err(|_| {
        HawDBError::Semantic(format!("read report field '{field}' exceeds usize range"))
    })
}

fn optional_u64(value: &serde_json::Value, field: &str) -> Result<Option<u64>> {
    let Some(value) = value.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| invalid_field(field, "integer"))
}

fn required_string_array(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let items = value
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| invalid_field(field, "string array"))?;
    required_string_array_items(items, field)
}

fn required_string_array_value(value: &serde_json::Value, field: &str) -> Result<Vec<String>> {
    let items = value
        .as_array()
        .ok_or_else(|| invalid_field(field, "string array"))?;
    required_string_array_items(items, field)
}

fn required_string_array_items(items: &[serde_json::Value], field: &str) -> Result<Vec<String>> {
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| invalid_field(field, "string array"))
        })
        .collect()
}

fn invalid_field(field: &str, expected: &str) -> HawDBError {
    HawDBError::Semantic(format!("read report field '{field}' must be a {expected}"))
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|_| {
        HawDBError::Execution("failed to read bounded read report JSON: io_error".to_string())
    })?;
    serde_json::from_str(&content).map_err(|_| {
        HawDBError::Semantic("failed to parse bounded read report JSON: invalid_json".to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::run_nowledge_bounded_read_evidence;
    use hawdb_route_ownership::graph::REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEST_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn bounded_read_evidence_command_accepts_ready_read_report() {
        let report_path = unique_test_file("bounded_read_ready");
        let route_readiness_path = unique_test_file("graph_route_readiness");
        std::fs::write(&report_path, ready_report().to_string()).unwrap();
        std::fs::write(
            &route_readiness_path,
            ready_graph_route_readiness().to_string(),
        )
        .unwrap();

        let mut args = vec!["--require-ready".to_string()];
        for route in REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES {
            args.extend(["--covered-route".to_string(), (*route).to_string()]);
        }
        args.extend([
            "--graph-route-readiness-json".to_string(),
            route_readiness_path.to_string_lossy().to_string(),
        ]);
        args.push(report_path.to_string_lossy().to_string());

        let (evidence, require_ready) =
            run_nowledge_bounded_read_evidence(args.into_iter()).unwrap();

        assert!(require_ready);
        assert_eq!(
            evidence["protocol"],
            "hawdb-nowledge-mem-bounded-read-evidence-v2"
        );
        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["mode"], "shadow_read_only");
        assert_eq!(evidence["execution_row_cap"], 513);
        assert_eq!(evidence["route_primary_ready"], true);
        assert_eq!(evidence["route_query_plan_evidence_ready"], true);
        assert_eq!(evidence["route_query_profile_evidence_ready"], true);
        assert_eq!(
            evidence["route_relationship_property_pruning_evidence_ready"],
            true
        );
        assert_eq!(
            evidence["covered_routes"].as_array().unwrap().len(),
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(evidence["missing_covered_routes"], serde_json::json!([]));
        assert_eq!(evidence["blocker_codes"], serde_json::json!([]));
        std::fs::remove_file(report_path).unwrap();
        std::fs::remove_file(route_readiness_path).unwrap();
    }

    #[test]
    fn bounded_read_input_errors_are_redacted_by_default() {
        let missing_path = unique_test_file("bounded_read_secret_path_do_not_emit");

        let read_error = super::read_json_file(&missing_path)
            .unwrap_err()
            .to_string();

        assert_eq!(
            read_error,
            "execution error: failed to read bounded read report JSON: io_error"
        );
        assert!(!read_error.contains("bounded_read_secret_path_do_not_emit"));

        let parse_path = unique_test_file("bounded_read_parse_secret_path_do_not_emit");
        std::fs::write(
            &parse_path,
            "{ \"query_text\": \"secret-bounded-query-do-not-emit\", \"unterminated\": ",
        )
        .unwrap();

        let parse_error = super::read_json_file(&parse_path).unwrap_err().to_string();

        assert_eq!(
            parse_error,
            "semantic error: failed to parse bounded read report JSON: invalid_json"
        );
        assert!(!parse_error.contains("bounded_read_parse_secret_path_do_not_emit"));
        assert!(!parse_error.contains("secret-bounded-query-do-not-emit"));
        std::fs::remove_file(parse_path).unwrap();
    }

    #[test]
    fn bounded_read_evidence_command_accepts_covered_routes_json() {
        let report_path = unique_test_file("bounded_read_ready");
        let routes_path = unique_test_file("bounded_read_routes");
        let route_readiness_path = unique_test_file("graph_route_readiness");
        std::fs::write(&report_path, ready_report().to_string()).unwrap();
        std::fs::write(
            &routes_path,
            serde_json::json!({
                "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &route_readiness_path,
            ready_graph_route_readiness().to_string(),
        )
        .unwrap();

        let (evidence, _) = run_nowledge_bounded_read_evidence(
            [
                "--covered-routes-json",
                routes_path.to_str().unwrap(),
                "--graph-route-readiness-json",
                route_readiness_path.to_str().unwrap(),
                report_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], true);
        assert_eq!(evidence["missing_covered_routes"], serde_json::json!([]));
        std::fs::remove_file(report_path).unwrap();
        std::fs::remove_file(routes_path).unwrap();
        std::fs::remove_file(route_readiness_path).unwrap();
    }

    #[test]
    fn bounded_read_evidence_command_uses_graph_route_readiness_routes_by_default() {
        let report_path = unique_test_file("bounded_read_ready");
        let route_readiness_path = unique_test_file("graph_route_readiness");
        std::fs::write(&report_path, ready_report().to_string()).unwrap();
        std::fs::write(
            &route_readiness_path,
            ready_graph_route_readiness().to_string(),
        )
        .unwrap();

        let (evidence, _) = run_nowledge_bounded_read_evidence(
            [
                "--graph-route-readiness-json",
                route_readiness_path.to_str().unwrap(),
                report_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], true);
        assert_eq!(
            evidence["covered_routes"].as_array().unwrap().len(),
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(evidence["missing_covered_routes"], serde_json::json!([]));
        std::fs::remove_file(report_path).unwrap();
        std::fs::remove_file(route_readiness_path).unwrap();
    }

    #[test]
    fn bounded_read_evidence_command_fails_closed_for_incomplete_report() {
        let path = unique_test_file("bounded_read_incomplete");
        let mut report = ready_report();
        report.as_object_mut().unwrap().remove("execution_row_cap");
        std::fs::write(&path, report.to_string()).unwrap();

        let (evidence, _) = run_nowledge_bounded_read_evidence(
            [path.to_str().unwrap()].into_iter().map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!([
                "missing_execution_row_cap",
                "missing_covered_routes",
                "graph_route_readiness_missing"
            ])
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn bounded_read_evidence_command_fails_closed_without_graph_route_readiness() {
        let report_path = unique_test_file("bounded_read_ready");
        let routes_path = unique_test_file("bounded_read_routes");
        std::fs::write(&report_path, ready_report().to_string()).unwrap();
        std::fs::write(
            &routes_path,
            serde_json::json!({
                "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            })
            .to_string(),
        )
        .unwrap();

        let (evidence, _) = run_nowledge_bounded_read_evidence(
            [
                "--covered-routes-json",
                routes_path.to_str().unwrap(),
                report_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(evidence["ready"], false);
        assert_eq!(
            evidence["blocker_codes"],
            serde_json::json!(["graph_route_readiness_missing"])
        );
        std::fs::remove_file(report_path).unwrap();
        std::fs::remove_file(routes_path).unwrap();
    }

    #[test]
    fn bounded_read_evidence_command_rejects_unknown_arguments() {
        let report_path = unique_test_file("bounded_read_ready");
        let route_readiness_path = unique_test_file("graph_route_readiness");
        std::fs::write(&report_path, ready_report().to_string()).unwrap();
        std::fs::write(
            &route_readiness_path,
            ready_graph_route_readiness().to_string(),
        )
        .unwrap();

        let error = run_nowledge_bounded_read_evidence(
            [
                "--unexpected",
                "--graph-route-readiness-json",
                route_readiness_path.to_str().unwrap(),
                report_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap_err()
        .to_string();

        assert_eq!(
            error,
            "semantic error: nowledge-bounded-read-evidence requires [--require-ready] [--covered-route <route> ...] [--covered-routes-json <path>] [--graph-route-readiness-json <path>] <read-report-json>"
        );
        std::fs::remove_file(report_path).unwrap();
        std::fs::remove_file(route_readiness_path).unwrap();
    }

    fn ready_report() -> serde_json::Value {
        serde_json::json!({
            "protocol": "hawdb-nowledge-mem-read-report",
            "mode": "shadow_read_only",
            "row_count": 4,
            "max_rows": 512,
            "execution_row_cap": 513,
            "estimated_payload_bytes": 128,
            "max_estimated_payload_bytes": 4194304,
            "row_budget_exceeded": false,
            "payload_budget_exceeded": false,
            "row_limit_enforced_before_output": true,
            "operator_row_cap_enabled": true,
            "blocking_operator_count": 0,
            "blocking_operator_kinds": [],
            "blocking_operator_memory_reports": [],
            "streaming": false
        })
    }

    fn ready_graph_route_readiness() -> serde_json::Value {
        serde_json::json!({
            "route_primary_ready": true,
            "primary_ready_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
            "route_query_plan_evidence_ready": true,
            "route_query_profile_evidence_ready": true,
            "route_query_api_behavior_evidence_ready": true,
            "relationship_property_pruning_required_count": 0,
            "relationship_property_pruning_report_count": 0,
            "route_relationship_property_pruning_evidence_ready": true
        })
    }

    fn unique_test_file(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEST_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "hawdb_{name}_{}_{nanos}_{counter}.json",
            std::process::id()
        ))
    }
}
