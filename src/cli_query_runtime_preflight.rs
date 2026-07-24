use skein::{Database, DatabaseConfig, Result, SkeinError, Value};
use std::collections::BTreeMap;
use std::path::Path;

const QUERY_RUNTIME_PREFLIGHT_PROTOCOL: &str = "skein-nowledge-query-runtime-preflight-v1";

pub fn nowledge_query_runtime_preflight_usage() -> String {
    "nowledge-query-runtime-preflight requires [--require-ready] --probe-json <path> <database-path>"
        .to_string()
}

#[derive(Debug, Clone)]
struct QueryRuntimeProbe {
    name: String,
    route: Option<String>,
    query_family: Option<String>,
    cypher: String,
    parameters: BTreeMap<String, Value>,
    require_scan_pruning: bool,
    require_pruned: bool,
    min_scan_pruning_reports: usize,
    max_output_rows: Option<usize>,
}

pub fn run_nowledge_query_runtime_preflight(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let mut require_ready = false;
    let mut probe_path = None;
    let mut database_path = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--require-ready" => {
                require_ready = true;
            }
            "--probe-json" => {
                probe_path = Some(args.next().ok_or_else(|| {
                    SkeinError::Semantic(nowledge_query_runtime_preflight_usage())
                })?);
            }
            value if value.starts_with("--") => {
                return Err(SkeinError::Semantic(
                    nowledge_query_runtime_preflight_usage(),
                ));
            }
            path => {
                if database_path.replace(path.to_string()).is_some() {
                    return Err(SkeinError::Semantic(
                        nowledge_query_runtime_preflight_usage(),
                    ));
                }
            }
        }
    }
    let probe_path =
        probe_path.ok_or_else(|| SkeinError::Semantic(nowledge_query_runtime_preflight_usage()))?;
    let database_path = database_path
        .ok_or_else(|| SkeinError::Semantic(nowledge_query_runtime_preflight_usage()))?;
    let probes = parse_probe_file(&read_json_file(Path::new(&probe_path))?)?;
    let report = query_runtime_preflight_json(&database_path, &probes);
    Ok((report, require_ready))
}

fn query_runtime_preflight_json(
    database_path: &str,
    probes: &[QueryRuntimeProbe],
) -> serde_json::Value {
    let mut db = match Database::open_with_config(
        database_path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    ) {
        Ok(db) => db,
        Err(error) => {
            return serde_json::json!({
                "protocol": QUERY_RUNTIME_PREFLIGHT_PROTOCOL,
                "ready": false,
                "database_opened": false,
                "probe_count": probes.len(),
                "passed_probe_count": 0,
                "failed_probe_count": probes.len(),
                "blocker_codes": ["database_open_failed"],
                "error_class": error_class(&error),
                "probes": [],
            });
        }
    };

    let probe_reports = probes
        .iter()
        .map(|probe| run_probe(&mut db, probe))
        .collect::<Vec<_>>();
    let passed_probe_count = probe_reports
        .iter()
        .filter(|probe| probe.get("ready").and_then(serde_json::Value::as_bool) == Some(true))
        .count();
    let failed_probe_count = probe_reports.len().saturating_sub(passed_probe_count);
    let blocker_codes = preflight_blocker_codes(probes.len(), failed_probe_count);

    serde_json::json!({
        "protocol": QUERY_RUNTIME_PREFLIGHT_PROTOCOL,
        "ready": blocker_codes.is_empty(),
        "database_opened": true,
        "probe_count": probes.len(),
        "passed_probe_count": passed_probe_count,
        "failed_probe_count": failed_probe_count,
        "blocker_codes": blocker_codes,
        "probes": probe_reports,
    })
}

fn run_probe(db: &mut Database, probe: &QueryRuntimeProbe) -> serde_json::Value {
    match db.explain_analyze_query_with_params(&probe.cypher, &probe.parameters) {
        Ok(output) => {
            let scan_pruning_report_count = output.execution_profile.scan_pruning_reports.len();
            let pruned_scan_count = output
                .execution_profile
                .scan_pruning_reports
                .iter()
                .filter(|report| report.pruned)
                .count();
            let output_row_count = output.output.rows.len();
            let blocker_codes = probe_blocker_codes(
                probe,
                scan_pruning_report_count,
                pruned_scan_count,
                output_row_count,
            );
            serde_json::json!({
                "name": probe.name,
                "route": probe.route,
                "query_family": probe.query_family,
                "ready": blocker_codes.is_empty(),
                "success": true,
                "output_row_count": output_row_count,
                "selected_plan_fingerprint": output.trace.selected_plan_fingerprint,
                "search_mode": output.trace.search_mode.as_str(),
                "selected_plan_operator_counts": output.trace.selected_plan_operator_counts,
                "selected_plan_class_counts": output.trace.selected_plan_class_counts,
                "work_request": {
                    "priority": output.work_request.priority.as_str(),
                    "class": output.work_request.class.as_str(),
                    "estimated_operations": output.work_request.estimated_operations,
                },
                "execution_profile": {
                    "max_rows": output.execution_profile.max_rows,
                    "detection_row_cap": output.execution_profile.detection_row_cap,
                    "row_limit_enforced_before_output": output.execution_profile.row_limit_enforced_before_output,
                    "operator_row_cap_enabled": output.execution_profile.operator_row_cap_enabled,
                    "blocking_operator_kinds": output.execution_profile.blocking_operator_kinds,
                    "scan_pruning_report_count": scan_pruning_report_count,
                    "pruned_scan_count": pruned_scan_count,
                },
                "blocker_codes": blocker_codes,
            })
        }
        Err(error) => serde_json::json!({
            "name": probe.name,
            "route": probe.route,
            "query_family": probe.query_family,
            "ready": false,
            "success": false,
            "error_class": error_class(&error),
            "blocker_codes": ["query_runtime_failed"],
        }),
    }
}

fn preflight_blocker_codes(probe_count: usize, failed_probe_count: usize) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if probe_count == 0 {
        blockers.push("query_runtime_probes_missing");
    }
    if failed_probe_count > 0 {
        blockers.push("query_runtime_probe_failed");
    }
    blockers
}

fn probe_blocker_codes(
    probe: &QueryRuntimeProbe,
    scan_pruning_report_count: usize,
    pruned_scan_count: usize,
    output_row_count: usize,
) -> Vec<&'static str> {
    let mut blockers = Vec::new();
    if probe.require_scan_pruning && scan_pruning_report_count < probe.min_scan_pruning_reports {
        blockers.push("scan_pruning_report_missing");
    }
    if probe.require_pruned && pruned_scan_count == 0 {
        blockers.push("scan_pruning_not_pruned");
    }
    if let Some(max_output_rows) = probe.max_output_rows {
        if output_row_count > max_output_rows {
            blockers.push("output_row_count_exceeded");
        }
    }
    blockers
}

fn parse_probe_file(value: &serde_json::Value) -> Result<Vec<QueryRuntimeProbe>> {
    if let Some(array) = value.as_array() {
        return array.iter().map(parse_probe).collect();
    }
    if let Some(array) = value.get("probes").and_then(serde_json::Value::as_array) {
        return array.iter().map(parse_probe).collect();
    }
    Ok(vec![parse_probe(value)?])
}

fn parse_probe(value: &serde_json::Value) -> Result<QueryRuntimeProbe> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("query runtime probe must be a JSON object".to_string())
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
            SkeinError::Semantic("query runtime probe field 'cypher' must be a string".to_string())
        })?
        .to_string();
    let parameters = object
        .get("parameters")
        .map(parse_parameters_json)
        .transpose()?
        .unwrap_or_default();
    let min_scan_pruning_reports = optional_usize(object, "min_scan_pruning_reports")?.unwrap_or(1);
    Ok(QueryRuntimeProbe {
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

fn parse_parameters_json(value: &serde_json::Value) -> Result<BTreeMap<String, Value>> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Semantic("query runtime probe field 'parameters' must be an object".to_string())
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
                Err(SkeinError::Semantic(
                    "unsupported JSON number in query runtime probe parameters".to_string(),
                ))
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
            SkeinError::Semantic(format!(
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
        SkeinError::Semantic(format!(
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
        SkeinError::Semantic(format!(
            "query runtime probe field '{field}' must be an integer"
        ))
    })?;
    usize::try_from(raw).map(Some).map_err(|_| {
        SkeinError::Semantic(format!(
            "query runtime probe field '{field}' exceeds usize range"
        ))
    })
}

fn error_class(error: &SkeinError) -> &'static str {
    match error {
        SkeinError::Parse(_) => "parse",
        SkeinError::Semantic(_) => "semantic",
        SkeinError::Storage(_) => "storage",
        SkeinError::Execution(_) => "execution",
    }
}

fn read_json_file(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path).map_err(|error| {
        SkeinError::Execution(format!(
            "failed to read query runtime preflight JSON: {}",
            error.kind()
        ))
    })?;
    serde_json::from_str(&content).map_err(|error| {
        SkeinError::Semantic(format!(
            "failed to parse query runtime preflight JSON: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::run_nowledge_query_runtime_preflight;
    use skein::Database;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn query_runtime_preflight_accepts_pruned_probe() {
        let root = unique_test_dir("query-runtime-preflight");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-b', kind: 'task', title: 'B'})")
            .unwrap();
        drop(db);
        std::fs::write(
            &probe_path,
            serde_json::json!({
                "probes": [
                    {
                        "name": "memory-by-kind",
                        "route": "node_details",
                        "query_family": "memory_lookup",
                        "cypher": "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.title AS title",
                        "parameters": {"kind": "note"},
                        "require_scan_pruning": true,
                        "require_pruned": true,
                        "max_output_rows": 1
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let (report, require_ready) = run_nowledge_query_runtime_preflight(
            [
                "--require-ready",
                "--probe-json",
                probe_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert!(require_ready);
        assert_eq!(
            report["protocol"],
            "skein-nowledge-query-runtime-preflight-v1"
        );
        assert_eq!(report["ready"], true);
        assert_eq!(report["probe_count"], 1);
        assert_eq!(report["passed_probe_count"], 1);
        assert_eq!(report["probes"][0]["ready"], true);
        assert_eq!(report["probes"][0]["output_row_count"], 1);
        assert_eq!(
            report["probes"][0]["execution_profile"]["scan_pruning_report_count"],
            1
        );
        assert_eq!(
            report["probes"][0]["execution_profile"]["pruned_scan_count"],
            1
        );
        assert!(report["probes"][0].get("rows").is_none());
        assert!(report["probes"][0].get("parameters").is_none());
        assert!(!report.to_string().contains(graph_path.to_str().unwrap()));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_fails_closed_without_probes() {
        let root = unique_test_dir("query-runtime-preflight-empty");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let db = Database::open(&graph_path).unwrap();
        drop(db);
        std::fs::write(&probe_path, serde_json::json!({"probes": []}).to_string()).unwrap();

        let (report, _) = run_nowledge_query_runtime_preflight(
            [
                "--probe-json",
                probe_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(
            report["blocker_codes"],
            serde_json::json!(["query_runtime_probes_missing"])
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_reports_query_error_class_without_raw_values() {
        let root = unique_test_dir("query-runtime-preflight-error");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let db = Database::open(&graph_path).unwrap();
        drop(db);
        std::fs::write(
            &probe_path,
            serde_json::json!({
                "name": "bad-query",
                "cypher": "MATCH (m:Memory RETURN m",
                "parameters": {"secret": "do-not-emit"}
            })
            .to_string(),
        )
        .unwrap();

        let (report, _) = run_nowledge_query_runtime_preflight(
            [
                "--probe-json",
                probe_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(report["ready"], false);
        assert_eq!(report["probes"][0]["success"], false);
        assert_eq!(report["probes"][0]["error_class"], "parse");
        assert!(!report.to_string().contains("do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}", std::process::id()))
    }
}
