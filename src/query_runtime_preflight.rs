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

use crate::{
    nowledge_mem_graph_read_route_catalog_digest, DatabaseConfig, HawDBError,
    NowledgeMemEmbeddedStore, NowledgeMemGraph, NowledgeQueryRuntimePreflightProbe, Result,
    NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
};
pub use hawdb_readiness::query_runtime_preflight_cli::nowledge_query_runtime_preflight_usage;
use hawdb_readiness::query_runtime_preflight_cli::parse_query_runtime_preflight_cli_inputs;

pub fn run_nowledge_query_runtime_preflight(
    args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let inputs = parse_query_runtime_preflight_cli_inputs(args)?;
    let report = query_runtime_preflight_json(&inputs.database_path, &inputs.probes);
    Ok((report, inputs.require_ready))
}

pub fn query_runtime_preflight_json(
    database_path: &str,
    probes: &[NowledgeQueryRuntimePreflightProbe],
) -> serde_json::Value {
    let graph = match NowledgeMemGraph::open_with_config(
        database_path,
        DatabaseConfig {
            read_only: true,
            ..DatabaseConfig::default()
        },
    ) {
        Ok(graph) => graph,
        Err(error) => {
            return serde_json::json!({
                "protocol": crate::NOWLEDGE_QUERY_RUNTIME_PREFLIGHT_PROTOCOL,
                "ready": false,
                "database_opened": false,
                "redaction": query_runtime_preflight_redaction_json(),
                "probe_count": probes.len(),
                "passed_probe_count": 0,
                "failed_probe_count": probes.len(),
                "required_route_count": 0,
                "covered_route_count": 0,
                "covered_routes": [],
                "missing_required_routes": [],
                "required_routes_covered": false,
                "unknown_routes": [],
                "duplicate_routes": [],
                "route_catalog_version": NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION,
                "route_catalog_digest": nowledge_mem_graph_read_route_catalog_digest(),
                "route_coverage_ready": false,
                "route_coverage_blocker_codes": [],
                "blocker_codes": database_open_blocker_codes(
                    probes.len(),
                    probes.len()
                ),
                "error_class": error_class(&error),
                "probes": [],
            });
        }
    };
    let mut store = NowledgeMemEmbeddedStore::new(graph, None);
    store.query_runtime_preflight_json(probes)
}

fn query_runtime_preflight_redaction_json() -> serde_json::Value {
    serde_json::json!({
        "ready": true,
        "rows_copied": false,
        "parameters_copied": false,
        "local_paths_copied": false,
        "raw_errors_copied": false,
    })
}

fn database_open_blocker_codes(probe_count: usize, failed_probe_count: usize) -> Vec<&'static str> {
    let mut blockers = vec!["database_open_failed"];
    if probe_count == 0 {
        blockers.push("query_runtime_probes_missing");
    }
    if failed_probe_count > 0 {
        blockers.push("query_runtime_probe_failed");
    }
    blockers
}

fn error_class(error: &HawDBError) -> &'static str {
    match error {
        HawDBError::Parse(_) => "parse",
        HawDBError::Semantic(_) => "semantic",
        HawDBError::Storage(_)
        | HawDBError::StorageIntegrity(_)
        | HawDBError::AppendSequenceExhausted { .. } => "storage",
        HawDBError::Execution(_) | HawDBError::TransactionConflict { .. } => "execution",
        HawDBError::CapabilityUnavailable { .. } => "capability_unavailable",
    }
}

#[cfg(test)]
mod tests {
    use super::run_nowledge_query_runtime_preflight;
    use crate::{Database, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn query_runtime_preflight_accepts_pruned_probe() {
        let root = unique_test_dir("query-runtime-preflight");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE INDEX ON :Memory(kind)").unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-b', kind: 'task', title: 'B'})")
            .unwrap();
        for id in 0..8 {
            db.query(&format!(
                "CREATE (:Memory {{id: 'mem-filler-{id}', kind: 'task', title: 'Filler {id}'}})"
            ))
            .unwrap();
        }
        drop(db);
        let probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                serde_json::json!({
                    "name": format!("memory-by-kind:{route}"),
                    "route": route,
                    "query_family": "memory_lookup",
                    "cypher": "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.title AS title",
                    "parameters": {"kind": "note"},
                    "require_scan_pruning": true,
                    "require_pruned": true,
                    "max_output_rows": 1
                })
            })
            .collect::<Vec<_>>();
        std::fs::write(
            &probe_path,
            serde_json::json!({ "probes": probes }).to_string(),
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
            "hawdb-nowledge-query-runtime-preflight-v1"
        );
        assert_eq!(report["ready"], true);
        assert_eq!(
            report["probe_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(
            report["passed_probe_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(
            report["required_route_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(
            report["covered_route_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(report["required_routes_covered"], true);
        assert_eq!(report["missing_required_routes"], serde_json::json!([]));
        assert_eq!(report["unknown_routes"], serde_json::json!([]));
        assert_eq!(report["duplicate_routes"], serde_json::json!([]));
        assert_eq!(report["route_coverage_ready"], true);
        assert_eq!(
            report["route_coverage_blocker_codes"],
            serde_json::json!([])
        );
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
        assert_eq!(
            report["probes"][0]["selected_plan_operator_counts"]["NodeProjectionScanExec"],
            1
        );
        assert_eq!(
            report["probes"][0]["selected_plan_class_counts"]["access"],
            1
        );
        assert!(report["probes"][0]["optimizer_decision_count"]
            .as_u64()
            .is_some());
        assert!(report["probes"][0]["optimizer_rule_event_count"]
            .as_u64()
            .is_some());
        assert_eq!(report["probes"][0]["plan_cache"]["lookup"], "miss");
        assert_eq!(report["probes"][0]["plan_cache"]["bypassed"], false);
        assert_eq!(
            report["probes"][0]["execution_profile"]["scan_pruning_reports"][0]["strategy"]["kind"],
            "property_eq"
        );
        assert_eq!(
            report["probes"][0]["execution_profile"]["scan_pruning_reports"][0]["strategy"]
                ["property"],
            "kind"
        );
        assert!(report["probes"][0].get("rows").is_none());
        assert!(report["probes"][0].get("parameters").is_none());
        assert!(!report.to_string().contains(graph_path.to_str().unwrap()));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_accepts_graph_route_query_inventory() {
        let root = unique_test_dir("query-runtime-preflight-route-inventory");
        let graph_path = root.join("graph");
        let probe_path = root.join("graph-route-queries.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE INDEX ON :Memory(kind)").unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        db.query("CREATE (:Memory {id: 'mem-b', kind: 'task', title: 'B'})")
            .unwrap();
        drop(db);
        let routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| {
                serde_json::json!({
                    "route": route,
                    "primary_ready": true,
                    "queries": [
                        {
                            "name": format!("memory-by-kind:{route}"),
                            "query_family": "memory_lookup",
                            "cypher": "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.title AS title",
                            "parameters": {"kind": "note"},
                            "require_scan_pruning": true,
                            "require_pruned": true,
                            "max_output_rows": 1
                        }
                    ],
                    "blocker_codes": []
                })
            })
            .collect::<Vec<_>>();
        std::fs::write(
            &probe_path,
            serde_json::json!({ "routes": routes }).to_string(),
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

        assert_eq!(report["ready"], true);
        assert_eq!(
            report["probe_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(
            report["passed_probe_count"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert_eq!(report["route_coverage_ready"], true);
        assert_eq!(
            report["probes"][0]["route"],
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0]
        );
        assert_eq!(report["probes"][0]["query_family"], "memory_lookup");
        assert_eq!(report["probes"][0]["ready"], true);
        assert!(report["probes"][0].get("rows").is_none());
        assert!(report["probes"][0].get("parameters").is_none());
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
            serde_json::json!([
                "query_runtime_probes_missing",
                "query_runtime_route_coverage_missing"
            ])
        );
        assert_eq!(
            report["route_coverage_blocker_codes"],
            serde_json::json!(["query_runtime_route_coverage_missing"])
        );
        assert_eq!(report["route_coverage_ready"], false);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_rejects_unknown_route_probe() {
        let root = unique_test_dir("query-runtime-preflight-unknown-route");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        drop(db);
        let mut probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_probe(route))
            .collect::<Vec<_>>();
        probes.push(ready_probe("/graph/manual-extra-route"));
        std::fs::write(
            &probe_path,
            serde_json::json!({ "probes": probes }).to_string(),
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
        assert_eq!(report["required_routes_covered"], true);
        assert_eq!(report["route_coverage_ready"], false);
        assert_eq!(
            report["unknown_routes"],
            serde_json::json!(["/graph/manual-extra-route"])
        );
        assert_eq!(
            report["route_coverage_blocker_codes"],
            serde_json::json!(["query_runtime_unknown_routes"])
        );
        assert!(report["blocker_codes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|code| code == "query_runtime_unknown_routes"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_reports_duplicate_route_probe_without_blocking() {
        let root = unique_test_dir("query-runtime-preflight-duplicate-route");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE INDEX ON :Memory(kind)").unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        drop(db);
        let mut probes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            .iter()
            .map(|route| ready_probe(route))
            .collect::<Vec<_>>();
        probes.push(ready_probe("/graph/overview"));
        std::fs::write(
            &probe_path,
            serde_json::json!({ "probes": probes }).to_string(),
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

        assert_eq!(report["ready"], true);
        assert_eq!(report["required_routes_covered"], true);
        assert_eq!(report["route_coverage_ready"], true);
        assert_eq!(
            report["duplicate_routes"],
            serde_json::json!(["/graph/overview"])
        );
        assert_eq!(
            report["route_coverage_blocker_codes"],
            serde_json::json!([])
        );
        assert_eq!(report["blocker_codes"], serde_json::json!([]));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_rejects_probe_without_identity() {
        let root = unique_test_dir("query-runtime-preflight-missing-identity");
        let graph_path = root.join("graph");
        let probe_path = root.join("probes.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'mem-a', kind: 'note', title: 'A'})")
            .unwrap();
        drop(db);
        std::fs::write(
            &probe_path,
            serde_json::json!({
                "probes": [
                    {
                        "cypher": "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.title AS title",
                        "parameters": {"kind": "note"}
                    }
                ]
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
        assert_eq!(report["probes"][0]["success"], true);
        assert_eq!(report["probes"][0]["ready"], false);
        assert_eq!(
            report["probes"][0]["blocker_codes"],
            serde_json::json!([
                "query_runtime_probe_name_missing",
                "query_runtime_probe_route_missing",
                "query_runtime_probe_query_family_missing"
            ])
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
        assert_eq!(report["redaction"]["ready"], true);
        assert_eq!(report["redaction"]["parameters_copied"], false);
        assert_eq!(report["redaction"]["raw_errors_copied"], false);
        assert_eq!(report["probes"][0]["success"], false);
        assert_eq!(report["probes"][0]["error_class"], "parse");
        assert!(!report.to_string().contains("do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn query_runtime_preflight_probe_json_parse_errors_are_redacted_by_default() {
        let root = unique_test_dir("query-runtime-preflight-parse-redaction");
        let probe_path = root.join("secret-probe-path-do-not-emit.json");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            &probe_path,
            "{ \"cypher\": \"MATCH (m {id: 'secret-cypher-do-not-emit'})\", \"parameters\": ",
        )
        .unwrap();

        let error = run_nowledge_query_runtime_preflight(
            [
                "--probe-json",
                probe_path.to_str().unwrap(),
                "unused-graph.db",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap_err()
        .to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse query runtime preflight JSON: invalid_json"
        );
        assert!(!error.contains("secret-probe-path-do-not-emit"));
        assert!(!error.contains("secret-cypher-do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("hawdb_{name}_{}_{nanos}", std::process::id()))
    }

    fn ready_probe(route: &str) -> serde_json::Value {
        serde_json::json!({
            "name": format!("memory-by-kind:{route}"),
            "route": route,
            "query_family": "memory_lookup",
            "cypher": "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.title AS title",
            "parameters": {"kind": "note"},
            "require_scan_pruning": true,
            "require_pruned": true,
            "max_output_rows": 1
        })
    }
}
