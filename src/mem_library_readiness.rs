use crate::{
    NowledgeGraphStatement, NowledgeMemEmbeddedStore, NowledgeMemLibraryReadinessReport,
    NowledgeMemOpenOptions, NowledgeMemOpenReport, NowledgeMemReadinessOptions, Result,
    SearchProjectionProbeOptions,
};
pub use skein_readiness::library_readiness_cli::{
    nowledge_mem_library_readiness_usage, parse_mem_library_covered_routes_json,
    parse_mem_library_graph_route_readiness_json, parse_mem_library_readiness_mode,
    parse_parameters_json, value_from_json,
};
use skein_readiness::library_readiness_cli::{
    parse_bounded_probe_json as parse_owner_bounded_probe_json,
    parse_nowledge_mem_library_readiness_inputs, NowledgeMemLibraryReadinessCliInputs,
};

pub fn run_nowledge_mem_library_readiness(
    mut args: impl Iterator<Item = String>,
) -> Result<(serde_json::Value, bool)> {
    let report = run_nowledge_mem_library_readiness_report(&mut args)?;
    let mut readiness = report.readiness.json();
    if let Some(object) = readiness.as_object_mut() {
        object.insert("open_report".to_string(), report.open_report.json());
    }
    Ok((readiness, report.require_ready))
}

#[derive(Debug, Clone, PartialEq)]
pub struct NowledgeMemLibraryReadinessRunReport {
    pub readiness: NowledgeMemLibraryReadinessReport,
    pub open_report: NowledgeMemOpenReport,
    pub require_ready: bool,
}

impl NowledgeMemLibraryReadinessRunReport {
    pub fn json(&self) -> serde_json::Value {
        let mut readiness = self.readiness.json();
        if let Some(object) = readiness.as_object_mut() {
            object.insert("open_report".to_string(), self.open_report.json());
        }
        readiness
    }
}

pub fn run_nowledge_mem_library_readiness_report(
    args: impl Iterator<Item = String>,
) -> Result<NowledgeMemLibraryReadinessRunReport> {
    let NowledgeMemLibraryReadinessCliInputs {
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
    } = parse_nowledge_mem_library_readiness_inputs(args)?;
    let open_options = match search_projection_path {
        Some(path) => NowledgeMemOpenOptions::with_search_projection(graph_path, path, mode),
        None => NowledgeMemOpenOptions::graph_only(graph_path, mode),
    };
    let (store, open_report) = NowledgeMemEmbeddedStore::open_with_options(open_options)?;
    let options = NowledgeMemReadinessOptions {
        bounded_read_probe: bounded_read_probe.map(|probe| NowledgeGraphStatement {
            cypher: probe.cypher,
            parameters: probe.parameters,
        }),
        bounded_read_evidence,
        covered_routes,
        graph_route_readiness,
        replacement_readiness_by_query_family,
        search_projection_evidence,
        search_projection_probe_options: SearchProjectionProbeOptions {
            active_embedding_model,
            active_embedding_dimension,
        },
        primary_search_projection_probe,
        search_projection_shadow_evidence,
        search_candidate_shadow_evidence,
        ..NowledgeMemReadinessOptions::default()
    };
    let readiness = store.library_readiness(&options);
    Ok(NowledgeMemLibraryReadinessRunReport {
        readiness,
        open_report,
        require_ready,
    })
}

pub fn parse_bounded_probe_json(value: &serde_json::Value) -> Result<NowledgeGraphStatement> {
    let probe = parse_owner_bounded_probe_json(value)?;
    Ok(NowledgeGraphStatement {
        cypher: probe.cypher,
        parameters: probe.parameters,
    })
}

#[cfg(test)]
mod tests {
    use super::{run_nowledge_mem_library_readiness, run_nowledge_mem_library_readiness_report};
    use crate::{Database, NowledgeMemGraphMode, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES};
    use serde_json::Value;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parser_facade_reexports_owner_entrypoints() {
        use skein_readiness::library_readiness_cli as owner;

        let usage: fn() -> String = super::nowledge_mem_library_readiness_usage;
        assert!(std::ptr::fn_addr_eq(
            usage,
            owner::nowledge_mem_library_readiness_usage as fn() -> _,
        ));
        let parse_mode: fn(&str) -> crate::Result<crate::NowledgeMemGraphMode> =
            super::parse_mem_library_readiness_mode;
        assert!(std::ptr::fn_addr_eq(
            parse_mode,
            owner::parse_mem_library_readiness_mode as fn(_) -> _,
        ));
        let parse_routes: fn(&serde_json::Value) -> crate::Result<Vec<String>> =
            super::parse_mem_library_covered_routes_json;
        assert!(std::ptr::fn_addr_eq(
            parse_routes,
            owner::parse_mem_library_covered_routes_json as fn(_) -> _,
        ));
    }

    #[test]
    fn library_readiness_command_emits_fail_closed_report() {
        let root = unique_test_dir("library-readiness");
        let graph_path = root.join("graph");
        let bounded_probe_path = root.join("bounded-probe.json");
        let covered_routes_path = root.join("covered-routes.json");
        let graph_route_readiness_path = root.join("graph-route-readiness.json");
        std::fs::create_dir_all(&root).unwrap();
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'mem-cli', title: 'CLI readiness'})")
            .unwrap();
        drop(db);
        std::fs::write(
            &bounded_probe_path,
            serde_json::json!({
                "cypher": "MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                "parameters": {
                    "id": "mem-cli"
                }
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &covered_routes_path,
            serde_json::json!({
                "covered_routes": REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &graph_route_readiness_path,
            ready_graph_route_readiness().to_string(),
        )
        .unwrap();

        let (readiness, require_ready) = run_nowledge_mem_library_readiness(
            [
                "--bounded-probe-json",
                bounded_probe_path.to_str().unwrap(),
                "--covered-routes-json",
                covered_routes_path.to_str().unwrap(),
                "--graph-route-readiness-json",
                graph_route_readiness_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert!(!require_ready);
        assert_eq!(
            readiness["protocol"],
            "skein-nowledge-mem-library-readiness-v1"
        );
        assert_eq!(readiness["ready"], false);
        assert_eq!(readiness["bounded_read_evidence"]["ready"], true);
        assert_eq!(
            readiness["readiness_by_area"]["query"]["ready"],
            serde_json::json!(true)
        );
        assert_eq!(
            readiness["graph_route_readiness"]["ready"],
            serde_json::json!(true)
        );
        assert_eq!(
            readiness["readiness_by_area"]["graph_route"]["ready"],
            serde_json::json!(true)
        );
        assert_eq!(
            readiness["query_family_evidence"]["blocker_codes"],
            serde_json::json!(["query_family_evidence_missing"])
        );
        assert_eq!(readiness["open_report"]["graph_opened"], true);
        assert_eq!(readiness["open_report"]["search_projection_opened"], false);
        assert!(readiness.get("graph_path").is_none());
        assert!(!readiness.to_string().contains(graph_path.to_str().unwrap()));

        let typed = run_nowledge_mem_library_readiness_report(
            [
                "--bounded-probe-json",
                bounded_probe_path.to_str().unwrap(),
                "--covered-routes-json",
                covered_routes_path.to_str().unwrap(),
                "--graph-route-readiness-json",
                graph_route_readiness_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();
        assert!(!typed.require_ready);
        assert_eq!(typed.readiness.mode, NowledgeMemGraphMode::ShadowReadOnly);
        assert!(typed.readiness.readiness_by_area.query.ready);
        assert!(typed.readiness.readiness_by_area.graph_route.ready);
        assert!(!typed.readiness.readiness_by_area.query_family.ready);
        assert!(typed.open_report.graph_opened);
        assert!(!typed.open_report.search_projection_opened);
        let typed_json = typed.json();
        assert_storage_open_timing_contract(&readiness);
        assert_storage_open_timing_contract(&typed_json);
        assert_eq!(
            normalize_storage_open_timings(typed_json),
            normalize_storage_open_timings(readiness)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    fn assert_storage_open_timing_contract(readiness: &Value) {
        let timings = &readiness["storage_recovery"]["open_timings"];
        let phase_sum = [
            "durable_manifest_open_micros",
            "checkpoint_root_open_micros",
            "wal_replay_micros",
            "post_replay_open_micros",
        ]
        .into_iter()
        .map(|field| timings[field].as_u64().unwrap())
        .sum::<u64>();
        let accounted = timings["accounted_micros"].as_u64().unwrap();
        let unaccounted = timings["unaccounted_micros"].as_u64().unwrap();
        let total = timings["total_open_micros"].as_u64().unwrap();

        assert_eq!(phase_sum, accounted);
        assert_eq!(accounted + unaccounted, total);
        assert_eq!(
            readiness["storage_recovery"]["readiness"]["open_timing_consistent"],
            Value::Bool(true)
        );
    }

    fn normalize_storage_open_timings(mut readiness: Value) -> Value {
        let timings = readiness["storage_recovery"]["open_timings"]
            .as_object_mut()
            .unwrap();
        for value in timings.values_mut() {
            *value = Value::from(0);
        }
        readiness
    }

    #[test]
    fn library_readiness_command_requires_graph_path() {
        let error = run_nowledge_mem_library_readiness([].into_iter().map(str::to_string))
            .expect_err("missing graph path should fail");

        assert!(error
            .to_string()
            .contains("nowledge-mem-library-readiness requires"));
    }

    #[test]
    fn library_readiness_input_parse_errors_are_redacted_by_default() {
        let root = unique_test_dir("library-readiness-parse-redaction");
        let path = root.join("secret-library-readiness-path-do-not-emit.json");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            &path,
            "{ \"secret\": \"library-readiness-parse-secret-do-not-emit\", \"unterminated\": ",
        )
        .unwrap();

        let error = run_nowledge_mem_library_readiness(
            [
                "--bounded-read-evidence-json",
                path.to_str().unwrap(),
                "unused-graph.db",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap_err()
        .to_string();

        assert_eq!(
            error,
            "semantic error: failed to parse nowledge mem library readiness JSON: invalid_json"
        );
        assert!(!error.contains("secret-library-readiness-path-do-not-emit"));
        assert!(!error.contains("library-readiness-parse-secret-do-not-emit"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn library_readiness_command_accepts_precomputed_evidence() {
        let root = unique_test_dir("library-readiness-precomputed");
        let graph_path = root.join("graph");
        let bounded_evidence_path = root.join("bounded-read-evidence.json");
        let query_family_evidence_path = root.join("query-family-evidence.json");
        let search_evidence_path = root.join("search-projection-evidence.json");
        let search_shadow_evidence_path = root.join("search-projection-shadow-evidence.json");
        let search_candidate_shadow_evidence_path =
            root.join("search-candidate-shadow-evidence.json");
        std::fs::create_dir_all(&root).unwrap();
        let db = Database::open(&graph_path).unwrap();
        drop(db);
        std::fs::write(
            &bounded_evidence_path,
            serde_json::json!({
                "protocol": "skein-nowledge-mem-bounded-read-evidence-v2",
                "present": true,
                "ready": true,
                "blocker_codes": []
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &query_family_evidence_path,
            ready_query_family_evidence().to_string(),
        )
        .unwrap();
        std::fs::write(
            &search_evidence_path,
            serde_json::json!({
                "protocol": "skein-nowledge-search-projection-evidence",
                "present": true,
                "ready": true,
                "blocker_codes": []
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &search_shadow_evidence_path,
            serde_json::json!({
                "protocol": "skein-nowledge-search-projection-shadow-evidence",
                "present": true,
                "ready": true,
                "blocker_codes": []
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &search_candidate_shadow_evidence_path,
            serde_json::json!({
                "protocol": "skein-nowledge-search-candidate-shadow-evidence",
                "route": "/search-index/skein-shadow/candidate-evidence",
                "evidence_source": "nmem-rust-bridge",
                "present": true,
                "ready": true,
                "candidate_primary_engine": "skein",
                "request_count": 1,
                "primary_candidate_count": 1,
                "shadow_candidate_count": 1,
                "matched_candidate_count": 1,
                "primary_only_candidate_count": 0,
                "candidate_identity": {
                    "ready": true,
                    "parity": true
                },
                "filter_pushdown_ready": true,
                "filter_pushdown": {
                    "ready": true,
                    "field_summary_count": 1,
                    "missing_required_fields": [],
                    "blocker_codes": []
                },
                "blocker_codes": []
            })
            .to_string(),
        )
        .unwrap();

        let (readiness, _) = run_nowledge_mem_library_readiness(
            [
                "--bounded-read-evidence-json",
                bounded_evidence_path.to_str().unwrap(),
                "--query-family-evidence-json",
                query_family_evidence_path.to_str().unwrap(),
                "--search-projection-evidence-json",
                search_evidence_path.to_str().unwrap(),
                "--search-projection-shadow-evidence-json",
                search_shadow_evidence_path.to_str().unwrap(),
                "--search-candidate-shadow-evidence-json",
                search_candidate_shadow_evidence_path.to_str().unwrap(),
                graph_path.to_str().unwrap(),
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap();

        assert_eq!(readiness["bounded_read_evidence"]["ready"], true);
        assert_eq!(readiness["query_family_evidence"]["ready"], true);
        assert_eq!(readiness["search_projection_evidence"]["ready"], true);
        assert_eq!(
            readiness["search_projection_shadow_evidence"]["ready"],
            true
        );
        assert_eq!(readiness["search_candidate_shadow_evidence"]["ready"], true);
        assert_eq!(
            readiness["graph_route_readiness"]["blocker_codes"],
            serde_json::json!(["graph_route_readiness_missing"])
        );
        assert_eq!(
            readiness["readiness_by_area"]["graph_route"]["ready"],
            false
        );
        assert_eq!(readiness["open_report"]["graph_opened"], true);
        assert_eq!(readiness["open_report"]["search_projection_opened"], false);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn ready_query_family_evidence() -> serde_json::Value {
        serde_json::json!({
            "replacement_readiness_by_query_family": [
                {
                    "query_family": "memory_lookup",
                    "replacement_readiness_per_million": 1_000_000
                },
                {
                    "query_family": "graph_traversal",
                    "replacement_readiness_per_million": 1_000_000
                },
                {
                    "query_family": "projected_graph",
                    "replacement_readiness_per_million": 1_000_000
                },
                {
                    "query_family": "label_stats_read",
                    "replacement_readiness_per_million": 1_000_000
                },
                {
                    "query_family": "search_projection",
                    "replacement_readiness_per_million": 1_000_000
                }
            ]
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

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_{name}_{}_{nanos}", std::process::id()))
    }
}
